// Package pricefeed polls a public spot-price API and exposes the latest
// price plus change notifications.
package pricefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const CoinGeckoBTCUSD = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

type Feed struct {
	url      string
	interval time.Duration
	client   *http.Client

	mu               sync.RWMutex
	price            decimal.Decimal
	ok               bool
	subs             []chan decimal.Decimal
	lastLog          time.Time
	rateLimitedUntil time.Time // non-zero while backing off after a 429
}

func New(interval time.Duration) *Feed {
	return NewWithURL(CoinGeckoBTCUSD, interval)
}

func NewWithURL(url string, interval time.Duration) *Feed {
	return &Feed{
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (f *Feed) Latest() (decimal.Decimal, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.price, f.ok
}

func (f *Feed) Subscribe() <-chan decimal.Decimal {
	ch := make(chan decimal.Decimal, 16)
	f.mu.Lock()
	f.subs = append(f.subs, ch)
	f.mu.Unlock()
	return ch
}

func (f *Feed) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	// Close all subscriber channels when the feed stops so that goroutines
	// blocking on Subscribe() channels are unblocked rather than leaked.
	defer f.closeSubs()
	f.poll()
	for {
		select {
		case <-ticker.C:
			f.poll()
		case <-ctx.Done():
			return
		}
	}
}

func (f *Feed) closeSubs() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs {
		close(ch)
	}
	f.subs = nil
}

func (f *Feed) poll() {
	// Honour the rate-limit backoff window before making another request.
	f.mu.RLock()
	limited := time.Now().Before(f.rateLimitedUntil)
	f.mu.RUnlock()
	if limited {
		return
	}

	if err := f.fetchOnce(); err != nil {
		f.mu.Lock()
		if time.Since(f.lastLog) > time.Minute {
			f.lastLog = time.Now()
			log.Printf("pricefeed: %v (keeping last price)", err)
		}
		f.mu.Unlock()
	}
}

type spotResponse struct {
	Bitcoin struct {
		USD json.Number `json:"usd"`
	} `json:"bitcoin"`
}

func (f *Feed) fetchOnce() error {
	resp, err := f.client.Get(f.url)
	if err != nil {
		return fmt.Errorf("fetch spot price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// Back off for at least 60 s, or as long as the server requests.
		backoff := 60 * time.Second
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				backoff = time.Duration(secs) * time.Second
			}
		}
		f.mu.Lock()
		f.rateLimitedUntil = time.Now().Add(backoff)
		f.mu.Unlock()
		return fmt.Errorf("fetch spot price: rate limited (429), backing off %s", backoff)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch spot price: status %d", resp.StatusCode)
	}
	var sr spotResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return fmt.Errorf("decode spot price: %w", err)
	}
	p, err := decimal.NewFromString(sr.Bitcoin.USD.String())
	if err != nil || p.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid spot price %q", sr.Bitcoin.USD)
	}

	f.mu.Lock()
	changed := !f.ok || !p.Equal(f.price)
	f.price = p
	f.ok = true
	subs := f.subs
	f.mu.Unlock()

	if changed {
		for _, ch := range subs {
			select {
			case ch <- p:
			default:
			}
		}
	}
	return nil
}
