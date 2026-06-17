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
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const coinGeckoBase = "https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids="

// CoinGeckoBTCUSD is kept for backward compatibility.
const CoinGeckoBTCUSD = coinGeckoBase + "bitcoin"

type Feed struct {
	url      string
	coinID   string // CoinGecko coin id, e.g. "bitcoin", "ethereum", "solana"
	interval time.Duration
	client   *http.Client

	mu               sync.RWMutex
	price            decimal.Decimal
	ok               bool
	subs             []chan decimal.Decimal
	lastLog          time.Time
	rateLimitedUntil time.Time
}

// New returns a feed for bitcoin/USD (backward-compatible shorthand).
func New(interval time.Duration) *Feed {
	return NewForCoin("bitcoin", interval)
}

// NewForCoin returns a feed for the given CoinGecko coin id (e.g. "ethereum").
func NewForCoin(coinID string, interval time.Duration) *Feed {
	return NewWithURL(coinGeckoBase+strings.ToLower(coinID), coinID, interval)
}

func NewWithURL(url, coinID string, interval time.Duration) *Feed {
	return &Feed{
		url:      url,
		coinID:   coinID,
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

func (f *Feed) fetchOnce() error {
	resp, err := f.client.Get(f.url)
	if err != nil {
		return fmt.Errorf("fetch spot price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
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

	var raw map[string]map[string]json.Number
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Errorf("decode spot price: %w", err)
	}
	coinData, ok := raw[f.coinID]
	if !ok {
		return fmt.Errorf("coin %q not found in price response", f.coinID)
	}
	p, err := decimal.NewFromString(coinData["usd"].String())
	if err != nil || p.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid spot price %q for %s", coinData["usd"], f.coinID)
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
