// Package pricefeed polls a public spot-price API and exposes the latest
// price plus change notifications.
package pricefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const CoinbaseBTCUSD = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

type Feed struct {
	url      string
	interval time.Duration
	client   *http.Client

	mu      sync.RWMutex
	price   decimal.Decimal
	ok      bool
	subs    []chan decimal.Decimal
	lastLog time.Time
}

// New polls the Coinbase BTC-USD spot price.
func New(interval time.Duration) *Feed {
	return NewWithURL(CoinbaseBTCUSD, interval)
}

func NewWithURL(url string, interval time.Duration) *Feed {
	return &Feed{
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

// Latest returns the last fetched price; ok is false until the first
// successful fetch.
func (f *Feed) Latest() (decimal.Decimal, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.price, f.ok
}

// Subscribe returns a channel notified on every price *change*.
// Slow subscribers miss updates rather than blocking the feed.
func (f *Feed) Subscribe() <-chan decimal.Decimal {
	ch := make(chan decimal.Decimal, 16)
	f.mu.Lock()
	f.subs = append(f.subs, ch)
	f.mu.Unlock()
	return ch
}

// Run polls until ctx is cancelled.
func (f *Feed) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
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

func (f *Feed) poll() {
	if err := f.fetchOnce(); err != nil {
		// Keep last known price; log at most once per minute.
		f.mu.Lock()
		if time.Since(f.lastLog) > time.Minute {
			f.lastLog = time.Now()
			log.Printf("pricefeed: %v (keeping last price)", err)
		}
		f.mu.Unlock()
	}
}

type spotResponse struct {
	Data struct {
		Amount string `json:"amount"`
	} `json:"data"`
}

func (f *Feed) fetchOnce() error {
	resp, err := f.client.Get(f.url)
	if err != nil {
		return fmt.Errorf("fetch spot price: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch spot price: status %d", resp.StatusCode)
	}
	var sr spotResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return fmt.Errorf("decode spot price: %w", err)
	}
	p, err := decimal.NewFromString(sr.Data.Amount)
	if err != nil || p.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid spot price %q", sr.Data.Amount)
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
