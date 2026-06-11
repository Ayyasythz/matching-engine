// Package marketmaker quotes a ladder of limit orders around an external
// index price so the demo book tracks the real market.
package marketmaker

import (
	"context"
	"fmt"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

// PriceSource is the part of pricefeed.Feed the maker needs.
type PriceSource interface {
	Latest() (decimal.Decimal, bool)
}

type Config struct {
	Levels      int             // price levels per side
	SpreadBps   int64           // half-spread from index to best quote, in basis points
	StepBps     int64           // distance between levels, in basis points
	QtyPerLevel decimal.Decimal // BTC quoted at each level
	Requote     time.Duration   // how often to refresh quotes
}

func DefaultConfig() Config {
	return Config{
		Levels:      5,
		SpreadBps:   5, // 0.05%
		StepBps:     2, // 0.02%
		QtyPerLevel: decimal.RequireFromString("0.05"),
		Requote:     2 * time.Second,
	}
}

type Maker struct {
	eng       *engine.Engine
	feed      PriceSource
	cfg       Config
	live      []string // resting bot order IDs
	seq       int64
	lastPrice decimal.Decimal
}

func New(eng *engine.Engine, feed PriceSource, cfg Config) *Maker {
	return &Maker{eng: eng, feed: feed, cfg: cfg}
}

// Run re-quotes on a fixed interval until ctx is cancelled.
func (m *Maker) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.Requote)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.requote()
		case <-ctx.Done():
			m.cancelAll()
			return
		}
	}
}

// requote cancels the previous ladder and places a fresh one around the
// current index price. No-op if the feed has no price yet, or if the price
// hasn't moved since the last quote.
func (m *Maker) requote() {
	price, ok := m.feed.Latest()
	if !ok {
		return
	}
	if len(m.live) > 0 && price.Equal(m.lastPrice) {
		return
	}
	m.cancelAll()

	bps := decimal.New(1, -4) // 0.0001
	for i := 0; i < m.cfg.Levels; i++ {
		offset := decimal.NewFromInt(m.cfg.SpreadBps + int64(i)*m.cfg.StepBps).Mul(bps)
		bidPrice := price.Mul(decimal.NewFromInt(1).Sub(offset)).Round(2)
		askPrice := price.Mul(decimal.NewFromInt(1).Add(offset)).Round(2)
		m.place(engine.Buy, bidPrice)
		m.place(engine.Sell, askPrice)
	}
	m.lastPrice = price
}

func (m *Maker) place(side engine.Side, price decimal.Decimal) {
	m.seq++
	id := fmt.Sprintf("mm-%d", m.seq)
	o := engine.NewOrder(id, "BTC-USD", side, engine.Limit, price, m.cfg.QtyPerLevel)
	if err := m.eng.Submit(o); err != nil {
		return // retried implicitly on the next requote
	}
	// Track only orders that are actually resting in the book.
	if o.Status == engine.StatusOpen || o.Status == engine.StatusPartial {
		m.live = append(m.live, id)
	}
}

func (m *Maker) cancelAll() {
	for _, id := range m.live {
		// Errors mean the order already filled — nothing to do.
		_ = m.eng.Cancel(id)
	}
	m.live = m.live[:0]
}
