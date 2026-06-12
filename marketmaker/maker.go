package marketmaker

import (
	"context"
	"fmt"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

type PriceSource interface {
	Latest() (decimal.Decimal, bool)
}

type Config struct {
	Levels      int
	SpreadBps   int64
	StepBps     int64
	QtyPerLevel decimal.Decimal
	Requote     time.Duration
}

func DefaultConfig() Config {
	return Config{
		Levels:      5,
		SpreadBps:   5,
		StepBps:     2,
		QtyPerLevel: decimal.RequireFromString("0.05"),
		Requote:     2 * time.Second,
	}
}

type Maker struct {
	eng        *engine.Engine
	feed       PriceSource
	cfg        Config
	live       []string
	seq        int64
	instanceID string // unique per-instance prefix prevents ID collisions across restarts
	lastPrice  decimal.Decimal
}

func New(eng *engine.Engine, feed PriceSource, cfg Config) *Maker {
	// Use startup timestamp (nanoseconds) as an instance discriminator.
	return &Maker{
		eng:        eng,
		feed:       feed,
		cfg:        cfg,
		instanceID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func (m *Maker) Run(ctx context.Context) {
	// Cancel all open maker quotes on exit, whether due to context cancellation
	// or a panic, so stale orders do not linger in the book.
	defer m.cancelAll()

	ticker := time.NewTicker(m.cfg.Requote)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.requote()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Maker) requote() {
	price, ok := m.feed.Latest()
	if !ok {
		return
	}
	if len(m.live) > 0 && price.Equal(m.lastPrice) {
		return
	}
	m.cancelAll()

	bps := decimal.New(1, -4)
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
	id := fmt.Sprintf("mm-%s-%d", m.instanceID, m.seq)
	o := engine.NewOrder(id, "BTC-USD", side, engine.Limit, price, m.cfg.QtyPerLevel)
	if err := m.eng.Submit(o); err != nil {
		return // retried implicitly on the next requote
	}
	if o.Status == engine.StatusOpen || o.Status == engine.StatusPartial {
		m.live = append(m.live, id)
	}
}

func (m *Maker) cancelAll() {
	for _, id := range m.live {
		_ = m.eng.Cancel(id)
	}
	m.live = m.live[:0]
}
