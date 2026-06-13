package marketmaker

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

// Arb keeps an AMM pool's spot price pegged to the live index price by
// submitting market orders against the pool — the same mechanism that keeps
// real AMMs (Uniswap etc.) in line with external markets.
type Arb struct {
	eng        *engine.Engine
	feed       PriceSource
	interval   time.Duration
	bandBps    float64 // do nothing while |spot − index| / index is inside this band
	seq        int64
	instanceID string
}

func NewArb(eng *engine.Engine, feed PriceSource, interval time.Duration) *Arb {
	return &Arb{
		eng:        eng,
		feed:       feed,
		interval:   interval,
		bandBps:    5, // 5 bps dead band
		instanceID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

func (a *Arb) Run(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.rebalance()
		case <-ctx.Done():
			return
		}
	}
}

// rebalance compares the pool spot price with the index and swaps exactly the
// amount that moves the constant-product spot back to the index:
// for k = x·y, the reserve at price P is x' = √(k/P), so the trade size is
// |x − x'|. Float math is fine here — the band absorbs any rounding.
func (a *Arb) rebalance() {
	index, ok := a.feed.Latest()
	if !ok {
		return
	}
	snap, err := a.eng.Snapshot()
	if err != nil || snap.AMM == nil {
		return
	}

	x, err1 := strconv.ParseFloat(snap.AMM.BaseReserve, 64)
	y, err2 := strconv.ParseFloat(snap.AMM.QuoteReserve, 64)
	idx, _ := index.Float64()
	if err1 != nil || err2 != nil || x <= 0 || y <= 0 || idx <= 0 {
		return
	}

	spot := y / x
	if math.Abs(spot-idx)/idx < a.bandBps/10000 {
		return
	}

	targetX := math.Sqrt(x * y / idx)
	dx := math.Abs(x - targetX)
	if dx < 1e-6 {
		return
	}

	side := engine.Buy // spot below index: buy from the pool to push the price up
	if spot > idx {
		side = engine.Sell
	}

	a.seq++
	id := fmt.Sprintf("arb-%s-%d", a.instanceID, a.seq)
	qty := decimal.NewFromFloat(dx).Truncate(8)
	if qty.LessThanOrEqual(decimal.Zero) {
		return
	}
	o := engine.NewOrder(id, "BTC-USD", side, engine.Market, decimal.Zero, qty)
	_ = a.eng.Submit(o)
}
