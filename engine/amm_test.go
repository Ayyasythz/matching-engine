package engine

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Pool math ─────────────────────────────────────────────────────────────────

func newTestPool(t *testing.T) *AMMPool {
	t.Helper()
	// 10 BTC / $600,000 → spot $60,000; fee 30 bps.
	pool, err := NewAMMPool(d("10"), d("600000"), 30)
	require.NoError(t, err)
	return pool
}

func TestAMMPool_Validation(t *testing.T) {
	_, err := NewAMMPool(d("0"), d("100"), 30)
	assert.Error(t, err, "zero base reserve must be rejected")
	_, err = NewAMMPool(d("10"), d("-5"), 30)
	assert.Error(t, err, "negative quote reserve must be rejected")
	_, err = NewAMMPool(d("10"), d("100"), 10000)
	assert.Error(t, err, "100% fee must be rejected")
}

func TestAMMPool_SpotPrice(t *testing.T) {
	pool := newTestPool(t)
	assert.True(t, pool.SpotPrice().Equal(d("60000")), "spot = quote/base = 60000, got %s", pool.SpotPrice())
}

func TestAMMPool_BuyCostAndInvariant(t *testing.T) {
	pool := newTestPool(t)
	k0 := pool.Base.Mul(pool.Quote)

	// Buying 1 BTC from a 10-BTC pool: cost = 600000·1/((10−1)·0.997) ≈ 66,866.97
	cost, err := pool.Buy(d("1"))
	require.NoError(t, err)
	expected := d("600000").Div(d("9").Mul(d("0.997")))
	assert.True(t, cost.Sub(expected).Abs().LessThan(d("0.01")),
		"cost %s should be ≈ %s", cost, expected.Round(2))

	// Price impact: cost per BTC must exceed the pre-trade spot.
	assert.True(t, cost.GreaterThan(d("60000")), "buying must cost more than spot")

	// The fee stays in the reserves, so k must not shrink.
	k1 := pool.Base.Mul(pool.Quote)
	assert.True(t, k1.GreaterThanOrEqual(k0.Sub(d("0.001"))), "k must not shrink: %s -> %s", k0, k1)
}

func TestAMMPool_BuyExceedingReserveFails(t *testing.T) {
	pool := newTestPool(t)
	_, err := pool.Buy(d("10"))
	assert.Error(t, err, "buying the entire reserve must fail")
	_, err = pool.Buy(d("15"))
	assert.Error(t, err)
}

func TestAMMPool_SellProceedsAndInvariant(t *testing.T) {
	pool := newTestPool(t)
	k0 := pool.Base.Mul(pool.Quote)

	// Selling 1 BTC: out = 600000·0.997/(10+0.997) ≈ 54,396.65
	out, err := pool.Sell(d("1"))
	require.NoError(t, err)
	expected := d("600000").Mul(d("0.997")).Div(d("10.997"))
	assert.True(t, out.Sub(expected).Abs().LessThan(d("0.01")),
		"proceeds %s should be ≈ %s", out, expected.Round(2))

	// Price impact: proceeds per BTC must be below the pre-trade spot.
	assert.True(t, out.LessThan(d("60000")), "selling must yield less than spot")

	k1 := pool.Base.Mul(pool.Quote)
	assert.True(t, k1.GreaterThanOrEqual(k0.Sub(d("0.001"))), "k must not shrink: %s -> %s", k0, k1)
}

func TestAMMPool_MaxBuyWithinAvgPrice(t *testing.T) {
	pool := newTestPool(t)

	// A limit below (or at) spot can buy nothing.
	assert.True(t, pool.MaxBuyWithinAvgPrice(d("59000")).IsZero())
	assert.True(t, pool.MaxBuyWithinAvgPrice(d("60000")).IsZero())

	// A limit above spot buys a positive amount whose realised average ≤ limit.
	limitP := d("61000")
	dx := pool.MaxBuyWithinAvgPrice(limitP)
	require.True(t, dx.GreaterThan(decimal.Zero), "limit above spot must be fillable")

	cost, err := pool.Buy(dx)
	require.NoError(t, err)
	avg := cost.Div(dx)
	assert.True(t, avg.LessThanOrEqual(limitP),
		"realised average %s must be within limit %s", avg.Round(4), limitP)
	// And it is maximal: even one more satoshi-scale step pushes the average over.
	assert.True(t, pool.MaxBuyWithinAvgPrice(limitP).LessThan(d("0.0001")),
		"after filling to the limit, almost nothing should remain fillable")
}

func TestAMMPool_MaxSellWithinAvgPrice(t *testing.T) {
	pool := newTestPool(t)

	// A limit above (or at) spot can sell nothing.
	assert.True(t, pool.MaxSellWithinAvgPrice(d("61000")).IsZero())
	assert.True(t, pool.MaxSellWithinAvgPrice(d("60000")).IsZero())

	limitP := d("59000")
	dx := pool.MaxSellWithinAvgPrice(limitP)
	require.True(t, dx.GreaterThan(decimal.Zero), "limit below spot must be fillable")

	out, err := pool.Sell(dx)
	require.NoError(t, err)
	avg := out.Div(dx)
	assert.True(t, avg.GreaterThanOrEqual(limitP),
		"realised average %s must be at or above limit %s", avg.Round(4), limitP)
}

// ── Engine in AMM mode ────────────────────────────────────────────────────────

func newTestAMMEngine(t *testing.T) (*Engine, chan Event) {
	t.Helper()
	events := make(chan Event, 2048)
	pool, err := NewAMMPool(d("10"), d("600000"), 30)
	require.NoError(t, err)
	eng := NewAMMEngine("BTC-USD", events, pool)
	go eng.Run()
	t.Cleanup(func() { eng.Stop() })
	return eng, events
}

func TestAMM_MarketBuyFillsAgainstPool(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	buy := market(Buy, "1")
	require.NoError(t, eng.Submit(buy))
	assert.Equal(t, StatusFilled, buy.Status)

	evs := drainAll(events)
	trades := tradesFrom(evs)
	require.Len(t, trades, 1)
	assert.Equal(t, AMMMakerID, trades[0].MakerID)
	assert.Equal(t, buy.ID, trades[0].TakerID)
	assert.True(t, trades[0].Price.GreaterThan(d("60000")),
		"buy must execute above spot, got %s", trades[0].Price)

	snap, err := eng.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap.AMM)
	assert.True(t, d(snap.AMM.BaseReserve).Equal(d("9")), "pool should hold 9 BTC, got %s", snap.AMM.BaseReserve)
}

func TestAMM_MarketBuyExceedingReserveRejected(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	buy := market(Buy, "10") // pool only holds 10 — cannot buy it all
	require.NoError(t, eng.Submit(buy))
	assert.Equal(t, StatusRejected, buy.Status)

	evs := drainAll(events)
	var rejected *Event
	for i := range evs {
		if evs[i].Type == EvOrderRejected && evs[i].OrderID == buy.ID {
			rejected = &evs[i]
		}
	}
	require.NotNil(t, rejected, "a rejection event must be emitted")
	assert.True(t, strings.Contains(rejected.Reason, "insufficient pool liquidity"),
		"reason should explain the pool shortfall, got %q", rejected.Reason)
}

func TestAMM_MarketSellFillsBelowSpot(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	sell := market(Sell, "1")
	require.NoError(t, eng.Submit(sell))
	assert.Equal(t, StatusFilled, sell.Status)

	trades := tradesFrom(drainAll(events))
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Price.LessThan(d("60000")),
		"sell must execute below spot, got %s", trades[0].Price)
}

func TestAMM_LimitFillsPartiallyAndRests(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	// Wants 5 BTC but the limit is only ~0.5% above spot (just past the
	// fee-adjusted floor of spot/0.997 ≈ 60180) — the pool can fill a fraction.
	buy := limit(Buy, "60300", "5")
	require.NoError(t, eng.Submit(buy))

	assert.Equal(t, StatusPartial, buy.Status)
	trades := tradesFrom(drainAll(events))
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Qty.LessThan(d("5")), "only part of the order can fill within the limit")
	assert.True(t, trades[0].Price.LessThanOrEqual(d("60300")), "fill must respect the limit")

	// The remainder rests and can be cancelled like any book order.
	require.NoError(t, eng.Cancel(buy.ID))
	assert.Equal(t, StatusCancelled, buy.Status)
}

func TestAMM_LimitBelowSpotRestsAndTriggersOnPriceDrop(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	// Resting bid 2% below spot — unfillable now.
	bid := limit(Buy, "58800", "0.5")
	require.NoError(t, eng.Submit(bid))
	assert.Equal(t, StatusOpen, bid.Status)
	assert.Empty(t, tradesFrom(drainAll(events)), "no trade while the limit is below spot")

	// A large market sell pushes the pool price down through the bid's limit…
	sell := market(Sell, "1")
	require.NoError(t, eng.Submit(sell))

	// …which must auto-trigger the resting bid against the pool.
	evs := drainAll(events)
	var bidTrades []Event
	for _, ev := range evs {
		if ev.Type == EvTrade && ev.TakerID == bid.ID {
			bidTrades = append(bidTrades, ev)
		}
	}
	require.NotEmpty(t, bidTrades, "resting bid must trigger when the pool price crosses its limit")
	for _, tr := range bidTrades {
		assert.Equal(t, AMMMakerID, tr.MakerID)
		assert.True(t, tr.Price.LessThanOrEqual(d("58800")),
			"triggered fill must respect the resting limit, got %s", tr.Price)
	}
}

func TestAMM_FOKRejectsWithReason(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	// 5 BTC within 0.1% of spot is impossible on a 10-BTC pool.
	order := fok(Buy, "60060", "5")
	require.NoError(t, eng.Submit(order))
	assert.Equal(t, StatusRejected, order.Status)

	evs := drainAll(events)
	var reason string
	for _, ev := range evs {
		if ev.Type == EvOrderRejected && ev.OrderID == order.ID {
			reason = ev.Reason
		}
	}
	assert.True(t, strings.Contains(reason, "FOK"), "reason should mention FOK, got %q", reason)
}

func TestAMM_FOKFillsFullyWithinLimit(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	order := fok(Buy, "70000", "0.5") // generous limit — easily fillable
	require.NoError(t, eng.Submit(order))
	assert.Equal(t, StatusFilled, order.Status)

	trades := tradesFrom(drainAll(events))
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Qty.Equal(d("0.5")))
}

func TestAMM_IOCCancelsRemainder(t *testing.T) {
	eng, events := newTestAMMEngine(t)

	order := ioc(Buy, "60300", "5")
	require.NoError(t, eng.Submit(order))
	assert.Equal(t, StatusCancelled, order.Status, "IOC remainder must be cancelled, not rest")

	evs := drainAll(events)
	require.NotEmpty(t, tradesFrom(evs), "the fillable portion must execute")
	var cancelled bool
	for _, ev := range evs {
		if ev.Type == EvOrderCancelled && ev.OrderID == order.ID {
			cancelled = true
		}
	}
	assert.True(t, cancelled, "a cancel event must be emitted for the remainder")

	// Nothing rests — cancel must fail.
	assert.Error(t, eng.Cancel(order.ID))
}

func TestAMM_SnapshotSynthesizesDepth(t *testing.T) {
	eng, _ := newTestAMMEngine(t)

	snap, err := eng.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap.AMM)
	assert.Equal(t, "60000", snap.AMM.SpotPrice)
	assert.Equal(t, int64(30), snap.AMM.FeeBps)

	require.NotEmpty(t, snap.Asks, "depth must be synthesized from the curve")
	require.NotEmpty(t, snap.Bids)

	// Asks ascending from just above spot; bids descending from just below.
	for i := 1; i < len(snap.Asks); i++ {
		assert.True(t, d(snap.Asks[i].Price).GreaterThan(d(snap.Asks[i-1].Price)), "asks must ascend")
	}
	for i := 1; i < len(snap.Bids); i++ {
		assert.True(t, d(snap.Bids[i].Price).LessThan(d(snap.Bids[i-1].Price)), "bids must descend")
	}
	assert.True(t, d(snap.Asks[0].Price).GreaterThan(d("60000")))
	assert.True(t, d(snap.Bids[0].Price).LessThan(d("60000")))
}

func TestAMM_SnapshotMergesRestingOrders(t *testing.T) {
	eng, _ := newTestAMMEngine(t)

	// Rest a bid far below the synthesized ladder.
	bid := limit(Buy, "50000", "2")
	require.NoError(t, eng.Submit(bid))

	snap, err := eng.Snapshot()
	require.NoError(t, err)
	var found bool
	for _, l := range snap.Bids {
		if l.Price == "50000" {
			found = true
			assert.Equal(t, "2", l.Qty)
		}
	}
	assert.True(t, found, "resting orders must appear in the snapshot alongside pool depth")
}
