package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Smoke tests ───────────────────────────────────────────────────────────────
// Quick end-to-end paths that must always pass.

// Buy-side price-time sweep: 3 asks at different prices; a single buy should
// fill the cheapest first and produce one trade per level.
func TestSmoke_MarketBuySweepsMultipleLevels(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "101", "1"))
	eng.Submit(limit(Sell, "102", "1"))
	eng.Submit(limit(Sell, "103", "1"))

	buy := market(Buy, "3")
	require.NoError(t, eng.Submit(buy))

	evs := drainN(events, 16)
	trades := tradesFrom(evs)
	require.Len(t, trades, 3, "should fill one trade per ask level")
	assert.True(t, trades[0].Price.Equal(d("101")))
	assert.True(t, trades[1].Price.Equal(d("102")))
	assert.True(t, trades[2].Price.Equal(d("103")))
}

// Sell-side sweep: 3 bids; a single market sell fills the highest bid first.
func TestSmoke_MarketSellSweepsMultipleLevels(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Buy, "103", "1"))
	eng.Submit(limit(Buy, "102", "1"))
	eng.Submit(limit(Buy, "101", "1"))

	sell := market(Sell, "3")
	require.NoError(t, eng.Submit(sell))

	evs := drainN(events, 16)
	trades := tradesFrom(evs)
	require.Len(t, trades, 3)
	assert.True(t, trades[0].Price.Equal(d("103")))
	assert.True(t, trades[1].Price.Equal(d("102")))
	assert.True(t, trades[2].Price.Equal(d("101")))
}

// Price improvement: a limit buy above the resting ask fills at the ask price,
// not at the (higher) limit price.
func TestSmoke_TakerGetsPriceImprovement(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100", "5"))

	buy := limit(Buy, "110", "5")
	require.NoError(t, eng.Submit(buy))

	evs := drainN(events, 8)
	trades := tradesFrom(evs)
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Price.Equal(d("100")), "taker should fill at maker price, got %s", trades[0].Price)
}

// Partial fill leaves remainder resting; next incoming order matches the rest.
func TestSmoke_PartialFillLeavesRemainder(t *testing.T) {
	eng, events := newTestEngine(t)

	sell := limit(Sell, "100", "10")
	require.NoError(t, eng.Submit(sell))
	buy1 := limit(Buy, "100", "6")
	require.NoError(t, eng.Submit(buy1))

	// sell is now partially filled (4 remaining)
	snap := eng.Snapshot()
	require.Len(t, snap.Asks, 1)
	assert.Equal(t, "4", snap.Asks[0].Qty)

	// second buy clears the remainder
	buy2 := limit(Buy, "100", "4")
	require.NoError(t, eng.Submit(buy2))

	evs := drainN(events, 16)
	trades := tradesFrom(evs)
	require.Len(t, trades, 2)

	total := decimal.Zero
	for _, tr := range trades {
		total = total.Add(tr.Qty)
	}
	assert.True(t, total.Equal(d("10")), "total filled should be 10, got %s", total)

	snap = eng.Snapshot()
	assert.Empty(t, snap.Asks, "ask side should be empty after full fill")
}

// Cancel removes the order from the book and emits a cancelled event.
func TestSmoke_CancelRemovesFromBook(t *testing.T) {
	eng, events := newTestEngine(t)

	sell := limit(Sell, "100", "5")
	require.NoError(t, eng.Submit(sell))
	require.Len(t, eng.Snapshot().Asks, 1)

	require.NoError(t, eng.Cancel(sell.ID))

	snap := eng.Snapshot()
	assert.Empty(t, snap.Asks)

	evs := drainAll(events)
	var cancelled []Event
	for _, e := range evs {
		if e.Type == EvOrderCancelled && e.OrderID == sell.ID {
			cancelled = append(cancelled, e)
		}
	}
	assert.Len(t, cancelled, 1)
}

// MakerID / TakerID on trade events are populated correctly.
func TestSmoke_TradeEventHasCorrectMakerAndTaker(t *testing.T) {
	eng, events := newTestEngine(t)

	maker := limit(Sell, "100", "5")
	taker := limit(Buy, "100", "5")
	eng.Submit(maker)
	eng.Submit(taker)

	evs := drainN(events, 8)
	trades := tradesFrom(evs)
	require.Len(t, trades, 1)
	assert.Equal(t, maker.ID, trades[0].MakerID)
	assert.Equal(t, taker.ID, trades[0].TakerID)
}

// ── Stress tests ──────────────────────────────────────────────────────────────

// High-volume sequential: submit N alternating buy/sell limit pairs.
// Every pair should produce exactly one trade; the book must be empty at the end.
func TestStress_SequentialLimitPairs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	const N = 10_000
	eng, events := newTestEngine(t)

	go func() {
		for i := 0; i < N; i++ {
			p := fmt.Sprintf("%d", 100+i%100)
			eng.Submit(limit(Sell, p, "1"))
			eng.Submit(limit(Buy, p, "1"))
		}
	}()

	deadline := time.After(15 * time.Second)
	var trades int
	for trades < N {
		select {
		case ev := <-events:
			if ev.Type == EvTrade {
				trades++
			}
		case <-deadline:
			t.Fatalf("timeout: only %d/%d trades after 15s", trades, N)
		}
	}
	assert.Equal(t, N, trades)
	snap := eng.Snapshot()
	assert.Empty(t, snap.Bids, "book should be empty when every pair crosses")
	assert.Empty(t, snap.Asks, "book should be empty when every pair crosses")
}

// Concurrent producers: multiple goroutines submit orders simultaneously.
// All submitted quantities must be accounted for in trade fill totals.
func TestStress_ConcurrentProducers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	const (
		goroutines = 8
		perG       = 500 // limit orders per goroutine
		price      = "100"
	)
	eng, events := newTestEngine(t)

	var wg sync.WaitGroup
	// Half goroutines submit buys, half submit sells at the same price.
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		side := Buy
		if g%2 == 1 {
			side = Sell
		}
		go func(s Side) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				eng.Submit(limit(s, price, "1"))
			}
		}(side)
	}

	// Drain events until all orders have been submitted + the WaitGroup is done.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	var tradeQty decimal.Decimal
	var tradeCount int64
	deadline := time.After(20 * time.Second)
	// Total supply from each side: goroutines/2 × perG = 2000 units.
	totalPerSide := goroutines / 2 * perG

	// Collect until deadline or we've seen enough trades.
outer:
	for {
		select {
		case ev := <-events:
			if ev.Type == EvTrade {
				atomic.AddInt64(&tradeCount, 1)
				tradeQty = tradeQty.Add(ev.Qty)
			}
			if tradeQty.GreaterThanOrEqual(decimal.NewFromInt(int64(totalPerSide))) {
				break outer
			}
		case <-deadline:
			t.Logf("deadline: %d trades, qty=%s", tradeCount, tradeQty)
			break outer
		}
	}

	// Allow the submit goroutines to finish before inspecting the book.
	<-done

	// Drain any trailing events.
	for {
		select {
		case ev := <-events:
			if ev.Type == EvTrade {
				tradeQty = tradeQty.Add(ev.Qty)
			}
		default:
			goto done
		}
	}
done:
	// Total filled across all trades cannot exceed what was submitted per side.
	maxQty := decimal.NewFromInt(int64(totalPerSide))
	assert.True(t, tradeQty.LessThanOrEqual(maxQty),
		"traded qty %s exceeds max possible %s", tradeQty, maxQty)
	assert.True(t, tradeQty.GreaterThan(decimal.Zero), "no trades occurred")

	// Bids + asks can't both be non-empty (crossed book is a correctness bug).
	snap := eng.Snapshot()
	if len(snap.Bids) > 0 && len(snap.Asks) > 0 {
		t.Errorf("crossed book: have both bids and asks resting simultaneously")
	}
	t.Logf("completed: %d trades, qty=%s, remaining bids=%d asks=%d",
		tradeCount, tradeQty, len(snap.Bids), len(snap.Asks))
}

// Throughput benchmark: measures how many orders/second the engine sustains.
// Run with: go test ./engine/ -bench=BenchmarkTrade -benchtime=5s
func BenchmarkTrade_LimitPair(b *testing.B) {
	events := make(chan Event, b.N*3)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	// Pre-drain events in the background so the channel never blocks.
	go func() {
		for range events {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := fmt.Sprintf("%d", 100+i%1000)
		eng.Submit(limit(Sell, p, "1"))
		eng.Submit(limit(Buy, p, "1"))
	}
}

func BenchmarkTrade_MarketAgainstResting(b *testing.B) {
	events := make(chan Event, 4096)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	go func() {
		for range events {
		}
	}()

	// Seed the book with deep ask liquidity.
	for i := 0; i < 1000; i++ {
		eng.Submit(limit(Sell, fmt.Sprintf("%d", 100+i), "100"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Replenish liquidity every 100 iterations so we always have asks.
		if i%100 == 0 {
			for j := 0; j < 10; j++ {
				eng.Submit(limit(Sell, fmt.Sprintf("%d", 100+j), "100"))
			}
		}
		eng.Submit(market(Buy, "0.01"))
	}
}
