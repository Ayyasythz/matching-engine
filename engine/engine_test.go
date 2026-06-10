package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Helpers ──────────────────────────────────────────────────────────────────

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func uid() string { return uuid.New().String() }

func newTestEngine(t *testing.T) (*Engine, chan Event) {
	t.Helper()
	events := make(chan Event, 2048)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	t.Cleanup(func() { eng.Stop() })
	return eng, events
}

func limit(side Side, price, qty string) *Order {
	return NewOrder(uid(), "BTC-USD", side, Limit, d(price), d(qty))
}

func market(side Side, qty string) *Order {
	return NewOrder(uid(), "BTC-USD", side, Market, decimal.Zero, d(qty))
}

func ioc(side Side, price, qty string) *Order {
	return NewOrder(uid(), "BTC-USD", side, IOC, d(price), d(qty))
}

func fok(side Side, price, qty string) *Order {
	return NewOrder(uid(), "BTC-USD", side, FOK, d(price), d(qty))
}

// drainN collects n events with a timeout, returning however many arrived.
func drainN(ch chan Event, n int) []Event {
	out := make([]Event, 0, n)
	deadline := time.After(500 * time.Millisecond)
	for len(out) < n {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
	return out
}

func drainAll(ch chan Event) []Event {
	var out []Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

func tradesFrom(events []Event) []Event {
	var out []Event
	for _, e := range events {
		if e.Type == EvTrade {
			out = append(out, e)
		}
	}
	return out
}

func cancelledFrom(events []Event, id string) []Event {
	var out []Event
	for _, e := range events {
		if e.Type == EvOrderCancelled && e.OrderID == id {
			out = append(out, e)
		}
	}
	return out
}

// ── Correctness tests ─────────────────────────────────────────────────────────

func TestLimitOrder_RestsWhenNoCross(t *testing.T) {
	eng, events := newTestEngine(t)

	sell := limit(Sell, "101.00", "10")
	require.NoError(t, eng.Submit(sell))

	buy := limit(Buy, "100.00", "10") // below ask — no match
	require.NoError(t, eng.Submit(buy))

	evs := drainN(events, 4)
	assert.Empty(t, tradesFrom(evs), "orders should not match when bid < ask")
}

func TestLimitOrder_MatchesWhenCrosses(t *testing.T) {
	eng, events := newTestEngine(t)

	sell := limit(Sell, "100.00", "10")
	require.NoError(t, eng.Submit(sell))

	buy := limit(Buy, "100.00", "10")
	require.NoError(t, eng.Submit(buy))

	evs := drainN(events, 8)
	trades := tradesFrom(evs)
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Price.Equal(d("100.00")))
	assert.True(t, trades[0].Qty.Equal(d("10")))
}

func TestFIFO_PriorityPreservedAtSamePrice(t *testing.T) {
	eng, events := newTestEngine(t)

	s1 := limit(Sell, "100.00", "5")
	s2 := limit(Sell, "100.00", "5")
	s3 := limit(Sell, "100.00", "5")
	eng.Submit(s1)
	eng.Submit(s2)
	eng.Submit(s3)

	buy := limit(Buy, "100.00", "10") // fills s1 fully, s2 partially
	eng.Submit(buy)

	evs := drainAll(events)
	
	trades := tradesFrom(evs)
	require.Len(t, trades, 2, "should produce exactly 2 trades (s1 full, s2 partial)")
	assert.Equal(t, s1.ID, trades[0].MakerID, "s1 must fill first (FIFO)")
	assert.Equal(t, s2.ID, trades[1].MakerID, "s2 must fill second (FIFO)")
}

func TestMarketOrder_SweepsMultipleLevels(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "5"))
	eng.Submit(limit(Sell, "101.00", "5"))
	eng.Submit(limit(Sell, "102.00", "5"))

	buy := market(Buy, "12") // sweeps 100 (5), 101 (5), 102 (2)
	eng.Submit(buy)

	evs := drainN(events, 30)
	trades := tradesFrom(evs)
	require.Len(t, trades, 3)
	assert.True(t, trades[0].Price.Equal(d("100.00")), "should fill cheapest first")
	assert.True(t, trades[1].Price.Equal(d("101.00")))
	assert.True(t, trades[2].Price.Equal(d("102.00")))
	assert.True(t, trades[2].Qty.Equal(d("2")), "last level partially filled")
}

func TestIOC_CancelsUnfilledRemainder(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "5")) // only 5 available

	o := ioc(Buy, "100.00", "10") // wants 10, gets 5, cancels 5
	eng.Submit(o)

	evs := drainN(events, 12)
	cancelled := cancelledFrom(evs, o.ID)
	require.Len(t, cancelled, 1)
	assert.True(t, cancelled[0].Qty.Equal(d("5")), "cancelled qty should be 5")
}

func TestFOK_RejectsWhenInsufficientQty(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "5")) // only 5 available

	o := fok(Buy, "100.00", "10") // needs 10 — must reject without touching book
	eng.Submit(o)

	evs := drainN(events, 10)

	var rejected []Event
	for _, e := range evs {
		if e.Type == EvOrderRejected {
			rejected = append(rejected, e)
		}
	}
	require.Len(t, rejected, 1, "FOK must be rejected")
	assert.Empty(t, tradesFrom(evs), "FOK must not partially consume the book")

	// Verify the book is intact: a follow-up order should still fill against
	// the original 5-qty ask that the FOK did not touch.
	buy := limit(Buy, "100.00", "5")
	eng.Submit(buy)
	evs2 := drainN(events, 6)
	require.Len(t, tradesFrom(evs2), 1, "book must be intact after FOK rejection")
}

func TestFOK_ExecutesWhenSufficientQty(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "10"))

	o := fok(Buy, "100.00", "10")
	eng.Submit(o)

	evs := drainN(events, 10)
	require.Len(t, tradesFrom(evs), 1)
}

func TestFOK_SweepsMultipleLevelsWhenNeeded(t *testing.T) {
	eng, events := newTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "5"))
	eng.Submit(limit(Sell, "101.00", "5"))

	o := fok(Buy, "101.00", "10") // 10 available across two levels
	eng.Submit(o)

	evs := drainN(events, 20)
	assert.Len(t, tradesFrom(evs), 2, "FOK should sweep both levels")
}

func TestCancel_RemovesRestingOrder(t *testing.T) {
	eng, events := newTestEngine(t)

	o := limit(Sell, "100.00", "10")
	eng.Submit(o)
	require.NoError(t, eng.Cancel(o.ID))

	evs := drainN(events, 5)
	cancelled := cancelledFrom(evs, o.ID)
	require.Len(t, cancelled, 1)

	// Verify book is empty: market buy should not fill
	eng.Submit(market(Buy, "5"))
	evs2 := drainN(events, 5)
	assert.Empty(t, tradesFrom(evs2), "cancelled order must not fill")
}

func TestCancel_ReturnsErrorForUnknownOrder(t *testing.T) {
	eng, _ := newTestEngine(t)
	err := eng.Cancel("nonexistent-id")
	assert.Error(t, err)
}

func TestPartialFill_ThenCancelRemainder(t *testing.T) {
	eng, events := newTestEngine(t)

	seller := limit(Sell, "100.00", "10")
	eng.Submit(seller)

	// Buy only 4 of the 10
	eng.Submit(limit(Buy, "100.00", "4"))
	drainN(events, 10)

	// Cancel the remaining 6
	require.NoError(t, eng.Cancel(seller.ID))
	evs := drainN(events, 5)
	cancelled := cancelledFrom(evs, seller.ID)
	require.Len(t, cancelled, 1)
	assert.True(t, cancelled[0].Qty.Equal(d("6")), "cancelled qty should be remaining 6")
}

// ── Benchmarks ────────────────────────────────────────────────────────────────
// Run: go test -bench=. -benchmem ./engine/

func BenchmarkLimitOrderInsert(b *testing.B) {
	events := make(chan Event, b.N+512)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Spread orders across 100 distinct price levels to exercise the tree
		price := fmt.Sprintf("%.2f", 100.0+float64(i%100)*0.01)
		o := NewOrder(uid(), "BTC-USD", Sell, Limit, d(price), d("10"))
		eng.Submit(o)
	}
}

func BenchmarkMarketOrderMatch_SingleLevel(b *testing.B) {
	events := make(chan Event, b.N*4+512)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	// Reload the book before each iteration via sub-benchmarks is impractical.
	// Instead: pre-load a very deep ask level so we never exhaust it.
	ask := NewOrder(uid(), "BTC-USD", Sell, Limit, d("100.00"), decimal.NewFromInt(int64(b.N+1000)))
	eng.Submit(ask)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := NewOrder(uid(), "BTC-USD", Buy, Market, decimal.Zero, d("1"))
		eng.Submit(o)
	}
}

func BenchmarkMarketOrderMatch_SweepMultipleLevels(b *testing.B) {
	events := make(chan Event, 4096)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	go func() {
		for range events {
		}
	}()

	// Pre-load 1,000 ask levels with 1,000 qty each
	for i := 0; i < 1000; i++ {
		price := fmt.Sprintf("%.2f", 100.0+float64(i)*0.01)
		o := NewOrder(uid(), "BTC-USD", Sell, Limit, d(price), d("1000"))
		eng.Submit(o)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := NewOrder(uid(), "BTC-USD", Buy, Market, decimal.Zero, d("1"))
		eng.Submit(o)
	}
}

func BenchmarkCancelOrder(b *testing.B) {
	events := make(chan Event, b.N*2+512)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	orders := make([]*Order, b.N)
	for i := 0; i < b.N; i++ {
		o := NewOrder(uid(), "BTC-USD", Sell, Limit, d("100.00"), d("10"))
		eng.Submit(o)
		orders[i] = o
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Cancel(orders[i].ID)
	}
}
