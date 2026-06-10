package engine

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newProRataTestEngine(t *testing.T) (*Engine, chan Event) {
	t.Helper()
	events := make(chan Event, 2048)
	eng := NewProRataEngine("BTC-USD", events)
	go eng.Run()
	t.Cleanup(func() { eng.Stop() })
	return eng, events
}

// TestProRata_DistributesProportionally is the canonical pro-rata scenario.
// Book: [A:200, B:500, C:300] — incoming: 400 (40% fill rate)
// Expected: A=80, B=200, C=120
func TestProRata_DistributesProportionally(t *testing.T) {
	eng, events := newProRataTestEngine(t)

	s1 := limit(Sell, "100.00", "200") // 20% of book
	s2 := limit(Sell, "100.00", "500") // 50% of book
	s3 := limit(Sell, "100.00", "300") // 30% of book
	eng.Submit(s1)
	eng.Submit(s2)
	eng.Submit(s3)

	eng.Submit(limit(Buy, "100.00", "400"))

	evs := drainAll(events)
	trades := tradesFrom(evs)
	require.Len(t, trades, 3, "all three makers should receive a fill")

	fillMap := make(map[string]decimal.Decimal)
	for _, tr := range trades {
		fillMap[tr.MakerID] = fillMap[tr.MakerID].Add(tr.Qty)
	}

	assert.True(t, fillMap[s1.ID].Equal(d("80")), "s1: got %s, want 80", fillMap[s1.ID])
	assert.True(t, fillMap[s2.ID].Equal(d("200")), "s2: got %s, want 200", fillMap[s2.ID])
	assert.True(t, fillMap[s3.ID].Equal(d("120")), "s3: got %s, want 120", fillMap[s3.ID])
}

// TestProRata_LargestRemainderAllocation verifies the rounding mechanism.
// Book: [A:3, B:7] total=10, incoming=6
// Raw:  A=1.8, B=4.2  →  floor: A=1, B=4, sum=5
// Remainder=1 → A has larger fractional part (0.8 > 0.2) → A gets +1
// Final: A=2, B=4, sum=6
func TestProRata_LargestRemainderAllocation(t *testing.T) {
	eng, events := newProRataTestEngine(t)

	s1 := limit(Sell, "100.00", "3")
	s2 := limit(Sell, "100.00", "7")
	eng.Submit(s1)
	eng.Submit(s2)

	eng.Submit(limit(Buy, "100.00", "6"))

	evs := drainAll(events)
	trades := tradesFrom(evs)
	require.Len(t, trades, 2)

	fillMap := make(map[string]decimal.Decimal)
	for _, tr := range trades {
		fillMap[tr.MakerID] = fillMap[tr.MakerID].Add(tr.Qty)
	}

	assert.True(t, fillMap[s1.ID].Equal(d("2")), "s1: got %s, want 2 (1+remainder)", fillMap[s1.ID])
	assert.True(t, fillMap[s2.ID].Equal(d("4")), "s2: got %s, want 4", fillMap[s2.ID])
}

// TestProRata_SumOfFillsEqualsIncoming is an invariant test.
// No matter how many orders or what sizes, the sum of maker fills
// must always equal exactly the incoming order's filled quantity.
func TestProRata_SumOfFillsEqualsIncoming(t *testing.T) {
	cases := []struct {
		qtys     []string
		incoming string
	}{
		{[]string{"100", "200", "300"}, "150"},
		{[]string{"7", "3"}, "6"},
		{[]string{"1", "1", "1", "1", "1"}, "3"},
		{[]string{"333", "333", "334"}, "500"},
		{[]string{"10"}, "7"},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("incoming=%s", tc.incoming), func(t *testing.T) {
			eng, events := newProRataTestEngine(t)

			for _, qty := range tc.qtys {
				eng.Submit(limit(Sell, "100.00", qty))
			}

			incoming := d(tc.incoming)
			buy := NewOrder(uid(), "BTC-USD", Buy, Limit, d("100.00"), incoming)
			eng.Submit(buy)

			evs := drainAll(events)
			trades := tradesFrom(evs)

			totalFilled := decimal.Zero
			for _, tr := range trades {
				totalFilled = totalFilled.Add(tr.Qty)
			}

			expected := decimal.Min(incoming, func() decimal.Decimal {
				total := decimal.Zero
				for _, q := range tc.qtys {
					total = total.Add(d(q))
				}
				return total
			}())

			assert.True(t, totalFilled.Equal(expected),
				"sum of fills %s ≠ expected %s", totalFilled, expected)
		})
	}
}

// TestProRata_DoesNotPreferEarlyArrivals contrasts with FIFO.
// With FIFO, s1 (first) would fill completely before s2 gets anything.
// With pro-rata, both receive proportional fills.
func TestProRata_DoesNotPreferEarlyArrivals(t *testing.T) {
	eng, events := newProRataTestEngine(t)

	// Equal size orders — each should get exactly half
	s1 := limit(Sell, "100.00", "100") // arrives first
	s2 := limit(Sell, "100.00", "100") // arrives second
	eng.Submit(s1)
	eng.Submit(s2)

	eng.Submit(limit(Buy, "100.00", "60"))

	evs := drainAll(events)
	trades := tradesFrom(evs)
	require.Len(t, trades, 2, "both makers should fill in pro-rata")

	fillMap := make(map[string]decimal.Decimal)
	for _, tr := range trades {
		fillMap[tr.MakerID] = fillMap[tr.MakerID].Add(tr.Qty)
	}

	// Both orders have equal size, so each gets exactly 30
	assert.True(t, fillMap[s1.ID].Equal(d("30")), "s1 should get 30, got %s", fillMap[s1.ID])
	assert.True(t, fillMap[s2.ID].Equal(d("30")), "s2 should get 30, got %s", fillMap[s2.ID])
}

// TestProRata_SweepsMultipleLevels verifies pro-rata still sweeps across
// price levels correctly (level-by-level behaviour is unchanged).
func TestProRata_SweepsMultipleLevels(t *testing.T) {
	eng, events := newProRataTestEngine(t)

	eng.Submit(limit(Sell, "100.00", "5"))
	eng.Submit(limit(Sell, "101.00", "5"))
	eng.Submit(limit(Sell, "102.00", "5"))

	eng.Submit(market(Buy, "12"))

	evs := drainAll(events)
	trades := tradesFrom(evs)
	require.Len(t, trades, 3)
	assert.True(t, trades[0].Price.Equal(d("100.00")))
	assert.True(t, trades[1].Price.Equal(d("101.00")))
	assert.True(t, trades[2].Price.Equal(d("102.00")))
}
