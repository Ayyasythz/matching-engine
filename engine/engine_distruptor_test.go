package engine

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
)

func newDisruptorEngine(t *testing.T) (*Engine, chan Event) {
	t.Helper()
	events := make(chan Event, 65536)
	eng := newEngine("BTC-USD", events, FIFOMatcher{})
	go eng.Run()
	t.Cleanup(func() { eng.Stop() })
	return eng, events
}

// ── throughput benchmarks ─────────────────────────────────────────────────────

func BenchmarkSequencer_Disruptor(b *testing.B) {
	events := make(chan Event, b.N*8+512)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		price := fmt.Sprintf("%.2f", 100.0+float64(i%100)*0.01)
		o := NewOrder(uid(), "BTC-USD", Sell, Limit,
			func() decimal.Decimal { v, _ := decimal.NewFromString(price); return v }(),
			decimal.NewFromInt(10))
		eng.Submit(o)
	}
}

func BenchmarkSequencer_Contention(b *testing.B) {
	const numProducers = 8

	events := make(chan Event, 4096)
	eng := NewEngine("BTC-USD", events)
	go eng.Run()
	defer eng.Stop()

	go func() {
		for range events {
		}
	}()

	b.SetParallelism(numProducers)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			price := fmt.Sprintf("%.2f", 100.0+float64(i%100)*0.01)
			o := NewOrder(uid(), "BTC-USD", Sell, Limit,
				func() decimal.Decimal { v, _ := decimal.NewFromString(price); return v }(),
				decimal.NewFromInt(10))
			eng.Submit(o)
			i++
		}
	})
}
