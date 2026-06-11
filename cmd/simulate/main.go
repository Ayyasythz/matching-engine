package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/Ayyasythz/matching-engine/engine"
)

const (
	simDuration = 10 * time.Second
	startPrice  = 50_000.0
	halfSpread  = 25.0 // $25 each side
	makerQty    = "10"
	traderQty   = "1"
	numTraders  = 8
	mmRefreshMs = 50 // market maker refreshes quotes every 50ms
)

var (
	totalOrders int64
	totalTrades int64

	latMu      sync.Mutex
	latSamples []int64
)

func submit(e *engine.Engine, o *engine.Order) {
	t0 := time.Now()
	_ = e.Submit(o)
	ns := time.Since(t0).Nanoseconds()

	atomic.AddInt64(&totalOrders, 1)

	latMu.Lock()
	latSamples = append(latSamples, ns)
	latMu.Unlock()
}

func limitOrder(side engine.Side, price, qty string) *engine.Order {
	p, _ := decimal.NewFromString(price)
	q, _ := decimal.NewFromString(qty)
	return engine.NewOrder(uuid.New().String(), "BTC-USD", side, engine.Limit, p, q)
}

func marketOrder(side engine.Side, qty string) *engine.Order {
	q, _ := decimal.NewFromString(qty)
	return engine.NewOrder(uuid.New().String(), "BTC-USD", side, engine.Market, decimal.Zero, q)
}

func price(f float64) string { return fmt.Sprintf("%.2f", f) }

func marketMaker(ctx context.Context, e *engine.Engine, getMid func() float64) {
	var bidID, askID string

	cancelQuotes := func() {
		if bidID != "" {
			e.Cancel(bidID)
			bidID = ""
		}
		if askID != "" {
			e.Cancel(askID)
			askID = ""
		}
	}

	tick := time.NewTicker(mmRefreshMs * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			cancelQuotes()
			return
		case <-tick.C:
		}

		cancelQuotes()

		mid := getMid()
		bid := limitOrder(engine.Buy, price(mid-halfSpread), makerQty)
		ask := limitOrder(engine.Sell, price(mid+halfSpread), makerQty)

		submit(e, bid)
		submit(e, ask)

		bidID = bid.ID
		askID = ask.ID
	}
}

func trader(ctx context.Context, e *engine.Engine, getMid func() float64, rng *rand.Rand) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		mid := getMid()

		switch rng.Intn(4) {
		case 0:
			submit(e, marketOrder(engine.Buy, traderQty))
		case 1:
			submit(e, marketOrder(engine.Sell, traderQty))
		case 2:
			// Limit buy slightly below mid — sometimes crosses the spread
			submit(e, limitOrder(engine.Buy, price(mid-float64(rng.Intn(30))), traderQty))
		case 3:
			// Limit sell slightly above mid
			submit(e, limitOrder(engine.Sell, price(mid+float64(rng.Intn(30))), traderQty))
		}
	}
}

func priceDrift(ctx context.Context, mid *float64, mu *sync.RWMutex) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			mu.Lock()
			*mid += (rng.Float64() - 0.5) * 10
			mu.Unlock()
		}
	}
}

func percentile(samples []int64, p float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	cp := make([]int64, len(samples))
	copy(cp, samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Ceil(float64(len(cp))*p/100)) - 1
	if idx < 0 {
		idx = 0
	}
	return cp[idx]
}

func fmtNum(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func main() {
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata")
	flag.Parse()

	events := make(chan engine.Event, 1<<20)

	var e *engine.Engine
	switch *mode {
	case "prorata":
		e = engine.NewProRataEngine("BTC-USD", events)
	default:
		e = engine.NewEngine("BTC-USD", events)
	}
	go e.Run()

	go func() {
		for ev := range events {
			if ev.Type == engine.EvTrade {
				atomic.AddInt64(&totalTrades, 1)
			}
		}
	}()

	mid := startPrice
	var midMu sync.RWMutex
	getMid := func() float64 {
		midMu.RLock()
		defer midMu.RUnlock()
		return mid
	}

	ctx, cancel := context.WithTimeout(context.Background(), simDuration)
	defer cancel()

	bar := strings.Repeat("─", 68)
	fmt.Printf("\n  BTC-USD  |  1 market maker  |  %d traders  |  %s run  |  mode: %s\n\n",
		numTraders, simDuration, *mode)
	fmt.Printf("  %-7s  %-14s  %-14s  %-11s  %s\n",
		"Time", "Orders/s", "Trades/s", "Fill rate", "Mid price")
	fmt.Printf("  %s\n", bar)

	var lastO, lastT int64
	elapsed := 0
	statTick := time.NewTicker(time.Second)

	go func() {
		defer statTick.Stop()
		for {
			select {
			case <-statTick.C:
				elapsed++
				co := atomic.LoadInt64(&totalOrders)
				ct := atomic.LoadInt64(&totalTrades)
				dO := co - lastO
				dT := ct - lastT
				lastO, lastT = co, ct

				fr := 0.0
				if dO > 0 {
					fr = float64(dT*2) / float64(dO) * 100
				}

				fmt.Printf("  %-7s  %-14s  %-14s  %10.1f%%  $%.2f\n",
					fmt.Sprintf("%ds", elapsed),
					fmtNum(dO),
					fmtNum(dT),
					fr,
					getMid(),
				)
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); priceDrift(ctx, &mid, &midMu) }()

	wg.Add(1)
	go func() { defer wg.Done(); marketMaker(ctx, e, getMid) }()

	for i := 0; i < numTraders; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(i*1000)))
		go func(r *rand.Rand) { defer wg.Done(); trader(ctx, e, getMid, r) }(rng)
	}

	wg.Wait()

	time.Sleep(200 * time.Millisecond)
	e.Stop()

	total := atomic.LoadInt64(&totalOrders)
	trades := atomic.LoadInt64(&totalTrades)

	latMu.Lock()
	samples := make([]int64, len(latSamples))
	copy(samples, latSamples)
	latMu.Unlock()

	fillRate := 0.0
	if total > 0 {
		fillRate = float64(trades*2) / float64(total) * 100
	}
	throughput := total / int64(simDuration.Seconds())

	fmt.Printf("  %s\n", bar)
	fmt.Printf("  %-28s %s\n", "Orders submitted:", fmtNum(total))
	fmt.Printf("  %-28s %s\n", "Trades executed:", fmtNum(trades))
	fmt.Printf("  %-28s %.1f%%\n", "Fill rate:", fillRate)
	fmt.Printf("  %-28s %s orders/sec\n", "Throughput:", fmtNum(throughput))
	fmt.Printf("  %-28s %d ns\n", "Latency p50:", percentile(samples, 50))
	fmt.Printf("  %-28s %d ns\n", "Latency p99:", percentile(samples, 99))
	fmt.Printf("  %-28s %d ns\n", "Latency p99.9:", percentile(samples, 99.9))
	fmt.Println()
}
