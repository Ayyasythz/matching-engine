package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { v, _ := decimal.NewFromString(s); return v }
func uid() string                { return uuid.New().String() }

// scenario is a fixed set of resting orders at one price level.
// The same scenario runs under both algorithms so the output is directly comparable.
var scenario = []struct {
	name string
	qty  int
}{
	{"A", 200}, // 20% of total book
	{"B", 500}, // 50% of total book
	{"C", 300}, // 30% of total book
}

const (
	levelPrice  = "300.00"
	incomingQty = 999 // 40% of total book (1000)
	maxBarWidth = 36  // chars for the largest order (B=500)
	maxQty      = 500
)

func main() {
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata")
	flag.Parse()

	// ── engine setup ──────────────────────────────────────────────────────────
	events := make(chan engine.Event, 512)
	var eng *engine.Engine
	if *mode == "prorata" {
		eng = engine.NewProRataEngine("BTC-USD", events)
	} else {
		eng = engine.NewEngine("BTC-USD", events)
	}
	go eng.Run()

	// ── post resting sell orders ──────────────────────────────────────────────
	type posted struct {
		name string
		id   string
		qty  int
	}
	resting := make([]posted, len(scenario))
	idToName := make(map[string]string)

	for i, s := range scenario {
		o := engine.NewOrder(uid(), "BTC-USD", engine.Sell, engine.Limit,
			d(levelPrice), d(fmt.Sprintf("%d", s.qty)))
		resting[i] = posted{name: s.name, id: o.ID, qty: s.qty}
		idToName[o.ID] = s.name
		eng.Submit(o)
	}

	// drain the order.accepted events so only trade events remain after the buy
	time.Sleep(20 * time.Millisecond)
	for len(events) > 0 {
		<-events
	}

	// ── submit the incoming market buy ────────────────────────────────────────
	buyer := engine.NewOrder(uid(), "BTC-USD", engine.Buy, engine.Market,
		decimal.Zero, d(fmt.Sprintf("%d", incomingQty)))
	eng.Submit(buyer)

	// Submit is synchronous — by the time it returns all events are buffered.
	eng.Stop()

	// ── collect fills from trade events ───────────────────────────────────────
	fills := make(map[string]int)
	for {
		select {
		case ev := <-events:
			if ev.Type == engine.EvTrade {
				if name, ok := idToName[ev.MakerID]; ok {
					qty, _ := ev.Qty.Float64()
					fills[name] += int(qty)
				}
			}
		default:
			goto print
		}
	}

print:
	// ── render ────────────────────────────────────────────────────────────────
	sep := strings.Repeat("─", 70)
	label := strings.ToUpper(*mode)

	fmt.Printf("\n  %s  |  3 orders at $%s  |  incoming buy: %d qty\n",
		label, levelPrice, incomingQty)

	totalBook := 0
	for _, r := range resting {
		totalBook += r.qty
	}
	fmt.Printf("  Total book: %d qty  |  Fill rate: %.0f%%\n\n",
		totalBook, float64(incomingQty)/float64(totalBook)*100)

	fmt.Printf("  %-6s  %-10s  %-10s  %s\n", "Order", "Resting", "Filled", "Distribution")
	fmt.Printf("  %s\n", sep)

	for _, r := range resting {
		filled := fills[r.name]
		pct := 0.0
		if r.qty > 0 {
			pct = float64(filled) / float64(r.qty) * 100
		}

		totalBars := r.qty * maxBarWidth / maxQty
		filledBars := filled * maxBarWidth / maxQty
		if filledBars > totalBars {
			filledBars = totalBars
		}
		emptyBars := totalBars - filledBars

		bar := strings.Repeat("█", filledBars) + strings.Repeat("░", emptyBars)

		var note string
		switch {
		case filled == 0:
			note = "never reached"
		case filled == r.qty:
			note = "fully consumed"
		default:
			note = fmt.Sprintf("%.0f%% of %d", pct, r.qty)
		}

		fmt.Printf("  %-6s  %-10d  %-10d  %-36s  %s\n",
			r.name, r.qty, filled, bar, note)
	}

	totalFilled := 0
	for _, v := range fills {
		totalFilled += v
	}
	fmt.Printf("\n  Total filled: %d / %d\n\n", totalFilled, incomingQty)

	// ── explanation ───────────────────────────────────────────────────────────
	if *mode == "fifo" {
		fmt.Println("  FIFO: A arrived first so it fills completely before B gets")
		fmt.Println("  anything. C is never touched. Queue position = everything.")
	} else {
		fmt.Println("  Pro-rata: all three orders fill simultaneously at the same")
		fmt.Println("  40% rate. Size = everything. Arrival time is irrelevant.")
	}
	fmt.Println()
}
