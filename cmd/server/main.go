package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	httpapi "github.com/Ayyasythz/matching-engine/api/httpapi"
	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/Ayyasythz/matching-engine/marketmaker"
	"github.com/Ayyasythz/matching-engine/pricefeed"
	"github.com/shopspring/decimal"
)

func main() {
	defaultAddr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		defaultAddr = ":" + port
	}
	addr := flag.String("addr", defaultAddr, "HTTP server address")
	frontend := flag.String("frontend", "./frontend", "path to frontend static files")
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata | amm")
	anchor := flag.Bool("anchor", true, "anchor prices to the live BTC index (market-maker bot, or arbitrage bot in AMM mode)")
	feedInterval := flag.Duration("feed-interval", 10*time.Second, "live price poll interval (CoinGecko free tier allows ~6/min)")
	ammBase := flag.Float64("amm-base", 25, "AMM mode: initial BTC reserve of the pool")
	ammPrice := flag.Float64("amm-price", 0, "AMM mode: initial pool price (0 = use live index price, fallback 60000)")
	ammFeeBps := flag.Int64("amm-fee-bps", 30, "AMM mode: swap fee in basis points")
	flag.Parse()

	events := make(chan engine.Event, 4096)
	ctx := context.Background()

	// Start the price feed early so AMM mode can seed the pool at the live price.
	var feed *pricefeed.Feed
	if *anchor {
		feed = pricefeed.New(*feedInterval)
		go feed.Run(ctx)
	}

	var eng *engine.Engine
	switch *mode {
	case "prorata":
		eng = engine.NewProRataEngine("BTC-USD", events)
	case "amm":
		seedPrice := decimal.NewFromFloat(*ammPrice)
		if seedPrice.LessThanOrEqual(decimal.Zero) {
			seedPrice = decimal.NewFromInt(60000)
			if feed != nil {
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if p, ok := feed.Latest(); ok {
						seedPrice = p
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}
		base := decimal.NewFromFloat(*ammBase)
		pool, err := engine.NewAMMPool(base, base.Mul(seedPrice), *ammFeeBps)
		if err != nil {
			log.Fatalf("amm: %v", err)
		}
		eng = engine.NewAMMEngine("BTC-USD", events, pool)
		log.Printf("AMM pool seeded: %s BTC / $%s (price $%s, fee %d bps)",
			base, base.Mul(seedPrice).Round(2), seedPrice.Round(2), *ammFeeBps)
	default:
		eng = engine.NewEngine("BTC-USD", events)
	}
	go eng.Run()

	srv := httpapi.NewServer(eng, events)

	if *anchor {
		go func() {
			for p := range feed.Subscribe() {
				srv.BroadcastIndexPrice(p)
			}
		}()

		if *mode == "amm" {
			arb := marketmaker.NewArb(eng, feed, 2*time.Second)
			go arb.Run(ctx)
			log.Printf("price anchor enabled: arbitrage bot pegging AMM pool to live BTC-USD (poll %s)", *feedInterval)
		} else {
			maker := marketmaker.New(eng, feed, marketmaker.DefaultConfig())
			go maker.Run(ctx)
			log.Printf("price anchor enabled: market maker quoting around live BTC-USD (poll %s)", *feedInterval)
		}
	}

	handler := srv.Handler(*frontend)

	log.Printf("matching engine server listening on http://localhost%s  (mode: %s)", *addr, *mode)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
