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

type marketDef struct {
	symbol string
	coinID string
}

var supportedMarkets = []marketDef{
	{"BTC-USD", "bitcoin"},
	{"ETH-USD", "ethereum"},
	{"SOL-USD", "solana"},
}

func main() {
	defaultAddr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		defaultAddr = ":" + port
	}
	addr := flag.String("addr", defaultAddr, "HTTP server address")
	frontend := flag.String("frontend", "./frontend", "path to frontend static files")
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata | amm")
	anchor := flag.Bool("anchor", true, "anchor prices to live index (market-maker bots)")
	feedInterval := flag.Duration("feed-interval", 10*time.Second, "live price poll interval (CoinGecko free tier allows ~6/min)")
	ammBase := flag.Float64("amm-base", 25, "AMM mode: initial BTC reserve of the pool")
	ammPrice := flag.Float64("amm-price", 0, "AMM mode: initial pool price (0 = use live index price, fallback 60000)")
	ammFeeBps := flag.Int64("amm-fee-bps", 30, "AMM mode: swap fee in basis points")
	flag.Parse()

	ctx := context.Background()

	// Start all price feeds early (needed to seed AMM pool price).
	feeds := make(map[string]*pricefeed.Feed, len(supportedMarkets))
	if *anchor {
		for _, m := range supportedMarkets {
			f := pricefeed.NewForCoin(m.coinID, *feedInterval)
			feeds[m.symbol] = f
			go f.Run(ctx)
		}
	}

	// Merged events channel — all engines write here.
	merged := make(chan engine.Event, 4096*len(supportedMarkets))

	engines := make(map[string]*engine.Engine, len(supportedMarkets))
	for _, m := range supportedMarkets {
		engEvents := make(chan engine.Event, 4096)

		var eng *engine.Engine
		if m.symbol == "BTC-USD" && *mode == "amm" {
			seedPrice := decimal.NewFromFloat(*ammPrice)
			if seedPrice.LessThanOrEqual(decimal.Zero) {
				seedPrice = decimal.NewFromInt(60000)
				if feed, ok := feeds[m.symbol]; ok {
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
			eng = engine.NewAMMEngine(m.symbol, engEvents, pool)
			log.Printf("AMM pool seeded: %s BTC / $%s (price $%s, fee %d bps)",
				base, base.Mul(seedPrice).Round(2), seedPrice.Round(2), *ammFeeBps)
		} else if *mode == "prorata" {
			eng = engine.NewProRataEngine(m.symbol, engEvents)
		} else {
			eng = engine.NewEngine(m.symbol, engEvents)
		}

		engines[m.symbol] = eng
		go eng.Run()

		// Forward this engine's events into the shared merged channel.
		go func(ch <-chan engine.Event) {
			for ev := range ch {
				merged <- ev
			}
		}(engEvents)
	}

	srv := httpapi.NewServer(engines, merged)

	if *anchor {
		for _, m := range supportedMarkets {
			m := m // capture loop variable
			feed, ok := feeds[m.symbol]
			if !ok {
				continue
			}
			// Broadcast index prices from this feed to all SSE clients.
			go func() {
				for p := range feed.Subscribe() {
					srv.BroadcastIndexPrice(m.symbol, p)
				}
			}()

			eng := engines[m.symbol]
			if m.symbol == "BTC-USD" && *mode == "amm" {
				arb := marketmaker.NewArb(eng, m.symbol, feed, 2*time.Second)
				go arb.Run(ctx)
			} else {
				maker := marketmaker.New(eng, m.symbol, feed, marketmaker.DefaultConfig())
				go maker.Run(ctx)
			}
		}
		log.Printf("price anchor enabled: market makers quoting BTC-USD, ETH-USD, SOL-USD (poll %s)", *feedInterval)
	}

	handler := srv.Handler(*frontend)

	log.Printf("matching engine server listening on http://localhost%s  (mode: %s, markets: BTC-USD ETH-USD SOL-USD)", *addr, *mode)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
