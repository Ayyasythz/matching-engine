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
)

func main() {
	defaultAddr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		defaultAddr = ":" + port
	}
	addr := flag.String("addr", defaultAddr, "HTTP server address")
	frontend := flag.String("frontend", "./frontend", "path to frontend static files")
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata")
	anchor := flag.Bool("anchor", true, "anchor the book to the live BTC price with a market-maker bot")
	feedInterval := flag.Duration("feed-interval", 10*time.Second, "live price poll interval (CoinGecko free tier allows ~6/min)")
	flag.Parse()

	events := make(chan engine.Event, 4096)

	var eng *engine.Engine
	switch *mode {
	case "prorata":
		eng = engine.NewProRataEngine("BTC-USD", events)
	default:
		eng = engine.NewEngine("BTC-USD", events)
	}
	go eng.Run()

	srv := httpapi.NewServer(eng, events)

	if *anchor {
		ctx := context.Background()
		feed := pricefeed.New(*feedInterval)
		go feed.Run(ctx)

		go func() {
			for p := range feed.Subscribe() {
				srv.BroadcastIndexPrice(p)
			}
		}()

		maker := marketmaker.New(eng, feed, marketmaker.DefaultConfig())
		go maker.Run(ctx)
		log.Printf("price anchor enabled: market maker quoting around live BTC-USD (poll %s)", *feedInterval)
	}

	handler := srv.Handler(*frontend)

	log.Printf("matching engine server listening on http://localhost%s  (mode: %s)", *addr, *mode)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
