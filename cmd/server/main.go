package main

import (
	"flag"
	"log"
	"net/http"

	httpapi "github.com/Ayyasythz/matching-engine/api/httpapi"
	"github.com/Ayyasythz/matching-engine/engine"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP server address")
	frontend := flag.String("frontend", "./frontend", "path to frontend static files")
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata")
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
	handler := srv.Handler(*frontend)

	log.Printf("matching engine server listening on http://localhost%s  (mode: %s)", *addr, *mode)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
