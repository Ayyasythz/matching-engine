package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func main() {
	events := make(chan engine.Event, 512)
	eng := engine.NewEngine("BTC-USD", events)
	go eng.Run()

	go func() {
		for ev := range events {
			fmt.Printf("[%d] %s  order=%s  price=%s  qty=%s  maker=%s  taker=%s\n",
				ev.Seq, ev.Type, ev.OrderID, ev.Price, ev.Qty, ev.MakerID, ev.TakerID)
		}
	}()

	sell := engine.NewOrder(uuid.New().String(), "BTC-USD", engine.Sell, engine.Limit, d("50000.00"), d("0.5"))
	if err := eng.Submit(sell); err != nil {
		log.Fatal(err)
	}

	buy := engine.NewOrder(uuid.New().String(), "BTC-USD", engine.Buy, engine.Market, decimal.Zero, d("0.5"))
	if err := eng.Submit(buy); err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	eng.Stop()
}
