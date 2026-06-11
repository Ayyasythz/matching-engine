package httpapi

import (
	"testing"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

func newTestServer() *Server {
	events := make(chan engine.Event, 4096)
	eng := engine.NewEngine("BTC-USD", events)
	go eng.Run()
	return NewServer(eng, events)
}

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestReserveReducesAvailable(t *testing.T) {
	s := newTestServer()

	s.balanceMu.Lock()
	s.reserve("sess1", "o1", engine.Buy, d("50000"), d("1"))
	bal := s.ensureBalance("sess1")
	availUSD := bal.USD.Sub(bal.ReservedUSD)
	s.balanceMu.Unlock()

	// startUSD is 100000; one open buy of 1 BTC @ 50000 leaves 50000 available
	if !availUSD.Equal(d("50000")) {
		t.Fatalf("available USD = %s, want 50000", availUSD)
	}
}

func TestCancelReleasesReservation(t *testing.T) {
	s := newTestServer()

	s.balanceMu.Lock()
	s.reserve("sess1", "o1", engine.Sell, decimal.Zero, d("1.5"))
	s.balanceMu.Unlock()

	s.applyEventToRecords(engine.Event{Type: engine.EvOrderCancelled, OrderID: "o1"})

	s.balanceMu.Lock()
	bal := s.ensureBalance("sess1")
	s.balanceMu.Unlock()
	if !bal.ReservedBTC.IsZero() {
		t.Fatalf("ReservedBTC = %s, want 0", bal.ReservedBTC)
	}
}

func TestFillReleasesFilledPortion(t *testing.T) {
	s := newTestServer()

	// Register the order so the trade event finds its record + session.
	s.orderMu.Lock()
	s.orders["o1"] = &orderRecord{ID: "o1", Side: "buy"}
	s.sessionByOrder["o1"] = "sess1"
	s.orderMu.Unlock()

	s.balanceMu.Lock()
	s.reserve("sess1", "o1", engine.Buy, d("50000"), d("1"))
	s.balanceMu.Unlock()

	// Half fills at a better price; hold released at the reserved rate.
	s.applyEventToRecords(engine.Event{
		Type: engine.EvTrade, TakerID: "o1", MakerID: "other",
		Price: d("49000"), Qty: d("0.5"),
	})

	s.balanceMu.Lock()
	bal := s.ensureBalance("sess1")
	s.balanceMu.Unlock()
	if !bal.ReservedUSD.Equal(d("25000")) { // 0.5 left × 50000
		t.Fatalf("ReservedUSD = %s, want 25000", bal.ReservedUSD)
	}
	// Balance debited at the actual trade price: 100000 − 49000×0.5
	if !bal.USD.Equal(d("75500")) {
		t.Fatalf("USD = %s, want 75500", bal.USD)
	}
}
