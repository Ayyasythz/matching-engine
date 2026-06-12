package marketmaker

import (
	"testing"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

type fakeFeed struct {
	price decimal.Decimal
	ok    bool
}

func (f *fakeFeed) Latest() (decimal.Decimal, bool) { return f.price, f.ok }

func newTestEngine() *engine.Engine {
	eng := engine.NewEngine("BTC-USD", make(chan engine.Event, 4096))
	go eng.Run()
	return eng
}

func TestRequotePlacesLadder(t *testing.T) {
	eng := newTestEngine()
	defer eng.Stop()
	feed := &fakeFeed{price: decimal.RequireFromString("100000"), ok: true}
	m := New(eng, feed, DefaultConfig())

	m.requote()

	snap, _ := eng.Snapshot()
	if len(snap.Bids) != 5 || len(snap.Asks) != 5 {
		t.Fatalf("got %d bids, %d asks; want 5 and 5", len(snap.Bids), len(snap.Asks))
	}
	// Best bid: 100000 × (1 − 5bps) = 99950.00; best ask: 100050.00
	if snap.Bids[0].Price != "99950" {
		t.Errorf("best bid %s, want 99950", snap.Bids[0].Price)
	}
	if snap.Asks[0].Price != "100050" {
		t.Errorf("best ask %s, want 100050", snap.Asks[0].Price)
	}
	// Second level: step is 2bps → bid 99930.00, ask 100070.00
	if snap.Bids[1].Price != "99930" {
		t.Errorf("second bid %s, want 99930", snap.Bids[1].Price)
	}
}

func TestRequoteReplacesOldLadder(t *testing.T) {
	eng := newTestEngine()
	defer eng.Stop()
	feed := &fakeFeed{price: decimal.RequireFromString("100000"), ok: true}
	m := New(eng, feed, DefaultConfig())

	m.requote()
	feed.price = decimal.RequireFromString("110000")
	m.requote()

	snap, _ := eng.Snapshot()
	if len(snap.Bids) != 5 || len(snap.Asks) != 5 {
		t.Fatalf("got %d bids, %d asks; want 5 and 5 (old ladder must be cancelled)",
			len(snap.Bids), len(snap.Asks))
	}
	if snap.Asks[0].Price != "110055" { // 110000 × 1.0005
		t.Errorf("best ask %s, want 110055", snap.Asks[0].Price)
	}
}

func TestRequoteIdleWithoutPrice(t *testing.T) {
	eng := newTestEngine()
	defer eng.Stop()
	m := New(eng, &fakeFeed{ok: false}, DefaultConfig())

	m.requote()

	snap, _ := eng.Snapshot()
	if len(snap.Bids) != 0 || len(snap.Asks) != 0 {
		t.Fatalf("expected empty book, got %d bids %d asks", len(snap.Bids), len(snap.Asks))
	}
}
