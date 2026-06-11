# Live L1 Price Anchor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Anchor the demo BTC-USD market to the live BTC price via a price feed + market-maker bot, and fix the balance-reservation gap in the HTTP API.

**Architecture:** A `pricefeed` package polls Coinbase's public spot endpoint and exposes the latest price plus change notifications. A `marketmaker` package quotes a 5-level ladder of bids/asks around that price directly into the existing engine (cancel-and-replace every ~2s). `httpapi` broadcasts the index price as an `INDEX_PRICE` SSE event and gains per-session fund reservations so open orders can't overdraw. Everything runs as goroutines inside `cmd/server`.

**Tech Stack:** Go, `shopspring/decimal`, stdlib `net/http` + `httptest`, existing engine/disruptor.

**Spec:** `docs/superpowers/specs/2026-06-11-l1-price-anchor-design.md`

**Key facts about the existing code (read before starting):**
- `engine.Engine.Submit(*Order) error` and `Cancel(id) error` are synchronous and goroutine-safe (commands go through a ring buffer consumed by `Run()`). `Snapshot()` returns `BookSnapshot{Bids, Asks []PriceLevelSnapshot}` where `PriceLevelSnapshot.Price` is a string.
- `engine.Event` (`engine/events.go`) is the SSE payload type. Trades emit `EvTrade` with `MakerID`/`TakerID`.
- `api/httpapi/server.go` tracks orders/sessions/balances. Bot orders submitted directly to the engine never appear in `s.orders`, so `applyTradeToBalances` already skips them (nil record) — the bot needs **no** house balance in httpapi.
- Module path: `github.com/Ayyasythz/matching-engine`.

---

### Task 1: `pricefeed` package

**Files:**
- Create: `pricefeed/feed.go`
- Test: `pricefeed/feed_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// pricefeed/feed_test.go
package pricefeed

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestFetchOnceParsesPrice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"amount":"104231.50","base":"BTC","currency":"USD"}}`))
	}))
	defer srv.Close()

	f := NewWithURL(srv.URL, time.Second)
	if err := f.fetchOnce(); err != nil {
		t.Fatalf("fetchOnce: %v", err)
	}
	p, ok := f.Latest()
	if !ok {
		t.Fatal("expected price to be available")
	}
	if !p.Equal(decimal.RequireFromString("104231.50")) {
		t.Fatalf("got %s, want 104231.50", p)
	}
}

func TestFetchErrorKeepsLastPrice(t *testing.T) {
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`{"data":{"amount":"100.00"}}`))
	}))
	defer srv.Close()

	f := NewWithURL(srv.URL, time.Second)
	if err := f.fetchOnce(); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	fail = true
	if err := f.fetchOnce(); err == nil {
		t.Fatal("expected error on failed fetch")
	}
	p, ok := f.Latest()
	if !ok || !p.Equal(decimal.RequireFromString("100.00")) {
		t.Fatalf("last price lost: ok=%v p=%s", ok, p)
	}
}

func TestSubscribeNotifiedOnChange(t *testing.T) {
	price := `"100.00"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"amount":` + price + `}}`))
	}))
	defer srv.Close()

	f := NewWithURL(srv.URL, time.Second)
	ch := f.Subscribe()

	f.fetchOnce()
	select {
	case p := <-ch:
		if !p.Equal(decimal.RequireFromString("100.00")) {
			t.Fatalf("got %s", p)
		}
	case <-time.After(time.Second):
		t.Fatal("no notification for first price")
	}

	// Same price again → no notification
	f.fetchOnce()
	select {
	case <-ch:
		t.Fatal("unexpected notification for unchanged price")
	case <-time.After(50 * time.Millisecond):
	}

	price = `"101.00"`
	f.fetchOnce()
	select {
	case p := <-ch:
		if !p.Equal(decimal.RequireFromString("101.00")) {
			t.Fatalf("got %s", p)
		}
	case <-time.After(time.Second):
		t.Fatal("no notification for changed price")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pricefeed/ -v`
Expected: FAIL — `undefined: NewWithURL`

- [ ] **Step 3: Implement the feed**

```go
// pricefeed/feed.go
//
// Package pricefeed polls a public spot-price API and exposes the latest
// price plus change notifications.
package pricefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const CoinbaseBTCUSD = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

type Feed struct {
	url      string
	interval time.Duration
	client   *http.Client

	mu      sync.RWMutex
	price   decimal.Decimal
	ok      bool
	subs    []chan decimal.Decimal
	lastLog time.Time
}

// New polls the Coinbase BTC-USD spot price.
func New(interval time.Duration) *Feed {
	return NewWithURL(CoinbaseBTCUSD, interval)
}

func NewWithURL(url string, interval time.Duration) *Feed {
	return &Feed{
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

// Latest returns the last fetched price; ok is false until the first
// successful fetch.
func (f *Feed) Latest() (decimal.Decimal, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.price, f.ok
}

// Subscribe returns a channel notified on every price *change*.
// Slow subscribers miss updates rather than blocking the feed.
func (f *Feed) Subscribe() <-chan decimal.Decimal {
	ch := make(chan decimal.Decimal, 16)
	f.mu.Lock()
	f.subs = append(f.subs, ch)
	f.mu.Unlock()
	return ch
}

// Run polls until ctx is cancelled.
func (f *Feed) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	f.poll()
	for {
		select {
		case <-ticker.C:
			f.poll()
		case <-ctx.Done():
			return
		}
	}
}

func (f *Feed) poll() {
	if err := f.fetchOnce(); err != nil {
		// Keep last known price; log at most once per minute.
		f.mu.Lock()
		if time.Since(f.lastLog) > time.Minute {
			f.lastLog = time.Now()
			log.Printf("pricefeed: %v (keeping last price)", err)
		}
		f.mu.Unlock()
	}
}

type spotResponse struct {
	Data struct {
		Amount string `json:"amount"`
	} `json:"data"`
}

func (f *Feed) fetchOnce() error {
	resp, err := f.client.Get(f.url)
	if err != nil {
		return fmt.Errorf("fetch spot price: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch spot price: status %d", resp.StatusCode)
	}
	var sr spotResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return fmt.Errorf("decode spot price: %w", err)
	}
	p, err := decimal.NewFromString(sr.Data.Amount)
	if err != nil || p.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid spot price %q", sr.Data.Amount)
	}

	f.mu.Lock()
	changed := !f.ok || !p.Equal(f.price)
	f.price = p
	f.ok = true
	subs := f.subs
	f.mu.Unlock()

	if changed {
		for _, ch := range subs {
			select {
			case ch <- p:
			default:
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pricefeed/ -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add pricefeed/
git commit -m "feat: add pricefeed package polling Coinbase spot price"
```

---

### Task 2: `marketmaker` package

**Files:**
- Create: `marketmaker/maker.go`
- Test: `marketmaker/maker_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// marketmaker/maker_test.go
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

	snap := eng.Snapshot()
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

	snap := eng.Snapshot()
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

	snap := eng.Snapshot()
	if len(snap.Bids) != 0 || len(snap.Asks) != 0 {
		t.Fatalf("expected empty book, got %d bids %d asks", len(snap.Bids), len(snap.Asks))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./marketmaker/ -v`
Expected: FAIL — `undefined: New`, `undefined: DefaultConfig`

- [ ] **Step 3: Implement the market maker**

```go
// marketmaker/maker.go
//
// Package marketmaker quotes a ladder of limit orders around an external
// index price so the demo book tracks the real market.
package marketmaker

import (
	"context"
	"fmt"
	"time"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/shopspring/decimal"
)

// PriceSource is the part of pricefeed.Feed the maker needs.
type PriceSource interface {
	Latest() (decimal.Decimal, bool)
}

type Config struct {
	Levels      int             // price levels per side
	SpreadBps   int64           // half-spread from index to best quote, in basis points
	StepBps     int64           // distance between levels, in basis points
	QtyPerLevel decimal.Decimal // BTC quoted at each level
	Requote     time.Duration   // how often to refresh quotes
}

func DefaultConfig() Config {
	return Config{
		Levels:      5,
		SpreadBps:   5, // 0.05%
		StepBps:     2, // 0.02%
		QtyPerLevel: decimal.RequireFromString("0.05"),
		Requote:     2 * time.Second,
	}
}

type Maker struct {
	eng       *engine.Engine
	feed      PriceSource
	cfg       Config
	live      []string // resting bot order IDs
	seq       int64
	lastPrice decimal.Decimal
}

func New(eng *engine.Engine, feed PriceSource, cfg Config) *Maker {
	return &Maker{eng: eng, feed: feed, cfg: cfg}
}

// Run re-quotes on a fixed interval until ctx is cancelled.
func (m *Maker) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.Requote)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.requote()
		case <-ctx.Done():
			m.cancelAll()
			return
		}
	}
}

// requote cancels the previous ladder and places a fresh one around the
// current index price. No-op if the feed has no price yet, or if the price
// hasn't moved since the last quote.
func (m *Maker) requote() {
	price, ok := m.feed.Latest()
	if !ok {
		return
	}
	if len(m.live) > 0 && price.Equal(m.lastPrice) {
		return
	}
	m.cancelAll()

	bps := decimal.New(1, -4) // 0.0001
	for i := 0; i < m.cfg.Levels; i++ {
		offset := decimal.NewFromInt(m.cfg.SpreadBps + int64(i)*m.cfg.StepBps).Mul(bps)
		bidPrice := price.Mul(decimal.NewFromInt(1).Sub(offset)).Round(2)
		askPrice := price.Mul(decimal.NewFromInt(1).Add(offset)).Round(2)
		m.place(engine.Buy, bidPrice)
		m.place(engine.Sell, askPrice)
	}
	m.lastPrice = price
}

func (m *Maker) place(side engine.Side, price decimal.Decimal) {
	m.seq++
	id := fmt.Sprintf("mm-%d", m.seq)
	o := engine.NewOrder(id, "BTC-USD", side, engine.Limit, price, m.cfg.QtyPerLevel)
	if err := m.eng.Submit(o); err != nil {
		return // retried implicitly on the next requote
	}
	// Track only orders that are actually resting in the book.
	if o.Status == engine.StatusOpen || o.Status == engine.StatusPartial {
		m.live = append(m.live, id)
	}
}

func (m *Maker) cancelAll() {
	for _, id := range m.live {
		// Errors mean the order already filled — nothing to do.
		_ = m.eng.Cancel(id)
	}
	m.live = m.live[:0]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./marketmaker/ -v`
Expected: PASS (3 tests)

Note: `PriceLevelSnapshot.Price` comes from `decimal.String()`, which trims trailing zeros (`99950` not `99950.00`). If the assertions fail on formatting, compare with `decimal.RequireFromString(snap.Bids[0].Price).Equal(...)` instead — fix the test, not the implementation.

- [ ] **Step 5: Commit**

```bash
git add marketmaker/
git commit -m "feat: add market maker quoting a ladder around the index price"
```

---

### Task 3: Balance reservations in `httpapi`

Fixes the overdraft gap: funds are now held when an order is placed and released on fill/cancel/reject. Market buys reserve `qty × bestAsk × 1.05` and are rejected when the book has no asks.

**Files:**
- Modify: `api/httpapi/server.go`
- Test: `api/httpapi/reservations_test.go` (new)

- [ ] **Step 1: Write the failing tests**

These test the reservation accounting directly (same package), calling `applyEventToRecords` synchronously to avoid racing the `fanOut` goroutine.

```go
// api/httpapi/reservations_test.go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./api/httpapi/ -v`
Expected: FAIL — `s.reserve undefined`, `bal.ReservedUSD undefined`

- [ ] **Step 3: Implement reservations**

In `api/httpapi/server.go`:

**3a.** Extend `balance` and add a `reservation` type:

```go
// balance holds a session's funds. All mutations happen under balanceMu.
type balance struct {
	USD decimal.Decimal
	BTC decimal.Decimal
	// Funds held by open orders. Available = total − reserved.
	ReservedUSD decimal.Decimal
	ReservedBTC decimal.Decimal
}

// reservation tracks the hold one open order has on its session's balance.
type reservation struct {
	sessID     string
	side       engine.Side
	perUnitUSD decimal.Decimal // USD held per unit qty (buy orders only)
	qtyLeft    decimal.Decimal // unfilled quantity still held
}
```

**3b.** Add the map to `Server` (next to `balances`) and initialise it in `NewServer`:

```go
	balanceMu sync.RWMutex
	balances     map[string]*balance     // sessID  → balance
	reservations map[string]*reservation // orderID → hold
```

```go
		balances:     make(map[string]*balance),
		reservations: make(map[string]*reservation),
```

**3c.** Add the three accounting methods (all assume caller holds `balanceMu`, except the event hooks which take it themselves):

```go
// reserve places a hold for a new open order. Caller must hold balanceMu.
func (s *Server) reserve(sessID, orderID string, side engine.Side, perUnitUSD, qty decimal.Decimal) {
	bal := s.ensureBalance(sessID)
	s.reservations[orderID] = &reservation{
		sessID: sessID, side: side, perUnitUSD: perUnitUSD, qtyLeft: qty,
	}
	if side == engine.Buy {
		bal.ReservedUSD = bal.ReservedUSD.Add(perUnitUSD.Mul(qty))
	} else {
		bal.ReservedBTC = bal.ReservedBTC.Add(qty)
	}
}

// releaseQty releases the hold on a filled portion of an order.
func (s *Server) releaseQty(orderID string, qty decimal.Decimal) {
	s.balanceMu.Lock()
	defer s.balanceMu.Unlock()
	r, ok := s.reservations[orderID]
	if !ok {
		return
	}
	if qty.GreaterThan(r.qtyLeft) {
		qty = r.qtyLeft
	}
	bal := s.ensureBalance(r.sessID)
	if r.side == engine.Buy {
		bal.ReservedUSD = bal.ReservedUSD.Sub(r.perUnitUSD.Mul(qty))
	} else {
		bal.ReservedBTC = bal.ReservedBTC.Sub(qty)
	}
	r.qtyLeft = r.qtyLeft.Sub(qty)
	if r.qtyLeft.LessThanOrEqual(decimal.Zero) {
		delete(s.reservations, orderID)
	}
}

// releaseAll drops an order's remaining hold (cancel/reject).
func (s *Server) releaseAll(orderID string) {
	s.balanceMu.Lock()
	defer s.balanceMu.Unlock()
	r, ok := s.reservations[orderID]
	if !ok {
		return
	}
	bal := s.ensureBalance(r.sessID)
	if r.side == engine.Buy {
		bal.ReservedUSD = bal.ReservedUSD.Sub(r.perUnitUSD.Mul(r.qtyLeft))
	} else {
		bal.ReservedBTC = bal.ReservedBTC.Sub(r.qtyLeft)
	}
	delete(s.reservations, orderID)
}
```

**3d.** Hook releases into `applyEventToRecords`. After the existing status-update block, before the trade block, add:

```go
	// Release holds: cancels/rejects free the remainder.
	if ev.OrderID != "" && (ev.Type == engine.EvOrderCancelled || ev.Type == engine.EvOrderRejected) {
		s.releaseAll(ev.OrderID)
	}
```

And inside `applyTradeToBalances`, at the top (before taking `balanceMu`):

```go
	s.releaseQty(ev.MakerID, ev.Qty)
	s.releaseQty(ev.TakerID, ev.Qty)
```

**3e.** Rewrite the balance-check section of `handleSubmit` (the block between `sess := sessionFrom(r)` and `o := engine.NewOrder(...)`) to check available funds and reserve:

```go
	// Check available balance (total minus holds from open orders), then
	// place a hold for this order.
	sess := sessionFrom(r)

	// Market buys have no limit price: hold bestAsk × 1.05 per unit.
	perUnitUSD := price
	if side == engine.Buy && otype == engine.Market {
		snap := s.eng.Snapshot()
		if len(snap.Asks) == 0 {
			writeJSON(w, submitResponse{
				OrderID: req.ID,
				Error:   "no liquidity — the book has no sell orders to buy from",
			}, http.StatusOK)
			return
		}
		bestAsk, err := decimal.NewFromString(snap.Asks[0].Price)
		if err != nil {
			writeError(w, "internal error reading book", http.StatusInternalServerError)
			return
		}
		perUnitUSD = bestAsk.Mul(decimal.RequireFromString("1.05"))
	}

	s.balanceMu.Lock()
	bal := s.ensureBalance(sess)
	if side == engine.Buy {
		required := qty.Mul(perUnitUSD)
		available := bal.USD.Sub(bal.ReservedUSD)
		if available.LessThan(required) {
			s.balanceMu.Unlock()
			writeJSON(w, submitResponse{
				OrderID: req.ID,
				Error:   fmt.Sprintf("insufficient USD balance — need $%s, have $%s available", required.StringFixed(2), available.StringFixed(2)),
			}, http.StatusOK)
			return
		}
	}
	if side == engine.Sell {
		available := bal.BTC.Sub(bal.ReservedBTC)
		if available.LessThan(qty) {
			s.balanceMu.Unlock()
			writeJSON(w, submitResponse{
				OrderID: req.ID,
				Error:   fmt.Sprintf("insufficient BTC balance — need %s BTC, have %s BTC available", qty.StringFixed(4), available.StringFixed(4)),
			}, http.StatusOK)
			return
		}
	}
	s.reserve(sess, req.ID, side, perUnitUSD, qty)
	s.balanceMu.Unlock()
```

**3f.** In `handleSubmit`'s engine-error path (`if err := s.eng.Submit(o); err != nil`), release the hold:

```go
	if err := s.eng.Submit(o); err != nil {
		s.releaseAll(o.ID)
		s.orderMu.Lock()
		rec.Status = "REJECTED"
		s.orderMu.Unlock()
		writeJSON(w, submitResponse{OrderID: o.ID, Error: err.Error()}, http.StatusOK)
		return
	}
```

(Market orders that can't fully fill emit `EvOrderCancelled` from the engine, which releases the remainder via 3d — no extra handling needed.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./api/httpapi/ -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Run the full suite**

Run: `go test ./...`
Expected: PASS everywhere (engine tests unaffected)

- [ ] **Step 6: Commit**

```bash
git add api/httpapi/
git commit -m "fix: reserve funds for open orders to prevent balance overdraft"
```

---

### Task 4: `INDEX_PRICE` SSE broadcast in `httpapi`

**Files:**
- Modify: `api/httpapi/server.go`
- Test: `api/httpapi/reservations_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `api/httpapi/reservations_test.go`:

```go
func TestBroadcastIndexPriceReachesSubscribers(t *testing.T) {
	s := newTestServer()
	ch := s.subscribe()
	defer s.unsubscribe(ch)

	s.BroadcastIndexPrice(d("104231.50"))

	select {
	case ev := <-ch:
		if ev.Type != "INDEX_PRICE" {
			t.Fatalf("type = %s, want INDEX_PRICE", ev.Type)
		}
		if !ev.Price.Equal(d("104231.50")) {
			t.Fatalf("price = %s", ev.Price)
		}
	default:
		t.Fatal("no event delivered to subscriber")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/httpapi/ -run TestBroadcastIndexPrice -v`
Expected: FAIL — `s.BroadcastIndexPrice undefined`

- [ ] **Step 3: Implement broadcast**

In `api/httpapi/server.go`, refactor the subscriber loop out of `fanOut` and add the public method:

```go
func (s *Server) fanOut() {
	for ev := range s.events {
		s.applyEventToRecords(ev)
		s.broadcast(ev)
	}
}

func (s *Server) broadcast(ev engine.Event) {
	s.mu.RLock()
	for _, sub := range s.subs {
		select {
		case sub <- ev:
		default:
		}
	}
	s.mu.RUnlock()
}

// BroadcastIndexPrice pushes the external index price to all SSE clients.
func (s *Server) BroadcastIndexPrice(p decimal.Decimal) {
	s.broadcast(engine.Event{
		Type:      "INDEX_PRICE",
		Symbol:    "BTC-USD",
		Price:     p,
		Timestamp: time.Now(),
	})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./api/httpapi/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/httpapi/
git commit -m "feat: broadcast INDEX_PRICE events over SSE"
```

---

### Task 5: Wire everything in `cmd/server`

**Files:**
- Modify: `cmd/server/main.go`

- [ ] **Step 1: Update main**

Replace `cmd/server/main.go` with:

```go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	httpapi "github.com/Ayyasythz/matching-engine/api/httpapi"
	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/Ayyasythz/matching-engine/marketmaker"
	"github.com/Ayyasythz/matching-engine/pricefeed"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP server address")
	frontend := flag.String("frontend", "./frontend", "path to frontend static files")
	mode := flag.String("mode", "fifo", "matching algorithm: fifo | prorata")
	anchor := flag.Bool("anchor", true, "anchor the book to the live BTC price with a market-maker bot")
	feedInterval := flag.Duration("feed-interval", 3*time.Second, "live price poll interval")
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

		// Forward index-price changes to SSE clients.
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
```

- [ ] **Step 2: Build and run smoke test**

Run: `go build ./... && go vet ./...`
Expected: clean build.

Run: `go run ./cmd/server` (let it run ~10 seconds, then Ctrl-C). In another shell: `curl -s localhost:8080/api/book`
Expected: log line `price anchor enabled...`; the book JSON shows 5 bids and 5 asks near the real BTC price.

- [ ] **Step 3: Commit**

```bash
git add cmd/server/
git commit -m "feat: wire price feed and market maker into server with -anchor flag"
```

---

### Task 6: Frontend — show live BTC price

**Files:**
- Modify: `frontend/index.html`

- [ ] **Step 1: Add the header element**

In the `<header>` (around line 889), after `<span id="h-change"></span>`, add:

```html
  <div class="hdiv"></div>
  <div class="symbol-wrap">
    <div class="symbol-name" id="h-index" style="font-variant-numeric:tabular-nums">—</div>
    <div class="symbol-full">Live BTC · Coinbase</div>
  </div>
```

- [ ] **Step 2: Handle the event in JS**

In `onEvent(ev)` (around line 1625), add as the **first** statement — index-price events must not fall through to the feed/seq logic:

```js
  if (ev.type === 'INDEX_PRICE') {
    const el = document.getElementById('h-index');
    el.textContent = '$' + fmt(parseFloat(ev.price));
    el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');
    return;
  }
```

- [ ] **Step 3: Manual verification**

Run: `go run ./cmd/server`, open `http://localhost:8080`.
Expected: header shows “Live BTC” price updating every few seconds; the order book shows the bot's ladder around that price; placing a market buy fills instantly near the live price; balances update; placing two limit buys that together exceed your USD gets the second rejected with “insufficient USD balance … available”.

- [ ] **Step 4: Commit**

```bash
git add frontend/index.html
git commit -m "feat: show live BTC index price in the header"
```

---

### Task 7: Final verification

- [ ] **Step 1: Full test suite**

Run: `go test ./... && go vet ./...`
Expected: all packages PASS, vet clean.

- [ ] **Step 2: Check off spec requirements**

- pricefeed polls Coinbase, keeps last price on error ✓ (Task 1)
- market maker: 5-level ladder, 5bps spread, 2bps step, cancel-and-replace ✓ (Task 2)
- INDEX_PRICE SSE event ✓ (Task 4)
- balance reservations incl. market-buy buffer and no-asks rejection ✓ (Task 3)
- `-anchor` / `-feed-interval` flags ✓ (Task 5)
- frontend live price ✓ (Task 6)
