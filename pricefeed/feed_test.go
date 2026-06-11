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
