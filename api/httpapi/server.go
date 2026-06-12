package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/Ayyasythz/matching-engine/engine"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type contextKey string

const ctxSession contextKey = "session"

var (
	startUSD = decimal.RequireFromString("100000.00")
	startBTC = decimal.RequireFromString("2.0000")
)

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

type orderRecord struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	Side      string    `json:"side"`
	Type      string    `json:"type"`
	Price     string    `json:"price"`
	Qty       string    `json:"qty"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type Server struct {
	eng    *engine.Engine
	events <-chan engine.Event

	// SSE fan-out
	mu   sync.RWMutex
	subs []chan engine.Event

	// Order & session tracking
	orderMu        sync.RWMutex
	orders         map[string]*orderRecord // orderID  → record
	sessions       map[string][]string     // sessID   → []orderID
	sessionByOrder map[string]string       // orderID  → sessID

	// Per-session balances
	balanceMu    sync.RWMutex
	balances     map[string]*balance     // sessID  → balance
	reservations map[string]*reservation // orderID → hold

	// Last external index price, replayed to new SSE connections.
	indexMu    sync.RWMutex
	indexPrice decimal.Decimal
	indexOK    bool

	activeConns int64 // atomic
}

func NewServer(eng *engine.Engine, events <-chan engine.Event) *Server {
	s := &Server{
		eng:            eng,
		events:         events,
		orders:         make(map[string]*orderRecord),
		sessions:       make(map[string][]string),
		sessionByOrder: make(map[string]string),
		balances:       make(map[string]*balance),
		reservations:   make(map[string]*reservation),
	}
	go s.fanOut()
	return s
}

// ── Fan-out ───────────────────────────────────────────────────────────────────

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
	s.indexMu.Lock()
	s.indexPrice = p
	s.indexOK = true
	s.indexMu.Unlock()
	s.broadcast(indexPriceEvent(p))
}

func indexPriceEvent(p decimal.Decimal) engine.Event {
	return engine.Event{
		Type:      "INDEX_PRICE",
		Symbol:    "BTC-USD",
		Price:     p,
		Timestamp: time.Now(),
	}
}

func (s *Server) applyEventToRecords(ev engine.Event) {
	// Update order status
	if ev.OrderID != "" {
		var status string
		switch ev.Type {
		case engine.EvOrderFilled:
			status = "FILLED"
		case engine.EvOrderPartiallyFilled:
			status = "PARTIAL"
		case engine.EvOrderCancelled:
			status = "CANCELLED"
		case engine.EvOrderRejected:
			status = "REJECTED"
		}
		if status != "" {
			s.orderMu.Lock()
			if rec, ok := s.orders[ev.OrderID]; ok {
				rec.Status = status
			}
			s.orderMu.Unlock()
		}
	}

	// Release holds on terminal events.
	// Cancels/rejects free the full remainder. Fills are drained incrementally
	// via trade events (releaseQty), but releaseAll on EvOrderFilled is a
	// defensive no-op that covers any residual if trade accounting ever drifts.
	if ev.OrderID != "" &&
		(ev.Type == engine.EvOrderCancelled ||
			ev.Type == engine.EvOrderRejected ||
			ev.Type == engine.EvOrderFilled) {
		s.releaseAll(ev.OrderID)
	}

	// Update balances on every trade
	if ev.Type == engine.EvTrade {
		s.applyTradeToBalances(ev)
	}
}

// applyTradeToBalances debits and credits both sides of a trade.
func (s *Server) applyTradeToBalances(ev engine.Event) {
	s.releaseQty(ev.MakerID, ev.Qty)
	s.releaseQty(ev.TakerID, ev.Qty)

	s.orderMu.RLock()
	makerRec := s.orders[ev.MakerID]
	takerRec := s.orders[ev.TakerID]
	makerSess := s.sessionByOrder[ev.MakerID]
	takerSess := s.sessionByOrder[ev.TakerID]
	s.orderMu.RUnlock()

	cost := ev.Qty.Mul(ev.Price)

	s.balanceMu.Lock()
	if makerRec != nil && makerSess != "" {
		bal := s.ensureBalance(makerSess)
		if makerRec.Side == "buy" {
			bal.USD = bal.USD.Sub(cost)
			bal.BTC = bal.BTC.Add(ev.Qty)
		} else {
			bal.USD = bal.USD.Add(cost)
			bal.BTC = bal.BTC.Sub(ev.Qty)
		}
	}
	if takerRec != nil && takerSess != "" {
		bal := s.ensureBalance(takerSess)
		if takerRec.Side == "buy" {
			bal.USD = bal.USD.Sub(cost)
			bal.BTC = bal.BTC.Add(ev.Qty)
		} else {
			bal.USD = bal.USD.Add(cost)
			bal.BTC = bal.BTC.Sub(ev.Qty)
		}
	}
	s.balanceMu.Unlock()
}

// reserve places a hold for a new open order. Caller must hold balanceMu write lock.
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

// releaseAll drops an order's remaining hold (cancel/reject/fill).
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

// ensureBalance returns the session's balance, initialising it if new.
// Caller must hold balanceMu write lock.
func (s *Server) ensureBalance(sessID string) *balance {
	if bal, ok := s.balances[sessID]; ok {
		return bal
	}
	bal := &balance{USD: startUSD, BTC: startBTC}
	s.balances[sessID] = bal
	return bal
}

// ── SSE ───────────────────────────────────────────────────────────────────────

func (s *Server) subscribe() chan engine.Event {
	ch := make(chan engine.Event, 256)
	s.mu.Lock()
	s.subs = append(s.subs, ch)
	s.mu.Unlock()
	return ch
}

func (s *Server) unsubscribe(ch chan engine.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sub := range s.subs {
		if sub == ch {
			s.subs = append(s.subs[:i], s.subs[i+1:]...)
			return
		}
	}
}

// ── Routing ───────────────────────────────────────────────────────────────────

func (s *Server) Handler(frontendDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/orders", s.handleSubmit)
	mux.HandleFunc("/api/orders/", s.handleCancel)
	mux.HandleFunc("/api/book", s.handleBook)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/me/orders", s.handleMyOrders)
	mux.HandleFunc("/api/me/balance", s.handleBalance)
	mux.HandleFunc("/api/presence", s.handlePresence)
	mux.Handle("/", http.FileServer(http.Dir(frontendDir)))
	return corsMiddleware(s.sessionMiddleware(mux))
}

// ── Session middleware ─────────────────────────────────────────────────────────

func (s *Server) sessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := ensureSession(w, r)
		ctx := context.WithValue(r.Context(), ctxSession, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ensureSession(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie("session_id"); err == nil && c.Value != "" {
		return c.Value
	}
	id := uuid.New().String()
	// Set Secure when the connection is HTTPS (direct TLS or behind a proxy).
	secure := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
	http.SetCookie(w, &http.Cookie{
		Name:     "session_id",
		Value:    id,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		MaxAge:   86400 * 30,
		SameSite: http.SameSiteLaxMode,
	})
	return id
}

func sessionFrom(r *http.Request) string {
	id, _ := r.Context().Value(ctxSession).(string)
	return id
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── Handlers ──────────────────────────────────────────────────────────────────

type submitRequest struct {
	ID       string `json:"id"`
	Symbol   string `json:"symbol"`
	Side     string `json:"side"`
	Type     string `json:"type"`
	Price    string `json:"price"`
	Qty      string `json:"qty"`
	Username string `json:"username"`
}

type submitResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

// sanitizeUsername trims whitespace, enforces a max length, and strips
// characters that are unsafe in HTML contexts to prevent stored XSS.
func sanitizeUsername(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "anonymous"
	}
	// Remove HTML-dangerous runes.
	s = strings.Map(func(r rune) rune {
		if r == '<' || r == '>' || r == '"' || r == '\'' || r == '&' || !unicode.IsPrint(r) {
			return -1
		}
		return r
	}, s)
	if len(s) > 32 {
		s = s[:32]
	}
	if s == "" {
		return "anonymous"
	}
	return s
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Symbol == "" {
		req.Symbol = "BTC-USD"
	}
	if req.ID == "" {
		req.ID = uuid.New().String()
	}

	side := engine.Side(strings.ToLower(req.Side))
	if side != engine.Buy && side != engine.Sell {
		writeError(w, "side must be 'buy' or 'sell'", http.StatusBadRequest)
		return
	}

	otype := engine.OrderType(strings.ToUpper(req.Type))
	if otype != engine.Limit && otype != engine.Market && otype != engine.IOC && otype != engine.FOK {
		writeError(w, "type must be LIMIT, MARKET, IOC, or FOK", http.StatusBadRequest)
		return
	}

	qty, err := decimal.NewFromString(req.Qty)
	if err != nil || qty.LessThanOrEqual(decimal.Zero) {
		writeError(w, "qty must be a positive number", http.StatusBadRequest)
		return
	}

	var price decimal.Decimal
	if otype != engine.Market {
		price, err = decimal.NewFromString(req.Price)
		if err != nil || price.LessThanOrEqual(decimal.Zero) {
			writeError(w, "price must be a positive number", http.StatusBadRequest)
			return
		}
	}

	// Check available balance (total minus holds from open orders), then
	// place a hold for this order.
	sess := sessionFrom(r)

	// Market buys have no limit price: hold bestAsk × 1.05 per unit.
	// The snapshot is taken before the lock; the check+reserve inside the lock
	// serialises concurrent requests correctly (second request sees the first's
	// reservation when it re-reads the available balance).
	perUnitUSD := price
	if side == engine.Buy && otype == engine.Market {
		snap, snapErr := s.eng.Snapshot()
		if snapErr != nil {
			writeError(w, "engine unavailable", http.StatusServiceUnavailable)
			return
		}
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

	o := engine.NewOrder(req.ID, req.Symbol, side, otype, price, qty)

	rec := &orderRecord{
		ID:        o.ID,
		Username:  sanitizeUsername(req.Username),
		Side:      string(side),
		Type:      string(otype),
		Price:     price.String(),
		Qty:       qty.String(),
		Status:    "OPEN",
		CreatedAt: time.Now(),
	}

	s.orderMu.Lock()
	s.orders[o.ID] = rec
	s.sessions[sess] = append(s.sessions[sess], o.ID)
	s.sessionByOrder[o.ID] = sess
	s.orderMu.Unlock()

	if err := s.eng.Submit(o); err != nil {
		s.releaseAll(o.ID)
		s.orderMu.Lock()
		rec.Status = "REJECTED"
		s.orderMu.Unlock()
		writeJSON(w, submitResponse{OrderID: o.ID, Error: err.Error()}, http.StatusOK)
		return
	}
	writeJSON(w, submitResponse{OrderID: o.ID, Status: string(o.Status)}, http.StatusOK)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/orders/")
	if id == "" {
		writeError(w, "order id required", http.StatusBadRequest)
		return
	}

	// Verify that the order belongs to the requesting session.
	sess := sessionFrom(r)
	s.orderMu.RLock()
	owner := s.sessionByOrder[id]
	s.orderMu.RUnlock()

	if owner == "" || owner != sess {
		writeError(w, "order not found", http.StatusNotFound)
		return
	}

	if err := s.eng.Cancel(id); err != nil {
		writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()}, http.StatusOK)
		return
	}
	writeJSON(w, map[string]interface{}{"success": true}, http.StatusOK)
}

func (s *Server) handleBook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap, err := s.eng.Snapshot()
	if err != nil {
		writeError(w, "engine unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, snap, http.StatusOK)
}

func (s *Server) handleMyOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sess := sessionFrom(r)
	s.orderMu.RLock()
	ids := s.sessions[sess]
	result := make([]orderRecord, 0, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		if rec, ok := s.orders[ids[i]]; ok {
			result = append(result, *rec)
		}
	}
	s.orderMu.RUnlock()

	writeJSON(w, result, http.StatusOK)
}

type balanceResponse struct {
	USD      string `json:"usd"`
	BTC      string `json:"btc"`
	StartUSD string `json:"start_usd"`
	StartBTC string `json:"start_btc"`
}

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sess := sessionFrom(r)

	// Fast path: read lock only, copy values before releasing.
	s.balanceMu.RLock()
	bal, exists := s.balances[sess]
	var usd, btc decimal.Decimal
	if exists {
		usd = bal.USD
		btc = bal.BTC
	}
	s.balanceMu.RUnlock()

	if !exists {
		// Slow path: initialise balance under write lock.
		s.balanceMu.Lock()
		bal = s.ensureBalance(sess)
		usd = bal.USD
		btc = bal.BTC
		s.balanceMu.Unlock()
	}

	writeJSON(w, balanceResponse{
		USD:      usd.StringFixed(2),
		BTC:      btc.StringFixed(4),
		StartUSD: startUSD.StringFixed(2),
		StartBTC: startBTC.StringFixed(4),
	}, http.StatusOK)
}

func (s *Server) handlePresence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]int64{
		"active_users": atomic.LoadInt64(&s.activeConns),
	}, http.StatusOK)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	atomic.AddInt64(&s.activeConns, 1)
	defer atomic.AddInt64(&s.activeConns, -1)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.subscribe()
	defer s.unsubscribe(ch)

	// Replay the last index price so new clients see it immediately.
	s.indexMu.RLock()
	if s.indexOK {
		if data, err := json.Marshal(indexPriceEvent(s.indexPrice)); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
	s.indexMu.RUnlock()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ev := <-ch:
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, map[string]string{"error": msg}, code)
}
