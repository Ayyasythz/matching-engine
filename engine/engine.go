package engine

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/shopspring/decimal"
)

// ErrEngineStopped is returned by Submit/Cancel/Snapshot when the engine is no
// longer running.
var ErrEngineStopped = errors.New("engine is stopped")

type Engine struct {
	symbol   string
	bids     *halfBook
	asks     *halfBook
	orderIdx map[string]*Order
	ring     *RingBuffer
	events   chan<- Event
	matcher  Matcher
	pool     *AMMPool // non-nil in AMM mode
	seq      uint64
	stopCh   chan struct{} // closed by Run() when it exits
}

type cmdType int

const (
	cmdSubmit cmdType = iota
	cmdCancel
	cmdStop
	cmdSnapshot
)

type command struct {
	ctype    cmdType
	order    *Order
	cancelID string
	errCh    chan error
	snapCh   chan BookSnapshot
}

func NewEngine(symbol string, events chan<- Event) *Engine {
	return newEngine(symbol, events, FIFOMatcher{})
}

func NewProRataEngine(symbol string, events chan<- Event) *Engine {
	return newEngine(symbol, events, ProRataMatcher{})
}

// NewAMMEngine creates an engine where orders execute against a constant-
// product liquidity pool instead of a counterparty order book. Limit orders
// that cannot fill within their price rest in the book and execute
// automatically once the pool price crosses their limit.
func NewAMMEngine(symbol string, events chan<- Event, pool *AMMPool) *Engine {
	e := newEngine(symbol, events, FIFOMatcher{})
	e.pool = pool
	return e
}

func newEngine(symbol string, events chan<- Event, matcher Matcher) *Engine {
	return &Engine{
		symbol:   symbol,
		bids:     newHalfBook(Buy),
		asks:     newHalfBook(Sell),
		orderIdx: make(map[string]*Order),
		ring:     NewRingBuffer(8192),
		events:   events,
		matcher:  matcher,
		stopCh:   make(chan struct{}),
	}
}

// Run processes engine commands. It must be called in its own goroutine and
// runs until a cmdStop command is received.
func (e *Engine) Run() {
	defer close(e.stopCh)
	for {
		seq, cmd, ok := e.ring.TryNext()
		if !ok {
			runtime.Gosched()
			continue
		}
		e.ring.Advance(seq)

		switch cmd.ctype {
		case cmdSubmit:
			e.processOrder(cmd.order)
			cmd.errCh <- nil
		case cmdCancel:
			cmd.errCh <- e.processCancel(cmd.cancelID)
		case cmdStop:
			cmd.errCh <- nil
			return
		case cmdSnapshot:
			cmd.snapCh <- e.snapshot()
		}
	}
}

func (e *Engine) Submit(o *Order) error {
	ch := make(chan error, 1)
	seq := e.ring.Claim()
	e.ring.Write(seq, command{ctype: cmdSubmit, order: o, errCh: ch})
	e.ring.Publish(seq)
	select {
	case err := <-ch:
		return err
	case <-e.stopCh:
		return ErrEngineStopped
	}
}

func (e *Engine) Cancel(id string) error {
	ch := make(chan error, 1)
	seq := e.ring.Claim()
	e.ring.Write(seq, command{ctype: cmdCancel, cancelID: id, errCh: ch})
	e.ring.Publish(seq)
	select {
	case err := <-ch:
		return err
	case <-e.stopCh:
		return ErrEngineStopped
	}
}

func (e *Engine) Stop() {
	ch := make(chan error, 1)
	seq := e.ring.Claim()
	e.ring.Write(seq, command{ctype: cmdStop, errCh: ch})
	e.ring.Publish(seq)
	<-ch
	// stopCh is closed by Run() via defer; wait for it to confirm exit.
	<-e.stopCh
}

func (e *Engine) Snapshot() (BookSnapshot, error) {
	ch := make(chan BookSnapshot, 1)
	seq := e.ring.Claim()
	e.ring.Write(seq, command{ctype: cmdSnapshot, snapCh: ch})
	e.ring.Publish(seq)
	select {
	case snap := <-ch:
		return snap, nil
	case <-e.stopCh:
		return BookSnapshot{}, ErrEngineStopped
	}
}

func (e *Engine) snapshot() BookSnapshot {
	if e.pool != nil {
		return e.snapshotAMM()
	}
	return BookSnapshot{
		Symbol: e.symbol,
		Bids:   e.bids.snapshot(),
		Asks:   e.asks.snapshot(),
	}
}

// emit sends an event to the events channel without blocking the engine
// goroutine. Events are dropped if the consumer is too slow; callers should
// use a generously buffered channel.
func (e *Engine) emit(ev Event) {
	e.seq++
	ev.Seq = e.seq
	ev.Timestamp = time.Now()
	if e.events != nil {
		select {
		case e.events <- ev:
		default:
		}
	}
}

func (e *Engine) processOrder(o *Order) {
	if e.pool != nil {
		e.processOrderAMM(o)
		return
	}

	// Engine-level validation: non-market orders must have a positive price.
	if o.Type != Market && o.Price.LessThanOrEqual(decimal.Zero) {
		o.Status = StatusRejected
		e.emit(Event{
			Type:    EvOrderRejected,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.Quantity,
			Reason:  fmt.Sprintf("invalid price %s — %s orders require a price greater than zero", o.Price, o.Type),
		})
		return
	}

	e.orderIdx[o.ID] = o
	e.emit(Event{
		Type:    EvOrderAccepted,
		OrderID: o.ID,
		Symbol:  e.symbol,
		Side:    o.Side,
		Qty:     o.Quantity,
		Price:   o.Price,
	})

	switch o.Type {
	case Market:
		e.matchMarket(o)
	case Limit:
		e.matchLimit(o)
	case IOC:
		e.matchIOC(o)
	case FOK:
		e.matchFOK(o)
	}

	// Remove non-resting orders from the index. Resting orders (open or
	// partially filled limit orders) stay so Cancel() can look them up.
	if o.Status != StatusOpen && o.Status != StatusPartial {
		delete(e.orderIdx, o.ID)
	}
}

func (e *Engine) matchMarket(o *Order) {
	e.sweep(o, e.opposing(o.Side), decimal.Zero)
	if !o.isFilled() {
		o.Status = StatusCancelled
		e.emit(Event{
			Type:    EvOrderCancelled,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.RemainingQuantity,
		})
	}
}

func (e *Engine) matchLimit(o *Order) {
	e.sweep(o, e.opposing(o.Side), o.Price)
	if !o.isFilled() {
		e.resting(o.Side).addOrder(o)
	}
}

func (e *Engine) matchIOC(o *Order) {
	e.sweep(o, e.opposing(o.Side), o.Price)
	if !o.isFilled() {
		o.Status = StatusCancelled
		e.emit(Event{
			Type:    EvOrderCancelled,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.RemainingQuantity,
		})
	}
}

func (e *Engine) matchFOK(o *Order) {
	opposing := e.opposing(o.Side)
	available := opposing.totalQtyAtOrBetterThan(o.Price, o.Side)
	// Use RemainingQuantity (not Quantity) so a pre-partially-filled order is
	// checked against what it still needs, not its original size.
	if available.LessThan(o.RemainingQuantity) {
		o.Status = StatusRejected
		e.emit(Event{
			Type:    EvOrderRejected,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.RemainingQuantity,
			Reason: fmt.Sprintf("insufficient liquidity for FOK order — need %s, only %s available at %s or better",
				o.RemainingQuantity, available, o.Price),
		})
		delete(e.orderIdx, o.ID)
		return
	}
	e.sweep(o, opposing, o.Price)
}

func (e *Engine) sweep(aggressor *Order, book *halfBook, limitPrice decimal.Decimal) {
	for !aggressor.isFilled() {
		level := book.bestLevel()
		if level == nil {
			break
		}
		if !limitPrice.IsZero() {
			if aggressor.Side == Buy && level.Price.GreaterThan(limitPrice) {
				break
			}
			if aggressor.Side == Sell && level.Price.LessThan(limitPrice) {
				break
			}
		}
		execPrice := level.Price

		fills := e.matcher.Distribute(level, aggressor.RemainingQuantity)
		if len(fills) == 0 {
			break
		}

		totalFilled := decimal.Zero
		for _, f := range fills {
			totalFilled = totalFilled.Add(f.FillQty)

			e.emit(Event{
				Type: EvTrade, Symbol: aggressor.Symbol,
				Price: execPrice, Qty: f.FillQty,
				MakerID: f.Order.ID, TakerID: aggressor.ID,
			})

			makerEv := EvOrderPartiallyFilled
			if f.Order.isFilled() {
				makerEv = EvOrderFilled
				delete(e.orderIdx, f.Order.ID)
			}
			e.emit(Event{Type: makerEv, OrderID: f.Order.ID,
				Symbol: f.Order.Symbol, Price: execPrice, Qty: f.FillQty})
		}

		aggressor.fill(totalFilled)
		aggressorEv := EvOrderPartiallyFilled
		if aggressor.isFilled() {
			aggressorEv = EvOrderFilled
		}
		e.emit(Event{Type: aggressorEv, OrderID: aggressor.ID,
			Symbol: aggressor.Symbol, Price: execPrice, Qty: totalFilled})

		book.pruneLevel(execPrice)
	}
}

func (e *Engine) processCancel(id string) error {
	o, exists := e.orderIdx[id]
	if !exists {
		return fmt.Errorf("order %s not found or not resting", id)
	}
	_, ok := e.resting(o.Side).removeOrderByID(id, o.Price)
	if !ok {
		return fmt.Errorf("order %s not resting", id)
	}
	o.Status = StatusCancelled
	delete(e.orderIdx, id)
	e.emit(Event{
		Type:    EvOrderCancelled,
		OrderID: id,
		Symbol:  o.Symbol,
		Qty:     o.RemainingQuantity,
	})
	return nil
}

func (e *Engine) opposing(side Side) *halfBook {
	if side == Buy {
		return e.asks
	}
	return e.bids
}

func (e *Engine) resting(side Side) *halfBook {
	if side == Buy {
		return e.bids
	}
	return e.asks
}
