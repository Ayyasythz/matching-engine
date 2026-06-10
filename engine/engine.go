package engine

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Engine struct {
	symbol   string
	bids     *halfBook
	asks     *halfBook
	orderIdx map[string]*Order

	in     chan command
	events chan<- Event

	seq uint64
}

type cmdType int

const (
	cmdSubmit cmdType = iota
	cmdCancel
	cmdStop
)

type command struct {
	ctype    cmdType
	order    *Order
	cancelID string
	errCh    chan error
}

func NewEngine(symbol string, events chan<- Event) *Engine {
	return &Engine{
		symbol:   symbol,
		bids:     newHalfBook(Buy),
		asks:     newHalfBook(Sell),
		orderIdx: make(map[string]*Order),
		in:       make(chan command, 8192),
		events:   events,
	}
}

func (e *Engine) Run() {
	for cmd := range e.in {
		switch cmd.ctype {
		case cmdSubmit:
			e.processOrder(cmd.order)
			cmd.errCh <- nil
		case cmdCancel:
			cmd.errCh <- e.processCancel(cmd.cancelID)
		case cmdStop:
			cmd.errCh <- nil
			return
		}
	}
}

func (e *Engine) Submit(o *Order) error {
	ch := make(chan error, 1)
	e.in <- command{
		ctype: cmdSubmit,
		order: o,
		errCh: ch,
	}
	return <-ch
}

func (e *Engine) Cancel(id string) error {
	ch := make(chan error, 1)
	e.in <- command{
		ctype:    cmdCancel,
		cancelID: id,
		errCh:    ch,
	}
	return <-ch
}

func (e *Engine) Stop() error {
	ch := make(chan error, 1)
	e.in <- command{
		ctype: cmdStop,
		errCh: ch,
	}
	return <-ch
}

func (e *Engine) emit(ev Event) {
	e.seq++
	ev.Seq = e.seq
	ev.Timestamp = time.Now()
	if e.events != nil {
		e.events <- ev
	}
}

func (e *Engine) processOrder(o *Order) {
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
	if available.LessThan(o.Quantity) {
		o.Status = StatusRejected
		e.emit(Event{
			Type:    EvOrderRejected,
			OrderID: o.ID,
			Symbol:  o.Symbol,
			Qty:     o.Quantity,
		})
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

		for !aggressor.isFilled() && !level.isEmpty() {
			fillQty := decimal.Min(aggressor.RemainingQuantity, level.head().RemainingQuantity)

			maker := level.consume(fillQty)
			aggressor.fill(fillQty)

			e.emit(Event{
				Type:    EvTrade,
				Symbol:  aggressor.Symbol,
				Price:   execPrice,
				Qty:     fillQty,
				MakerID: maker.ID,
				TakerID: aggressor.ID,
			})

			aggressorEvType := EvOrderPartiallyFilled
			if aggressor.isFilled() {
				aggressorEvType = EvOrderFilled
			}
			e.emit(Event{
				Type:    aggressorEvType,
				OrderID: aggressor.ID,
				Symbol:  aggressor.Symbol,
				Price:   execPrice,
				Qty:     fillQty,
			})

			makerEvType := EvOrderPartiallyFilled
			if maker.isFilled() {
				makerEvType = EvOrderFilled
				delete(e.orderIdx, maker.ID)
			}
			e.emit(Event{
				Type:    makerEvType,
				OrderID: maker.ID,
				Symbol:  aggressor.Symbol,
				Price:   execPrice,
				Qty:     fillQty,
			})
		}
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
