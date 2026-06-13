package engine

import (
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
)

// minAMMFill is the smallest pool fill worth executing (one satoshi).
var minAMMFill = decimal.New(1, -ammQtyPrec)

// processOrderAMM executes an order against the liquidity pool.
//
//   - MARKET orders swap their full quantity at whatever price the curve gives.
//   - LIMIT orders fill as much as possible while keeping the average
//     execution price within their limit; any remainder rests in the book and
//     is triggered automatically when the pool price later crosses the limit.
//   - IOC behaves like LIMIT but cancels the remainder instead of resting.
//   - FOK executes fully within the limit or is rejected.
func (e *Engine) processOrderAMM(o *Order) {
	// Non-market orders must carry a positive limit price.
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
		e.ammMarket(o)
	case Limit:
		e.ammLimit(o)
	case IOC:
		e.ammIOC(o)
	case FOK:
		e.ammFOK(o)
	}

	if o.Status != StatusOpen && o.Status != StatusPartial {
		delete(e.orderIdx, o.ID)
	}

	// The pool price moved — resting limit orders may now be in range.
	e.triggerRestingAMM()
}

// ammSwap executes dx of the order against the pool and emits the trade and
// taker fill events. Returns false if the pool rejected the swap.
func (e *Engine) ammSwap(o *Order, dx decimal.Decimal) bool {
	var quote decimal.Decimal
	var err error
	if o.Side == Buy {
		quote, err = e.pool.Buy(dx)
	} else {
		quote, err = e.pool.Sell(dx)
	}
	if err != nil {
		return false
	}

	avgPrice := quote.Div(dx).Round(2)
	o.fill(dx)

	e.emit(Event{
		Type: EvTrade, Symbol: e.symbol,
		Price: avgPrice, Qty: dx,
		MakerID: AMMMakerID, TakerID: o.ID,
	})

	takerEv := EvOrderPartiallyFilled
	if o.isFilled() {
		takerEv = EvOrderFilled
	}
	e.emit(Event{Type: takerEv, OrderID: o.ID,
		Symbol: e.symbol, Price: avgPrice, Qty: dx})
	return true
}

func (e *Engine) ammMarket(o *Order) {
	if o.Side == Buy && o.RemainingQuantity.GreaterThanOrEqual(e.pool.Base) {
		o.Status = StatusRejected
		e.emit(Event{
			Type:    EvOrderRejected,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.RemainingQuantity,
			Reason: fmt.Sprintf("insufficient pool liquidity — pool holds %s BTC, order needs %s",
				e.pool.Base.Truncate(ammQtyPrec), o.RemainingQuantity),
		})
		return
	}
	e.ammSwap(o, o.RemainingQuantity)
}

func (e *Engine) ammLimit(o *Order) {
	fillable := e.ammFillable(o)
	if dx := decimal.Min(fillable, o.RemainingQuantity); dx.GreaterThanOrEqual(minAMMFill) {
		e.ammSwap(o, dx)
	}
	if !o.isFilled() {
		e.resting(o.Side).addOrder(o)
	}
}

func (e *Engine) ammIOC(o *Order) {
	fillable := e.ammFillable(o)
	if dx := decimal.Min(fillable, o.RemainingQuantity); dx.GreaterThanOrEqual(minAMMFill) {
		e.ammSwap(o, dx)
	}
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

func (e *Engine) ammFOK(o *Order) {
	fillable := e.ammFillable(o)
	if fillable.LessThan(o.RemainingQuantity) {
		o.Status = StatusRejected
		e.emit(Event{
			Type:    EvOrderRejected,
			OrderID: o.ID,
			Symbol:  e.symbol,
			Qty:     o.RemainingQuantity,
			Reason: fmt.Sprintf("insufficient pool liquidity for FOK order — only %s fillable within $%s, need %s",
				fillable, o.Price, o.RemainingQuantity),
		})
		return
	}
	e.ammSwap(o, o.RemainingQuantity)
}

// ammFillable returns how much of the order the pool can fill while keeping
// the average execution price within the order's limit.
func (e *Engine) ammFillable(o *Order) decimal.Decimal {
	if o.Side == Buy {
		return e.pool.MaxBuyWithinAvgPrice(o.Price)
	}
	return e.pool.MaxSellWithinAvgPrice(o.Price)
}

// triggerRestingAMM executes resting limit orders whose limits the pool price
// has crossed. Each fill moves the pool price, which can bring further orders
// into range, so it loops until neither side has a fillable order. The swap
// fee guarantees convergence (every round trip shrinks the fillable window),
// and the iteration cap is a hard safety net.
func (e *Engine) triggerRestingAMM() {
	for i := 0; i < 256; i++ {
		progressed := false

		// Resting bids buy from the pool; fillable once the price drops to the limit.
		if lvl := e.bids.bestLevel(); lvl != nil {
			fillable := e.pool.MaxBuyWithinAvgPrice(lvl.Price)
			if dx := decimal.Min(fillable, lvl.head().RemainingQuantity); dx.GreaterThanOrEqual(minAMMFill) {
				e.triggerFill(e.bids, lvl, dx, true)
				progressed = true
			}
		}

		// Resting asks sell to the pool; fillable once the price rises to the limit.
		if lvl := e.asks.bestLevel(); lvl != nil {
			fillable := e.pool.MaxSellWithinAvgPrice(lvl.Price)
			if dx := decimal.Min(fillable, lvl.head().RemainingQuantity); dx.GreaterThanOrEqual(minAMMFill) {
				e.triggerFill(e.asks, lvl, dx, false)
				progressed = true
			}
		}

		if !progressed {
			return
		}
	}
}

// triggerFill executes dx of the resting head order at lvl against the pool.
func (e *Engine) triggerFill(book *halfBook, lvl *PriceLevel, dx decimal.Decimal, isBuy bool) {
	var quote decimal.Decimal
	var err error
	if isBuy {
		quote, err = e.pool.Buy(dx)
	} else {
		quote, err = e.pool.Sell(dx)
	}
	if err != nil {
		return
	}

	avgPrice := quote.Div(dx).Round(2)
	o := lvl.consume(dx) // fills the order and maintains level TotalQty

	e.emit(Event{
		Type: EvTrade, Symbol: e.symbol,
		Price: avgPrice, Qty: dx,
		MakerID: AMMMakerID, TakerID: o.ID,
	})

	fillEv := EvOrderPartiallyFilled
	if o.isFilled() {
		fillEv = EvOrderFilled
		delete(e.orderIdx, o.ID)
	}
	e.emit(Event{Type: fillEv, OrderID: o.ID,
		Symbol: e.symbol, Price: avgPrice, Qty: dx})

	book.pruneLevel(lvl.Price)
}

// snapshotAMM synthesises an order-book view of the pool curve so clients can
// render depth: 12 levels per side, 10 bps apart, where each level's quantity
// is how much can execute between the previous price band and this one.
// Resting limit orders are merged in at their own prices.
func (e *Engine) snapshotAMM() BookSnapshot {
	const numLevels = 12
	step := decimal.New(1, -3) // 10 bps per level
	one := decimal.NewFromInt(1)
	spot := e.pool.SpotPrice()

	var asks, bids []PriceLevelSnapshot

	prevCum := decimal.Zero
	for i := 1; i <= numLevels; i++ {
		p := spot.Mul(one.Add(step.Mul(decimal.NewFromInt(int64(i))))).Round(2)
		cum := e.pool.MaxBuyWithinAvgPrice(p)
		if q := cum.Sub(prevCum); q.GreaterThan(decimal.Zero) {
			asks = append(asks, PriceLevelSnapshot{Price: p.String(), Qty: q.String(), Count: 1})
			prevCum = cum
		}
	}

	prevCum = decimal.Zero
	for i := 1; i <= numLevels; i++ {
		p := spot.Mul(one.Sub(step.Mul(decimal.NewFromInt(int64(i))))).Round(2)
		cum := e.pool.MaxSellWithinAvgPrice(p)
		if q := cum.Sub(prevCum); q.GreaterThan(decimal.Zero) {
			bids = append(bids, PriceLevelSnapshot{Price: p.String(), Qty: q.String(), Count: 1})
			prevCum = cum
		}
	}

	asks = mergeLevels(asks, e.asks.snapshot(), true)
	bids = mergeLevels(bids, e.bids.snapshot(), false)

	return BookSnapshot{
		Symbol: e.symbol,
		Bids:   bids,
		Asks:   asks,
		AMM:    e.pool.info(),
	}
}

// mergeLevels combines synthesised pool levels with real resting-order levels,
// summing quantities at equal prices and sorting (ascending for asks,
// descending for bids).
func mergeLevels(synth, real []PriceLevelSnapshot, ascending bool) []PriceLevelSnapshot {
	byPrice := make(map[string]*PriceLevelSnapshot, len(synth)+len(real))
	order := make([]string, 0, len(synth)+len(real))

	add := func(l PriceLevelSnapshot) {
		if existing, ok := byPrice[l.Price]; ok {
			q1, _ := decimal.NewFromString(existing.Qty)
			q2, _ := decimal.NewFromString(l.Qty)
			existing.Qty = q1.Add(q2).String()
			existing.Count += l.Count
			return
		}
		cp := l
		byPrice[l.Price] = &cp
		order = append(order, l.Price)
	}
	for _, l := range synth {
		add(l)
	}
	for _, l := range real {
		add(l)
	}

	out := make([]PriceLevelSnapshot, 0, len(order))
	for _, p := range order {
		out = append(out, *byPrice[p])
	}
	sort.Slice(out, func(a, b int) bool {
		pa, _ := decimal.NewFromString(out[a].Price)
		pb, _ := decimal.NewFromString(out[b].Price)
		if ascending {
			return pa.LessThan(pb)
		}
		return pa.GreaterThan(pb)
	})
	return out
}
