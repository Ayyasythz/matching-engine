package engine

import (
	"sort"

	"github.com/shopspring/decimal"
)

type LevelFill struct {
	Order   *Order
	FillQty decimal.Decimal
}

type Matcher interface {
	Distribute(level *PriceLevel, incomingQty decimal.Decimal) []LevelFill
}

type FIFOMatcher struct{}

func (FIFOMatcher) Distribute(level *PriceLevel, incomingQty decimal.Decimal) []LevelFill {
	var fills []LevelFill
	rem := incomingQty

	for !rem.IsZero() && !level.isEmpty() {
		fillQty := decimal.Min(rem, level.head().RemainingQuantity)
		maker := level.consume(fillQty)
		rem = rem.Sub(fillQty)
		fills = append(fills, LevelFill{Order: maker, FillQty: fillQty})
	}

	return fills
}

type ProRataMatcher struct{}

func (ProRataMatcher) Distribute(level *PriceLevel, incomingQty decimal.Decimal) []LevelFill {
	if level.TotalQty.IsZero() || incomingQty.IsZero() {
		return nil
	}

	fillable := decimal.Min(incomingQty, level.TotalQty)
	n := len(level.orders)

	truncated := make([]decimal.Decimal, n)
	fracPart := make([]decimal.Decimal, n)
	allocated := decimal.Zero

	for i, o := range level.orders {
		raw := fillable.Mul(o.RemainingQuantity).Div(level.TotalQty)
		tr := raw.Floor()
		truncated[i] = tr
		fracPart[i] = raw.Sub(tr)
		allocated = allocated.Add(tr)
	}

	remainder := fillable.Sub(allocated)
	unit := decimal.NewFromInt(1)

	if remainder.GreaterThan(decimal.Zero) {
		indices := make([]int, n)
		for i := range indices {
			indices[i] = i
		}
		sort.Slice(indices, func(a, b int) bool {
			return fracPart[indices[a]].GreaterThan(fracPart[indices[b]])
		})
		for _, idx := range indices {
			if remainder.LessThanOrEqual(decimal.Zero) {
				break
			}
			// Cap each allocation at the order's remaining capacity so that
			// fractional-qty orders (e.g. 0.7 BTC) are never overfilled when the
			// unit step (1) exceeds what the order can actually absorb.
			avail := level.orders[idx].RemainingQuantity.Sub(truncated[idx])
			if avail.LessThanOrEqual(decimal.Zero) {
				continue
			}
			give := decimal.Min(unit, decimal.Min(avail, remainder))
			truncated[idx] = truncated[idx].Add(give)
			remainder = remainder.Sub(give)
		}
	}

	var fills []LevelFill
	for i, o := range level.orders {
		qty := truncated[i]
		if qty.IsZero() {
			continue
		}
		o.fill(qty)
		level.TotalQty = level.TotalQty.Sub(qty)
		fills = append(fills, LevelFill{Order: o, FillQty: qty})
	}

	remaining := make([]*Order, 0, n)
	for _, o := range level.orders {
		if !o.isFilled() {
			remaining = append(remaining, o)
		}
	}
	level.orders = remaining

	return fills
}
