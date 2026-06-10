package engine

import (
	"sort"

	"github.com/shopspring/decimal"
)

type halfBook struct {
	side   Side
	levels map[string]*PriceLevel
	prices []decimal.Decimal
}

func newHalfBook(side Side) *halfBook {
	return &halfBook{
		side:   side,
		levels: make(map[string]*PriceLevel),
	}
}

func (b *halfBook) bestLevel() *PriceLevel {
	if len(b.levels) == 0 {
		return nil
	}
	return b.levels[b.prices[0].String()]
}

func (b *halfBook) getLevel(price decimal.Decimal) *PriceLevel {
	return b.levels[price.String()]
}

func (b *halfBook) addOrder(o *Order) {
	key := o.Price.String()
	level, exists := b.levels[key]
	if !exists {
		level = newPriceLevel(o.Price)
		b.levels[key] = level
		b.insertPrice(o.Price)
	}
	level.add(o)
}

func (b *halfBook) insertPrice(price decimal.Decimal) {
	idx := sort.Search(len(b.prices), func(i int) bool {
		if b.side == Buy {
			return b.prices[i].LessThan(price)
		}
		return b.prices[i].GreaterThan(price)
	})
	b.prices = append(b.prices, decimal.Zero)
	copy(b.prices[idx+1:], b.prices[idx:])
	b.prices[idx] = price
}

func (b *halfBook) pruneLevel(price decimal.Decimal) {
	key := price.String()
	level, ok := b.levels[key]
	if !ok || !level.isEmpty() {
		return
	}
	delete(b.levels, key)
	for i, p := range b.prices {
		if p.Equal(price) {
			b.prices = append(b.prices[:i], b.prices[i+1:]...)
			return
		}
	}
}

func (b *halfBook) removeOrderByID(id string, price decimal.Decimal) (*Order, bool) {
	level := b.getLevel(price)
	if level == nil {
		return nil, false
	}

	o, ok := level.removeByID(id)
	if ok {
		b.pruneLevel(o.Price)
	}

	return o, ok
}

func (b *halfBook) totalQtyAtOrBetterThan(price decimal.Decimal, aggressorSide Side) decimal.Decimal {
	total := decimal.Zero
	for _, p := range b.prices {
		if aggressorSide == Buy && p.GreaterThan(price) {
			break
		}

		if aggressorSide == Sell && p.LessThan(price) {
			break
		}
		total = total.Add(b.levels[p.String()].TotalQty)
	}
	return total
}
