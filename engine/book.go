package engine

import "github.com/shopspring/decimal"

// halfBook holds one side of the order book (all bids or all asks).
// Price levels are indexed by a red-black tree for O(log n) insert/delete
// and O(1) best-price access via the cached tree min/max.
type halfBook struct {
	side   Side
	levels map[string]*PriceLevel
	tree   rbTree
}

func newHalfBook(side Side) *halfBook {
	return &halfBook{
		side:   side,
		levels: make(map[string]*PriceLevel),
	}
}

// bestLevel returns the highest-priority level for this side:
// highest price for bids, lowest price for asks.
func (b *halfBook) bestLevel() *PriceLevel {
	var best *rbNode
	if b.side == Buy {
		best = b.tree.max()
	} else {
		best = b.tree.min()
	}
	if best == nil {
		return nil
	}
	return b.levels[best.price.String()]
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
		b.tree.insert(o.Price) // O(log n)
	}
	level.add(o)
}

func (b *halfBook) pruneLevel(price decimal.Decimal) {
	key := price.String()
	level, ok := b.levels[key]
	if !ok || !level.isEmpty() {
		return
	}
	delete(b.levels, key)
	b.tree.delete(price) // O(log n)
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

func (b *halfBook) snapshot() []PriceLevelSnapshot {
	result := make([]PriceLevelSnapshot, 0, b.tree.size)
	collect := func(p decimal.Decimal) bool {
		level := b.levels[p.String()]
		result = append(result, PriceLevelSnapshot{
			Price: p.String(),
			Qty:   level.TotalQty.String(),
			Count: len(level.orders),
		})
		return true
	}
	// Bids: highest price first; asks: lowest price first.
	if b.side == Buy {
		b.tree.descend(collect)
	} else {
		b.tree.ascend(collect)
	}
	return result
}

func (b *halfBook) totalQtyAtOrBetterThan(price decimal.Decimal, aggressorSide Side) decimal.Decimal {
	total := decimal.Zero
	if aggressorSide == Buy {
		// Aggressor is buying: count asks at or below the limit price.
		b.tree.ascend(func(p decimal.Decimal) bool {
			if p.GreaterThan(price) {
				return false
			}
			total = total.Add(b.levels[p.String()].TotalQty)
			return true
		})
	} else {
		// Aggressor is selling: count bids at or above the limit price.
		b.tree.descend(func(p decimal.Decimal) bool {
			if p.LessThan(price) {
				return false
			}
			total = total.Add(b.levels[p.String()].TotalQty)
			return true
		})
	}
	return total
}
