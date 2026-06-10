package engine

import "github.com/shopspring/decimal"

type PriceLevel struct {
	Price    decimal.Decimal
	orders   []*Order
	TotalQty decimal.Decimal
}

func newPriceLevel(price decimal.Decimal) *PriceLevel {
	return &PriceLevel{
		Price: price,
	}
}

func (p *PriceLevel) add(order *Order) {
	p.orders = append(p.orders, order)
	p.TotalQty = p.TotalQty.Add(order.RemainingQuantity)
}

func (p *PriceLevel) head() *Order {
	if len(p.orders) == 0 {
		return nil
	}
	return p.orders[0]
}

func (p *PriceLevel) consume(qty decimal.Decimal) *Order {
	o := p.orders[0]
	o.fill(qty)
	p.TotalQty = p.TotalQty.Sub(qty)
	if o.isFilled() {
		p.orders = p.orders[1:]
	}
	return o
}

func (p *PriceLevel) isEmpty() bool {
	return len(p.orders) == 0
}

func (p *PriceLevel) removeByID(id string) (*Order, bool) {
	for i, o := range p.orders {
		if o.ID == id {
			p.TotalQty = p.TotalQty.Sub(o.RemainingQuantity)
			p.orders = append(p.orders[:i], p.orders[i+1:]...)
			return o, true
		}
	}
	return nil, false
}
