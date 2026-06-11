package engine

import (
	"time"

	"github.com/shopspring/decimal"
)

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

type OrderType string

const (
	Limit  OrderType = "LIMIT"
	Market OrderType = "MARKET"
	IOC    OrderType = "IOC"
	FOK    OrderType = "FOK"
)

type OrderStatus string

const (
	StatusOpen      OrderStatus = "OPEN"
	StatusFilled    OrderStatus = "FILLED"
	StatusPartial   OrderStatus = "PARTIAL"
	StatusCancelled OrderStatus = "CANCELLED"
	StatusRejected  OrderStatus = "REJECTED"
)

type Order struct {
	ID                string
	Symbol            string
	Side              Side
	Type              OrderType
	Price             decimal.Decimal
	Quantity          decimal.Decimal
	FilledQuantity    decimal.Decimal
	RemainingQuantity decimal.Decimal
	Status            OrderStatus
	CreatedAt         time.Time
}

func NewOrder(id, symbol string, side Side, otype OrderType, price, qty decimal.Decimal) *Order {
	return &Order{
		ID:                id,
		Symbol:            symbol,
		Side:              side,
		Type:              otype,
		Price:             price,
		Quantity:          qty,
		RemainingQuantity: qty,
		Status:            StatusOpen,
		CreatedAt:         time.Now(),
	}
}

func (o *Order) fill(qty decimal.Decimal) {
	o.FilledQuantity = o.FilledQuantity.Add(qty)
	o.RemainingQuantity = o.RemainingQuantity.Sub(qty)
	if o.RemainingQuantity.Equal(decimal.Zero) {
		o.Status = StatusFilled
	} else {
		o.Status = StatusPartial
	}
}

func (o *Order) isFilled() bool {
	return o.RemainingQuantity.Equal(decimal.Zero)
}

type BookSnapshot struct {
	Symbol string               `json:"symbol"`
	Bids   []PriceLevelSnapshot `json:"bids"`
	Asks   []PriceLevelSnapshot `json:"asks"`
}

type PriceLevelSnapshot struct {
	Price string `json:"price"`
	Qty   string `json:"qty"`
	Count int    `json:"count"`
}
