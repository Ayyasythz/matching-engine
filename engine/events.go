package engine

import (
	"time"

	"github.com/shopspring/decimal"
)

type EventType string

const (
	EvOrderAccepted        EventType = "order.accepted"
	EvOrderFilled          EventType = "order.filled"
	EvOrderRejected        EventType = "order.rejected"
	EvOrderCancelled       EventType = "order.cancelled"
	EvOrderPartiallyFilled EventType = "order.partially_filled"
	EvTrade                EventType = "trade"
)

type Event struct {
	Type      EventType       `json:"type"`
	Seq       uint64          `json:"seq"`
	Timestamp time.Time       `json:"timestamp"`
	OrderID   string          `json:"order_id,omitempty"`
	Symbol    string          `json:"symbol"`
	Price     decimal.Decimal `json:"price,omitempty"`
	Qty       decimal.Decimal `json:"qty,omitempty"`
	Side      Side            `json:"side,omitempty"`
	MakerID   string          `json:"maker_id,omitempty"`
	TakerID   string          `json:"taker_id,omitempty"`
}
