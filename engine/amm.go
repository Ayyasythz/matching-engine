package engine

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// AMMMakerID is the synthetic maker ID used on trades executed against the pool.
const AMMMakerID = "AMM"

// ammQtyPrec is the decimal precision used for pool quantities (satoshi).
const ammQtyPrec = int32(8)

// AMMPool is a constant-product (x·y = k) automated market maker, Uniswap-v2
// style. Base is the BTC reserve (x), Quote the USD reserve (y). The swap fee
// stays in the reserves, so k grows with every trade.
//
// The pool is owned by the engine goroutine — all access goes through the
// engine's command loop, so no locking is needed.
type AMMPool struct {
	Base   decimal.Decimal // x — base asset reserve (BTC)
	Quote  decimal.Decimal // y — quote asset reserve (USD)
	FeeBps int64           // swap fee in basis points (30 = 0.30%)
}

func NewAMMPool(base, quote decimal.Decimal, feeBps int64) (*AMMPool, error) {
	if base.LessThanOrEqual(decimal.Zero) || quote.LessThanOrEqual(decimal.Zero) {
		return nil, fmt.Errorf("amm: reserves must be positive (got base=%s quote=%s)", base, quote)
	}
	if feeBps < 0 || feeBps >= 10000 {
		return nil, fmt.Errorf("amm: fee must be in [0, 10000) bps, got %d", feeBps)
	}
	return &AMMPool{Base: base, Quote: quote, FeeBps: feeBps}, nil
}

// SpotPrice returns the instantaneous marginal price y/x.
func (p *AMMPool) SpotPrice() decimal.Decimal {
	return p.Quote.Div(p.Base)
}

// feeMult returns (10000 − FeeBps) / 10000, the fraction kept after fees.
func (p *AMMPool) feeMult() decimal.Decimal {
	return decimal.NewFromInt(10000 - p.FeeBps).Div(decimal.NewFromInt(10000))
}

// BuyCost quotes the USD required to buy dx base from the pool without
// mutating state: cost = y·dx / ((x−dx)·f).
func (p *AMMPool) BuyCost(dx decimal.Decimal) (decimal.Decimal, error) {
	if dx.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("amm: buy quantity must be positive")
	}
	if dx.GreaterThanOrEqual(p.Base) {
		return decimal.Zero, fmt.Errorf("amm: insufficient pool liquidity — pool holds %s, order needs %s", p.Base.Truncate(ammQtyPrec), dx)
	}
	return p.Quote.Mul(dx).Div(p.Base.Sub(dx).Mul(p.feeMult())).Round(ammQtyPrec), nil
}

// SellProceeds quotes the USD received for selling dx base to the pool
// without mutating state: out = y·dx·f / (x + dx·f).
func (p *AMMPool) SellProceeds(dx decimal.Decimal) (decimal.Decimal, error) {
	if dx.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("amm: sell quantity must be positive")
	}
	dxEff := dx.Mul(p.feeMult())
	return p.Quote.Mul(dxEff).Div(p.Base.Add(dxEff)).Round(ammQtyPrec), nil
}

// Buy executes a buy of dx base, mutating the reserves, and returns the USD cost.
func (p *AMMPool) Buy(dx decimal.Decimal) (decimal.Decimal, error) {
	cost, err := p.BuyCost(dx)
	if err != nil {
		return decimal.Zero, err
	}
	p.Base = p.Base.Sub(dx)
	p.Quote = p.Quote.Add(cost)
	return cost, nil
}

// Sell executes a sale of dx base to the pool and returns the USD proceeds.
func (p *AMMPool) Sell(dx decimal.Decimal) (decimal.Decimal, error) {
	out, err := p.SellProceeds(dx)
	if err != nil {
		return decimal.Zero, err
	}
	p.Base = p.Base.Add(dx)
	p.Quote = p.Quote.Sub(out)
	return out, nil
}

// MaxBuyWithinAvgPrice returns the largest base quantity that can be bought
// with an average execution price ≤ limit. Average price for a buy of dx is
// y / ((x−dx)·f); solving avg = limit gives dx = x − y/(limit·f). Returns
// zero when the limit is at or below the effective spot price.
func (p *AMMPool) MaxBuyWithinAvgPrice(limit decimal.Decimal) decimal.Decimal {
	if limit.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	dx := p.Base.Sub(p.Quote.Div(limit.Mul(p.feeMult())))
	// Truncate down so the realised average never exceeds the limit.
	dx = dx.Truncate(ammQtyPrec)
	if dx.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	return dx
}

// MaxSellWithinAvgPrice returns the largest base quantity that can be sold
// with an average execution price ≥ limit. Average price for a sale of dx is
// y·f / (x + dx·f); solving avg = limit gives dx = y/limit − x/f. Returns
// zero when the limit is at or above the effective spot price.
func (p *AMMPool) MaxSellWithinAvgPrice(limit decimal.Decimal) decimal.Decimal {
	if limit.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	dx := p.Quote.Div(limit).Sub(p.Base.Div(p.feeMult()))
	dx = dx.Truncate(ammQtyPrec)
	if dx.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	return dx
}

// AMMInfo is the pool state attached to book snapshots in AMM mode.
type AMMInfo struct {
	BaseReserve  string `json:"base_reserve"`
	QuoteReserve string `json:"quote_reserve"`
	SpotPrice    string `json:"spot_price"`
	FeeBps       int64  `json:"fee_bps"`
}

func (p *AMMPool) info() *AMMInfo {
	return &AMMInfo{
		BaseReserve:  p.Base.Truncate(ammQtyPrec).String(),
		QuoteReserve: p.Quote.Round(2).String(),
		SpotPrice:    p.SpotPrice().Round(2).String(),
		FeeBps:       p.FeeBps,
	}
}
