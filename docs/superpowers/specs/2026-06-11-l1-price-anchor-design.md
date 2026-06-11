# Live L1 Price Anchor — Design

**Date:** 2026-06-11
**Status:** Approved

## Goal

Anchor the demo BTC-USD market to the real-world BTC price. A price feed
fetches the live spot price and a market-maker bot continuously quotes a
ladder of limit orders around it inside the existing engine, so users always
have realistic liquidity to trade against and the book's mid-price tracks
real BTC. Single pair (BTC-USD) only.

Also fixes a balance-accounting gap that the bot would make worse: funds are
not reserved when an order is placed, so sessions can overdraw by placing
multiple orders or market buys.

## Architecture

```
Coinbase spot API ──poll──▶ pricefeed ──updates──▶ marketmaker ──Submit/Cancel──▶ engine
                                │                                                   │
                                └────EvIndexPrice──▶ httpapi SSE ──▶ frontend       └──events──▶ httpapi
```

All new components run as goroutines inside `cmd/server` (one binary).

## Components

### `pricefeed` package (new)

- `New(pair string, interval time.Duration) *Feed` — polls
  `https://api.coinbase.com/v2/prices/BTC-USD/spot` (no API key required).
  Default interval: 3s.
- `Latest() (decimal.Decimal, bool)` — last known price; `false` until the
  first successful fetch.
- `Subscribe() <-chan decimal.Decimal` — notified on each price change.
- Error handling: network/HTTP errors keep the last known price and log at
  most once per minute. No price ever → subscribers receive nothing.

### `marketmaker` package (new)

- `New(eng *engine.Engine, feed *pricefeed.Feed, cfg Config) *Maker`
- Config defaults: `Levels: 5`, `Spread: 0.05%` (half-spread from index to
  best quote), `Step: 0.02%` between levels, `QtyPerLevel: 0.05 BTC`.
- Loop: on each feed update (debounced to at most once per 2s), cancel all
  resting bot orders, then place `Levels` bids below and `Levels` asks above
  the live price as LIMIT orders.
- Bot orders use a reserved session ID (`"__house__"`) with an effectively
  unlimited balance so trade accounting in `httpapi` stays consistent.
- Bot order IDs are prefixed (`mm-`) so they are distinguishable in events.

### `httpapi` changes

- Accept the price feed; broadcast a new SSE event `EvIndexPrice`
  (`{type:"INDEX_PRICE", price:"104231.50"}`) whenever it updates.
- House session: trades involving the house session skip user balance
  mutation overdraft concerns (house balance allowed to float).
- **Balance reservation fix:** each session tracks `reservedUSD` and
  `reservedBTC` for open orders.
  - Submit (limit buy): require `USD − reservedUSD ≥ price×qty`, then reserve.
  - Submit (sell): require `BTC − reservedBTC ≥ qty`, then reserve.
  - Market buy: reserve `qty × bestAsk × 1.05` (5% buffer); reject if the
    book has no asks.
  - Fills release the filled portion of the reservation; cancels/rejections
    release the remainder.

### `cmd/server` changes

- New flags: `-anchor` (default `true`), `-feed-interval` (default `3s`).
- Wires `pricefeed` → `marketmaker` → engine, and the feed into `httpapi`.

### `engine` changes

- None expected. Cancel-and-replace uses existing `Submit`/`Cancel`.

### Frontend changes

- Header shows "Live BTC" price from the `INDEX_PRICE` SSE event next to the
  existing demo mid-price, with the existing flash animation on change.

## Error handling

- Feed down at startup: bot idles, market behaves as today.
- Feed goes stale mid-run: bot keeps quoting at the last known price.
- Bot submit/cancel errors: logged, retried on the next quote cycle.

## Testing

- `pricefeed`: mock HTTP server — parse success, error keeps last price,
  subscriber notification.
- `marketmaker`: fake feed + real engine — asserts ladder shape (5+5 levels,
  correct spread/step), re-quote on price move, idle without a price.
- `httpapi` reservations: overdraft via two orders is rejected; cancel
  releases reservation; market-buy buffer; fills release reservations.

## Out of scope

- Multiple trading pairs, persistence, self-trade prevention, gRPC wiring,
  renaming `distruptor.go`.
