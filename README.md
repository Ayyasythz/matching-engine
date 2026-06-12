# Matching Engine

A high-throughput order matching engine in Go with a live web UI, real-time BTC price anchoring, and two matching algorithms. Built for learning and demo purposes.

---

## Features

- **FIFO and Pro-rata matching** — switchable at startup
- **Order types** — Limit, Market, IOC, FOK
- **Live BTC price anchor** — a market-maker bot quotes around the real BTC-USD price (via CoinGecko) so the demo book tracks the real market
- **Web UI** — order book, trade feed, balance, and portfolio value; updates via Server-Sent Events
- **Balance accounting** — per-session USD/BTC balances with fund reservation on open orders (no overdraft)
- **O(log n) price-level insert/delete** — order book backed by a red-black tree; best-price lookup remains O(1)
- **~297k orders/sec** throughput, p50 latency ~22µs on Apple M1

---

## Quick Start

```bash
git clone https://github.com/Ayyasythz/matching-engine
cd matching-engine
go run ./cmd/server
```

Open http://localhost:8080. The market-maker bot starts quoting around the live BTC price immediately.

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:8080` | HTTP listen address |
| `-frontend` | `./frontend` | Path to static files |
| `-mode` | `fifo` | Matching algorithm: `fifo` or `prorata` |
| `-anchor` | `true` | Enable live BTC price anchor |
| `-feed-interval` | `10s` | Price poll interval (CoinGecko free tier) |

```bash
# Pro-rata mode without price anchor
go run ./cmd/server -mode prorata -anchor=false
```

---

## Project Structure

```
engine/          Core matching engine (order book, FIFO/pro-rata matchers, disruptor ring buffer)
  rbtree.go      Intrusive red-black tree used to index price levels in the order book
api/
  httpapi/       HTTP server — REST API + SSE event stream
  grpc/          gRPC server (generated from proto/)
pricefeed/       Polls live BTC-USD spot price (CoinGecko)
marketmaker/     Quotes a 5-level bid/ask ladder around the index price
cmd/
  server/        Main HTTP server binary
  simulate/      10-second throughput/latency benchmark
  demo/          Visual FIFO vs pro-rata comparison
  main.go        Minimal standalone demo
frontend/        Single-file web UI (vanilla JS + SSE)
```

---

## API

All endpoints return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/orders` | Submit an order |
| `DELETE` | `/api/orders/:id` | Cancel a resting order |
| `GET` | `/api/book` | Current order book snapshot |
| `GET` | `/api/events` | SSE stream of engine events |
| `GET` | `/api/me/orders` | Orders for the current session |
| `GET` | `/api/me/balance` | USD/BTC balance for the current session |
| `GET` | `/api/presence` | Count of active SSE connections |

**Submit order:**
```json
POST /api/orders
{
  "side":     "buy",
  "type":     "LIMIT",
  "price":    "61500.00",
  "qty":      "0.5",
  "username": "alice"
}
```

**SSE event types:** `order.accepted`, `order.filled`, `order.partially_filled`, `order.cancelled`, `order.rejected`, `trade`, `INDEX_PRICE`

---

## Matching Algorithms

### FIFO
Orders at the same price fill in arrival order. First in, first served — queue position is everything.

### Pro-rata
All orders at the same price fill simultaneously, proportional to their size. Used in interest-rate futures markets (e.g. CME Eurodollar).

Run the visual demo to see the difference:

```
go run ./cmd/demo -mode fifo
go run ./cmd/demo -mode prorata
```

```
FIFO  |  3 orders at $300.00  |  incoming buy: 999 qty

  Order   Resting    Filled     Distribution
  ──────────────────────────────────────────────────────────────────────
  A       200        200        ████████████░░░░░░░░░░░░░░░░░░░░░░░░  fully consumed
  B       500        499        ████████████████████████████░░░░░░░░  99% of 500
  C       300        300        never reached
```

```
PRORATA  |  3 orders at $300.00  |  incoming buy: 999 qty

  Order   Resting    Filled     Distribution
  ──────────────────────────────────────────────────────────────────────
  A       200        79         ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  39% of 200
  B       500        499        ████████████████████░░░░░░░░░░░░░░░░  99% of 500
  C       300        299        ████████████░░░░░░░░░░░░░░░░░░░░░░░░  99% of 300
```

---

## Performance

Run the built-in simulation (1 market maker + 8 aggressive traders, 10 seconds):

```bash
go run ./cmd/simulate
```

```
BTC-USD  |  1 market maker  |  8 traders  |  10s run  |  mode: fifo

  Time     Orders/s        Trades/s        Fill rate    Mid price
  ────────────────────────────────────────────────────────────────────
  1s       288.8k          144.3k              100.0%  $50021.45
  ...
  ────────────────────────────────────────────────────────────────────
  Orders submitted:            2.97M
  Trades executed:             1.48M
  Fill rate:                   100.0%
  Throughput:                  297.0k orders/sec
  Latency p50:                 22417 ns
  Latency p99:                 67000 ns
  Latency p99.9:               100583 ns
```

Benchmarks (Apple M1 Pro):

```bash
go test ./engine/ -bench=BenchmarkTrade -benchtime=5s
```

```
BenchmarkTrade_LimitPair-10               447375    7811 ns/op    2006 B/op    75 allocs/op
BenchmarkTrade_MarketAgainstResting-10    619536    5898 ns/op    2282 B/op    73 allocs/op
```

---

## Testing

```bash
# Unit tests only (fast, ~10s)
go test ./engine/... ./api/httpapi/... ./pricefeed/... ./marketmaker/... -short

# All tests including stress tests
go test ./engine/... ./api/httpapi/... ./pricefeed/... ./marketmaker/...
```

The test suite covers:
- Engine correctness (FIFO priority, partial fills, price improvement, IOC/FOK)
- Pro-rata fill distribution
- Disruptor ring buffer concurrency
- Red-black tree correctness (insertion, deletion, rotations, balance invariants)
- Smoke tests: multi-level sweeps, cancel, maker/taker event fields
- Stress tests: 10k sequential pairs, 8-goroutine concurrent producers
- HTTP API balance reservation and overdraft prevention
- Price feed: parse, error recovery, subscriber notification

---

## How It Works

```
HTTP clients
     │  POST /api/orders
     ▼
  httpapi ──── reserve funds ──────────────────────────────────┐
     │                                                          │
     │  engine.Submit(*Order)                                   │
     ▼                                                          │
  Ring buffer (disruptor)                                       │
     │                                                          │
     ▼                                                          │
  engine.Run() goroutine                                        │
     │  ┌── FIFO matcher                                        │
     │  └── Pro-rata matcher                                    │
     │                                                          │
     ├─ emit Event (trade / filled / cancelled / ...)           │
     ▼                                                          │
  events channel ──► httpapi.fanOut() ──► SSE subscribers ◄────┘
                              │
                              └─ release fund reservations on fill/cancel

pricefeed (CoinGecko) ──► marketmaker ──► engine.Submit / Cancel
                      └──► BroadcastIndexPrice ──► SSE (INDEX_PRICE)
```

Orders flow through a lock-free ring buffer into a single-goroutine engine loop, keeping the matching path contention-free. The HTTP layer uses per-session fund reservations to prevent overdraft before submitting to the engine.

Each side of the book (`halfBook`) indexes price levels in a red-black tree so that insert and delete are O(log n) regardless of book depth. The best bid/ask is always the tree's min/max node, accessed in O(1) without a separate sorted slice.
