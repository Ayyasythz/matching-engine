BINARY      := server
BUILD_DIR   := ./build
SERVER_PKG  := ./cmd/server
DEMO_PKG    := ./cmd/demo
SIMULATE_PKG := ./cmd/simulate
PROTO_DIR   := ./proto
PROTO_OUT   := ./api/grpc/pb

# Default port; override with: make run PORT=9090
PORT        ?= 8080
MODE        ?= fifo
FRONTEND    ?= ./frontend

.PHONY: all build run dev test test-verbose lint proto clean help \
        run-demo run-simulate build-all

all: build

## ── Build ────────────────────────────────────────────────────────────────────

build: ## Build the main server binary
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) $(SERVER_PKG)

build-all: ## Build all binaries (server, demo, simulate)
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/server   $(SERVER_PKG)
	go build -o $(BUILD_DIR)/demo     $(DEMO_PKG)
	go build -o $(BUILD_DIR)/simulate $(SIMULATE_PKG)

## ── Run ──────────────────────────────────────────────────────────────────────

run: build ## Build and run the server (BTC-USD, ETH-USD, SOL-USD)
	$(BUILD_DIR)/$(BINARY) \
		-addr :$(PORT) \
		-frontend $(FRONTEND) \
		-mode $(MODE)

dev: ## Run without rebuilding (go run, live-ish)
	go run $(SERVER_PKG) \
		-addr :$(PORT) \
		-frontend $(FRONTEND) \
		-mode $(MODE)

run-noanchor: build ## Run without price-feed market makers
	$(BUILD_DIR)/$(BINARY) \
		-addr :$(PORT) \
		-frontend $(FRONTEND) \
		-mode $(MODE) \
		-anchor=false

run-amm: build ## Run BTC-USD in AMM mode
	$(BUILD_DIR)/$(BINARY) \
		-addr :$(PORT) \
		-frontend $(FRONTEND) \
		-mode amm

run-prorata: build ## Run in pro-rata matching mode
	$(BUILD_DIR)/$(BINARY) \
		-addr :$(PORT) \
		-frontend $(FRONTEND) \
		-mode prorata

run-demo: ## Run the quick order-book demo script
	go run $(DEMO_PKG)

run-simulate: ## Run the market simulation benchmark
	go run $(SIMULATE_PKG)

## ── Test ─────────────────────────────────────────────────────────────────────

test: ## Run all tests
	go test ./...

test-verbose: ## Run all tests with verbose output
	go test -v ./...

test-race: ## Run tests with race detector
	go test -race ./...

test-cover: ## Run tests and show coverage
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## ── Code quality ─────────────────────────────────────────────────────────────

lint: ## Run go vet
	go vet ./...

fmt: ## Format all Go source files
	gofmt -w .

tidy: ## Tidy go.mod / go.sum
	go mod tidy

## ── Proto ────────────────────────────────────────────────────────────────────

proto: ## Regenerate gRPC code from proto/exchange.proto
	protoc \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/exchange.proto

## ── Clean ────────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts and coverage reports
	rm -rf $(BUILD_DIR) coverage.out

## ── Help ─────────────────────────────────────────────────────────────────────

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
