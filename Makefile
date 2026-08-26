.PHONY: help build run seed migrate dev test test-race test-integration test-cover bench \
        lint fmt tidy db-up db-down docker-up docker-down docker-logs clean

PG_DSN ?= postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable
REDIS_ADDR ?= localhost:6379

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Build and run ────────────────────────────────────────────────────────────
build: ## Compile the binary into bin/
	go build -trimpath -o bin/axentra ./cmd/axentra

run: build ## Build, then run the service
	./bin/axentra

dev: ## Run straight from source
	go run ./cmd/axentra

migrate: ## Apply pending migrations, then exit
	go run ./cmd/axentra -migrate

seed: ## Load the demo timetable and seat data, then exit
	go run ./cmd/axentra -seed

# ── Tests ────────────────────────────────────────────────────────────────────
test: ## Unit tests (no infrastructure needed)
	go test ./...

test-race: ## Unit tests under the race detector
	go test -race ./...

test-integration: ## All tests, including those that need Postgres and Redis
	PG_TEST_DSN="$(PG_DSN)" REDIS_TEST_ADDR="$(REDIS_ADDR)" go test -race ./...

test-cover: ## Coverage report at coverage.html
	PG_TEST_DSN="$(PG_DSN)" REDIS_TEST_ADDR="$(REDIS_ADDR)" \
	  go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

bench: ## Search benchmarks
	go test -run '^$$' -bench 'BenchmarkSearch' -benchmem ./internal/raptor/

# ── Code quality ─────────────────────────────────────────────────────────────
fmt: ## Format all Go source
	gofmt -w .

lint: ## Vet, plus golangci-lint when installed
	go vet ./...
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || \
	  echo "golangci-lint not installed; ran go vet only"

tidy: ## Tidy and verify module dependencies
	go mod tidy
	go mod verify

# ── Infrastructure ───────────────────────────────────────────────────────────
db-up: ## Start Postgres and Redis only
	docker compose up -d postgres redis

db-down: ## Stop the infrastructure containers (volumes preserved)
	docker compose down

docker-up: ## Build and start the whole stack
	docker compose up -d --build

docker-down: ## Stop everything and delete volumes
	docker compose down -v

docker-logs: ## Follow the application logs
	docker compose logs -f axentra

clean: ## Remove build and coverage artefacts
	rm -rf bin/ coverage.out coverage.html
