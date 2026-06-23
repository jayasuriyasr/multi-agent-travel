.PHONY: build run seed dev test test-cover lint \
        db-up db-down docker-up docker-down docker-logs \
        migrate-up migrate-down clean

# ── Default DSN (override via env: PG_DSN=... make migrate-up) ──────────────
PG_DSN ?= postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable

# ── Build & Run ─────────────────────────────────────────────────────────────
build:
	go build -o bin/axentra ./cmd/axentra

## Quick connectivity test — no binary produced.
run:
	go run cmd/axentra/main.go

## Seed the database with mock schedule data (requires running Postgres).
seed:
	go run cmd/axentra/main.go -seed

dev:
	go run ./cmd/axentra

# ── Testing ──────────────────────────────────────────────────────────────────
test:
	go test -race -v ./...

test-cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# ── Linting ──────────────────────────────────────────────────────────────────
lint:
	golangci-lint run ./...

# ── Infrastructure (preferred short aliases) ─────────────────────────────────
## Bring up PostgreSQL + Redis in detached mode.
db-up:
	docker compose up -d postgres redis

## Stop and remove infra containers (data volumes are preserved).
db-down:
	docker compose down

# ── Full-stack Docker (includes the axentra app service) ─────────────────────
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v

docker-logs:
	docker compose logs -f axentra

# ── Migrations (requires golang-migrate CLI) ─────────────────────────────────
## Run all pending up-migrations.
## Usage: make migrate-up
##        PG_DSN="postgres://..." make migrate-up
migrate-up:
	migrate -path migrations -database "$(PG_DSN)" up

## Roll back the last migration.
migrate-down:
	migrate -path migrations -database "$(PG_DSN)" down 1

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/ coverage.out coverage.html
