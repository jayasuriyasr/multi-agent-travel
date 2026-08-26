# ── Build ────────────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Dependencies first, so a source-only change reuses the cached layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Vet and test at build time: an image that cannot pass its own tests should
# never reach a registry.
RUN go vet ./... && go test ./...

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /axentra ./cmd/axentra

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata wget

COPY --from=builder /axentra /usr/local/bin/axentra
COPY --from=builder /app/web /web
# Migrations ship with the image so AUTO_MIGRATE works in a container.
COPY --from=builder /app/migrations /migrations

RUN adduser -D -u 1000 axentra
USER axentra

WORKDIR /

ENV WEB_ROOT=/web \
    MIGRATIONS_DIR=/migrations \
    FIBER_PORT=8080

EXPOSE 8080

# Readiness, not liveness: the container should only take traffic once cold
# start has finished.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/healthz/ready >/dev/null || exit 1

ENTRYPOINT ["axentra"]
