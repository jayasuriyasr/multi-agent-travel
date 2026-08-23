# ── Build Stage ─────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Cache dependency downloads
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /axentra ./cmd/axentra

# ── Runtime Stage ──────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /axentra /usr/local/bin/axentra
COPY --from=builder /app/web /web

# Non-root user
RUN adduser -D -u 1000 axentra
USER axentra

WORKDIR /

EXPOSE 8080

ENTRYPOINT ["axentra"]
