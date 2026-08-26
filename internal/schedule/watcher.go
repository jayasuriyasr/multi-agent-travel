package schedule

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultWatcherInterval is the fallback poll cadence for schema_version.
// It exists so a silent LISTEN channel cannot leave the buffer stale forever.
const DefaultWatcherInterval = 30 * time.Second

// listenRetryDelay is how long to wait before re-establishing a dropped
// LISTEN connection.
const listenRetryDelay = 5 * time.Second

// WatcherLoop keeps the in-memory schedule in sync with Postgres.
//
// Two independent triggers, either of which causes a reload:
//   - a Postgres NOTIFY on the "schema_changed" channel (near-instant), and
//   - a periodic poll of schema_version.updated_at (the safety net).
//
// The loop returns only when ctx is cancelled, and it does not return until
// the LISTEN goroutine has released its connection — so a caller that waits on
// this function knows the pool is safe to close.
func WatcherLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, opts LoadOptions) {
	if interval <= 0 {
		interval = DefaultWatcherInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastTS time.Time
	slog.Info("schedule watcher started", "interval", interval)

	notifyCh := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		listenForChanges(ctx, pool, notifyCh)
	}()

	reload := func(trigger string) {
		var ts time.Time
		err := pool.QueryRow(ctx, `SELECT updated_at FROM schema_version WHERE id = 1`).Scan(&ts)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("watcher could not read schema_version", "error", err)
			}
			return
		}
		if !ts.After(lastTS) {
			return
		}
		lastTS = ts
		slog.Info("schedule change detected, reloading", "trigger", trigger, "updated_at", ts)
		if err := ReloadRouteArrays(ctx, pool, opts); err != nil && ctx.Err() == nil {
			slog.Error("schedule reload failed", "error", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("schedule watcher stopping")
			wg.Wait()
			return
		case <-notifyCh:
			reload("notify")
		case <-ticker.C:
			reload("poll")
		}
	}
}

// listenForChanges holds a dedicated connection on LISTEN schema_changed and
// forwards notifications, reconnecting on failure.
//
// The reconnect matters: without it a single TCP blip or a Postgres restart
// permanently disables push notifications, and the system silently degrades to
// poll-only with nothing in the logs to say so.
func listenForChanges(ctx context.Context, pool *pgxpool.Pool, out chan<- struct{}) {
	for ctx.Err() == nil {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("watcher could not acquire LISTEN connection; retrying", "error", err, "retry_in", listenRetryDelay)
			sleepCtx(ctx, listenRetryDelay)
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN schema_changed"); err != nil {
			conn.Release()
			if ctx.Err() != nil {
				return
			}
			slog.Warn("watcher LISTEN failed; retrying", "error", err, "retry_in", listenRetryDelay)
			sleepCtx(ctx, listenRetryDelay)
			continue
		}
		slog.Info("watcher listening on schema_changed")

		for {
			_, err := conn.Conn().WaitForNotification(ctx)
			if ctx.Err() != nil {
				conn.Release()
				return
			}
			if err != nil {
				slog.Warn("watcher notification stream broke; reconnecting", "error", err, "retry_in", listenRetryDelay)
				conn.Release()
				sleepCtx(ctx, listenRetryDelay)
				break
			}
			select {
			case out <- struct{}{}:
			default: // a reload is already pending; coalesce
			}
		}
	}
}

// sleepCtx sleeps for d, returning early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
