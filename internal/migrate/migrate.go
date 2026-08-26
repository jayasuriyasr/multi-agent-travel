// Package migrate applies ordered SQL migrations to Postgres.
//
// It exists so that "start the app" really does mean the schema is ready. The
// README previously told operators that running the binary applied migrations
// while nothing in the binary did; the only working path was a golang-migrate
// CLI that was never listed as a prerequisite.
//
// Each migration runs inside its own transaction together with the bookkeeping
// insert, so a migration and the record that it ran commit or roll back as one.
package migrate

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration is one versioned .up.sql file.
type Migration struct {
	Version int
	Name    string
	Path    string
	SQL     string
}

const bookkeepingDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

// Up applies every migration in dir that has not been applied yet, in version
// order, and returns how many it ran.
func Up(ctx context.Context, pool *pgxpool.Pool, dir string) (int, error) {
	migrations, err := Load(dir)
	if err != nil {
		return 0, err
	}
	if len(migrations) == 0 {
		slog.Warn("no migrations found", "dir", dir)
		return 0, nil
	}

	if _, err := pool.Exec(ctx, bookkeepingDDL); err != nil {
		return 0, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return 0, err
	}

	// Pre-existing databases were migrated by the golang-migrate CLI and have
	// no bookkeeping rows. Detect that (the tables exist but nothing is
	// recorded) and adopt the current state instead of replaying DDL that would
	// fail on "already exists".
	if len(applied) == 0 {
		adopted, err := adoptExistingSchema(ctx, pool, migrations)
		if err != nil {
			return 0, err
		}
		if adopted {
			slog.Info("adopted pre-existing schema into schema_migrations", "versions", len(migrations))
			return 0, nil
		}
	}

	count := 0
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		if err := applyOne(ctx, pool, m); err != nil {
			return count, err
		}
		slog.Info("migration applied", "version", m.Version, "name", m.Name)
		count++
	}

	if count == 0 {
		slog.Info("schema up to date", "migrations", len(migrations))
	}
	return count, nil
}

// applyOne runs a single migration and records it, atomically.
func applyOne(ctx context.Context, pool *pgxpool.Pool, m Migration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.Version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return fmt.Errorf("migration %d (%s) failed: %w", m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
		m.Version, m.Name); err != nil {
		return fmt.Errorf("record migration %d: %w", m.Version, err)
	}
	return tx.Commit(ctx)
}

// Load reads and parses every *.up.sql file in dir.
func Load(dir string) ([]Migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", dir, err)
	}

	var out []Migration
	seen := make(map[int]string)

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		version, label, err := parseName(name)
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", name, err)
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d: %q and %q", version, prev, name)
		}
		seen[version] = name

		path := filepath.Join(dir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", path, err)
		}
		if strings.TrimSpace(string(body)) == "" {
			return nil, fmt.Errorf("migration %q is empty", name)
		}
		out = append(out, Migration{Version: version, Name: label, Path: path, SQL: string(body)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// parseName splits "007_add_arrival_unix.up.sql" into (7, "add_arrival_unix").
func parseName(filename string) (int, string, error) {
	base := strings.TrimSuffix(filename, ".up.sql")
	idx := strings.Index(base, "_")
	if idx <= 0 {
		return 0, "", fmt.Errorf("expected <version>_<name>.up.sql")
	}
	version, err := strconv.Atoi(base[:idx])
	if err != nil {
		return 0, "", fmt.Errorf("version prefix %q is not a number", base[:idx])
	}
	return version, base[idx+1:], nil
}

// appliedVersions reads the set of migrations already recorded.
func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()

	out := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan schema_migrations row: %w", err)
		}
		out[v] = true
	}
	return out, rows.Err()
}

// adoptExistingSchema records all migrations as applied when the schema is
// clearly already in place but unrecorded, and reports whether it did so.
func adoptExistingSchema(ctx context.Context, pool *pgxpool.Pool, migrations []Migration) (bool, error) {
	// Resolved against the connection's search_path rather than a hard-coded
	// "public", so a deployment that isolates Axentra in its own schema is
	// probed correctly instead of seeing another schema's tables.
	var exists bool
	err := pool.QueryRow(ctx, `SELECT to_regclass('stop_times') IS NOT NULL`).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("probe existing schema: %w", err)
	}
	if !exists {
		return false, nil
	}
	for _, m := range migrations {
		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)
			 ON CONFLICT (version) DO NOTHING`, m.Version, m.Name); err != nil {
			return false, fmt.Errorf("adopt migration %d: %w", m.Version, err)
		}
	}
	return true, nil
}
