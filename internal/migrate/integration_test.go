package migrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool gives each test an isolated Postgres schema, so runs never collide.
// Skipped unless PG_TEST_DSN is set.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("set PG_TEST_DSN to run migration integration tests")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("cannot connect to PG_TEST_DSN: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		t.Skipf("PG_TEST_DSN not reachable: %v", err)
	}

	schema := fmt.Sprintf("migrate_test_%d_%d", os.Getpid(), len(t.Name()))
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	admin.Close()

	pool, err := pgxpool.New(ctx, dsn+"&search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		pool.Close()
	})
	return pool
}

func repoMigrations(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "..", "migrations")
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("migrations directory not available: %v", err)
	}
	return dir
}

func TestUp_AppliesAndIsIdempotent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	dir := repoMigrations(t)

	want, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	applied, err := Up(ctx, pool, dir)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if applied != len(want) {
		t.Fatalf("applied %d migrations, want %d", applied, len(want))
	}

	// Every table the engine queries must now exist.
	for _, table := range []string{"stations", "routes", "trips", "stop_times", "schema_version", "footpaths"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("table %q was not created", table)
		}
	}

	// Running again must be a no-op, so AUTO_MIGRATE is safe on every boot and
	// on every replica starting at once.
	again, err := Up(ctx, pool, dir)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if again != 0 {
		t.Fatalf("second run applied %d migrations, want 0", again)
	}
}

func TestUp_RecordsVersionsInOrder(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	if _, err := Up(ctx, pool, repoMigrations(t)); err != nil {
		t.Fatal(err)
	}

	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var prev int
	var count int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v <= prev {
			t.Errorf("versions must be strictly increasing, saw %d after %d", v, prev)
		}
		prev = v
		count++
	}
	if count == 0 {
		t.Fatal("no migrations recorded")
	}
}

// A failing migration must leave no trace: neither its DDL nor its bookkeeping
// row, so a retry starts from a clean, known state.
func TestUp_FailedMigrationIsAtomic(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("001_ok.up.sql", `CREATE TABLE ok_table (id INT PRIMARY KEY);`)
	write("002_broken.up.sql", `CREATE TABLE broken (id INT); SELECT this_function_does_not_exist();`)

	applied, err := Up(ctx, pool, dir)
	if err == nil {
		t.Fatal("want an error from the broken migration")
	}
	if applied != 1 {
		t.Errorf("want 1 successful migration before the failure, got %d", applied)
	}

	var brokenExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('broken') IS NOT NULL`).Scan(&brokenExists); err != nil {
		t.Fatal(err)
	}
	if brokenExists {
		t.Error("a failed migration must roll back the tables it created")
	}

	var recorded int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version = 2`).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != 0 {
		t.Error("a failed migration must not be recorded as applied")
	}
}

// A database migrated by an external tool has the tables but no bookkeeping.
// Replaying the DDL would fail on "already exists", so the state is adopted.
func TestUp_AdoptsExistingSchema(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	dir := repoMigrations(t)

	all, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all {
		if _, err := pool.Exec(ctx, m.SQL); err != nil {
			t.Fatalf("pre-apply %d: %v", m.Version, err)
		}
	}

	applied, err := Up(ctx, pool, dir)
	if err != nil {
		t.Fatalf("adopting an existing schema should succeed: %v", err)
	}
	if applied != 0 {
		t.Errorf("nothing should be re-applied, got %d", applied)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(all) {
		t.Errorf("recorded %d versions, want %d", count, len(all))
	}
}
