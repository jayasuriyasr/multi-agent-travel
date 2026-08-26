package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestLoad_OrdersByVersionNotFilename(t *testing.T) {
	// Lexicographic order would put 10 before 2.
	dir := writeDir(t, map[string]string{
		"002_second.up.sql":  "SELECT 2;",
		"010_tenth.up.sql":   "SELECT 10;",
		"001_first.up.sql":   "SELECT 1;",
		"001_first.down.sql": "SELECT -1;", // down files are ignored
		"notes.txt":          "ignore me",
	})

	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d migrations, want 3", len(got))
	}
	want := []int{1, 2, 10}
	for i, m := range got {
		if m.Version != want[i] {
			t.Errorf("position %d has version %d, want %d", i, m.Version, want[i])
		}
	}
	if got[0].Name != "first" {
		t.Errorf("name = %q, want \"first\"", got[0].Name)
	}
}

func TestLoad_RejectsBadInput(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{
			name:    "duplicate version",
			files:   map[string]string{"001_a.up.sql": "SELECT 1;", "001_b.up.sql": "SELECT 2;"},
			wantErr: "duplicate migration version",
		},
		{
			name:    "non-numeric version prefix",
			files:   map[string]string{"abc_a.up.sql": "SELECT 1;"},
			wantErr: "not a number",
		},
		{
			name:    "no version prefix",
			files:   map[string]string{"createtables.up.sql": "SELECT 1;"},
			wantErr: "expected <version>_<name>.up.sql",
		},
		{
			name:    "empty migration",
			files:   map[string]string{"001_a.up.sql": "   \n"},
			wantErr: "is empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeDir(t, tc.files))
			if err == nil {
				t.Fatalf("want an error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want an error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoad_MissingDirectory(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("want an error for a missing migrations directory")
	}
}

func TestLoad_EmptyDirectory(t *testing.T) {
	got, err := Load(t.TempDir())
	if err != nil {
		t.Fatalf("an empty directory is not an error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 migrations, got %d", len(got))
	}
}

func TestParseName(t *testing.T) {
	version, name, err := parseName("007_add_arrival_unix.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if version != 7 || name != "add_arrival_unix" {
		t.Fatalf("got (%d, %q), want (7, \"add_arrival_unix\")", version, name)
	}
}

// The repository's own migrations must always load cleanly — this is what
// makes AUTO_MIGRATE safe to leave on by default.
func TestLoad_RepositoryMigrations(t *testing.T) {
	dir := filepath.Join("..", "..", "migrations")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skip("migrations directory not present")
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatalf("the shipped migrations must load: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one shipped migration")
	}
	for i, m := range got {
		if i > 0 && m.Version <= got[i-1].Version {
			t.Errorf("versions must be strictly increasing: %d then %d", got[i-1].Version, m.Version)
		}
	}
}
