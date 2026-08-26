package schedule

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain silences logging so test and benchmark output shows results rather
// than reload traces.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
