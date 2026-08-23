package main

import (
	"context"
	"fmt"

	"axentra/internal/config"
	"axentra/internal/model"
	"axentra/internal/raptor"
	"axentra/internal/schedule"
	"axentra/internal/seat"
)

func main() {
	ctx := context.Background()
	pool := config.InitPostgres(ctx, "postgres://axentra_user:axentra_pass@localhost:5432/axentra_db?sslmode=disable")
	defer pool.Close()

	rdb := config.InitRedis(ctx, "redis://localhost:6379/0")
	defer rdb.Close()

	if err := schedule.ReloadRouteArrays(ctx, pool); err != nil {
		fmt.Println("Reload error:", err)
		return
	}
	if err := seat.ColdStart(ctx, rdb); err != nil {
		fmt.Println("ColdStart error:", err)
	}

	params := model.SearchParams{
		Origin:      "STA-001",
		Destination: "STA-004",
		Date:        "2026-07-07",
		DepTime:     1783407600, 
		SeatClass:   "lower",
		Passengers:  1,
	}

	candidates := raptor.RaptorSearch(params, 100)
	fmt.Printf("RaptorSearch found %d paths\n", len(candidates))

	final := raptor.ValidateAndTruncate(ctx, rdb, candidates, params.SeatClass, params.Passengers, 5)
	fmt.Printf("ValidateAndTruncate returned %d paths\n", len(final))
}
