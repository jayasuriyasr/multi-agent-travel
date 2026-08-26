package schedule

import (
	"fmt"
	"testing"

	"axentra/internal/model"
)

// densWalkGraph builds a walk graph shaped like a dense city interchange
// district: `clusters` groups of `perCluster` stations, every station in a
// cluster linked to every other, and neighbouring clusters bridged.
//
// This is the shape that makes closure expensive — a Shinjuku or a King's
// Cross, not a suburban pair of platforms.
func denseWalkGraph(clusters, perCluster, hopSeconds int) map[string][]model.Footpath {
	g := make(map[string][]model.Footpath)
	name := func(c, i int) string { return fmt.Sprintf("C%d_S%d", c, i) }

	for c := 0; c < clusters; c++ {
		for i := 0; i < perCluster; i++ {
			for j := 0; j < perCluster; j++ {
				if i == j {
					continue
				}
				g[name(c, i)] = append(g[name(c, i)], model.Footpath{
					NeighbourStop: name(c, j), WalkSeconds: hopSeconds,
				})
			}
		}
		if c > 0 {
			g[name(c-1, 0)] = append(g[name(c-1, 0)], model.Footpath{
				NeighbourStop: name(c, 0), WalkSeconds: hopSeconds * 2,
			})
			g[name(c, 0)] = append(g[name(c, 0)], model.Footpath{
				NeighbourStop: name(c-1, 0), WalkSeconds: hopSeconds * 2,
			})
		}
	}
	return g
}

// hubWalkGraph: one interchange every station can reach on foot. Sparse in
// edges (two per spoke) but every source discovers the entire component, so
// node selection rather than edge relaxation dominates.
//
// This is the shape that made the previous scan-for-minimum implementation
// pathological: 800 spokes took 12.3s to close, against 309ms with the heap.
func hubWalkGraph(spokes, hopSeconds int) map[string][]model.Footpath {
	g := make(map[string][]model.Footpath)
	const hub = "HUB"
	for i := 0; i < spokes; i++ {
		s := fmt.Sprintf("S%d", i)
		g[s] = append(g[s], model.Footpath{NeighbourStop: hub, WalkSeconds: hopSeconds})
		g[hub] = append(g[hub], model.Footpath{NeighbourStop: s, WalkSeconds: hopSeconds})
	}
	return g
}

func benchClosure(b *testing.B, clusters, perCluster, hop int) {
	g := denseWalkGraph(clusters, perCluster, hop)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		closeFootpaths(g, DefaultLoadOptions())
	}
}

// Sparse: a handful of platform-to-platform links, the common case.
func BenchmarkCloseFootpaths_Sparse(b *testing.B) { benchClosure(b, 20, 3, 120) }

// Dense: interchange districts where hundreds of stations sit inside one
// walking radius.
func BenchmarkCloseFootpaths_Dense(b *testing.B) { benchClosure(b, 10, 30, 60) }

// Pathological: everything reachable from everything within the walk cap.
func BenchmarkCloseFootpaths_Pathological(b *testing.B) { benchClosure(b, 4, 100, 30) }

// Hub: one shared interchange, the worst case for node selection.
func BenchmarkCloseFootpaths_Hub(b *testing.B) {
	g := hubWalkGraph(800, 60)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		closeFootpaths(g, DefaultLoadOptions())
	}
}
