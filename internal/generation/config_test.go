package generation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestResolveSeedAndDate(t *testing.T) {
	seed := uint64(0)
	cfg, err := Resolve(&seed, "2026-11-21", 4, 2)
	if err != nil || cfg.Seed != 0 || cfg.AsOf.Format("2006-01-02") != "2026-11-21" {
		t.Fatalf("%+v, %v", cfg, err)
	}
	for _, tc := range []struct {
		date          string
		rows, workers int
	}{{"invalid", 4, 2}, {"2026-02-30", 4, 2}, {"2026-11-21", 0, 2}, {"2026-11-21", 4, 0}} {
		if _, err := Resolve(&seed, tc.date, tc.rows, tc.workers); err == nil {
			t.Fatalf("accepted invalid config %+v", tc)
		}
	}
	if cfg.StreamSeed("a", 0) == cfg.StreamSeed("b", 0) || cfg.StreamSeed("a", 0) == cfg.StreamSeed("a", 1) {
		t.Fatal("streams collide")
	}
}

func TestPartitionFailureCancelsPendingWork(t *testing.T) {
	seed := uint64(0)
	cfg, err := Resolve(&seed, "2026-11-21", 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("write failed")
	var calls atomic.Int32
	err = Partitions(t.Context(), 100, cfg, func(context.Context, Partition) error { calls.Add(1); return want })
	if !errors.Is(err, want) || calls.Load() != 1 {
		t.Fatalf("error=%v, calls=%d", err, calls.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Partitions(ctx, 100, cfg, func(context.Context, Partition) error { t.Fatal("ran canceled job"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
