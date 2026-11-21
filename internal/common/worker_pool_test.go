package common

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPoolExecutesOnce(t *testing.T) {
	p := NewGenericWorkerPool(2)
	var calls, active atomic.Int32
	fs := make([]Future, 100)
	for i := range fs {
		fs[i] = p.Submit(NewSimpleTask(func(context.Context) (interface{}, error) {
			if active.Add(1) > 2 {
				t.Error("worker limit exceeded")
			}
			defer active.Add(-1)
			calls.Add(1)
			return i, nil
		}))
	}
	for i, f := range fs {
		v, err := f.Get()
		if err != nil || v != i {
			t.Fatalf("task %d got %v, %v", i, v, err)
		}
	}
	p.Close()
	if calls.Load() != 100 {
		t.Fatalf("calls %d", calls.Load())
	}
}
func TestPoolCloseDuringSubmit(t *testing.T) {
	p := NewGenericWorkerPool(1)
	task := NewSimpleTask(func(ctx context.Context) (interface{}, error) { <-ctx.Done(); return nil, ctx.Err() })
	first := p.Submit(task)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Submit(task).Get()
			if err != context.Canceled {
				t.Errorf("unexpected result %v", err)
			}
		}()
	}
	p.Close()
	p.Close()
	wg.Wait()
	if _, err := first.Get(); err != context.Canceled {
		t.Fatal(err)
	}
}
