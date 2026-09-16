package workload

import (
	"context"
	"testing"
	"time"
)

func TestAbsoluteSchedule(t *testing.T) {
	start := time.Now()
	var times []time.Time
	n := Schedule(context.Background(), start, 30*time.Millisecond, 100, 1, 0, 0, func(d time.Time) { times = append(times, d) })
	if n != 3 {
		t.Fatalf("arrivals %d", n)
	}
	for i, d := range times {
		if d.Sub(start) != time.Duration(i)*10*time.Millisecond {
			t.Fatal("deadline drift")
		}
	}
}
func TestCancellationAndIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := Schedule(ctx, time.Now(), time.Hour, 0, 1, 0, 0, func(time.Time) { t.Fatal("idle emit") }); n != 0 {
		t.Fatal(n)
	}
}
