package workload

import (
	"testing"
	"time"
)

func TestArrivalDoesNotAccumulateRounding(t *testing.T) {
	if got := arrival(3000000, 3000000, 1, 0, 0); got != time.Second {
		t.Fatalf("million arrivals drifted: %s", got)
	}
}

func TestArrivalIntegratesBurstBoundaries(t *testing.T) {
	// Each ten-second cycle contributes 1.9 arrivals. A naive interval
	// calculated from the instantaneous rate skips subsequent burst windows.
	for i, want := range []time.Duration{0, time.Second, 10100 * time.Millisecond, 12 * time.Second} {
		if got := arrival(uint64(i), .1, 10, 10*time.Second, time.Second); got != want {
			t.Fatalf("arrival %d: got %s, want %s", i, got, want)
		}
	}
}
