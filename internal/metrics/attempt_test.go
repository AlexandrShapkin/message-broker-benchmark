package metrics

import "testing"

func TestAttemptBoundsWithoutPerMessageState(t *testing.T) {
	r := New(100)
	for seq := uint64(1); seq <= 10000; seq++ {
		r.Attempt(3, seq)
	}
	if r.Expected(3, 0) || r.Expected(3, 10001) || r.Expected(4, 1) {
		t.Fatal("unsent message considered expected")
	}
	if !r.Expected(3, 1) || !r.Expected(3, 10000) || len(r.attempted) != 1 || r.Snapshot()["sent"] != 10000 {
		t.Fatal("attempt prefix accounting failed")
	}
	r.Attempt(3, 10002)
	if r.Invalid == "" || r.Expected(3, 10002) {
		t.Fatal("non-contiguous prefix accepted")
	}
}
