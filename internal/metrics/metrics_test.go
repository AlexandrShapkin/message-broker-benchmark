package metrics

import "testing"

func TestSequenceRanges(t *testing.T) {
	s := Sequence{}
	for _, n := range []uint64{1, 3, 2, 5, 4} {
		dup, _, e := s.Add(n, 3)
		if dup || e != nil {
			t.Fatal(n, e)
		}
	}
	if len(s.ranges) != 1 {
		t.Fatal(s.ranges)
	}
	if d, _, _ := s.Add(2, 3); !d {
		t.Fatal("duplicate missed")
	}
	s = Sequence{}
	s.Add(1, 1)
	if _, _, e := s.Add(3, 1); e == nil {
		t.Fatal("unbounded fragmentation")
	}
}
func TestEnvelope(t *testing.T) {
	var token [16]byte
	token[0] = 7
	b := Encode(64, token, 3, 12, 999, 2)
	p, s, d, phase, e := Decode(b, token)
	if e != nil || p != 3 || s != 12 || d != 999 || phase != 2 {
		t.Fatal("roundtrip")
	}
	b[63] ^= 1
	if _, _, _, _, e = Decode(b, token); e == nil {
		t.Fatal("payload corruption accepted")
	}
	b[63] ^= 1
	token[0]++
	if _, _, _, _, e = Decode(b, token); e == nil {
		t.Fatal("cross-run accepted")
	}
}
