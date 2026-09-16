package metrics

import "testing"

func TestEnvelopeRejectsImpossiblePhaseAndDeadline(t *testing.T) {
	var token [16]byte
	for _, tc := range []struct {
		phase    uint32
		deadline int64
	}{{0, 0}, {3, 0}, {2, -1}} {
		b := Encode(Header, token, 0, 1, tc.deadline, tc.phase)
		if _, _, _, _, err := Decode(b, token); err == nil {
			t.Fatalf("invalid envelope accepted: %+v", tc)
		}
	}
}
