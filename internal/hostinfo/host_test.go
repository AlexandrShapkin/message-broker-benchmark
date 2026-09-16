package hostinfo

import "testing"

func TestUnavailableSourceIsNotReportedClean(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s := Source()
	if s["dirty"] != nil || s["dirty_status_available"] != false || s["commit"] != nil {
		t.Fatal("unavailable Git metadata must remain unknown", s)
	}
}
