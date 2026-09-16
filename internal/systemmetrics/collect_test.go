package systemmetrics

import "testing"

func TestTCPCounts(t *testing.T) {
	row := map[string]any{"files": map[string]string{"/proc/net/tcp": "0: 0100007F:107E 0100007F:C001 01\n1: 0100007F:201E 0100007F:C002 01\n2: 00000000:107E 00000000:0000 0A\n"}}
	n, ok := BrokerConnections(row, "nats")
	if !ok || n != 1 {
		t.Fatal(n, ok)
	}
	if _, ok = BrokerConnections(map[string]any{}, "nats"); ok {
		t.Fatal("missing data became zero")
	}
}
func TestProcessResources(t *testing.T) {
	m := Process()
	if m["rss_bytes"] == nil || m["cpu_ticks"] == nil || m["file_descriptors"] == nil {
		t.Fatal(m)
	}
}
