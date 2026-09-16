package scenario

import (
	"os"
	"strings"
	"testing"
)

func TestExamples(t *testing.T) {
	b, e := os.ReadFile("../../scenarios/smoke/nats.yaml")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Parse(b); e != nil {
		t.Fatal(e)
	}
	for _, pair := range [][2]string{{"broker: nats", "broker: unknown"}, {"measurement_seconds: 10", "measurement_seconds: 0"}, {"producers: 10", "producers: -1"}, {"payload_bytes: 256", "payload_bytes: 0"}, {"model: open_loop", "model: closed_loop"}, {"repetitions: 1", "repetitions: 0"}, {"profile: fast", "profile: replicated"}, {"channels: 1", "channels: 2"}} {
		t.Run(pair[1], func(t *testing.T) {
			if _, e := Parse([]byte(strings.Replace(string(b), pair[0], pair[1], 1))); e == nil {
				t.Fatal("invalid scenario accepted")
			}
		})
	}
}
func TestMatrixIndependent(t *testing.T) {
	s, e := Load("../../scenarios/smoke/nats.yaml")
	if e != nil {
		t.Fatal(e)
	}
	s.Experiment = "connection_density"
	s.Clients.ActiveProducers = 1
	s.Clients.ActiveConsumers = 1
	s.Matrix = Matrix{"clients", []float64{100, 1000}}
	expanded, e := s.Expand()
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range expanded {
		if n.Traffic.Rate != s.Traffic.Rate {
			t.Fatal("density changed rate")
		}
	}
	s.Experiment = "message_rate"
	s.Matrix = Matrix{"rate", []float64{100, 10000}}
	expanded, e = s.Expand()
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range expanded {
		if n.Clients != s.Clients {
			t.Fatal("rate changed client count")
		}
	}
}
