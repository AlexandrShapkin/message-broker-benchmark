package integration

import (
	"context"
	"encoding/json"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/kafka"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/mosquitto"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/nats"
	"github.com/message-broker-benchmark/message-broker-benchmark/drivers/rabbitmq"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/runner"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBrokerSmoke(t *testing.T) {
	if os.Getenv("BENCH_INTEGRATION") != "1" {
		t.Skip("set BENCH_INTEGRATION=1 on a Docker host")
	}
	if _, e := exec.LookPath("docker"); e != nil {
		t.Fatal(e)
	}
	registry := driver.Registry{"nats": nats.New, "mosquitto": mosquitto.New, "rabbitmq": rabbitmq.New, "kafka": kafka.New}
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	for _, broker := range scenario.Brokers() {
		t.Run(broker, func(t *testing.T) {
			s, e := scenario.Load(filepath.Join(root, "scenarios/smoke", broker+".yaml"))
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			dir, e := runner.Run(ctx, root, t.TempDir(), s, registry, 1)
			if e != nil {
				t.Fatal(e, dir)
			}
			b, e := os.ReadFile(filepath.Join(dir, "correctness/summary.json"))
			if e != nil {
				t.Fatal(e)
			}
			var c map[string]uint64
			if e = json.Unmarshal(b, &c); e != nil {
				t.Fatal(e)
			}
			if c["delivered"] == 0 || c["lost"] > 0 || c["errors"] > 0 {
				t.Fatal(c)
			}
		})
	}
}

// TestBrokerProfilesAndRecovery exercises real adapters, including their native
// reconnect behavior. It never substitutes performance from an in-memory driver.
func TestBrokerProfilesAndRecovery(t *testing.T) {
	if os.Getenv("BENCH_RECOVERY") != "1" {
		t.Skip("set BENCH_RECOVERY=1 for real profile/recovery contract tests")
	}
	registry := driver.Registry{"nats": nats.New, "mosquitto": mosquitto.New, "rabbitmq": rabbitmq.New, "kafka": kafka.New}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, broker := range scenario.Brokers() {
		for _, profile := range []string{"fast", "durable"} {
			for _, experiment := range []string{"many_to_many", "reconnect_storm", "broker_restart", "mostly_idle"} {
				t.Run(broker+"/"+profile+"/"+experiment, func(t *testing.T) {
					s, err := scenario.Load(filepath.Join(root, "scenarios/smoke", broker+".yaml"))
					if err != nil {
						t.Fatal(err)
					}
					s.Name, s.Experiment, s.SUT.Profile = "contract-"+experiment, experiment, profile
					s.Clients.Producers, s.Clients.Consumers = 2, 2
					s.Connection.OperationTimeoutMS = 5000
					s.Timing.Measurement, s.Timing.Cooldown = 20, 3
					if experiment == "many_to_many" {
						s.Timing.Measurement = 5
					} else if experiment == "mostly_idle" {
						s.Traffic.Rate = 0
						s.Clients.ActiveProducers, s.Clients.ActiveConsumers = 1, 1
						if broker == "kafka" {
							// Exceed franz-go's recorded 60 s idle timeout: a
							// shorter test cannot verify transport keepalives.
							s.Timing.Measurement = 70
						}
					} else {
						s.Fault.AtSeconds, s.Fault.DowntimeSeconds, s.Fault.Fraction = 5, 1, 1
						if broker == "kafka" && experiment == "broker_restart" {
							// KRaft plus offset-partition recovery exceeds the 15 s
							// post-fault window on a laptop. Observe resumed traffic.
							s.Timing.Measurement = 60
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
					defer cancel()
					output := os.Getenv("BENCH_TEST_RESULTS")
					if output == "" {
						output = t.TempDir()
					}
					dir, err := runner.Run(ctx, root, output, s, registry, 1)
					t.Log("raw run:", dir)
					if err != nil {
						t.Fatal(err)
					}
					b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
					if err != nil {
						t.Fatal(err)
					}
					var m struct {
						Workload map[string]any `json:"workload"`
						Result   struct {
							Counters map[string]uint64 `json:"counters"`
						} `json:"result"`
					}
					if err = json.Unmarshal(b, &m); err != nil {
						t.Fatal(err)
					}
					c := m.Result.Counters
					if c["unexpected"] != 0 || m.Workload["actual_connections"] == nil {
						t.Fatal("invalid delivery or unavailable transport evidence", c)
					}
					if experiment == "mostly_idle" {
						if c["sent"] != 0 || m.Workload["actual_connections"].(float64) < 4 {
							t.Fatal("idle transports were not retained", m.Workload)
						}
					} else if c["delivered"] == 0 {
						t.Fatal("no delivery", c)
					}
					if experiment == "many_to_many" && (c["lost"] != 0 || c["errors"] != 0 || c["duplicate"] != 0) {
						t.Fatal("steady-state delivery failed", c)
					}
					if experiment == "reconnect_storm" && c["reconnections"] != 4 {
						t.Fatal("clients failed to reconnect", c)
					}
					if experiment == "broker_restart" || experiment == "reconnect_storm" {
						b, err := os.ReadFile(filepath.Join(dir, "metrics/generator.jsonl"))
						if err != nil {
							t.Fatal(err)
						}
						var first, last uint64
						for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
							var row struct {
								Time     time.Time         `json:"time"`
								Counters map[string]uint64 `json:"counters"`
							}
							if err := json.Unmarshal([]byte(line), &row); err != nil {
								t.Fatal(err)
							}
							start, _ := time.Parse(time.RFC3339Nano, m.Workload["measurement_started_at_utc"].(string))
							if row.Time.After(start.Add(15 * time.Second)) {
								if first == 0 {
									first = row.Counters["delivered"]
								}
								last = row.Counters["delivered"]
							}
						}
						if last <= first {
							t.Fatal("delivery did not resume after fault", c)
						}
					}
				})
			}
		}
	}
}
