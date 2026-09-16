package scenario

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/message-broker-benchmark/message-broker-benchmark/schemas"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

type Scenario struct {
	SchemaVersion int        `json:"schema_version" yaml:"schema_version"`
	Name          string     `json:"name" yaml:"name"`
	Experiment    string     `json:"experiment" yaml:"experiment"`
	SUT           SUT        `json:"sut" yaml:"sut"`
	Clients       Clients    `json:"clients" yaml:"clients"`
	Traffic       Traffic    `json:"traffic" yaml:"traffic"`
	Topology      Topology   `json:"topology" yaml:"topology"`
	Connection    Connection `json:"connection" yaml:"connection"`
	Timing        Timing     `json:"timing" yaml:"timing"`
	Collection    Collection `json:"collection" yaml:"collection"`
	Repetitions   int        `json:"repetitions" yaml:"repetitions"`
	RandomSeed    int64      `json:"random_seed" yaml:"random_seed"`
	Fault         Fault      `json:"fault" yaml:"fault"`
	Matrix        Matrix     `json:"matrix" yaml:"matrix"`
}
type SUT struct {
	ImageDigest string `json:"image_digest" yaml:"image_digest"`
	Broker      string `json:"broker" yaml:"broker"`
	Profile     string `json:"profile" yaml:"profile"`
	Mode        string `json:"mode" yaml:"mode"`
}
type Clients struct {
	ActiveProducers int `json:"active_producers" yaml:"active_producers"`
	ActiveConsumers int `json:"active_consumers" yaml:"active_consumers"`
	Producers       int `json:"producers" yaml:"producers"`
	Consumers       int `json:"consumers" yaml:"consumers"`
}
type Traffic struct {
	Model                string  `json:"model" yaml:"model"`
	Rate                 float64 `json:"total_rate_messages_per_sec" yaml:"total_rate_messages_per_sec"`
	PayloadBytes         int     `json:"payload_bytes" yaml:"payload_bytes"`
	ConsumerDelayMS      int     `json:"consumer_delay_ms" yaml:"consumer_delay_ms"`
	QueueCapacity        int     `json:"queue_capacity" yaml:"queue_capacity"`
	RateTolerance        float64 `json:"rate_tolerance" yaml:"rate_tolerance"`
	BurstMultiplier      float64 `json:"burst_multiplier" yaml:"burst_multiplier"`
	BurstEverySeconds    int     `json:"burst_every_seconds" yaml:"burst_every_seconds"`
	BurstDurationSeconds int     `json:"burst_duration_seconds" yaml:"burst_duration_seconds"`
}
type Topology struct {
	Type     string `json:"type" yaml:"type"`
	Channels int    `json:"channels" yaml:"channels"`
	Delivery string `json:"delivery" yaml:"delivery"`
}
type Connection struct {
	Ramp               int `json:"ramp_clients_per_second" yaml:"ramp_clients_per_second"`
	TimeoutSeconds     int `json:"timeout_seconds" yaml:"timeout_seconds"`
	OperationTimeoutMS int `json:"operation_timeout_ms" yaml:"operation_timeout_ms"`
}
type Timing struct {
	Warmup      int `json:"warmup_seconds" yaml:"warmup_seconds"`
	Measurement int `json:"measurement_seconds" yaml:"measurement_seconds"`
	Cooldown    int `json:"cooldown_seconds" yaml:"cooldown_seconds"`
}
type Collection struct {
	SystemIntervalMS  int  `json:"system_interval_ms" yaml:"system_interval_ms"`
	LatencyHistograms bool `json:"latency_histograms" yaml:"latency_histograms"`
	OperationSamples  bool `json:"operation_samples" yaml:"operation_samples"`
	MaxIntervals      int  `json:"max_intervals" yaml:"max_intervals"`
}
type Fault struct {
	RepeatSeconds   int     `json:"repeat_seconds" yaml:"repeat_seconds"`
	AtSeconds       int     `json:"at_seconds" yaml:"at_seconds"`
	Fraction        float64 `json:"fraction" yaml:"fraction"`
	DowntimeSeconds int     `json:"downtime_seconds" yaml:"downtime_seconds"`
}
type Matrix struct {
	Axis   string    `json:"axis" yaml:"axis"`
	Values []float64 `json:"values" yaml:"values"`
}

func ValidateDocument(name string, value any) error {
	b, err := schemas.Files.ReadFile(name)
	if err != nil {
		return err
	}
	var doc any
	if err = json.Unmarshal(b, &doc); err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	if err = c.AddResource("schema.json", doc); err != nil {
		return err
	}
	s, err := c.Compile("schema.json")
	if err != nil {
		return err
	}
	return s.Validate(value)
}
func Parse(b []byte) (Scenario, error) {
	var s Scenario
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if err := d.Decode(&s); err != nil {
		return s, fmt.Errorf("scenario YAML: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return s, fmt.Errorf("only one YAML document is allowed")
	}
	var raw any
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return s, err
	}
	jb, err := json.Marshal(raw)
	if err != nil {
		return s, err
	}
	if err = json.Unmarshal(jb, &raw); err != nil {
		return s, err
	}
	if err = ValidateDocument("scenario.schema.json", raw); err != nil {
		return s, err
	}
	if err := s.Validate(); err != nil {
		return s, err
	}
	return s, nil
}
func Load(path string) (Scenario, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return Scenario{}, e
	}
	return Parse(b)
}
func (s Scenario) Validate() error {
	if s.Clients.ActiveProducers > s.Clients.Producers || s.Clients.ActiveConsumers > s.Clients.Consumers {
		return fmt.Errorf("active client counts exceed total logical clients")
	}
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	var value any
	if e = json.Unmarshal(b, &value); e != nil {
		return e
	}
	if e = ValidateDocument("scenario.schema.json", value); e != nil {
		return fmt.Errorf("scenario: %w", e)
	}
	if s.Topology.Type == "one_to_many" && s.Clients.Producers != 1 {
		return fmt.Errorf("one_to_many requires exactly one producer")
	}
	if s.Topology.Type == "many_to_one" && s.Clients.Consumers != 1 {
		return fmt.Errorf("many_to_one requires exactly one consumer")
	}
	if s.Traffic.BurstMultiplier > 1 && (s.Traffic.BurstEverySeconds <= 0 || s.Traffic.BurstDurationSeconds <= 0 || s.Traffic.BurstDurationSeconds >= s.Traffic.BurstEverySeconds) {
		return fmt.Errorf("burst duration must be positive and less than burst period")
	}
	if s.Experiment == "reconnect_storm" || s.Experiment == "broker_restart" {
		if s.Fault.RepeatSeconds > 0 && (s.Experiment != "reconnect_storm" || s.Fault.RepeatSeconds <= s.Fault.DowntimeSeconds) {
			return fmt.Errorf("repeat_seconds is supported for reconnect only and must exceed downtime")
		}
		if s.Fault.AtSeconds <= 0 || s.Fault.AtSeconds >= s.Timing.Measurement || s.Fault.DowntimeSeconds >= s.Timing.Measurement-s.Fault.AtSeconds {
			return fmt.Errorf("fault must fit inside measurement window")
		}
		if s.Experiment == "reconnect_storm" && s.Fault.Fraction <= 0 {
			return fmt.Errorf("reconnect fraction must be positive")
		}
	}
	if s.Experiment == "slow_consumer" && s.Traffic.ConsumerDelayMS == 0 {
		return fmt.Errorf("slow_consumer requires consumer_delay_ms > 0")
	}
	if s.Matrix.Axis != "" {
		if s.Matrix.Axis == "clients" && (s.Clients.ActiveProducers == 0 || s.Clients.ActiveConsumers == 0) {
			return fmt.Errorf("density matrix requires fixed positive active_producers and active_consumers to keep traffic topology constant")
		}
		if len(s.Matrix.Values) == 0 {
			return fmt.Errorf("matrix values cannot be empty")
		}
		allowed := map[string]string{"connection_density": "clients", "mostly_idle": "clients", "message_rate": "rate", "payload_size": "payload", "fan_out": "consumers"}
		if allowed[s.Experiment] != s.Matrix.Axis {
			return fmt.Errorf("matrix axis %s is incompatible with experiment %s", s.Matrix.Axis, s.Experiment)
		}
	}
	return nil
}
func (s Scenario) Expand() ([]Scenario, error) {
	if s.Matrix.Axis == "" {
		return []Scenario{s}, s.Validate()
	}
	out := []Scenario{}
	for _, v := range s.Matrix.Values {
		n := s
		n.Matrix = Matrix{}
		switch s.Matrix.Axis {
		case "clients":
			if v != float64(int(v)) || int(v) < 2 || int(v)%2 != 0 {
				return nil, fmt.Errorf("clients matrix requires even integer totals >= 2")
			}
			n.Clients.Producers = int(v) / 2
			n.Clients.Consumers = int(v) / 2
		case "rate":
			n.Traffic.Rate = v
		case "payload":
			if v != float64(int(v)) {
				return nil, fmt.Errorf("payload must be integral")
			}
			n.Traffic.PayloadBytes = int(v)
		case "consumers":
			if v != float64(int(v)) {
				return nil, fmt.Errorf("consumers must be integral")
			}
			n.Clients.Consumers = int(v)
		}
		if err := n.Validate(); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
func Brokers() []string { return strings.Fields("nats mosquitto rabbitmq kafka") }

func (s Scenario) ActiveProducers() int {
	if s.Clients.ActiveProducers == 0 {
		return s.Clients.Producers
	}
	return s.Clients.ActiveProducers
}
func (s Scenario) ActiveConsumers() int {
	if s.Clients.ActiveConsumers == 0 {
		return s.Clients.Consumers
	}
	return s.Clients.ActiveConsumers
}
