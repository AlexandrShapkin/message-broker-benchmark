// Package driver defines broker-independent workload contracts.
package driver

import (
	"context"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
)

type Capabilities struct {
	Persistence          bool   `json:"persistence"`
	PublisherAck         bool   `json:"publisher_acknowledgements"`
	ConsumerAck          bool   `json:"explicit_consumer_acknowledgements"`
	Competing            bool   `json:"competing_consumers"`
	Broadcast            bool   `json:"independent_subscribers"`
	Partitions           bool   `json:"partitions"`
	Ordering             string `json:"ordering"`
	QoS                  bool   `json:"configurable_qos"`
	Replication          bool   `json:"replication"`
	DurableSubscriptions bool   `json:"durable_subscriptions"`
	NativeMetrics        bool   `json:"native_metrics"`
}
type Message struct {
	Data []byte
	Ack  func(context.Context) error
}
type Producer interface {
	Publish(context.Context, []byte) error
	Close() error
}
type Consumer interface {
	Receive(context.Context) (Message, error)
	Close() error
}

type idle struct{ Producer }

func (i *idle) Receive(ctx context.Context) (Message, error) {
	<-ctx.Done()
	return Message{}, ctx.Err()
}

// Idle retains a real transport client without subscription/group membership.
func Idle(p Producer) Consumer { return &idle{p} }

type Driver interface {
	Name() string
	Capabilities() Capabilities
	Settings() map[string]any
	Prepare(context.Context) error
	NewProducer(context.Context, int) (Producer, error)
	NewConsumer(context.Context, int) (Consumer, error)
	Cleanup(context.Context) error
}
type Factory func(scenario.Scenario, string) (Driver, error)
type Registry map[string]Factory

func (r Registry) New(s scenario.Scenario, id string) (Driver, error) {
	f, ok := r[s.SUT.Broker]
	if !ok {
		return nil, fmt.Errorf("unknown broker %q", s.SUT.Broker)
	}
	d, e := f(s, id)
	if e != nil {
		return nil, e
	}
	c := d.Capabilities()
	if s.SUT.Profile == "durable" && !c.Persistence {
		return nil, fmt.Errorf("%s does not support durable profile", d.Name())
	}
	if s.Topology.Delivery == "competing" && !c.Competing {
		return nil, fmt.Errorf("%s does not support competing delivery", d.Name())
	}
	if s.Topology.Delivery == "broadcast" && !c.Broadcast {
		return nil, fmt.Errorf("%s does not support broadcast", d.Name())
	}
	return d, nil
}
