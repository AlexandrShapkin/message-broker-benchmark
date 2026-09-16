package nats

import (
	"context"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"github.com/nats-io/nats.go"
	"time"
)

type Driver struct {
	s       scenario.Scenario
	subject string
}

func New(s scenario.Scenario, id string) (driver.Driver, error) {
	return &Driver{s: s, subject: "bench." + id}, nil
}
func (d *Driver) Name() string { return "nats" }
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Persistence: true, PublisherAck: true, ConsumerAck: true, Competing: true, Broadcast: true, Ordering: "per producer per subscriber", QoS: true, DurableSubscriptions: true, NativeMetrics: true}
}
func (d *Driver) Settings() map[string]any {
	durable := d.s.SUT.Profile == "durable"
	ack := "server flush (not persistence acknowledgement)"
	mode := "Core NATS"
	if durable {
		ack = "JetStream publish acknowledgement"
		mode = "JetStream file storage"
	}
	return map[string]any{"process_name": "nats-server", "protocol_port": 4222, "protocol": "NATS", "client_library": "github.com/nats-io/nats.go", "client_library_version": "v1.47.0", "mode": mode, "publisher_ack": ack, "consumer_ack": durable, "persistence": durable, "replicas": 1, "subject": d.subject, "max_reconnects": -1, "reconnect_wait_ms": 100, "pending_messages": 65536, "pending_bytes": 67108864, "batching": "none at runner; client buffering with per-publish flush", "compression": "none", "stream_max_bytes": 1073741824, "stream_max_age_seconds": 86400}
}
func (d *Driver) connect() (*nats.Conn, error) {
	return nats.Connect("nats://127.0.0.1:4222", nats.Timeout(time.Duration(d.s.Connection.OperationTimeoutMS)*time.Millisecond), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.ReconnectBufSize(0))
}
func (d *Driver) Prepare(ctx context.Context) error {
	c, e := d.connect()
	if e != nil {
		return e
	}
	defer c.Close()
	if d.s.SUT.Profile == "durable" {
		js, e := c.JetStream()
		if e != nil {
			return e
		}
		_, e = js.AddStream(&nats.StreamConfig{Name: d.stream(), Subjects: []string{d.subject}, Storage: nats.FileStorage, Replicas: 1, Retention: nats.LimitsPolicy, MaxBytes: 1 << 30, MaxAge: 24 * time.Hour}, nats.Context(ctx))
		return e
	}
	return nil
}
func (d *Driver) stream() string { return "B_" + d.subject[6:] }

type producer struct {
	c       *nats.Conn
	js      nats.JetStreamContext
	subject string
}

func (d *Driver) NewProducer(ctx context.Context, id int) (driver.Producer, error) {
	c, e := d.connect()
	if e != nil {
		return nil, e
	}
	p := &producer{c: c, subject: d.subject}
	if d.s.SUT.Profile == "durable" {
		p.js, e = c.JetStream()
		if e != nil {
			c.Close()
			return nil, e
		}
	}
	return p, nil
}
func (p *producer) Publish(ctx context.Context, b []byte) error {
	if p.js != nil {
		_, e := p.js.Publish(p.subject, b, nats.Context(ctx))
		return e
	}
	if e := p.c.Publish(p.subject, b); e != nil {
		return e
	}
	return p.c.FlushWithContext(ctx)
}
func (p *producer) Close() error { p.c.Close(); return nil }

type consumer struct {
	c        *nats.Conn
	s        *nats.Subscription
	explicit bool
}

func (d *Driver) NewConsumer(ctx context.Context, id int) (driver.Consumer, error) {
	if id >= d.s.ActiveConsumers() {
		p, e := d.NewProducer(ctx, id+d.s.Clients.Producers)
		if e != nil {
			return nil, e
		}
		return driver.Idle(p), nil
	}
	c, e := d.connect()
	if e != nil {
		return nil, e
	}
	var sub *nats.Subscription
	durable := d.s.SUT.Profile == "durable"
	if durable {
		js, err := c.JetStream()
		if err != nil {
			c.Close()
			return nil, err
		}
		opts := []nats.SubOpt{nats.BindStream(d.stream()), nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll(), nats.MaxAckPending(65536), nats.AckWait(30 * time.Second)}
		if d.s.Topology.Delivery == "competing" {
			opts = append(opts, nats.Durable("shared"))
			sub, e = js.QueueSubscribeSync(d.subject, "workers", opts...)
		} else {
			opts = append(opts, nats.Durable(fmt.Sprintf("c%d", id)))
			sub, e = js.SubscribeSync(d.subject, opts...)
		}
	} else if d.s.Topology.Delivery == "competing" {
		sub, e = c.QueueSubscribeSync(d.subject, "workers")
	} else {
		sub, e = c.SubscribeSync(d.subject)
	}
	if e == nil {
		e = sub.SetPendingLimits(65536, 64<<20)
	}
	if e == nil {
		e = c.FlushWithContext(ctx)
	}
	if e != nil {
		c.Close()
		return nil, e
	}
	return &consumer{c, sub, durable}, nil
}
func (c *consumer) Receive(ctx context.Context) (driver.Message, error) {
	m, e := c.s.NextMsgWithContext(ctx)
	if e != nil {
		return driver.Message{}, e
	}
	r := driver.Message{Data: m.Data}
	if c.explicit {
		r.Ack = func(ctx context.Context) error { return m.AckSync(nats.Context(ctx)) }
	}
	return r, nil
}
func (c *consumer) Close() error                    { c.c.Close(); return nil }
func (d *Driver) Cleanup(ctx context.Context) error { return nil }
