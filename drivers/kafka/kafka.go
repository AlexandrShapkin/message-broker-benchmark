package kafka

import (
	"context"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"time"
)

type Driver struct {
	s     scenario.Scenario
	topic string
}

func New(s scenario.Scenario, id string) (driver.Driver, error) {
	return &Driver{s, "bench-" + id}, nil
}
func (d *Driver) Name() string { return "kafka" }
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Persistence: true, PublisherAck: true, ConsumerAck: true, Broadcast: true, Partitions: true, Ordering: "per producer per partition; retries may reorder in fast profile", QoS: true, DurableSubscriptions: true, NativeMetrics: false}
}
func (d *Driver) Settings() map[string]any {
	return map[string]any{"process_name": "java", "protocol_port": 9092, "protocol": "Kafka", "client_connection_idle_timeout_seconds": 60, "idle_metadata_keepalive_seconds": 10, "idle_keepalive_scope": "inactive producer and consumer transports; all producers for zero-rate scenarios", "client_library": "github.com/twmb/franz-go", "client_library_version": "v1.19.5", "partitions": 1, "replication_factor": 1, "producer_acks": map[bool]string{true: "all ISR (one replica)", false: "leader"}[d.s.SUT.Profile == "durable"], "publisher_ack": "Kafka Produce response", "consumer_ack": d.s.SUT.Profile == "durable", "persistence": true, "fsync": "OS page cache; acks do not imply per-message fsync", "idempotent": d.s.SUT.Profile == "durable", "linger_ms": 0, "compression": "none", "batch_max_bytes": 33554432, "max_buffered_records": 64, "fetch_max_bytes": 33554432, "fetch_min_bytes": 1, "fetch_max_wait_ms": 100, "auto_commit": false, "offset_reset": "earliest", "topic": d.topic, "consumer_groups": "one per subscriber", "connection_model": "one franz-go client per logical client; broker/coordinator sockets are library managed", "retention_ms": 86400000, "cleanup_policy": "delete"}
}
func (d *Driver) opts() []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers("127.0.0.1:9092"), kgo.ConnIdleTimeout(time.Minute), kgo.DialTimeout(time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond), kgo.RequestTimeoutOverhead(time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond)}
}
func (d *Driver) Prepare(ctx context.Context) error {
	c, e := kgo.NewClient(d.opts()...)
	if e != nil {
		return e
	}
	defer c.Close()
	retention := "86400000"
	cleanup := "delete"
	max := "16777344"
	r, e := kadm.NewClient(c).CreateTopics(ctx, 1, 1, map[string]*string{"retention.ms": &retention, "cleanup.policy": &cleanup, "max.message.bytes": &max}, d.topic)
	if e != nil {
		return e
	}
	for _, v := range r {
		if v.Err != nil {
			return v.Err
		}
	}
	return nil
}

type producer struct {
	c             *kgo.Client
	topic         string
	stopKeepalive func()
}

func (d *Driver) NewProducer(ctx context.Context, id int) (driver.Producer, error) {
	opts := append(d.opts(), kgo.DefaultProduceTopic(d.topic), kgo.ProducerLinger(0), kgo.ProducerBatchMaxBytes(33554432), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.MaxBufferedRecords(64), kgo.RecordDeliveryTimeout(time.Duration(d.s.Connection.OperationTimeoutMS)*time.Millisecond))
	if d.s.SUT.Profile == "fast" {
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	} else {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	c, e := kgo.NewClient(opts...)
	if e != nil {
		return nil, e
	}
	if e = c.Ping(ctx); e != nil {
		c.Close()
		return nil, e
	}
	p := &producer{c: c, topic: d.topic}
	if id >= d.s.ActiveProducers() || d.s.Traffic.Rate == 0 {
		p.stopKeepalive = keepalive(10*time.Second, time.Duration(d.s.Connection.OperationTimeoutMS)*time.Millisecond, c.Ping)
	}
	return p, nil
}
func (p *producer) Publish(ctx context.Context, b []byte) error {
	return p.c.ProduceSync(ctx, &kgo.Record{Topic: p.topic, Value: b}).FirstErr()
}
func (p *producer) Close() error {
	if p.stopKeepalive != nil {
		p.stopKeepalive()
	}
	p.c.Close()
	return nil
}

// Idle Kafka clients otherwise lose sockets to library idle reaping and do not
// reconnect after broker restart until used. Metadata requests maintain their
// transport; this control traffic is explicitly recorded in driver settings.
func keepalive(interval, timeout time.Duration, ping func(context.Context) error) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				call, stop := context.WithTimeout(ctx, timeout)
				_ = ping(call) // outages remain visible in physical connection measurements
				stop()
			}
		}
	}()
	return func() { cancel(); <-done }
}

type consumer struct {
	c       *kgo.Client
	ack     bool
	pending []*kgo.Record
}

func (d *Driver) NewConsumer(ctx context.Context, id int) (driver.Consumer, error) {
	if id >= d.s.ActiveConsumers() {
		p, e := d.NewProducer(ctx, id+d.s.Clients.Producers)
		if e != nil {
			return nil, e
		}
		return driver.Idle(p), nil
	}
	opts := append(d.opts(), kgo.ConsumerGroup(fmt.Sprintf("%s-c%d", d.topic, id)), kgo.ConsumeTopics(d.topic), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(33554432), kgo.FetchMaxPartitionBytes(33554432), kgo.FetchMinBytes(1), kgo.FetchMaxWait(100*time.Millisecond))
	assigned := make(chan struct{})
	var once sync.Once
	opts = append(opts, kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { once.Do(func() { close(assigned) }) }))
	c, e := kgo.NewClient(opts...)
	if e != nil {
		return nil, e
	}
	if e = c.Ping(ctx); e != nil {
		c.Close()
		return nil, e
	}
	pollCtx, cancel := context.WithCancel(ctx)
	done := make(chan kgo.Fetches, 1)
	go func() { done <- c.PollRecords(pollCtx, 1) }()
	select {
	case <-assigned:
	case <-ctx.Done():
		cancel()
		<-done
		c.Close()
		return nil, ctx.Err()
	}
	cancel()
	f := <-done
	return &consumer{c: c, ack: d.s.SUT.Profile == "durable", pending: f.Records()}, nil
}
func (c *consumer) Receive(ctx context.Context) (driver.Message, error) {
	if len(c.pending) > 0 {
		record := c.pending[0]
		c.pending = c.pending[1:]
		return c.message(record), nil
	}
	f := c.c.PollRecords(ctx, 1)
	if e := f.Errors(); len(e) > 0 {
		return driver.Message{}, e[0].Err
	}
	records := f.Records()
	if len(records) == 0 {
		if ctx.Err() != nil {
			return driver.Message{}, ctx.Err()
		}
		return driver.Message{}, fmt.Errorf("empty Kafka fetch")
	}
	return c.message(records[0]), nil
}
func (c *consumer) message(record *kgo.Record) driver.Message {
	m := driver.Message{Data: record.Value}
	if c.ack {
		m.Ack = func(ctx context.Context) error { return c.c.CommitRecords(ctx, record) }
	}
	return m
}
func (c *consumer) Close() error                { c.c.Close(); return nil }
func (d *Driver) Cleanup(context.Context) error { return nil }
