package mosquitto

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"sync"
	"time"
)

type Driver struct {
	s     scenario.Scenario
	topic string
}

func New(s scenario.Scenario, id string) (driver.Driver, error) {
	return &Driver{s, "bench/" + id}, nil
}
func (d *Driver) Name() string { return "mosquitto" }
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Persistence: true, PublisherAck: true, ConsumerAck: true, Broadcast: true, Ordering: "per producer per subscriber; QoS1 duplicates possible", QoS: true, DurableSubscriptions: true, NativeMetrics: true}
}
func (d *Driver) qos() byte {
	if d.s.SUT.Profile == "durable" {
		return 1
	}
	return 0
}
func (d *Driver) Settings() map[string]any {
	return map[string]any{"process_name": "mosquitto", "protocol_port": 1883, "protocol": "MQTT 3.1.1", "client_library": "github.com/eclipse/paho.mqtt.golang", "client_library_version": "v1.5.1", "qos": d.qos(), "publisher_ack": map[byte]string{0: "client write completion only", 1: "PUBACK; not per-message fsync"}[d.qos()], "consumer_ack": d.qos() == 1, "persistence": d.qos() == 1, "broker_persistence_enabled": true, "clean_session": d.qos() == 0, "retain": false, "topic": d.topic, "inflight": 20, "queued_messages": 10000, "keepalive_seconds": 30, "client_store": "memory; client process crash persistence unavailable", "reconnect_seconds": 1, "compression": "none", "batching": "none"}
}
func (d *Driver) options(id string) *mqtt.ClientOptions {
	return mqtt.NewClientOptions().AddBroker("tcp://127.0.0.1:1883").SetClientID(id).SetProtocolVersion(4).SetCleanSession(d.qos() == 0).SetAutoReconnect(true).SetConnectRetry(false).SetResumeSubs(true).SetMaxReconnectInterval(time.Second).SetConnectTimeout(time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond).SetWriteTimeout(time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond).SetKeepAlive(30 * time.Second).SetOrderMatters(true).SetAutoAckDisabled(true)
}
func wait(ctx context.Context, t mqtt.Token) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.Done():
		return t.Error()
	}
}
func (d *Driver) Prepare(ctx context.Context) error {
	c := mqtt.NewClient(d.options(d.topic + "-probe"))
	if e := wait(ctx, c.Connect()); e != nil {
		c.Disconnect(0)
		return e
	}
	c.Disconnect(0)
	return nil
}

type producer struct {
	c     mqtt.Client
	topic string
	qos   byte
}

func (d *Driver) NewProducer(ctx context.Context, id int) (driver.Producer, error) {
	c := mqtt.NewClient(d.options(fmt.Sprintf("%s-p%d", d.topic, id)))
	if e := wait(ctx, c.Connect()); e != nil {
		c.Disconnect(0)
		return nil, e
	}
	return &producer{c, d.topic, d.qos()}, nil
}
func (p *producer) Publish(ctx context.Context, b []byte) error {
	return wait(ctx, p.c.Publish(p.topic, p.qos, false, b))
}
func (p *producer) Close() error { p.c.Disconnect(0); return nil }

type consumer struct {
	c    mqtt.Client
	q    chan mqtt.Message
	done chan struct{}
	once sync.Once
	qos  byte
}

func (d *Driver) NewConsumer(ctx context.Context, id int) (driver.Consumer, error) {
	if id >= d.s.ActiveConsumers() {
		p, e := d.NewProducer(ctx, id+d.s.Clients.Producers)
		if e != nil {
			return nil, e
		}
		return driver.Idle(p), nil
	}
	c := &consumer{q: make(chan mqtt.Message, 1024), done: make(chan struct{}), qos: d.qos()}
	ready := make(chan error, 1)
	opts := d.options(fmt.Sprintf("%s-c%d", d.topic, id))
	handler := func(_ mqtt.Client, m mqtt.Message) {
		select {
		case c.q <- m:
		case <-c.done:
		}
	}
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		t := client.Subscribe(d.topic, d.qos(), handler)
		ok := t.WaitTimeout(time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond)
		var err error
		if !ok {
			err = context.DeadlineExceeded
		} else {
			err = t.Error()
		}
		select {
		case ready <- err:
		default:
		}
	})
	c.c = mqtt.NewClient(opts)
	if err := wait(ctx, c.c.Connect()); err != nil {
		_ = c.Close()
		return nil, err
	}
	select {
	case <-ctx.Done():
		_ = c.Close()
		return nil, ctx.Err()
	case err := <-ready:
		if err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	return c, nil
}
func (c *consumer) Receive(ctx context.Context) (driver.Message, error) {
	select {
	case <-ctx.Done():
		return driver.Message{}, ctx.Err()
	case <-c.done:
		return driver.Message{}, fmt.Errorf("consumer closed")
	case m := <-c.q:
		r := driver.Message{Data: m.Payload()}
		if c.qos > 0 {
			r.Ack = func(context.Context) error { m.Ack(); return nil }
		}
		return r, nil
	}
}
func (c *consumer) Close() error                { c.once.Do(func() { close(c.done); c.c.Disconnect(0) }); return nil }
func (d *Driver) Cleanup(context.Context) error { return nil }
