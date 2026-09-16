package rabbitmq

import (
	"context"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"time"
)

type Driver struct {
	s        scenario.Scenario
	exchange string
}

func New(s scenario.Scenario, id string) (driver.Driver, error) {
	return &Driver{s, "bench-" + id}, nil
}
func (d *Driver) Name() string { return "rabbitmq" }
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Persistence: true, PublisherAck: true, ConsumerAck: true, Competing: true, Broadcast: true, Ordering: "per producer per queue before redelivery", QoS: true, DurableSubscriptions: true, NativeMetrics: true}
}
func (d *Driver) durable() bool { return d.s.SUT.Profile == "durable" }
func (d *Driver) Settings() map[string]any {
	return map[string]any{"process_name": "beam.smp", "protocol_port": 5672, "protocol": "AMQP 0-9-1", "client_library": "github.com/rabbitmq/amqp091-go", "client_library_version": "v1.10.0", "queue_type": "classic", "durable": d.durable(), "persistence": d.durable(), "publisher_confirms": d.durable(), "publisher_ack": map[bool]string{true: "publisher confirm", false: "client write completion only"}[d.durable()], "consumer_ack": d.durable(), "prefetch": 256, "exchange_type": "fanout", "exchange": d.exchange, "auto_delete": false, "heartbeat_seconds": 10, "connection_model": "one TCP connection and channel per logical client", "replication": 1, "batching": "none", "compression": "none", "recovery": "driver reconnects closed connections on next operation"}
}

type link struct {
	c   *amqp.Connection
	ch  *amqp.Channel
	net net.Conn
}

func (d *Driver) connect(ctx context.Context) (*link, error) {
	l := &link{}
	timeout := time.Duration(d.s.Connection.OperationTimeoutMS) * time.Millisecond
	c, e := amqp.DialConfig("amqp://guest:guest@127.0.0.1:5672/", amqp.Config{Heartbeat: 10 * time.Second, Locale: "en_US", Dial: func(network, addr string) (net.Conn, error) {
		n, e := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, addr)
		if e == nil {
			l.net = n
			_ = n.SetDeadline(time.Now().Add(timeout))
		}
		return n, e
	}})
	if e != nil {
		return nil, e
	}
	l.c = c
	l.ch, e = c.Channel()
	if e == nil {
		e = l.ch.ExchangeDeclare(d.exchange, "fanout", d.durable(), false, false, false, nil)
	}
	if e != nil {
		_ = c.CloseDeadline(time.Now().Add(timeout))
		return nil, e
	}
	_ = l.net.SetDeadline(time.Time{})
	return l, nil
}
func (l *link) close() error { return l.c.CloseDeadline(time.Now().Add(time.Second)) }
func (d *Driver) Prepare(ctx context.Context) error {
	l, e := d.connect(ctx)
	if e != nil {
		return e
	}
	return l.close()
}

type producer struct {
	d *Driver
	l *link
}

func (d *Driver) NewProducer(ctx context.Context, id int) (driver.Producer, error) {
	p := &producer{d: d}
	return p, p.open(ctx)
}
func (p *producer) open(ctx context.Context) error {
	l, e := p.d.connect(ctx)
	if e != nil {
		return e
	}
	if p.d.durable() {
		_ = l.net.SetDeadline(time.Now().Add(time.Duration(p.d.s.Connection.OperationTimeoutMS) * time.Millisecond))
		e = l.ch.Confirm(false)
		_ = l.net.SetDeadline(time.Time{})
		if e != nil {
			_ = l.close()
			return e
		}
	}
	p.l = l
	return nil
}
func (p *producer) Publish(ctx context.Context, b []byte) error {
	if p.l == nil || p.l.c.IsClosed() {
		if e := p.open(ctx); e != nil {
			return e
		}
	}
	deadline, _ := ctx.Deadline()
	_ = p.l.net.SetWriteDeadline(deadline)
	defer p.l.net.SetWriteDeadline(time.Time{})
	mode := uint8(amqp.Transient)
	if p.d.durable() {
		mode = amqp.Persistent
	}
	m := amqp.Publishing{ContentType: "application/octet-stream", DeliveryMode: mode, Body: b}
	if !p.d.durable() {
		return p.l.ch.PublishWithContext(ctx, p.d.exchange, "", false, false, m)
	}
	confirmation, e := p.l.ch.PublishWithDeferredConfirmWithContext(ctx, p.d.exchange, "", false, false, m)
	if e != nil {
		return e
	}
	ok, e := confirmation.WaitContext(ctx)
	if e == nil && !ok {
		return fmt.Errorf("publisher nack")
	}
	return e
}
func (p *producer) Close() error {
	if p.l == nil {
		return nil
	}
	return p.l.close()
}

type consumer struct {
	d        *Driver
	l        *link
	id       int
	messages <-chan amqp.Delivery
}

func (d *Driver) NewConsumer(ctx context.Context, id int) (driver.Consumer, error) {
	if id >= d.s.ActiveConsumers() {
		p, e := d.NewProducer(ctx, id+d.s.Clients.Producers)
		if e != nil {
			return nil, e
		}
		return driver.Idle(p), nil
	}
	c := &consumer{d: d, id: id}
	return c, c.open(ctx)
}
func (c *consumer) open(ctx context.Context) error {
	l, e := c.d.connect(ctx)
	if e != nil {
		return e
	}
	_ = l.net.SetDeadline(time.Now().Add(time.Duration(c.d.s.Connection.OperationTimeoutMS) * time.Millisecond))
	name := fmt.Sprintf("%s-c%d", c.d.exchange, c.id)
	if c.d.s.Topology.Delivery == "competing" {
		name = c.d.exchange + "-shared"
	}
	q, e := l.ch.QueueDeclare(name, c.d.durable(), false, false, false, amqp.Table{"x-queue-type": "classic"})
	if e == nil {
		e = l.ch.QueueBind(q.Name, "", c.d.exchange, false, nil)
	}
	if e == nil {
		e = l.ch.Qos(256, 0, false)
	}
	var messages <-chan amqp.Delivery
	if e == nil {
		messages, e = l.ch.Consume(q.Name, "", !c.d.durable(), false, false, false, nil)
	}
	_ = l.net.SetDeadline(time.Time{})
	if e != nil {
		_ = l.close()
		return e
	}
	c.l = l
	c.messages = messages
	return nil
}
func (c *consumer) Receive(ctx context.Context) (driver.Message, error) {
	if c.l == nil || c.l.c.IsClosed() {
		if e := c.open(ctx); e != nil {
			return driver.Message{}, e
		}
	}
	select {
	case <-ctx.Done():
		return driver.Message{}, ctx.Err()
	case m, ok := <-c.messages:
		if !ok {
			_ = c.l.close()
			return driver.Message{}, fmt.Errorf("AMQP consumer disconnected")
		}
		r := driver.Message{Data: m.Body}
		l := c.l
		if c.d.durable() {
			r.Ack = func(ctx context.Context) error {
				deadline, _ := ctx.Deadline()
				_ = l.net.SetWriteDeadline(deadline)
				defer l.net.SetWriteDeadline(time.Time{})
				return m.Ack(false)
			}
		}
		return r, nil
	}
}
func (c *consumer) Close() error {
	if c.l == nil {
		return nil
	}
	return c.l.close()
}
func (d *Driver) Cleanup(context.Context) error { return nil }
