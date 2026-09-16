package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/metrics"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/result"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryDriver struct {
	mu      sync.Mutex
	qs      []chan []byte
	blocked bool
}

func (d *memoryDriver) Name() string { return "fixture" }
func (d *memoryDriver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Broadcast: true}
}
func (d *memoryDriver) Settings() map[string]any      { return map[string]any{} }
func (d *memoryDriver) Prepare(context.Context) error { return nil }
func (d *memoryDriver) Cleanup(context.Context) error { return nil }
func (d *memoryDriver) NewProducer(context.Context, int) (driver.Producer, error) {
	return &memoryProducer{d}, nil
}
func (d *memoryDriver) NewConsumer(context.Context, int) (driver.Consumer, error) {
	q := make(chan []byte, 1000)
	d.qs = append(d.qs, q)
	return &memoryConsumer{q}, nil
}

type memoryProducer struct{ d *memoryDriver }

func (p *memoryProducer) Publish(ctx context.Context, b []byte) error {
	if p.d.blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	p.d.mu.Lock()
	defer p.d.mu.Unlock()
	for _, q := range p.d.qs {
		select {
		case q <- append([]byte(nil), b...):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (p *memoryProducer) Close() error { return nil }

type memoryConsumer struct{ q chan []byte }

func (c *memoryConsumer) Receive(ctx context.Context) (driver.Message, error) {
	select {
	case <-ctx.Done():
		return driver.Message{}, ctx.Err()
	case b := <-c.q:
		return driver.Message{Data: b, Ack: func(context.Context) error { return nil }}, nil
	}
}
func (c *memoryConsumer) Close() error { return nil }
func testEngine(t *testing.T, blocked bool) *Engine {
	t.Helper()
	s, e := scenario.Load("../../scenarios/smoke/nats.yaml")
	if e != nil {
		t.Fatal(e)
	}
	s.Clients.Producers = 2
	s.Clients.Consumers = 3
	s.Connection.Ramp = 100000
	s.Traffic.Rate = 200
	s.Connection.OperationTimeoutMS = 20
	s.Traffic.QueueCapacity = 1
	store, e := result.Create(t.TempDir(), "unit-fixture")
	if e != nil {
		t.Fatal(e)
	}
	engine := &Engine{s: s, d: &memoryDriver{blocked: blocked}, r: metrics.New(10000), store: store, origin: time.Now()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = engine.connect(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(engine.close)
	return engine
}
func TestEngineBroadcastAndWarmupExclusion(t *testing.T) {
	e := testEngine(t, false)
	ctx := context.Background()
	e.receive(ctx)
	if err := e.traffic(ctx, 30*time.Millisecond, 1); err != nil {
		t.Fatal(err)
	}
	if err := e.traffic(ctx, 100*time.Millisecond, 2); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c := e.r.Snapshot()
		if c["delivered"] == c["sent"]*3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	e.close()
	c := e.r.Snapshot()
	if c["sent"] == 0 || c["delivered"] != 3*c["sent"] || c["producer_accepted"] != c["sent"] || c["duplicate"] > 0 || c["unexpected"] > 0 {
		t.Fatalf("counters: %+v", c)
	}
	if c["sent"] > 20 {
		t.Fatal("warmup included")
	}
}
func TestBoundedOpenLoopDropsAndCancellation(t *testing.T) {
	e := testEngine(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.receive(ctx)
	e.s.Traffic.Rate = 10000
	if err := e.traffic(ctx, 80*time.Millisecond, 2); err != nil {
		t.Fatal(err)
	}
	c := e.r.Snapshot()
	if c["scheduled"] <= c["sent"] || c["queue_dropped"] == 0 {
		t.Fatalf("hidden backlog: %+v", c)
	}
	cancel()
	start := time.Now()
	_ = e.traffic(ctx, time.Hour, 2)
	e.close()
	if time.Since(start) > time.Second {
		t.Fatal("cancellation blocked")
	}
}
func TestFailedDeployPreservesManifest(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	// This fixture never executes Docker and must not contend with live tests.
	acquire := acquireDeployment
	acquireDeployment = func(string, string) (func(), error) { return func() {}, nil }
	t.Cleanup(func() { acquireDeployment = acquire })
	s, e := scenario.Load("../../scenarios/smoke/nats.yaml")
	if e != nil {
		t.Fatal(e)
	}
	sourceRoot, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	for _, name := range []string{"deploy", "configs", "versions.lock.yaml", "go.mod", "go.sum"} {
		if e := os.Symlink(filepath.Join(sourceRoot, name), filepath.Join(root, name)); e != nil {
			t.Fatal(e)
		}
	}
	registry := driver.Registry{"nats": func(scenario.Scenario, string) (driver.Driver, error) { return &memoryDriver{}, nil }}
	dir, e := Run(context.Background(), root, t.TempDir(), s, registry, 1)
	if e == nil {
		t.Fatal("missing docker accepted")
	}
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	var m map[string]any
	if e = json.Unmarshal(b, &m); e != nil {
		t.Fatal(e)
	}
	if m["status"] != "failed" || m["failure_stage"] != "DEPLOY" {
		t.Fatal(fmt.Sprint(m))
	}
	if _, e = os.Stat(filepath.Join(dir, "latency/delivery.hdr.json")); e != nil {
		t.Fatal("partial histogram missing", e)
	}
	logs, err := os.ReadFile(filepath.Join(dir, "logs/runner.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(logs)), "\n")
	if !strings.Contains(lines[len(lines)-2], "TEARDOWN") || !strings.Contains(lines[len(lines)-1], "FINALIZE") {
		t.Fatalf("failure cleanup stages not recorded: %s", logs)
	}
}

func TestDisconnectedArrivalsAreNotPublishInvocations(t *testing.T) {
	e := testEngine(t, false)
	for _, slot := range e.producers {
		slot.p = nil
	}
	if err := e.traffic(context.Background(), 50*time.Millisecond, 2); err != nil {
		t.Fatal(err)
	}
	c := e.r.Snapshot()
	if c["sent"] != 0 || c["disconnected_arrivals"] == 0 || c["scheduled"] != c["queue_dropped"] {
		t.Fatalf("disconnected arrivals counted as sent: %+v", c)
	}
}

func TestNoPublishAfterWaitingPastMeasurementEnd(t *testing.T) {
	e := testEngine(t, false)
	for _, slot := range e.producers {
		slot.mu.Lock()
	}
	done := make(chan error, 1)
	go func() { done <- e.traffic(context.Background(), 20*time.Millisecond, 2) }()
	time.Sleep(80 * time.Millisecond)
	for _, slot := range e.producers {
		slot.mu.Unlock()
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c := e.r.Snapshot(); c["sent"] != 0 {
		t.Fatalf("published outside measurement window: %+v", c)
	}
}
