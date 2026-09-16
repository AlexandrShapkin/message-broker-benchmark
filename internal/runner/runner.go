package runner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/message-broker-benchmark/message-broker-benchmark/internal/deployment"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/driver"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/hostinfo"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/metrics"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/result"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/systemmetrics"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/workload"
	"gopkg.in/yaml.v3"
)

type producerSlot struct {
	mu sync.Mutex
	p  driver.Producer
}

var acquireDeployment = deployment.Acquire

type consumerSlot struct {
	mu sync.Mutex
	c  driver.Consumer
}
type Engine struct {
	s            scenario.Scenario
	d            driver.Driver
	r            *metrics.Recorder
	store        *result.Store
	origin       time.Time
	token        [16]byte
	producers    []*producerSlot
	consumers    []*consumerSlot
	wg           sync.WaitGroup
	cancel       context.CancelFunc
	measureStart time.Time
	measureEnd   time.Time
	phaseMu      sync.RWMutex
}

func (e *Engine) opctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(e.s.Connection.OperationTimeoutMS)*time.Millisecond)
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (e *Engine) connect(ctx context.Context) error {
	start := time.Now()
	n := 0
	ramp := func() error {
		deadline := start.Add(time.Duration(float64(n) * float64(time.Second) / float64(e.s.Connection.Ramp)))
		n++
		if wait := time.Until(deadline); wait > 0 {
			return pause(ctx, wait)
		}
		return ctx.Err()
	}
	for i := 0; i < e.s.Clients.Consumers; i++ {
		if err := ramp(); err != nil {
			return err
		}
		cctx, cancel := e.opctx(ctx)
		t := time.Now()
		c, err := e.d.NewConsumer(cctx, i)
		cancel()
		e.r.Record("connect", time.Since(t))
		if err != nil {
			e.r.Inc("connections_failed", 1)
			return fmt.Errorf("consumer %d: %w", i, err)
		}
		e.consumers = append(e.consumers, &consumerSlot{c: c})
		e.r.Inc("connections_established", 1)
	}
	for i := 0; i < e.s.Clients.Producers; i++ {
		if err := ramp(); err != nil {
			return err
		}
		cctx, cancel := e.opctx(ctx)
		t := time.Now()
		p, err := e.d.NewProducer(cctx, i)
		cancel()
		e.r.Record("connect", time.Since(t))
		if err != nil {
			e.r.Inc("connections_failed", 1)
			return fmt.Errorf("producer %d: %w", i, err)
		}
		e.producers = append(e.producers, &producerSlot{p: p})
		e.r.Inc("connections_established", 1)
	}
	return nil
}
func (e *Engine) receive(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	for i, slot := range e.consumers {
		if i >= e.s.ActiveConsumers() {
			break
		}
		e.wg.Add(1)
		go func(id int, s *consumerSlot) {
			defer e.wg.Done()
			for ctx.Err() == nil {
				cctx, cancel := e.opctx(ctx)
				s.mu.Lock()
				if s.c == nil {
					s.mu.Unlock()
					cancel()
					_ = pause(ctx, 10*time.Millisecond)
					continue
				}
				m, err := s.c.Receive(cctx)
				s.mu.Unlock()
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if !errors.Is(err, context.DeadlineExceeded) {
						e.r.Inc("errors", 1)
						_ = pause(ctx, 10*time.Millisecond)
					}
					continue
				}
				p, seq, deadline, phase, err := metrics.Decode(m.Data, e.token)
				if err != nil || int(p) >= e.s.ActiveProducers() || len(m.Data) != e.s.Traffic.PayloadBytes ||
					(phase == 2 && !e.r.Expected(int(p), seq)) || time.Duration(deadline) > time.Since(e.origin) {
					e.r.Inc("unexpected", 1)
					continue
				}
				if e.s.Traffic.ConsumerDelayMS > 0 {
					if pause(ctx, time.Duration(e.s.Traffic.ConsumerDelayMS)*time.Millisecond) != nil {
						return
					}
				}
				if phase == 2 {
					key := id
					ordered := e.s.Topology.Delivery == "broadcast"
					if !ordered {
						key = 0
					}
					unique := e.r.Delivery(key, int(p), seq, ordered)
					e.r.Record("delivery", time.Since(e.origin)-time.Duration(deadline))
					e.phaseMu.RLock()
					inWindow := time.Now().Before(e.measureEnd)
					e.phaseMu.RUnlock()
					if inWindow {
						e.r.Inc("delivered_window", 1)
						if unique {
							e.r.Inc("unique_delivered_window", 1)
						}
					}
				}
				if m.Ack != nil {
					actx, acancel := e.opctx(ctx)
					err = m.Ack(actx)
					acancel()
					if err != nil {
						e.r.Inc("errors", 1)
					} else if phase == 2 {
						e.r.Inc("consumer_acked", 1)
					}
				}
			}
		}(i, slot)
	}
}
func (e *Engine) traffic(ctx context.Context, duration time.Duration, phase uint32) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	end := start.Add(duration)
	if phase == 2 {
		e.phaseMu.Lock()
		e.measureStart = start
		e.measureEnd = end
		e.phaseMu.Unlock()
	}
	queues := make([]chan time.Time, e.s.ActiveProducers())
	var workers sync.WaitGroup
	for i, s := range e.producers {
		if i >= len(queues) {
			break
		}
		queues[i] = make(chan time.Time, e.s.Traffic.QueueCapacity)
		workers.Add(1)
		go func(id int, slot *producerSlot, q <-chan time.Time) {
			defer workers.Done()
			var seq uint64
			for deadline := range q {
				slot.mu.Lock()
				if ctx.Err() != nil || time.Now().After(end) {
					slot.mu.Unlock()
					if phase == 2 {
						e.r.Inc("queue_dropped", 1)
					}
					continue
				}
				if slot.p == nil {
					slot.mu.Unlock()
					if phase == 2 {
						e.r.Inc("queue_dropped", 1)
						e.r.Inc("disconnected_arrivals", 1)
					}
					continue
				}
				seq++
				b := metrics.Encode(e.s.Traffic.PayloadBytes, e.token, uint32(id), seq, int64(deadline.Sub(e.origin)), phase)
				if phase == 2 {
					e.r.Attempt(id, seq)
					e.r.Record("scheduling_lag", time.Since(deadline))
				}
				pctx, pcancel := e.opctx(ctx)
				t := time.Now()
				err := slot.p.Publish(pctx, b)
				slot.mu.Unlock()
				pcancel()
				if phase == 2 {
					e.r.Record("publish", time.Since(t))
					if err != nil {
						e.r.Inc("errors", 1)
					} else {
						e.r.Inc("producer_accepted", 1)
						if time.Now().Before(end) {
							e.r.Inc("producer_accepted_window", 1)
						}
					}
					if e.s.Collection.OperationSamples {
						if err := e.store.Append("metrics/operations.jsonl", map[string]any{"schema_version": 1, "producer": id, "sequence": seq, "scheduled_ns": deadline.Sub(e.origin).Nanoseconds(), "started_ns": t.Sub(e.origin).Nanoseconds(), "duration_ns": time.Since(t).Nanoseconds(), "accepted": err == nil}); err != nil {
							e.r.Mu.Lock()
							e.r.Invalid = err.Error()
							e.r.Mu.Unlock()
						}
					}
				}
			}
		}(i, s, queues[i])
	}
	next := int(rand.New(rand.NewSource(e.s.RandomSeed)).Int63n(int64(len(queues))))
	n := workload.Schedule(ctx, start, duration, e.s.Traffic.Rate, e.s.Traffic.BurstMultiplier, time.Duration(e.s.Traffic.BurstEverySeconds)*time.Second, time.Duration(e.s.Traffic.BurstDurationSeconds)*time.Second, func(t time.Time) {
		q := queues[next]
		next = (next + 1) % len(queues)
		select {
		case q <- t:
		default:
			if phase == 2 {
				e.r.Inc("queue_dropped", 1)
			}
		}
	})
	if phase == 2 {
		e.r.Inc("scheduled", n)
	}
	for _, q := range queues {
		close(q)
	}
	workers.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return pause(ctx, max(0, time.Until(end)))
}
func (e *Engine) close() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	var wg sync.WaitGroup
	slots := make(chan struct{}, 256)
	closeOne := func(fn func()) {
		slots <- struct{}{}
		wg.Add(1)
		go func() { defer wg.Done(); defer func() { <-slots }(); fn() }()
	}
	for _, s := range e.producers {
		closeOne(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.p != nil {
				_ = s.p.Close()
				s.p = nil
			}
		})
	}
	for _, s := range e.consumers {
		closeOne(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.c != nil {
				_ = s.c.Close()
				s.c = nil
			}
		})
	}
	wg.Wait()
}
func (e *Engine) reconnect(ctx context.Context) error {
	countP := int(float64(len(e.producers)) * e.s.Fault.Fraction)
	countC := int(float64(len(e.consumers)) * e.s.Fault.Fraction)
	if countP+countC == 0 {
		return fmt.Errorf("fault fraction selects no clients")
	}
	rng := rand.New(rand.NewSource(e.s.RandomSeed))
	pi := rng.Perm(len(e.producers))[:countP]
	ci := rng.Perm(len(e.consumers))[:countC]
	var disconnect sync.WaitGroup
	for _, i := range pi {
		disconnect.Add(1)
		go func(i int) {
			defer disconnect.Done()
			slot := e.producers[i]
			slot.mu.Lock()
			defer slot.mu.Unlock()
			if slot.p != nil {
				_ = slot.p.Close()
				slot.p = nil
				e.r.Inc("disconnects", 1)
			}
		}(i)
	}
	for _, i := range ci {
		disconnect.Add(1)
		go func(i int) {
			defer disconnect.Done()
			slot := e.consumers[i]
			slot.mu.Lock()
			defer slot.mu.Unlock()
			if slot.c != nil {
				_ = slot.c.Close()
				slot.c = nil
				e.r.Inc("disconnects", 1)
			}
		}(i)
	}
	disconnect.Wait()
	if err := pause(ctx, time.Duration(e.s.Fault.DowntimeSeconds)*time.Second); err != nil {
		return err
	}
	// Reconnect storm opens selected clients concurrently, unlike normal ramp.
	var wg sync.WaitGroup
	errs := make(chan error, countP+countC)
	for _, i := range pi {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, cancel := e.opctx(ctx)
			defer cancel()
			p, err := e.d.NewProducer(c, i)
			if err != nil {
				errs <- err
				return
			}
			s := e.producers[i]
			s.mu.Lock()
			s.p = p
			s.mu.Unlock()
			e.r.Inc("reconnections", 1)
		}(i)
	}
	for _, i := range ci {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, cancel := e.opctx(ctx)
			defer cancel()
			p, err := e.d.NewConsumer(c, i)
			if err != nil {
				errs <- err
				return
			}
			s := e.consumers[i]
			s.mu.Lock()
			s.c = p
			s.mu.Unlock()
			e.r.Inc("reconnections", 1)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
func Run(ctx context.Context, root, results string, s scenario.Scenario, registry driver.Registry, repetition int) (dir string, retErr error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	store, err := result.Create(results, s.SUT.Broker+"-"+s.Name)
	if err != nil {
		return "", err
	}
	dir = store.Dir
	m := manifest.New(filepath.Base(dir))
	stage := "PREPARE"
	dpl := deployment.New(root, s)
	rec := metrics.New(s.Collection.MaxIntervals)
	var brokerDriver driver.Driver
	var releaseDeployment func()
	var engine *Engine
	var collectorCancel context.CancelFunc
	var collectorWG sync.WaitGroup
	logStage := func(st string) error {
		stage = st
		return store.Append("logs/runner.log", map[string]any{"time": time.Now().UTC().Format(time.RFC3339Nano), "stage": st})
	}
	defer func() {
		failureStage := stage
		if releaseDeployment != nil {
			defer releaseDeployment()
		}
		if collectorCancel != nil {
			collectorCancel()
			collectorWG.Wait()
		}
		if engine != nil {
			engine.close()
			if !engine.measureStart.IsZero() {
				m.Workload["measurement_started_at_utc"] = engine.measureStart.UTC().Format(time.RFC3339Nano)
				m.Workload["measurement_finished_at_utc"] = engine.measureEnd.UTC().Format(time.RFC3339Nano)
			}
		}
		if retErr != nil {
			m.Result["partial_counters"] = rec.Snapshot()
			for _, k := range []string{"publish", "delivery", "scheduling_lag", "connect"} {
				name := "latency/" + k + ".hdr.json"
				if _, e := os.Stat(filepath.Join(store.Dir, name)); os.IsNotExist(e) {
					retErr = errors.Join(retErr, store.JSON(name, rec.Histogram(k)))
				}
			}
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if retErr == nil {
			failureStage = "TEARDOWN"
		}
		retErr = errors.Join(retErr, logStage("TEARDOWN"))
		if brokerDriver != nil {
			retErr = errors.Join(retErr, brokerDriver.Cleanup(cleanupCtx))
		}
		if dpl.Owned {
			logs, e := dpl.Logs(cleanupCtx)
			if e != nil {
				m.Warnings = append(m.Warnings, "broker log collection: "+e.Error())
			}
			if e = store.Write("logs/broker.log", logs); e != nil {
				retErr = errors.Join(retErr, e)
			}
			if e = dpl.Stop(cleanupCtx, s.SUT.Mode); e != nil {
				retErr = errors.Join(retErr, e)
			}
		}
		if retErr == nil {
			failureStage = "FINALIZE"
		}
		retErr = errors.Join(retErr, logStage("FINALIZE"))
		if retErr != nil {
			m.Status = "failed"
			m.FailureStage = failureStage
			m.FailureReason = retErr.Error()
		} else {
			m.Status = "completed"
		}
		if e := store.Finalize(&m); e != nil {
			retErr = errors.Join(retErr, e)
		}
	}()
	b, err := yaml.Marshal(s)
	if err != nil {
		return dir, err
	}
	m.Scenario = map[string]any{"name": s.Name, "sha256": manifest.Hash(b), "random_seed": s.RandomSeed, "repetition": repetition, "configuration": s}
	m.Source = hostinfo.Source()
	m.Host = hostinfo.Collect(root)
	m.Workload = map[string]any{"requested_clients": s.Clients.Producers + s.Clients.Consumers, "actual_logical_clients": 0, "actual_connections": nil, "requested_rate": s.Traffic.Rate, "payload_bytes": s.Traffic.PayloadBytes, "timing": s.Timing}
	m.Warnings = append(m.Warnings, "Single-host measurement; host network counters include unrelated traffic. Physical sockets and logical clients are distinct.")
	if err = store.Write("scenario.yaml", b); err != nil {
		return dir, err
	}
	if err = store.JSON("manifest.partial.json", m); err != nil {
		return dir, err
	}
	if err = logStage("PREPARE"); err != nil {
		return dir, err
	}
	d, err := registry.New(s, m.RunID)
	if err != nil {
		return dir, err
	}
	brokerDriver = d
	release, lockErr := acquireDeployment(root, s.SUT.Broker)
	if lockErr != nil {
		return dir, lockErr
	}
	releaseDeployment = release
	m.SUT = d.Settings()
	var lock struct {
		Brokers map[string]struct {
			Version string `yaml:"version"`
			Image   string `yaml:"image"`
			Digest  string `yaml:"digest"`
		} `yaml:"brokers"`
	}
	lockBytes, lockErr := os.ReadFile(filepath.Join(root, "versions.lock.yaml"))
	if lockErr != nil {
		return dir, lockErr
	}
	if lockErr = yaml.Unmarshal(lockBytes, &lock); lockErr != nil {
		return dir, lockErr
	}
	m.SUT["version"] = lock.Brokers[s.SUT.Broker].Version
	m.SUT["image"] = lock.Brokers[s.SUT.Broker].Image
	dpl.Image = lock.Brokers[s.SUT.Broker].Image
	requestedDigest := s.SUT.ImageDigest
	if requestedDigest == "" {
		requestedDigest = lock.Brokers[s.SUT.Broker].Digest
	}
	if requestedDigest != "" {
		dpl.Image += "@" + requestedDigest
	}
	m.SUT["broker"] = s.SUT.Broker
	m.SUT["profile"] = s.SUT.Profile
	m.SUT["capabilities"] = d.Capabilities()
	configs, err := dpl.Configuration()
	if err != nil {
		return dir, err
	}
	hashes := map[string]string{}
	for name, b := range configs {
		if err = store.Write("sut-config/"+name, b); err != nil {
			return dir, err
		}
		hashes[name] = manifest.Hash(b)
	}
	cb, _ := json.Marshal(hashes)
	m.SUT["configuration_sha256"] = manifest.Hash(cb)
	if err = store.JSON("sut-config/effective-settings.json", m.SUT); err != nil {
		return dir, err
	}
	if err = logStage("DEPLOY"); err != nil {
		return dir, err
	}
	deployCtx, cancel := context.WithTimeout(ctx, time.Duration(s.Connection.TimeoutSeconds)*time.Second)
	defer cancel()
	if err = dpl.Start(deployCtx, s.SUT.Mode); err != nil {
		return dir, err
	}
	if err = logStage("WAIT_HEALTH"); err != nil {
		return dir, err
	}
	if err = dpl.Wait(deployCtx); err != nil {
		return dir, err
	}
	identity, err := dpl.Identity(ctx)
	if err != nil {
		return dir, err
	}
	for k, v := range identity {
		m.SUT[k] = v
	}
	if requestedDigest != "" {
		matched := false
		for _, digest := range identity["all_repo_digests"].([]string) {
			if strings.HasSuffix(digest, "@"+requestedDigest) {
				matched = true
			}
		}
		if !matched {
			return dir, fmt.Errorf("resolved image digest differs from requested digest")
		}
	}
	if err = logStage("SETUP"); err != nil {
		return dir, err
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, time.Duration(s.Connection.TimeoutSeconds)*time.Second)
	err = d.Prepare(setupCtx)
	setupCancel()
	if err != nil {
		return dir, err
	}
	if err = logStage("CONNECT"); err != nil {
		return dir, err
	}
	token := sha256.Sum256([]byte(m.RunID))
	engine = &Engine{s: s, d: d, r: rec, store: store, origin: time.Now()}
	copy(engine.token[:], token[:16])
	collectCtx, cc := context.WithCancel(ctx)
	collectorCancel = cc
	collect := func(file string, fn func(context.Context) map[string]any) {
		collectorWG.Add(1)
		go func() {
			defer collectorWG.Done()
			t := time.NewTicker(time.Duration(s.Collection.SystemIntervalMS) * time.Millisecond)
			defer t.Stop()
			for {
				c, cancel := context.WithTimeout(collectCtx, 3*time.Second)
				row := fn(c)
				cancel()
				if e := store.Append(file, row); e != nil {
					rec.Mu.Lock()
					rec.Invalid = e.Error()
					rec.Mu.Unlock()
				}
				select {
				case <-collectCtx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	collect("metrics/generator.jsonl", func(context.Context) map[string]any {
		v := systemmetrics.Process()
		v["counters"] = rec.Snapshot()
		v["delivery_latency"] = rec.Window()
		return v
	})
	collect("metrics/system.jsonl", func(c context.Context) map[string]any { return systemmetrics.Container(c, dpl.Container) })
	processName, _ := m.SUT["process_name"].(string)
	collect("metrics/broker-proc.jsonl", func(c context.Context) map[string]any {
		return systemmetrics.ProcContainer(c, dpl.Container, processName)
	})
	if native, ok := systemmetrics.NativeCollectors[s.SUT.Broker]; ok {
		collect("metrics/broker.jsonl", native)
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, time.Duration(s.Connection.TimeoutSeconds)*time.Second)
	err = engine.connect(connectCtx)
	connectCancel()
	m.Workload["actual_logical_clients"] = len(engine.producers) + len(engine.consumers)
	if err != nil {
		return dir, err
	}
	engine.receive(ctx)
	if err = logStage("WARMUP"); err != nil {
		return dir, err
	}
	if err = engine.traffic(ctx, time.Duration(s.Timing.Warmup)*time.Second, 1); err != nil {
		return dir, err
	}
	if err = logStage("MEASURE"); err != nil {
		return dir, err
	}
	faultDone := make(chan error, 1)
	faultCtx, faultCancel := context.WithCancel(ctx)
	defer faultCancel()
	if s.Experiment == "reconnect_storm" || s.Experiment == "broker_restart" {
		go func() {
			origin := time.Now()
			for at := s.Fault.AtSeconds; at+s.Fault.DowntimeSeconds < s.Timing.Measurement; {
				if err := pause(faultCtx, max(0, time.Until(origin.Add(time.Duration(at)*time.Second)))); err != nil {
					faultDone <- err
					return
				}
				start := time.Now()
				var err error
				if s.Experiment == "reconnect_storm" {
					err = engine.reconnect(faultCtx)
				} else {
					err = dpl.Restart(faultCtx, time.Duration(s.Fault.DowntimeSeconds)*time.Second)
				}
				writeErr := store.Append("metrics/events.jsonl", map[string]any{"schema_version": 1, "event": s.Experiment, "started_at_utc": start.UTC().Format(time.RFC3339Nano), "finished_at_utc": time.Now().UTC().Format(time.RFC3339Nano), "duration_seconds": time.Since(start).Seconds(), "success": err == nil})
				if err = errors.Join(err, writeErr); err != nil {
					faultDone <- err
					return
				}
				if s.Fault.RepeatSeconds == 0 {
					break
				}
				at += s.Fault.RepeatSeconds
			}
			faultDone <- nil
		}()
	} else {
		faultDone <- nil
	}
	trafficErr := engine.traffic(ctx, time.Duration(s.Timing.Measurement)*time.Second, 2)
	faultCancel()
	faultErr := <-faultDone
	if err = errors.Join(trafficErr, faultErr); err != nil {
		return dir, err
	}
	if err = logStage("COOLDOWN"); err != nil {
		return dir, err
	}
	if err = pause(ctx, time.Duration(s.Timing.Cooldown)*time.Second); err != nil {
		return dir, err
	}
	connCtx, connCancel := context.WithTimeout(ctx, 3*time.Second)
	connRow := systemmetrics.ProcContainer(connCtx, dpl.Container, processName)
	connCancel()
	protocolPort, _ := m.SUT["protocol_port"].(int)
	if n, ok := systemmetrics.ConnectionsOnPort(connRow, protocolPort); ok {
		m.Workload["actual_connections"] = n
		m.Workload["connection_count_method"] = "established server TCP endpoints at end of cooldown; protocol port only"
	} else {
		m.Warnings = append(m.Warnings, "broker-side TCP count unavailable")
	}
	engine.close()
	collectorCancel()
	collectorWG.Wait()
	collectorCancel = nil
	if err = logStage("VALIDATE"); err != nil {
		return dir, err
	}
	counters := rec.Snapshot()
	copies := uint64(1)
	if s.Topology.Delivery == "broadcast" {
		copies = uint64(s.ActiveConsumers())
	}
	expected := counters["sent"] * copies
	if expected > counters["unique_delivered"] {
		counters["lost"] = expected - counters["unique_delivered"]
	}
	requested := workload.Requested(time.Duration(s.Timing.Measurement)*time.Second, s.Traffic.Rate, s.Traffic.BurstMultiplier, time.Duration(s.Traffic.BurstEverySeconds)*time.Second, time.Duration(s.Traffic.BurstDurationSeconds)*time.Second)
	saturated := requested > 0 && float64(counters["sent"])/requested < 1-s.Traffic.RateTolerance
	m.Result = map[string]any{"generator_saturated": saturated, "broker_saturated": nil, "counters": counters, "correctness_invalid_reason": rec.Invalid, "requested_messages": requested}
	m.Workload["actual_offered_rate"] = float64(counters["sent"]) / float64(s.Timing.Measurement)
	if saturated {
		m.Result["generator_saturation_reason"] = "actual publish invocations below requested arrivals tolerance; inspect scheduling lag and generator CPU"
	}
	if rec.Invalid != "" {
		m.Warnings = append(m.Warnings, rec.Invalid)
	}
	if err = logStage("COLLECT"); err != nil {
		return dir, err
	}
	if err = store.JSON("correctness/summary.json", counters); err != nil {
		return dir, err
	}
	for _, k := range []string{"publish", "delivery", "scheduling_lag", "connect"} {
		if err = store.JSON("latency/"+k+".hdr.json", rec.Histogram(k)); err != nil {
			return dir, err
		}
	}
	if rec.Invalid != "" {
		stage = "VALIDATE"
		return dir, fmt.Errorf("invalid measurement: %s", rec.Invalid)
	}
	return dir, nil
}

// Root resolves the repository containing the deployment and schema assets.
func Root() (string, error) {
	p, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, e := os.Stat(filepath.Join(p, "versions.lock.yaml")); e == nil {
			return p, nil
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", fmt.Errorf("run bench inside the repository")
		}
		p = parent
	}
}
