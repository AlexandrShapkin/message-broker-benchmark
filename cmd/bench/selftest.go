package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/metrics"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/result"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/systemmetrics"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/workload"
	"math"
	"net"
	"path/filepath"
	"runtime"
	"time"
)

func selfTest(ctx context.Context, args []string, root string) (runErr error) {
	f := flag.NewFlagSet("self-test", flag.ContinueOnError)
	clients := f.Int("clients", 1000, "local idle TCP sockets")
	rate := f.Float64("rate", 100000, "scheduled envelopes per second")
	duration := f.Int("seconds", 10, "duration")
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 || *clients < 0 || *clients > 100000 || *rate <= 0 || *rate > 1e9 || math.IsNaN(*rate) || *duration <= 0 || *duration > 86400 {
		return fmt.Errorf("invalid self-test dimensions")
	}
	store, e := result.Create(filepath.Join(root, "results"), "generator-self-test")
	if e != nil {
		return e
	}
	m := manifest.New(filepath.Base(store.Dir))
	m.Scenario["name"] = "generator-self-test"
	m.Result["synthetic"] = true
	stage := "PREPARE"
	defer func() {
		if runErr != nil {
			m.Status = "failed"
			m.FailureStage = stage
			m.FailureReason = runErr.Error()
		} else {
			m.Status = "completed"
		}
		if e := store.Finalize(&m); e != nil {
			runErr = e
		}
	}()
	if e = store.JSON("manifest.partial.json", m); e != nil {
		return e
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	defer listener.Close()
	accepted := make(chan net.Conn)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			select {
			case accepted <- c:
			case <-done:
				_ = c.Close()
				return
			}
		}
	}()
	var sockets []net.Conn
	stage = "CONNECT"
	defer func() {
		for _, c := range sockets {
			_ = c.Close()
		}
	}()
	for i := 0; i < *clients; i++ {
		c, e := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if e != nil {
			return e
		}
		sockets = append(sockets, c)
		select {
		case c := <-accepted:
			sockets = append(sockets, c)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	before := systemmetrics.Process()
	stage = "MEASURE"
	r := metrics.New(1000)
	var token [16]byte
	start := time.Now()
	n := workload.Schedule(ctx, start, time.Duration(*duration)*time.Second, *rate, 1, 0, 0, func(d time.Time) {
		b := metrics.Encode(256, token, 0, 1, int64(d.Sub(start)), 2)
		runtime.KeepAlive(b)
		r.Record("scheduling_lag", time.Since(d))
	})
	after := systemmetrics.Process()
	m.Workload = map[string]any{"requested_rate": *rate, "actual_generation_rate": float64(n) / time.Since(start).Seconds(), "idle_tcp_pairs": *clients, "process_socket_endpoints": 2 * *clients}
	m.Result["resource_before"] = before
	m.Result["resource_after"] = after
	m.Warnings = []string{"Synthetic local TCP/envelope calibration; not a broker result or proof of broker-client-library capacity."}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	runErr = store.JSON("latency/scheduling_lag.hdr.json", r.Histogram("scheduling_lag"))
	fmt.Println(store.Dir)
	return runErr
}
