package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClientOptionsAreValid(t *testing.T) {
	d := &Driver{}
	d.s.Connection.OperationTimeoutMS = 1000
	c, err := kgo.NewClient(d.opts()...)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestIdleKeepaliveCancellation(t *testing.T) {
	started := make(chan struct{})
	stop := keepalive(time.Millisecond, time.Hour, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		stop()
		t.Fatal("idle keepalive never started")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not cancel in-flight request")
	}
}

func TestIdlePolicyIsDisclosed(t *testing.T) {
	d := &Driver{}
	if d.Settings()["idle_metadata_keepalive_seconds"] != 10 {
		t.Fatal("idle control traffic must be disclosed")
	}
}
