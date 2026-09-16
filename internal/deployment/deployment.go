package deployment

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Deployment struct {
	Image                            string
	Root, Broker, Project, Container string
	Owned                            bool
}

func New(root string, s scenario.Scenario) *Deployment {
	return &Deployment{Root: root, Broker: s.SUT.Broker, Project: "mbb-" + s.SUT.Broker}
}
func (d *Deployment) Command(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{"compose", "-p", d.Project, "-f", filepath.Join(d.Root, "deploy", d.Broker, "compose.yaml")}
	cmd := exec.CommandContext(ctx, "docker", append(base, args...)...)
	cmd.Env = append(os.Environ(), "BENCH_IMAGE="+d.Image)
	b, e := cmd.CombinedOutput()
	if e != nil {
		return b, fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), e, string(b))
	}
	return b, nil
}
func (d *Deployment) Start(ctx context.Context, mode string) error {
	if mode == "clean" {
		if _, e := d.Command(ctx, "down", "--volumes", "--remove-orphans"); e != nil {
			return e
		}
	}
	d.Owned = true
	_, e := d.Command(ctx, "up", "-d")
	return e
}
func (d *Deployment) Wait(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		b, e := d.Command(ctx, "ps", "-q", "broker")
		if e == nil && strings.TrimSpace(string(b)) != "" {
			d.Container = strings.TrimSpace(string(b))
			cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Health.Status}}", d.Container)
			b, e = cmd.Output()
			if e == nil && strings.TrimSpace(string(b)) == "healthy" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("broker readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
func (d *Deployment) Restart(ctx context.Context, downtime time.Duration) error {
	if _, e := d.Command(ctx, "stop", "broker"); e != nil {
		return e
	}
	timer := time.NewTimer(downtime)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if _, e := d.Command(ctx, "start", "broker"); e != nil {
		return e
	}
	return d.Wait(ctx)
}
func (d *Deployment) Stop(ctx context.Context, mode string) error {
	if !d.Owned {
		return nil
	}
	args := []string{"down", "--remove-orphans"}
	if mode == "clean" {
		args = append(args, "--volumes")
	}
	_, e := d.Command(ctx, args...)
	return e
}
func (d *Deployment) Logs(ctx context.Context) ([]byte, error) {
	return d.Command(ctx, "logs", "--no-color", "--timestamps", "broker")
}
func (d *Deployment) Identity(ctx context.Context) (map[string]any, error) {
	b, e := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .Image}}", d.Container).Output()
	if e != nil {
		return nil, e
	}
	var imageID string
	if e = json.Unmarshal(b, &imageID); e != nil {
		return nil, e
	}
	b, e = exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{json .RepoDigests}}", imageID).Output()
	if e != nil {
		return nil, e
	}
	var digests []string
	if e = json.Unmarshal(b, &digests); e != nil {
		return nil, e
	}
	if len(digests) == 0 {
		return nil, fmt.Errorf("image has no repository digest; research run requires pulled immutable image")
	}
	return map[string]any{"image_id": imageID, "image_digest": digests[0], "all_repo_digests": digests}, nil
}
func (d *Deployment) Configuration() (map[string][]byte, error) {
	out := map[string][]byte{}
	paths := []string{filepath.Join("deploy", d.Broker, "compose.yaml"), "versions.lock.yaml", "go.mod", "go.sum"}
	files, e := os.ReadDir(filepath.Join(d.Root, "configs", d.Broker))
	if e != nil {
		return nil, e
	}
	for _, f := range files {
		if !f.IsDir() {
			paths = append(paths, filepath.Join("configs", d.Broker, f.Name()))
		}
	}
	for _, p := range paths {
		b, e := os.ReadFile(filepath.Join(d.Root, p))
		if e != nil {
			return nil, e
		}
		out[filepath.Base(p)] = b
	}
	return out, nil
}

// Acquire excludes simultaneous benchmark instances sharing the same ports.
func Acquire(root, broker string) (func(), error) {
	// Abstract Unix sockets are shared across checkouts in the same Linux network
	// namespace. A repository-local flock alone cannot protect shared Docker ports.
	socket, e := net.Listen("unix", "\x00message-broker-benchmark-"+broker)
	if e != nil {
		return nil, fmt.Errorf("another %s benchmark owns deployment: %w", broker, e)
	}
	dir := filepath.Join(root, ".cache", "locks")
	if e := os.MkdirAll(dir, 0755); e != nil {
		_ = socket.Close()
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(dir, broker+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		_ = socket.Close()
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		_ = f.Close()
		_ = socket.Close()
		return nil, fmt.Errorf("another %s benchmark owns deployment: %w", broker, e)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close(); _ = socket.Close() }, nil
}
