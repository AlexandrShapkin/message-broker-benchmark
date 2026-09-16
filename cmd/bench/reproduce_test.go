package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/message-broker-benchmark/message-broker-benchmark/internal/deployment"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
)

func replayFixture(t *testing.T) (string, string, manifest.Manifest) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	m := manifest.New("replay-fixture")
	m.Status, m.FailureStage, m.FailureReason = "failed", "CONNECT", "fixture timeout"
	m.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	m.Source = map[string]any{"dirty": false, "commit": strings.Repeat("a", 40)}
	s, err := scenario.Load(filepath.Join(root, "scenarios/smoke/nats.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	configs, err := deployment.New(root, s).Configuration()
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]string{}
	for name, b := range configs {
		writeReplayFile(t, filepath.Join(dir, "sut-config", name), b)
		hashes[name] = manifest.Hash(b)
		m.Artifacts["sut-config/"+name] = hashes[name]
	}
	b, _ := json.Marshal(hashes)
	m.SUT = map[string]any{"broker": "nats", "profile": "fast", "configuration_sha256": manifest.Hash(b), "image_digest": "nats@sha256:" + strings.Repeat("b", 64)}
	b, err = os.ReadFile(filepath.Join(root, "scenarios/smoke/nats.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeReplayFile(t, filepath.Join(dir, "scenario.yaml"), b)
	m.Artifacts["scenario.yaml"] = manifest.Hash(b)
	m.Scenario["sha256"] = manifest.Hash(b)
	return root, dir, m
}

func writeReplayFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyReplay(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string, *manifest.Manifest, map[string]any)
		want   string
	}{
		{"complete failed archive", func(*testing.T, string, *manifest.Manifest, map[string]any) {}, ""},
		{"dirty checkout", func(_ *testing.T, _ string, _ *manifest.Manifest, source map[string]any) { source["dirty"] = true }, "clean source commit"},
		{"wrong commit", func(_ *testing.T, _ string, _ *manifest.Manifest, source map[string]any) {
			source["commit"] = "different"
		}, "clean source commit"},
		{"missing scenario seal", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			delete(m.Artifacts, "scenario.yaml")
		}, "missing sealed scenario"},
		{"scenario mismatch", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			m.Scenario["sha256"] = strings.Repeat("c", 64)
		}, "scenario hash mismatch"},
		{"missing config seal", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			delete(m.Artifacts, "sut-config/nats.conf")
		}, "restore recorded nats.conf"},
		{"edited config", func(t *testing.T, dir string, _ *manifest.Manifest, _ map[string]any) {
			writeReplayFile(t, filepath.Join(dir, "sut-config/nats.conf"), []byte("changed"))
		}, "artifact mismatch"},
		{"resealed config", func(t *testing.T, dir string, m *manifest.Manifest, _ map[string]any) {
			b := []byte("changed")
			writeReplayFile(t, filepath.Join(dir, "sut-config/nats.conf"), b)
			m.Artifacts["sut-config/nats.conf"] = manifest.Hash(b)
		}, "restore recorded nats.conf"},
		{"config identity", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			m.SUT["configuration_sha256"] = strings.Repeat("c", 64)
		}, "configuration hash mismatch"},
		{"wrong broker", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) { m.SUT["broker"] = "kafka" }, "SUT identity differ"},
		{"bad digest", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			m.SUT["image_digest"] = "nats@sha256:invalid"
		}, "scenario:"},
		{"unfinished", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) { m.Status = "running" }, "finalized run"},
		{"unsupported schema", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) { m.Version = 99 }, "replay manifest"},
		{"path traversal", func(_ *testing.T, _ string, m *manifest.Manifest, _ map[string]any) {
			m.Artifacts["../outside"] = strings.Repeat("c", 64)
		}, "unsafe artifact path"},
		{"escaping symlink", func(t *testing.T, dir string, m *manifest.Manifest, _ map[string]any) {
			outside := filepath.Join(t.TempDir(), "outside")
			writeReplayFile(t, outside, []byte("fixture"))
			if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
			m.Artifacts["link"] = manifest.Hash([]byte("fixture"))
		}, "escapes run directory"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, dir, m := replayFixture(t)
			source := map[string]any{"dirty": false, "commit": m.Source["commit"]}
			tc.mutate(t, dir, &m, source)
			b, err := m.JSON()
			if err != nil {
				t.Fatal(err)
			}
			writeReplayFile(t, filepath.Join(dir, "manifest.json"), b)
			digest, err := verifyReplay(root, dir, source)
			if tc.want == "" {
				if err != nil || digest != "sha256:"+strings.Repeat("b", 64) {
					t.Fatalf("%q: %v", digest, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}
