package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/deployment"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/hostinfo"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"os"
	"path/filepath"
	"strings"
)

func reproduce(ctx context.Context, args []string, root string) error {
	dir := args[0]
	if _, e := os.Stat(dir); e != nil {
		dir = filepath.Join(root, "results", dir)
	}
	digest, e := verifyReplay(root, dir, hostinfo.Source())
	if e != nil {
		return e
	}
	return run(ctx, []string{"run", "--scenario", filepath.Join(dir, "scenario.yaml"), "--image-digest", digest})
}

// verifyReplay performs all archive checks before deployment can mutate SUT state.
// Failed runs can be replayed when their reproduction inputs are complete.
func verifyReplay(root, dir string, source map[string]any) (string, error) {
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return "", e
	}
	var document any
	if e = json.Unmarshal(b, &document); e != nil {
		return "", e
	}
	if e = scenario.ValidateDocument("manifest.schema.json", document); e != nil {
		return "", fmt.Errorf("replay manifest: %w", e)
	}
	var m manifest.Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return "", e
	}
	if m.Status != "completed" && m.Status != "failed" {
		return "", fmt.Errorf("replay requires a finalized run")
	}
	if m.Source["dirty"] != false || source["dirty"] != false || m.Source["commit"] == nil || source["commit"] != m.Source["commit"] {
		return "", fmt.Errorf("exact replay requires the recorded clean source commit; dirty runs require an archived source checkout")
	}
	archive, e := filepath.EvalSymlinks(dir)
	if e != nil {
		return "", e
	}
	for name, want := range m.Artifacts {
		if !filepath.IsLocal(name) {
			return "", fmt.Errorf("unsafe artifact path: %s", name)
		}
		resolved, e := filepath.EvalSymlinks(filepath.Join(archive, name))
		if e != nil {
			return "", e
		}
		relative, e := filepath.Rel(archive, resolved)
		if e != nil || !filepath.IsLocal(relative) {
			return "", fmt.Errorf("artifact path escapes run directory: %s", name)
		}
		hash, e := manifest.FileHash(filepath.Join(dir, name))
		if e != nil {
			return "", e
		}
		if hash != want {
			return "", fmt.Errorf("artifact mismatch: %s", name)
		}
	}
	if m.Artifacts["scenario.yaml"] == "" || m.Artifacts["scenario.yaml"] != m.Scenario["sha256"] {
		return "", fmt.Errorf("missing sealed scenario or scenario hash mismatch")
	}
	s, e := scenario.Load(filepath.Join(dir, "scenario.yaml"))
	if e != nil {
		return "", e
	}
	if s.SUT.Broker != m.SUT["broker"] || s.SUT.Profile != m.SUT["profile"] {
		return "", fmt.Errorf("saved scenario and SUT identity differ")
	}
	configs, e := deployment.New(root, s).Configuration()
	if e != nil {
		return "", e
	}
	hashes := map[string]string{}
	for name, current := range configs {
		want := m.Artifacts["sut-config/"+name]
		if want == "" || manifest.Hash(current) != want {
			return "", fmt.Errorf("restore recorded %s before replay (missing seal or configuration mismatch)", name)
		}
		hashes[name] = want
	}
	configuration, e := json.Marshal(hashes)
	if e != nil {
		return "", e
	}
	if manifest.Hash(configuration) != m.SUT["configuration_sha256"] {
		return "", fmt.Errorf("configuration hash mismatch")
	}
	digest, ok := m.SUT["image_digest"].(string)
	if !ok || !strings.Contains(digest, "@sha256:") {
		return "", fmt.Errorf("saved run lacks immutable image digest")
	}
	_, digest, _ = strings.Cut(digest, "@")
	s.SUT.ImageDigest = digest
	if e = s.Validate(); e != nil {
		return "", e
	}
	return digest, nil
}
