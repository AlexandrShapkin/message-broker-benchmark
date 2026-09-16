package result

import (
	"encoding/json"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
	"os"
	"path/filepath"
	"testing"
)

func TestImmutableManifest(t *testing.T) {
	root := t.TempDir()
	s, e := Create(root, "test")
	if e != nil {
		t.Fatal(e)
	}
	s2, e := Create(root, "test")
	if e != nil || s.Dir == s2.Dir {
		t.Fatal("not unique")
	}
	if e = s.Write("scenario.yaml", []byte("fixture")); e != nil {
		t.Fatal(e)
	}
	if s.Write("scenario.yaml", nil) == nil {
		t.Fatal("overwrite allowed")
	}
	if s.Write("../escape", nil) == nil {
		t.Fatal("path escape")
	}
	m := manifest.New(filepath.Base(s.Dir))
	m.Status = "failed"
	m.FailureStage = "CONNECT"
	m.FailureReason = "fixture"
	if e = s.Finalize(&m); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(s.Dir, "manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	var v any
	if e = json.Unmarshal(b, &v); e != nil {
		t.Fatal(e)
	}
	if e = scenario.ValidateDocument("manifest.schema.json", v); e != nil {
		t.Fatal(e)
	}
	if m.Artifacts["scenario.yaml"] != manifest.Hash([]byte("fixture")) {
		t.Fatal("hash mismatch")
	}
	if s.Append("metrics/test.jsonl", 1) == nil || s.Finalize(&m) == nil {
		t.Fatal("sealed write allowed")
	}
}

func TestIncompleteCompletedManifestBecomesFailedEvidence(t *testing.T) {
	s, err := Create(t.TempDir(), "incomplete")
	if err != nil {
		t.Fatal(err)
	}
	m := manifest.New(filepath.Base(s.Dir))
	m.Status = "completed"
	if err = s.Finalize(&m); err == nil {
		t.Fatal("incomplete metadata accepted as completed")
	}
	b, err := os.ReadFile(filepath.Join(s.Dir, "manifest.json"))
	if err != nil {
		t.Fatal("failed metadata evidence missing", err)
	}
	var document any
	if err = json.Unmarshal(b, &document); err != nil {
		t.Fatal(err)
	}
	if err = scenario.ValidateDocument("manifest.schema.json", document); err != nil {
		t.Fatal(err)
	}
	if m.Status != "failed" || m.FailureStage != "FINALIZE" {
		t.Fatal(m.Status, m.FailureStage)
	}
}
