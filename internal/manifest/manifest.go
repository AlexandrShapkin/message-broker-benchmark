package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"time"
)

type Manifest struct {
	Version       int               `json:"manifest_version"`
	RunID         string            `json:"run_id"`
	Status        string            `json:"status"`
	FailureStage  string            `json:"failure_stage"`
	FailureReason string            `json:"failure_reason"`
	Started       string            `json:"started_at_utc"`
	Finished      string            `json:"finished_at_utc"`
	Scenario      map[string]any    `json:"scenario"`
	Source        map[string]any    `json:"source"`
	SUT           map[string]any    `json:"sut"`
	Host          map[string]any    `json:"host"`
	Workload      map[string]any    `json:"workload"`
	Result        map[string]any    `json:"result"`
	Warnings      []string          `json:"warnings"`
	Artifacts     map[string]string `json:"artifacts"`
}

func New(id string) Manifest {
	return Manifest{Version: 1, RunID: id, Status: "running", Started: time.Now().UTC().Format(time.RFC3339Nano), Scenario: map[string]any{}, Source: map[string]any{}, SUT: map[string]any{}, Host: map[string]any{}, Workload: map[string]any{}, Result: map[string]any{}, Artifacts: map[string]string{}}
}
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func FileHash(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (m Manifest) JSON() ([]byte, error) { return json.MarshalIndent(m, "", "  ") }
