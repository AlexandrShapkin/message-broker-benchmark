package result

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/message-broker-benchmark/message-broker-benchmark/internal/manifest"
	"github.com/message-broker-benchmark/message-broker-benchmark/internal/scenario"
)

type Store struct {
	Dir    string
	mu     sync.Mutex
	sealed bool
}

func Create(root, name string) (*Store, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	var b [8]byte
	if _, e := rand.Read(b[:]); e != nil {
		return nil, e
	}
	id := time.Now().UTC().Format("20060102T150405Z") + "-" + name + "-" + hex.EncodeToString(b[:])
	dir := filepath.Join(root, id)
	if e := os.Mkdir(dir, 0755); e != nil {
		return nil, e
	}
	for _, p := range []string{"logs", "sut-config", "metrics", "latency", "correctness", "summary"} {
		if e := os.Mkdir(filepath.Join(dir, p), 0755); e != nil {
			return nil, e
		}
	}
	return &Store{Dir: dir}, nil
}
func (s *Store) path(name string) (string, error) {
	if filepath.IsAbs(name) || name == "." || strings.HasPrefix(filepath.Clean(name), "..") {
		return "", fmt.Errorf("unsafe artifact path %q", name)
	}
	return filepath.Join(s.Dir, name), nil
}
func (s *Store) Write(name string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return fmt.Errorf("run is sealed")
	}
	p, e := s.path(name)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}
func (s *Store) JSON(name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return s.Write(name, append(b, '\n'))
}
func (s *Store) Append(name string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return fmt.Errorf("run is sealed")
	}
	p, e := s.path(name)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	e = json.NewEncoder(f).Encode(v)
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}
func (s *Store) Finalize(m *manifest.Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return fmt.Errorf("run already sealed")
	}
	m.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	err := filepath.WalkDir(s.Dir, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(s.Dir, p)
		if e != nil {
			return e
		}
		if rel == "manifest.json" || rel == "manifest.partial.json" {
			return nil
		}
		hash, e := manifest.FileHash(p)
		if e != nil {
			return e
		}
		m.Artifacts[filepath.ToSlash(rel)] = hash
		return nil
	})
	if err != nil {
		return err
	}
	b, e := m.JSON()
	if e != nil {
		return e
	}
	var document any
	if e = json.Unmarshal(b, &document); e != nil {
		return e
	}
	validationErr := scenario.ValidateDocument("manifest.schema.json", document)
	if validationErr != nil {
		// Even an internal metadata defect must leave a failed final manifest.
		m.Status = "failed"
		m.FailureStage = "FINALIZE"
		m.FailureReason = "manifest schema validation: " + validationErr.Error()
		b, e = m.JSON()
		if e != nil {
			return e
		}
	}
	f, e := os.OpenFile(filepath.Join(s.Dir, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0444)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	s.sealed = true
	return validationErr
}
