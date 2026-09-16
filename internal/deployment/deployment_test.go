package deployment

import (
	"path/filepath"
	"testing"
)

func TestExclusiveOwnership(t *testing.T) {
	root := t.TempDir()
	broker := "fixture-" + filepath.Base(filepath.Dir(root))
	release, e := Acquire(root, broker)
	if e != nil {
		t.Fatal(e)
	}
	if next, e := Acquire(root, broker); e == nil {
		next()
		t.Fatal("concurrent owner accepted")
	}
	if next, e := Acquire(t.TempDir(), broker); e == nil {
		next()
		t.Fatal("a different checkout bypassed deployment ownership")
	}
	release()
	next, e := Acquire(root, broker)
	if e != nil {
		t.Fatal(e)
	}
	next()
}
