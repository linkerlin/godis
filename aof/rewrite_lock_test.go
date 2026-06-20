package aof

import (
	"errors"
	"testing"
)

func TestAcquireRewriteSlotExclusive(t *testing.T) {
	p := &Persister{}
	if err := p.acquireRewriteSlot(); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := p.acquireRewriteSlot(); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("second acquire = %v, want ErrRewriteInProgress", err)
	}
	p.releaseRewriteSlot()
	if err := p.acquireRewriteSlot(); err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	p.releaseRewriteSlot()
}

func TestRunRewriteAsyncRejectsConcurrent(t *testing.T) {
	p := &Persister{}
	if err := p.acquireRewriteSlot(); err != nil {
		t.Fatal(err)
	}
	defer p.releaseRewriteSlot()

	if err := p.RunRewriteAsync(); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("RunRewriteAsync while held = %v", err)
	}
}
