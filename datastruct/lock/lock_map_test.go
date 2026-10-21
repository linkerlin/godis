package lock

import (
	"testing"
	"time"
)

func TestSingleAndMultiLocks(t *testing.T) {
	locks := Make(16)
	locks.Lock("a")
	locks.UnLock("a")
	locks.RLock("a")
	locks.RUnLock("a")

	locks.Locks("a", "b", "a")
	locks.UnLocks("a", "b")
	locks.RLocks("a", "b", "b")
	locks.RUnLocks("a", "b")
}

func TestRWLocksRespectWriteExclusion(t *testing.T) {
	locks := Make(32)
	writeKey, readKey := "write", "read"
	for i := 0; locks.spread(fnv32(writeKey)) == locks.spread(fnv32(readKey)); i++ {
		readKey = "read-" + string(rune('a'+i))
	}
	locks.RWLocks([]string{writeKey}, []string{readKey})
	readAcquired := make(chan struct{})
	writeAcquired := make(chan struct{})
	go func() {
		locks.RLock(readKey)
		close(readAcquired)
		locks.RUnLock(readKey)
	}()
	select {
	case <-readAcquired:
	case <-time.After(time.Second):
		t.Fatal("read lock should be shared while RWLocks holds a read key")
	}
	go func() {
		locks.Lock(writeKey)
		close(writeAcquired)
		locks.UnLock(writeKey)
	}()
	select {
	case <-writeAcquired:
		t.Fatal("write lock must wait for RWLocks")
	case <-time.After(20 * time.Millisecond):
	}
	locks.RWUnLocks([]string{writeKey}, []string{readKey})
	select {
	case <-writeAcquired:
	case <-time.After(time.Second):
		t.Fatal("write lock did not proceed after RWUnLocks")
	}
}

func TestMultiLockSerializes(t *testing.T) {
	locks := Make(8)
	locks.Locks("x", "y")
	acquired := make(chan struct{})
	go func() {
		locks.Lock("x")
		close(acquired)
		locks.UnLock("x")
	}()
	select {
	case <-acquired:
		t.Fatal("second lock must wait")
	case <-time.After(20 * time.Millisecond):
	}
	locks.UnLocks("x", "y")
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second lock did not acquire")
	}
}

func TestNilLocksPanics(t *testing.T) {
	var locks *Locks
	defer func() {
		if recover() == nil {
			t.Fatal("nil Locks should panic")
		}
	}()
	locks.Lock("x")
}
