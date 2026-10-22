package raft

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

func newTestBoltStore(t *testing.T, path string) *boltStore {
	t.Helper()
	store, err := newBoltStore(path)
	if err != nil {
		t.Fatalf("newBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestBoltStore_LogRoundTrip(t *testing.T) {
	store := newTestBoltStore(t, filepath.Join(t.TempDir(), "raft.db"))
	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("b")},
		{Index: 3, Term: 2, Type: raft.LogNoop},
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs: %v", err)
	}
	if first, err := store.FirstIndex(); err != nil || first != 1 {
		t.Fatalf("FirstIndex = %d, %v; want 1, nil", first, err)
	}
	if last, err := store.LastIndex(); err != nil || last != 3 {
		t.Fatalf("LastIndex = %d, %v; want 3, nil", last, err)
	}
	var got raft.Log
	if err := store.GetLog(2, &got); err != nil {
		t.Fatalf("GetLog: %v", err)
	}
	if got.Index != 2 || got.Term != 1 || got.Type != raft.LogCommand || string(got.Data) != "b" {
		t.Fatalf("GetLog(2) = %+v", got)
	}
	if err := store.GetLog(99, &got); !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("GetLog(99) err = %v; want raft.ErrLogNotFound", err)
	}
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if first, err := store.FirstIndex(); err != nil || first != 3 {
		t.Fatalf("after DeleteRange FirstIndex = %d, %v; want 3, nil", first, err)
	}
	if last, err := store.LastIndex(); err != nil || last != 3 {
		t.Fatalf("after DeleteRange LastIndex = %d, %v; want 3, nil", last, err)
	}
}

func TestBoltStore_Stable(t *testing.T) {
	store := newTestBoltStore(t, filepath.Join(t.TempDir(), "raft.db"))
	if err := store.SetUint64([]byte("term"), 42); err != nil {
		t.Fatalf("SetUint64: %v", err)
	}
	if v, err := store.GetUint64([]byte("term")); err != nil || v != 42 {
		t.Fatalf("GetUint64 = %d, %v; want 42, nil", v, err)
	}
	if _, err := store.GetUint64([]byte("missing")); !errors.Is(err, errKeyNotFound) {
		t.Fatalf("GetUint64(missing) err = %v; want errKeyNotFound", err)
	}
	if err := store.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, err := store.Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("Get = %q, %v; want \"v\", nil", v, err)
	}
}

func TestBoltStore_Reopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	store := newTestBoltStore(t, path)
	want := raft.Log{Index: 7, Term: 3, Type: raft.LogCommand, Data: []byte("x"), AppendedAt: time.Now()}
	if err := store.StoreLog(&want); err != nil {
		t.Fatalf("StoreLog: %v", err)
	}
	if err := store.SetUint64([]byte("term"), 5); err != nil {
		t.Fatalf("SetUint64: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := newTestBoltStore(t, path)
	var got raft.Log
	if err := reopened.GetLog(7, &got); err != nil {
		t.Fatalf("GetLog after reopen: %v", err)
	}
	if got.Index != want.Index || got.Term != want.Term || got.Type != want.Type || string(got.Data) != string(want.Data) || !got.AppendedAt.Equal(want.AppendedAt) {
		t.Fatalf("reopened log = %+v; want %+v", got, want)
	}
	if v, err := reopened.GetUint64([]byte("term")); err != nil || v != 5 {
		t.Fatalf("GetUint64 after reopen = %d, %v; want 5, nil", v, err)
	}
}

// TestBoltStore_ReadsRaftBoltdbFile verifies the on-disk format stays
// compatible with files written by hashicorp/raft-boltdb: same bucket names,
// big-endian uint64 keys, msgpack-encoded raft.Log values.
func TestBoltStore_ReadsRaftBoltdbFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	legacy := raft.Log{Index: 11, Term: 2, Type: raft.LogCommand, Data: []byte("legacy")}
	enc, err := encodeMsgPack(&legacy)
	if err != nil {
		t.Fatalf("encodeMsgPack: %v", err)
	}
	raw, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(dbLogs)
		if err != nil {
			return err
		}
		return bucket.Put(uint64ToBytes(11), enc.Bytes())
	})
	if err != nil {
		t.Fatalf("seed legacy record: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}

	store := newTestBoltStore(t, path)
	var got raft.Log
	if err := store.GetLog(11, &got); err != nil {
		t.Fatalf("GetLog legacy record: %v", err)
	}
	if got.Index != 11 || got.Term != 2 || got.Type != raft.LogCommand || string(got.Data) != "legacy" {
		t.Fatalf("legacy log = %+v", got)
	}
	if last, err := store.LastIndex(); err != nil || last != 11 {
		t.Fatalf("LastIndex = %d, %v; want 11, nil", last, err)
	}
}
