package raft

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// boltStore persists raft logs and stable state in a bbolt database. It is an
// in-tree replacement for the archived hashicorp/raft-boltdb: its boltdb/bolt
// backend fatals under checkptr (enabled by -race) when committing an empty
// inline bucket, which made every raft-backed test crash the nightly race
// gate. The on-disk layout is byte-compatible with raft-boltdb so existing
// raft.db files keep working: buckets "logs"/"conf", 8-byte big-endian keys,
// msgpack-encoded raft.Log values.
var (
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	errKeyNotFound = errors.New("not found")
)

// boltStore implements both raft.LogStore and raft.StableStore.
var (
	_ raft.LogStore    = (*boltStore)(nil)
	_ raft.StableStore = (*boltStore)(nil)
)

type boltStore struct {
	conn *bolt.DB
}

func newBoltStore(path string) (*boltStore, error) {
	conn, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	store := &boltStore{conn: conn}
	if err := store.initialize(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (b *boltStore) initialize() error {
	return b.conn.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(dbConf)
		return err
	})
}

func (b *boltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log, 0 if empty.
func (b *boltStore) FirstIndex() (uint64, error) {
	var idx uint64
	err := b.conn.View(func(tx *bolt.Tx) error {
		curs := tx.Bucket(dbLogs).Cursor()
		if first, _ := curs.First(); first != nil {
			idx = bytesToUint64(first)
		}
		return nil
	})
	return idx, err
}

// LastIndex returns the last known index from the Raft log, 0 if empty.
func (b *boltStore) LastIndex() (uint64, error) {
	var idx uint64
	err := b.conn.View(func(tx *bolt.Tx) error {
		curs := tx.Bucket(dbLogs).Cursor()
		if last, _ := curs.Last(); last != nil {
			idx = bytesToUint64(last)
		}
		return nil
	})
	return idx, err
}

// GetLog retrieves a log from the Raft log at the given index.
func (b *boltStore) GetLog(idx uint64, log *raft.Log) error {
	return b.conn.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(dbLogs).Get(uint64ToBytes(idx))
		if val == nil {
			return raft.ErrLogNotFound
		}
		return decodeMsgPack(val, log)
	})
}

// StoreLog stores a single raft log.
func (b *boltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores a set of raft logs atomically.
func (b *boltStore) StoreLogs(logs []*raft.Log) error {
	return b.conn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dbLogs)
		for _, log := range logs {
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			if err := bucket.Put(uint64ToBytes(log.Index), val.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteRange deletes logs within [min, max] inclusively.
func (b *boltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)
	return b.conn.Update(func(tx *bolt.Tx) error {
		curs := tx.Bucket(dbLogs).Cursor()
		for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
			if bytesToUint64(k) > max {
				break
			}
			if err := curs.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Set stores a raw value in the stable store.
func (b *boltStore) Set(k, v []byte) error {
	return b.conn.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(dbConf).Put(k, v)
	})
}

// Get retrieves a raw value from the stable store.
func (b *boltStore) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.conn.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(dbConf).Get(k)
		if raw == nil {
			return errKeyNotFound
		}
		val = append([]byte(nil), raw...)
		return nil
	})
	return val, err
}

// SetUint64 stores a uint64 value in the stable store.
func (b *boltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 retrieves a uint64 value from the stable store.
func (b *boltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// decodeMsgPack reverses encodeMsgPack on a byte slice input.
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewReader(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// encodeMsgPack writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	if err := enc.Encode(in); err != nil {
		return nil, err
	}
	return buf, nil
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
