package database

import (
	"testing"

	"github.com/linkerlin/godis/interface/database"
)

// TestE5IncrementalAccountingMatchesWalk pins the E5-3 invariant: the
// incremental estimatedBytes counter (hot path for maxmemory) must equal the
// full-walk reconciliation across put / overwrite / conditional put / remove.
func TestE5IncrementalAccountingMatchesWalk(t *testing.T) {
	db := makeTestDB()

	db.PutEntity("k1", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("k2", &database.DataEntity{Data: make([]byte, 4096)})
	db.PutEntity("k1", &database.DataEntity{Data: []byte("overwritten-longer-value")})
	if got, want := db.estimatedBytes.Load(), walkEstimatedBytes(db); got != want {
		t.Fatalf("after puts: counter=%d walk=%d", got, want)
	}

	db.PutIfAbsent("k3", &database.DataEntity{Data: []byte("x")})
	db.PutIfAbsent("k1", &database.DataEntity{Data: []byte("must-not-count")}) // k1 exists
	db.PutIfExists("k3", &database.DataEntity{Data: make([]byte, 2048)})
	db.PutIfExists("missing", &database.DataEntity{Data: []byte("no-op")})
	if got, want := db.estimatedBytes.Load(), walkEstimatedBytes(db); got != want {
		t.Fatalf("after conditional puts: counter=%d walk=%d", got, want)
	}

	db.Remove("k3")
	db.Remove("k2")
	db.Remove("missing") // no-op
	if got, want := db.estimatedBytes.Load(), walkEstimatedBytes(db); got != want {
		t.Fatalf("after removes: counter=%d walk=%d", got, want)
	}
	if db.estimatedBytes.Load() <= 0 {
		t.Fatalf("remaining key k1 must still be accounted, got %d", db.estimatedBytes.Load())
	}
}
