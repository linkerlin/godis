//go:build sqlite_backend

package database

import (
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func clearNativeSearchEngines() {
	searchEnginesMu.Lock()
	searchEngines = make(map[string]*redisearch.RediSearchEngine)
	searchEnginesMu.Unlock()
}

func withSearchBackend(t *testing.T, backend, sqlitePath string, fn func(db *DB)) {
	t.Helper()
	clearNativeSearchEngines()
	resetSQLiteIndexDBForTest()

	old := config.Properties
	config.Properties = &config.ServerProperties{
		SearchBackend:    backend,
		VectorBackend:    backendNative,
		SearchSQLitePath: sqlitePath,
	}
	defer func() {
		resetSQLiteIndexDBForTest()
		config.Properties = old
	}()

	fn(makeTestDB())
}

func withVectorBackend(t *testing.T, backend, sqlitePath string, fn func(db *DB)) {
	t.Helper()
	resetSQLiteIndexDBForTest()

	old := config.Properties
	config.Properties = &config.ServerProperties{
		SearchBackend:    backendNative,
		VectorBackend:    backend,
		SearchSQLitePath: sqlitePath,
	}
	defer func() {
		resetSQLiteIndexDBForTest()
		config.Properties = old
	}()

	fn(makeTestDB())
}

func execTestCmd(db *DB, parts ...string) redis.Reply {
	return db.Exec(nil, utils.ToCmdLine(parts...))
}

func mustStatusOK(t *testing.T, reply redis.Reply, step string) {
	t.Helper()
	if protocol.IsErrorReply(reply) {
		t.Fatalf("%s: %s", step, reply.ToBytes())
	}
	if _, ok := reply.(*protocol.OkReply); !ok {
		t.Fatalf("%s: expected OK, got %s", step, reply.ToBytes())
	}
}

type ftSearchSnapshot struct {
	total  int
	docIDs []string
}

func parseFTSearchSnapshot(reply redis.Reply) ftSearchSnapshot {
	if protocol.IsErrorReply(reply) {
		return ftSearchSnapshot{}
	}
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) == 0 {
		return ftSearchSnapshot{}
	}
	totalN := 0
	if total, ok := multi.Replies[0].(*protocol.IntReply); ok {
		totalN = int(total.Code)
	}
	ids := make([]string, 0)
	// Replies layout: [total, docId, [fields], docId, [fields], ...]
	for i := 1; i+1 < len(multi.Replies); i += 2 {
		if id, ok := multi.Replies[i].(*protocol.BulkReply); ok {
			ids = append(ids, string(id.Arg))
		}
	}
	sort.Strings(ids)
	return ftSearchSnapshot{total: totalN, docIDs: ids}
}

func assertFTSnapshotsEqual(t *testing.T, native, sqlite ftSearchSnapshot) {
	t.Helper()
	if native.total != sqlite.total {
		t.Fatalf("FT total mismatch: native=%d sqlite=%d", native.total, sqlite.total)
	}
	if !stringSliceEqual(native.docIDs, sqlite.docIDs) {
		t.Fatalf("FT doc IDs mismatch: native=%v sqlite=%v", native.docIDs, sqlite.docIDs)
	}
}

func runFTScenario(t *testing.T, backend, sqlitePath string) ftSearchSnapshot {
	t.Helper()
	var snap ftSearchSnapshot
	withSearchBackend(t, backend, sqlitePath, func(db *DB) {
		mustStatusOK(t, execTestCmd(db,
			"FT.CREATE", "articles", "SCHEMA", "title", "TEXT", "body", "TEXT",
		), "ft.create")
		mustStatusOK(t, execTestCmd(db,
			"FT.ADD", "articles", "doc1", "FIELDS",
			"title", "hello", "body", "godis sqlite backend",
		), "ft.add doc1")
		mustStatusOK(t, execTestCmd(db,
			"FT.ADD", "articles", "doc2", "FIELDS",
			"title", "other topic", "body", "unrelated content",
		), "ft.add doc2")
		snap = parseFTSearchSnapshot(execTestCmd(db, "FT.SEARCH", "articles", "hello"))
	})
	return snap
}

func TestFTSearchNativeSQLiteConsistency(t *testing.T) {
	dir := t.TempDir()
	native := runFTScenario(t, backendNative, filepath.Join(dir, "native.db"))
	sqlite := runFTScenario(t, backendSQLite, filepath.Join(dir, "sqlite.db"))
	assertFTSnapshotsEqual(t, native, sqlite)

	if len(native.docIDs) == 0 || native.docIDs[0] != "doc1" {
		t.Fatalf("expected doc1 in results, got %v", native.docIDs)
	}
}

func TestFTSearchNativeSQLiteNoMatch(t *testing.T) {
	dir := t.TempDir()
	var native, sqlite ftSearchSnapshot

	withSearchBackend(t, backendNative, filepath.Join(dir, "native_nomatch.db"), func(db *DB) {
		mustStatusOK(t, execTestCmd(db,
			"FT.CREATE", "idx", "SCHEMA", "title", "TEXT",
		), "create")
		mustStatusOK(t, execTestCmd(db,
			"FT.ADD", "idx", "d1", "FIELDS", "title", "alpha",
		), "add")
		native = parseFTSearchSnapshot(execTestCmd(db, "FT.SEARCH", "idx", "zzzznotfound"))
	})

	withSearchBackend(t, backendSQLite, filepath.Join(dir, "sqlite_nomatch.db"), func(db *DB) {
		mustStatusOK(t, execTestCmd(db,
			"FT.CREATE", "idx", "SCHEMA", "title", "TEXT",
		), "create")
		mustStatusOK(t, execTestCmd(db,
			"FT.ADD", "idx", "d1", "FIELDS", "title", "alpha",
		), "add")
		sqlite = parseFTSearchSnapshot(execTestCmd(db, "FT.SEARCH", "idx", "zzzznotfound"))
	})

	assertFTSnapshotsEqual(t, native, sqlite)
	if native.total != 0 {
		t.Fatalf("expected zero hits, got %d", native.total)
	}
}

func parseVSSearchIDs(reply redis.Reply) []string {
	if protocol.IsEmptyMultiBulkReply(reply) {
		return nil
	}
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) == 0 {
		return nil
	}
	ids := make([]string, 0, len(multi.Args)/3)
	for i := 0; i+2 < len(multi.Args); i += 3 {
		ids = append(ids, string(multi.Args[i]))
	}
	return ids
}

func assertVSTopKEqual(t *testing.T, native, sqlite []string) {
	t.Helper()
	if len(native) != len(sqlite) {
		t.Fatalf("VS hit count mismatch: native=%v sqlite=%v", native, sqlite)
	}
	if len(native) == 0 {
		return
	}
	// Top-1 must agree; for k>1 allow same set with possibly different tie order.
	if native[0] != sqlite[0] {
		t.Fatalf("VS top-1 mismatch: native=%q sqlite=%q", native[0], sqlite[0])
	}
	nCopy := append([]string(nil), native...)
	sCopy := append([]string(nil), sqlite...)
	sort.Strings(nCopy)
	sort.Strings(sCopy)
	if !stringSliceEqual(nCopy, sCopy) {
		t.Fatalf("VS hit set mismatch: native=%v sqlite=%v", native, sqlite)
	}
}

func runVSScenario(t *testing.T, backend, sqlitePath string, k int) []string {
	t.Helper()
	var ids []string
	withVectorBackend(t, backend, sqlitePath, func(db *DB) {
		reply := execTestCmd(db, "VSADD", "embeddings", "near", "[1,0,0]")
		if _, ok := reply.(*protocol.IntReply); !ok {
			t.Fatalf("vs.add near: %s", reply.ToBytes())
		}
		reply = execTestCmd(db, "VSADD", "embeddings", "far", "[0,1,0]")
		if _, ok := reply.(*protocol.IntReply); !ok {
			t.Fatalf("vs.add far: %s", reply.ToBytes())
		}
		ids = parseVSSearchIDs(execTestCmd(db,
			"VSSEARCH", "embeddings", "K", strconv.Itoa(k), "METRIC", "COSINE", "[1,0,0]",
		))
	})
	return ids
}

func TestVSSearchNativeSQLiteConsistency(t *testing.T) {
	dir := t.TempDir()
	native := runVSScenario(t, backendNative, filepath.Join(dir, "vec_native.db"), 2)
	sqlite := runVSScenario(t, backendSQLite, filepath.Join(dir, "vec_sqlite.db"), 2)
	assertVSTopKEqual(t, native, sqlite)

	if len(native) == 0 || native[0] != "near" {
		t.Fatalf("expected near as top hit, got %v", native)
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
