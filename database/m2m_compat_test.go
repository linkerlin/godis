package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2mClientListKillNoEvict(t *testing.T) {
	db := makeTestDB()
	c1 := connection.NewFakeConn()
	c2 := connection.NewFakeConn()
	RegisterClient(c1)
	RegisterClient(c2)
	defer UnregisterClient(c1)
	defer UnregisterClient(c2)

	c1.SetClientName("alice")
	list := db.Exec(c1, utils.ToCmdLine("CLIENT", "LIST"))
	bulk, ok := list.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT LIST: %T", list)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "name=alice") {
		t.Fatalf("CLIENT LIST missing alice: %q", s)
	}
	id1 := c1.GetClientID()
	id2 := c2.GetClientID()
	if !strings.Contains(s, "id="+strconv.FormatInt(id1, 10)) ||
		!strings.Contains(s, "id="+strconv.FormatInt(id2, 10)) {
		t.Fatalf("CLIENT LIST missing ids: %q", s)
	}

	asserts.AssertStatusReply(t, db.Exec(c1, utils.ToCmdLine("CLIENT", "NO-EVICT", "ON")), "OK")
	if !c1.GetNoEvict() {
		t.Fatal("NO-EVICT ON did not set flag")
	}
	asserts.AssertStatusReply(t, db.Exec(c1, utils.ToCmdLine("CLIENT", "NO-EVICT", "OFF")), "OK")

	kill := db.Exec(c1, utils.ToCmdLine("CLIENT", "KILL", "ID", strconv.FormatInt(id2, 10), "SKIPME", "YES"))
	asserts.AssertIntReply(t, kill, 1)
}

func TestM2mObjectEncodingExtras(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", "$", `{"a":1}`)), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "j")), "json")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "bf", "0.01", "100")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "bf")), "bloomflate")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "cf", "100")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "cf")), "cuckoo")
}

func TestM2mBitFieldRo(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "b", "\x00"))
	r := db.Exec(nil, utils.ToCmdLine("BITFIELD_RO", "b", "GET", "u8", "0"))
	if _, ok := r.(*protocol.MultiRawReply); !ok {
		t.Fatalf("BITFIELD_RO GET: %T %s", r, r.ToBytes())
	}
	bad := db.Exec(nil, utils.ToCmdLine("BITFIELD_RO", "b", "SET", "u8", "0", "1"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("BITFIELD_RO SET should err, got %s", bad.ToBytes())
	}
}

func TestM2mTSCreateOptions(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.CREATE", "ts1", "CHUNK_SIZE", "128", "ENCODING", "COMPRESSED", "DUPLICATE_POLICY", "LAST",
	)), "OK")
	bad := db.Exec(nil, utils.ToCmdLine("TS.CREATE", "ts2", "ENCODING", "ZLIB"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("bad ENCODING should err: %s", bad.ToBytes())
	}
	badPol := db.Exec(nil, utils.ToCmdLine("TS.CREATE", "ts3", "DUPLICATE_POLICY", "FOO"))
	if !protocol.IsErrorReply(badPol) {
		t.Fatalf("bad policy should err: %s", badPol.ToBytes())
	}
}

func TestM2mXReadGroupHistoryAndXPendingIdle(t *testing.T) {
	db := makeTestDB()
	const key = "s:m2m"
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", key, "*", "f", "v"))
	bulk, ok := idReply.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("XADD: %s", idReply.ToBytes())
	}
	entryID := string(bulk.Arg)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0")), "OK")

	read := db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, ">"))
	if _, ok := read.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("XREADGROUP >: %s", read.ToBytes())
	}

	hist := db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, "0-0"))
	hm, ok := hist.(*protocol.MultiBulkReply)
	if !ok || len(hm.Args) < 2 {
		t.Fatalf("XREADGROUP history empty: %s", hist.ToBytes())
	}

	time.Sleep(5 * time.Millisecond)
	detail := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g", "IDLE", "0", "0-0", entryID, "10"))
	dm, ok := detail.(*protocol.MultiBulkReply)
	if !ok || len(dm.Args) < 4 {
		t.Fatalf("XPENDING IDLE: %s", detail.ToBytes())
	}
	if string(dm.Args[0]) != entryID {
		t.Fatalf("XPENDING id: got %q want %q", dm.Args[0], entryID)
	}

	filtered := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g", "IDLE", "999999999", "0-0", entryID, "10"))
	fm, ok := filtered.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("XPENDING high IDLE: %T", filtered)
	}
	if len(fm.Args) != 0 {
		t.Fatalf("expected empty pending for high IDLE, got %s", filtered.ToBytes())
	}
}

func TestM2mACLListKeyPatterns(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("ACL", "SETUSER", "u1", "on", "nopass", "~cache:*", "+@all")), "OK")
	list := db.Exec(nil, utils.ToCmdLine("ACL", "LIST"))
	multi, ok := list.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL LIST: %T", list)
	}
	found := false
	for _, line := range multi.Args {
		if strings.Contains(string(line), "user u1") && strings.Contains(string(line), "~cache:*") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL LIST missing ~cache:*: %s", list.ToBytes())
	}
}
