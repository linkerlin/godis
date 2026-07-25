package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2anInfoSyncCounters(t *testing.T) {
	resetServerStats()
	noteSyncFull()
	noteSyncFull()
	noteSyncPartialOK()
	noteSyncPartialErr()
	noteSyncPartialErr()
	noteSyncPartialErr()

	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"sync_full:2",
		"sync_partial_ok:1",
		"sync_partial_err:3",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("INFO missing %q in:\n%s", want, s)
		}
	}

	resetServerStats()
	r = server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok = r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO after reset: %T", r)
	}
	s = string(bulk.Arg)
	for _, want := range []string{
		"sync_full:0",
		"sync_partial_ok:0",
		"sync_partial_err:0",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("RESETSTAT should clear %q in:\n%s", want, s)
		}
	}
}

func TestM2anDumpRestoreCMSTopKTDigest(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYDIM", "cms", "10", "4")), "OK")
	db.Exec(nil, utils.ToCmdLine("CMS.INCRBY", "cms", "apple", "5"))
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "cms"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP cms: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "cms"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("cms2"), []byte("0"), bulk.Arg}), "OK")
	q := db.Exec(nil, utils.ToCmdLine("CMS.QUERY", "cms2", "apple"))
	mr, ok := q.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "5" {
		t.Fatalf("CMS after RESTORE: %s", q.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "tk", "3")), "OK")
	db.Exec(nil, utils.ToCmdLine("TOPK.ADD", "tk", "x", "x", "y"))
	dump = db.Exec(nil, utils.ToCmdLine("DUMP", "tk"))
	bulk, ok = dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP topk: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "tk"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("tk2"), []byte("0"), bulk.Arg}), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "tk2")), "topk")
	qr := db.Exec(nil, utils.ToCmdLine("TOPK.QUERY", "tk2", "x"))
	qmr, ok := qr.(*protocol.MultiBulkReply)
	if !ok || len(qmr.Args) < 1 || string(qmr.Args[0]) != "1" {
		t.Fatalf("TOPK after RESTORE: %s", qr.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.ADD", "td", "1", "2", "3")), "OK")
	dump = db.Exec(nil, utils.ToCmdLine("DUMP", "td"))
	bulk, ok = dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP tdigest: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "td"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("td2"), []byte("0"), bulk.Arg}), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "td2")), "tdigest")
	minR := db.Exec(nil, utils.ToCmdLine("TDIGEST.MIN", "td2"))
	minBulk, ok := minR.(*protocol.BulkReply)
	if !ok || string(minBulk.Arg) != "1" {
		t.Fatalf("TDIGEST.MIN after RESTORE: %s", minR.ToBytes())
	}
}
