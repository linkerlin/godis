package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2fSScanZScanCursor(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c", "d"))
	seen := map[string]bool{}
	cursor := "0"
	for i := 0; i < 20; i++ {
		r := db.Exec(nil, utils.ToCmdLine("SSCAN", "s", cursor, "COUNT", "1"))
		mr := r.(*protocol.MultiRawReply)
		cursor = string(mr.Replies[0].(*protocol.BulkReply).Arg)
		arr := mr.Replies[1].(*protocol.MultiBulkReply)
		for _, m := range arr.Args {
			seen[string(m)] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 4 {
		t.Fatalf("SSCAN incomplete: %v", seen)
	}

	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "a", "2", "b", "3", "c"))
	zseen := map[string]string{}
	cursor = "0"
	for i := 0; i < 20; i++ {
		r := db.Exec(nil, utils.ToCmdLine("ZSCAN", "z", cursor, "COUNT", "1"))
		mr := r.(*protocol.MultiRawReply)
		cursor = string(mr.Replies[0].(*protocol.BulkReply).Arg)
		arr := mr.Replies[1].(*protocol.MultiBulkReply)
		for j := 0; j+1 < len(arr.Args); j += 2 {
			zseen[string(arr.Args[j])] = string(arr.Args[j+1])
		}
		if cursor == "0" {
			break
		}
	}
	if zseen["a"] != "1.5" || zseen["b"] != "2" || zseen["c"] != "3" {
		t.Fatalf("ZSCAN scores: %v", zseen)
	}
}

func TestM2fObjectEncodingHash(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "h")), "hashtable")
	db.Exec(nil, utils.ToCmdLine("SET", "s", "x"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "s")), "raw")
}

func TestM2fEasyCompatFixes(t *testing.T) {
	db := makeTestDB()

	r := db.Exec(nil, utils.ToCmdLine("RPUSHX", "onlyone"))
	asserts.AssertErrReply(t, r, "ERR wrong number of arguments for 'rpushx' command")

	r = db.Exec(nil, utils.ToCmdLine("SETEX", "k", "0", "v"))
	asserts.AssertErrReply(t, r, "ERR invalid expire time in 'setex' command")
	r = db.Exec(nil, utils.ToCmdLine("PSETEX", "k", "0", "v"))
	asserts.AssertErrReply(t, r, "ERR invalid expire time in 'psetex' command")

	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "noz", "[a", "[z")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "noz", "0", "1")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "px", "5000", "v")), "OK")
	if godisVersion != "8.0.0" {
		t.Fatalf("godisVersion=%s want 8.0.0", godisVersion)
	}
	if (&protocol.UnknownErrReply{}).Error() != "ERR unknown error" {
		t.Fatalf("UnknownErrReply: %s", (&protocol.UnknownErrReply{}).Error())
	}
}
