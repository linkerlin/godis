package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2baFTSearchPhraseSlop(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "ph", "ON", "HASH", "PREFIX", "1", "p:", "SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ph", "p:1", "FIELDS", "t", "hello world"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ph", "p:2", "FIELDS", "t", "hello big world"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ph", "p:3", "FIELDS", "t", "world hello"))

	// Exact phrase (slop 0): only adjacent "hello world"
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ph", `"hello world"`, "NOCONTENT",
	))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("phrase: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)

	// SLOP 1: allows one intervening word
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ph", `"hello world"`, "SLOP", "1", "NOCONTENT",
	))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("SLOP: %T %s", r, r.ToBytes())
	}
	total, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || total.Code < 2 {
		t.Fatalf("expected >=2 with SLOP 1, got %s", r.ToBytes())
	}

	// TIMEOUT accepted
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ph", "hello", "TIMEOUT", "100", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TIMEOUT: %s", r.ToBytes())
	}

	// INORDER accepted (quoted phrases already ordered)
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ph", `"hello world"`, "INORDER", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("INORDER: %s", r.ToBytes())
	}
}

func TestM2baJSONEnhancedLenMutators(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", "$", `{"s":"ab","a":[1],"b":[1,2]}`,
	)), "OK")

	r := db.Exec(nil, utils.ToCmdLine("JSON.STRAPPEND", "j", "$.s", `"c"`))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("STRAPPEND $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 3)

	r = db.Exec(nil, utils.ToCmdLine("JSON.ARRAPPEND", "j", "$.a", "2"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("ARRAPPEND $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 2)

	r = db.Exec(nil, utils.ToCmdLine("JSON.STRLEN", "j", "$.s"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("STRLEN $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 3)
}

func TestM2baVInfoHNSWStubsAndVAddNoop(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v", "VALUES", "2", "1", "0", "ELE", "e1", "NOQUANT", "EF", "200",
	)), 1)
	r := db.Exec(nil, utils.ToCmdLine("VINFO", "v"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("VINFO: %T", r)
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "hnsw-m") || !strings.Contains(body, "max-level") {
		t.Fatalf("VINFO missing hnsw fields: %s", body)
	}
	// EF 200 on first VADD should surface in VINFO (real HNSW, not stub zeros).
	if !strings.Contains(body, "200") {
		t.Fatalf("VINFO should include ef-construction 200: %s", body)
	}
	_ = mr
}
