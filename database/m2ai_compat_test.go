package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2aiMemoryUsageSamples(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("HSET", "h",
		"a", "1", "b", "22", "c", "333", "d", "4444", "e", "55555",
		"f", "6", "g", "7", "h", "8", "i", "9", "j", "10")), 10)

	full := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "h", "SAMPLES", "0"))
	sampled := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "h", "SAMPLES", "3"))
	def := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "h"))
	fi, ok1 := full.(*protocol.IntReply)
	si, ok2 := sampled.(*protocol.IntReply)
	di, ok3 := def.(*protocol.IntReply)
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("MEMORY USAGE types: full=%T sampled=%T def=%T", full, sampled, def)
	}
	if fi.Code <= 0 || si.Code <= 0 || di.Code <= 0 {
		t.Fatalf("MEMORY USAGE sizes: full=%d sampled=%d def=%d", fi.Code, si.Code, di.Code)
	}
	// Sampled estimate should be in the same ballpark as full scan
	ratio := float64(si.Code) / float64(fi.Code)
	if ratio < 0.3 || ratio > 3.0 {
		t.Fatalf("SAMPLES estimate too far: sampled=%d full=%d ratio=%f", si.Code, fi.Code, ratio)
	}
}

func TestM2aiLCSMissingKeyAsEmpty(t *testing.T) {
	db := makeTestDB()
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "no1", "no2")), "")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "no1", "no2", "LEN")), 0)
	db.Exec(nil, utils.ToCmdLine("SET", "k1", "abc"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "k1", "missing")), "")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "k1", "missing", "LEN")), 0)
}

func TestM2aiProtocolErrQuotes(t *testing.T) {
	r := &protocol.ProtocolErrReply{Msg: "expected '$', got 'x'"}
	got := string(r.ToBytes())
	want := "-ERR Protocol error: \"expected '$', got 'x'\"\r\n"
	if got != want {
		t.Fatalf("ProtocolErrReply: got %q want %q", got, want)
	}
}

func TestM2aiSRandMemberNegativeDistribution(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c", "d", "e"))
	seen := make(map[string]int)
	for i := 0; i < 40; i++ {
		r := db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "s", "-5"))
		mr, ok := r.(*protocol.MultiBulkReply)
		if !ok || len(mr.Args) != 5 {
			t.Fatalf("SRANDMEMBER -5: %T %s", r, r.ToBytes())
		}
		for _, a := range mr.Args {
			seen[string(a)]++
		}
	}
	if len(seen) < 3 {
		t.Fatalf("SRANDMEMBER negative count too biased: %v", seen)
	}
}

func TestM2aiSimpleDictRandomKeys(t *testing.T) {
	d := dict.MakeSimple()
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		d.Put(k, nil)
	}
	seen := make(map[string]int)
	for i := 0; i < 100; i++ {
		for _, k := range d.RandomKeys(3) {
			seen[k]++
		}
	}
	if len(seen) < 4 {
		t.Fatalf("SimpleDict.RandomKeys biased: %v", seen)
	}
	distinct := d.RandomDistinctKeys(4)
	if len(distinct) != 4 {
		t.Fatalf("RandomDistinctKeys len=%d", len(distinct))
	}
	uniq := make(map[string]struct{})
	for _, k := range distinct {
		uniq[k] = struct{}{}
	}
	if len(uniq) != 4 {
		t.Fatalf("RandomDistinctKeys not distinct: %v", distinct)
	}
}

func TestM2aiMemoryUsageSamplesSyntax(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "k", "SAMPLES"))
	if !strings.Contains(string(r.ToBytes()), "syntax error") {
		t.Fatalf("expected syntax error, got %s", r.ToBytes())
	}
}
