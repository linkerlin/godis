package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// M1 regression: crash / data-corruption / protocol fixes from 兼容性改进计划.md

func TestM1NegativeBitOffsetNoPanic(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SETBIT", key, "-1", "1")),
		"ERR bit offset is not an integer or out of range")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("GETBIT", key, "-1")),
		"ERR bit offset is not an integer or out of range")
}

func TestM1NegativeSetRangeNoPanic(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SETRANGE", key, "-1", "x")),
		"ERR offset is out of range")
}

func TestM1BitCountSingleIndexNoPanic(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("SETBIT", key, "0", "1"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", key, "0")),
		"ERR syntax error")
}

func TestM1BitPosStartOnlyNoPanic(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("SETBIT", key, "15", "1"))
	result := testDB.Exec(nil, utils.ToCmdLine("BITPOS", key, "1", "0"))
	asserts.AssertIntReply(t, result, 15)
}

func TestM1LPopRPopNegativeCountNoPanic(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("LPUSH", key, "a", "b"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("LPOP", key, "-1")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("RPOP", key, "-1")),
		"ERR value is out of range, must be positive")
}

func TestM1ZAddRejectsNaN(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "nan", "m")),
		"ERR value is not a valid float")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "NaN", "m")),
		"ERR value is not a valid float")
	// Inf is allowed
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "inf", "m")), 1)
}

func TestM1LTrimEmptyDeletesKey(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", key, "a", "b", "c"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("LTRIM", key, "1", "0")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", key)), 0)
}

func TestM1SPopEmptyDeletesKey(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("SADD", key, "a"))
	testDB.Exec(nil, utils.ToCmdLine("SPOP", key))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", key)), 0)
}

func TestM1RenameSameKeyNoDelete(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(8)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, "v"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("RENAME", key, key)), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", key)), "v")
}

func TestM1RenameClearsDestTTL(t *testing.T) {
	testDB.Flush()
	src := utils.RandString(8)
	dest := src + "d"
	testDB.Exec(nil, utils.ToCmdLine("SET", src, "v"))
	testDB.Exec(nil, utils.ToCmdLine("SET", dest, "old", "EX", "1000"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("RENAME", src, dest)), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("TTL", dest)), -1)
}

func TestM1XTrimDoesNotPolluteStream(t *testing.T) {
	testDB.Flush()
	for i := 0; i < 5; i++ {
		testDB.Exec(nil, utils.ToCmdLine("XADD", "s:m1", "*", "n", "1"))
	}
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XTRIM", "s:m1", "MAXLEN", "2")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "s:m1")), 2)
	// No fake high-id entry should remain
	rng := testDB.Exec(nil, utils.ToCmdLine("XRANGE", "s:m1", "-", "+"))
	raw, ok := rng.(*protocol.MultiRawReply)
	if !ok {
		// MultiBulk is also acceptable depending on encoding
		mb, ok2 := rng.(*protocol.MultiBulkReply)
		if !ok2 {
			t.Fatalf("XRANGE: %s", rng.ToBytes())
		}
		if len(mb.Args) != 2 && len(mb.Args) != 4 {
			// 2 entries → typically nested; just ensure no 9999999999999 id string
		}
	}
	if strings.Contains(string(rng.ToBytes()), "9999999999999") {
		t.Fatalf("XTRIM left fake entry: %s", rng.ToBytes())
	}
	_ = raw
}

func TestM1ZMPopNestedReplyNotDoubleEncoded(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("ZADD", "z:m1", "1", "a", "2", "b"))
	result := testDB.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "z:m1", "MIN"))
	raw, ok := result.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("expected MultiRawReply, got %T %s", result, result.ToBytes())
	}
	if len(raw.Replies) != 2 {
		t.Fatalf("expected [key, elements], got %d parts", len(raw.Replies))
	}
	elems, ok := raw.Replies[1].(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("elements should be nested MultiRawReply, got %T", raw.Replies[1])
	}
	if len(elems.Replies) != 1 {
		t.Fatalf("expected 1 popped pair, got %d", len(elems.Replies))
	}
	pair, ok := elems.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(pair.Replies) != 2 {
		t.Fatalf("expected [member, score] pair, got %T", elems.Replies[0])
	}
	// Double-encoding would put the inner array as a bulk string ($N\r\n*2\r\n...)
	body := string(result.ToBytes())
	if strings.Contains(body, "$") && strings.Contains(body, "\r\n*") {
		// Look for bulk length immediately followed by array marker as payload
		for i := 0; i+3 < len(body); i++ {
			if body[i] == '$' {
				j := i + 1
				for j < len(body) && body[j] >= '0' && body[j] <= '9' {
					j++
				}
				if j+3 < len(body) && body[j] == '\r' && body[j+1] == '\n' && body[j+2] == '*' {
					t.Fatalf("ZMPOP appears double-encoded: %q", body)
				}
			}
		}
	}
}

func TestM1JSONNumMultByAndStrLen(t *testing.T) {
	testDB.Flush()
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("JSON.SET", "j:m1", "$", `{"n":2,"s":"ab"}`)), "OK")
	got := testDB.Exec(nil, utils.ToCmdLine("JSON.NUMMULTBY", "j:m1", "$.n", "3"))
	bulk, ok := got.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("NUMMULTBY: %s", got.ToBytes())
	}
	if string(bulk.Arg) != "6" {
		t.Fatalf("NUMMULTBY expected 6, got %s", bulk.Arg)
	}
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("JSON.STRLEN", "j:m1", "$.s")), 2)
}

func TestM1ACLGenPassIsRandom(t *testing.T) {
	testDB.Flush()
	a := testDB.Exec(nil, utils.ToCmdLine("ACL", "GENPASS"))
	b := testDB.Exec(nil, utils.ToCmdLine("ACL", "GENPASS"))
	ba, ok1 := a.(*protocol.BulkReply)
	bb, ok2 := b.(*protocol.BulkReply)
	if !ok1 || !ok2 {
		t.Fatalf("GENPASS: %s / %s", a.ToBytes(), b.ToBytes())
	}
	if len(ba.Arg) != 64 { // 256 bits → 64 hex chars
		t.Fatalf("default genpass length: %d", len(ba.Arg))
	}
	if string(ba.Arg) == string(bb.Arg) {
		t.Fatal("GENPASS returned identical passwords (not random)")
	}
	// Must not be the old fixed cyclic alphabet prefix
	if strings.HasPrefix(string(ba.Arg), "abcdefghijklmnopqrstuvwxyz") {
		t.Fatal("GENPASS still using fixed cyclic alphabet")
	}
}

func TestM1GeoSearchStorePreservesScore(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("GEOADD", "g:m1", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania"))
	n := testDB.Exec(nil, utils.ToCmdLine("GEOSEARCHSTORE", "g:out", "g:m1",
		"FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"))
	asserts.AssertIntReplyGreaterThan(t, n, 0)
	score := testDB.Exec(nil, utils.ToCmdLine("ZSCORE", "g:out", "Catania"))
	bulk, ok := score.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) == "0" || bulk.Arg == nil {
		t.Fatalf("GEOSEARCHSTORE should store geohash score, got %s", score.ToBytes())
	}
}
