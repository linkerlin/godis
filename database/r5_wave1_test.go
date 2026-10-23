package database

// R5 第一波（Redis 8.4–8.10 命令面）测试：注册名 / arity / 缺键 / 错类型 /
// 一条成功路径。规格以 Redis 8.10 命令页为准。

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/memory"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// ---- R5-1 error replies must not carry raw CR/LF ----

func TestR5ErrorReplyNoRawCRLF(t *testing.T) {
	r := protocol.MakeErrReply("ERR unknown command 'x\r\nSET 1 2'")
	b := string(r.ToBytes())
	if strings.ContainsAny(strings.TrimSuffix(b, "\r\n"), "\r") {
		t.Errorf("error reply contains inner CR: %q", b)
	}
	if strings.ContainsAny(strings.TrimSuffix(b, "\r\n")[1:], "\n") {
		t.Errorf("error reply contains inner LF: %q", b)
	}
	if !strings.HasPrefix(b, "-ERR unknown command 'x") {
		t.Errorf("error reply prefix broken: %q", b)
	}
	// Error() keeps the original text for programmatic matching.
	if r.Error() != "ERR unknown command 'x\r\nSET 1 2'" {
		t.Errorf("Error() should keep original text, got %q", r.Error())
	}
	p := &protocol.ProtocolErrReply{Msg: "expecting '$', got bad\r\ninput"}
	if strings.Count(string(p.ToBytes()), "\n") != 1 {
		t.Errorf("protocol error reply must stay one line: %q", p.ToBytes())
	}
}

// ---- R5-2 SUNIONCARD / SDIFFCARD ----

func r5SetupSets(t *testing.T) {
	t.Helper()
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5k1", "r5k2"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SADD", "r5k1", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SADD", "r5k2", "c", "d", "e")), 3)
}

func TestR5SUnionCard(t *testing.T) {
	r5SetupSets(t)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "2", "r5k1", "r5k2")), 5)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "2", "r5k1", "r5k2", "LIMIT", "3")), 3)
	// Missing keys count as empty sets.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "1", "r5missing")), 0)
	// numkeys / LIMIT validation.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "0", "r5k1")),
		"ERR numkeys should be greater than 0")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "1", "r5k1", "LIMIT", "-1")),
		"ERR LIMIT can't be negative")
	// Wrong type.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5str", "v"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SUNIONCARD", "1", "r5str")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5str"))
}

func TestR5SDiffCard(t *testing.T) {
	r5SetupSets(t)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SDIFFCARD", "2", "r5k1", "r5k2")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SDIFFCARD", "2", "r5k1", "r5k2", "LIMIT", "1")), 1)
	// Missing first key → empty difference.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SDIFFCARD", "1", "r5missing")), 0)
	// SDIFFCARD has no APPROX.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SDIFFCARD", "2", "r5k1", "r5k2", "APPROX")),
		"ERR syntax error")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5k1", "r5k2"))
}

// ---- R5-3 LMOVEM / BLMOVEM ----

func TestR5LMoveM(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5mylist", "r5myotherlist"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5mylist", "1", "2", "3", "4")), 4)

	// Docs example: COUNT 2 OBO pops 1,2 and pushes to head one by one → dest [2,1].
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5mylist", "r5myotherlist", "LEFT", "LEFT", "COUNT", "2", "OBO")), []string{"2", "1"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5mylist", "0", "-1")), []string{"3", "4"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5myotherlist", "0", "-1")), []string{"2", "1"})

	// EXACTLY with insufficient elements → nil, nothing moved.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5mylist", "r5myotherlist", "LEFT", "LEFT", "EXACTLY", "5", "BULK")))
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5mylist", "0", "-1")), []string{"3", "4"})

	// Without the how-many block: behaves like LMOVE but returns a one-element array.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5mylist", "r5myotherlist", "LEFT", "RIGHT")), []string{"3"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5myotherlist", "0", "-1")), []string{"2", "1", "3"})

	// Missing source → empty array.
	asserts.AssertMultiBulkReplySize(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5missing", "r5myotherlist", "LEFT", "LEFT")), 0)

	// Source emptied → key deleted.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5mylist", "r5myotherlist", "LEFT", "LEFT", "COUNT", "10", "BULK")), []string{"4"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "r5mylist")), 0)

	// Same-key OBO reverses the moved block in place (docs example).
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5somelist"))
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5somelist", "a", "b", "c"))
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5somelist", "r5somelist", "LEFT", "LEFT", "EXACTLY", "2", "OBO")), []string{"b", "a"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5somelist", "0", "-1")), []string{"b", "a", "c"})

	// BULK on the same key is a no-op for content.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5somelist", "r5somelist", "LEFT", "LEFT", "COUNT", "3", "BULK")), []string{"b", "a", "c"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "r5somelist", "0", "-1")), []string{"b", "a", "c"})

	// Arity / option validation.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("LMOVEM", "a", "b", "LEFT", "LEFT", "COUNT")),
		"ERR syntax error")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"LMOVEM", "r5somelist", "r5somelist", "LEFT", "LEFT", "COUNT", "0", "OBO")),
		"ERR value is out of range, must be positive")

	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5myotherlist", "r5somelist"))
}

func TestR5BLMoveM(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5blsrc", "r5bldst"))
	// Immediate satisfaction: no blocking (COUNT 2 drains the source).
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5blsrc", "x", "y")), 2)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"BLMOVEM", "r5blsrc", "r5bldst", "LEFT", "RIGHT", "0.05", "COUNT", "2", "OBO")), []string{"x", "y"})

	// Timeout with empty source → nil.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine(
		"BLMOVEM", "r5blsrc", "r5bldst", "LEFT", "RIGHT", "0.05", "COUNT", "1", "OBO")))

	// Timeout with EXACTLY unmet → nil, nothing moved.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine(
		"BLMOVEM", "r5blsrc", "r5bldst", "LEFT", "RIGHT", "0.05", "EXACTLY", "2", "BULK")))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "r5blsrc")), 0)
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5bldst"))
}

func TestR5BLMoveMWokenByPush(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5blsrc2", "r5bldst2"))
	go func() {
		time.Sleep(150 * time.Millisecond)
		testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5blsrc2", "m1", "m2"))
	}()
	start := time.Now()
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"BLMOVEM", "r5blsrc2", "r5bldst2", "LEFT", "RIGHT", "3", "EXACTLY", "2", "BULK")), []string{"m1", "m2"})
	if time.Since(start) < 100*time.Millisecond {
		t.Error("BLMOVEM should have blocked until RPUSH")
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5bldst2"))
}

// ---- R5-4 XREAD / XREADGROUP MAXCOUNT / MAXSIZE ----

func TestR5XReadMaxCount(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5s1", "r5s2", "r5s3"))
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			testDB.Exec(nil, utils.ToCmdLine("XADD", "r5s"+string(rune('0'+i)), "*", "f", "v"))
		}
	}
	// MAXCOUNT alone bounds the total, filled from the first stream onward.
	r := testDB.Exec(nil, utils.ToCmdLine("XREAD", "MAXCOUNT", "4", "STREAMS", "r5s1", "r5s2", "0", "0"))
	sr, ok := r.(*StreamReadReply)
	if !ok {
		t.Fatalf("expected StreamReadReply, got %s", r.ToBytes())
	}
	total := 0
	for _, b := range sr.buckets {
		total += len(b.entries)
	}
	if total != 4 {
		t.Errorf("expected 4 entries under MAXCOUNT 4, got %d", total)
	}
	// MAXCOUNT smaller than COUNT → error.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XREAD", "COUNT", "5", "MAXCOUNT", "4", "STREAMS", "r5s1", "0")), "ERR MAXCOUNT must be greater than or equal to COUNT")
	// MAXSIZE always returns at least one entry.
	r = testDB.Exec(nil, utils.ToCmdLine("XREAD", "MAXSIZE", "1", "STREAMS", "r5s1", "0"))
	if _, isNil := r.(*protocol.NullBulkReply); isNil {
		t.Error("MAXSIZE must still return the first entry")
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5s1", "r5s2", "r5s3"))
}

func TestR5XReadGroupMaxCount(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5gs"))
	for j := 0; j < 5; j++ {
		testDB.Exec(nil, utils.ToCmdLine("XADD", "r5gs", "*", "f", "v"))
	}
	testDB.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "r5gs", "g", "0"))
	r := testDB.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c", "MAXCOUNT", "2", "STREAMS", "r5gs", ">"))
	sr, ok := r.(*StreamReadReply)
	if !ok {
		t.Fatalf("expected StreamReadReply, got %s", r.ToBytes())
	}
	if len(sr.buckets) != 1 {
		t.Fatalf("expected 1 stream bucket, got %d", len(sr.buckets))
	}
	entries := sr.buckets[0].entries
	if len(entries) != 2 {
		t.Fatalf("expected 2 delivered entries under MAXCOUNT 2, got %d", len(entries))
	}
	// Group LastID must equal the last delivered entry, not the last collected.
	s, _ := testDB.getAsStream("r5gs")
	group, _ := s.GetGroup("g")
	if group.LastID.String() != entries[len(entries)-1].ID.String() {
		t.Errorf("group LastID %s != last delivered %s", group.LastID.String(), entries[len(entries)-1].ID.String())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5gs"))
}

// ---- R5-5 SLOWLOG GET seventh field ----

func TestR5SlowlogSeventhField(t *testing.T) {
	sl := NewSlowLogger(10, 0) // log everything
	sl.Record(time.Now(), utils.ToCmdLine("SADD", "myset", "1", "2", "3"), "127.0.0.1:1", "")
	entries := sl.GetEntries(1)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	reply := formatSlowlogEntries(entries)
	mr, ok := reply.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("expected MultiRawReply, got %s", reply.ToBytes())
	}
	row := mr.Replies[0].(*protocol.MultiRawReply)
	if len(row.Replies) != 7 {
		t.Fatalf("expected 7 fields, got %d", len(row.Replies))
	}
	asserts.AssertIntReply(t, row.Replies[6], 5) // SADD + key + 3 members
}

// ---- R5-6 AGGREGATE COUNT ----

func TestR5ZUnionAggregateCount(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5z1", "r5z2", "r5zout"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "r5z1", "1", "one", "2", "two")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "r5z2", "1", "one", "2", "two", "3", "three")), 3)

	// one/two appear in both sets → 2; three in one set → 1.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZUNION", "2", "r5z1", "r5z2", "AGGREGATE", "COUNT", "WITHSCORES")),
		[]string{"three", "1", "one", "2", "two", "2"})

	// WEIGHTS scale the per-set contribution.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZUNION", "2", "r5z1", "r5z2", "WEIGHTS", "2", "3", "AGGREGATE", "COUNT", "WITHSCORES")),
		[]string{"three", "3", "one", "5", "two", "5"})

	// ZUNIONSTORE variant.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZUNIONSTORE", "r5zout", "2", "r5z1", "r5z2", "AGGREGATE", "COUNT")), 3)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZRANGE", "r5zout", "0", "-1", "WITHSCORES")), []string{"three", "1", "one", "2", "two", "2"})

	// ZINTER counts membership across all inputs.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZINTER", "2", "r5z1", "r5z2", "AGGREGATE", "COUNT", "WITHSCORES")), []string{"one", "2", "two", "2"})

	// ZDIFF has no AGGREGATE COUNT.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ZDIFF", "2", "r5z1", "r5z2", "AGGREGATE", "COUNT")), "ERR syntax error")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5z1", "r5z2", "r5zout"))
}

// ---- R5-7 SET IF* / DELEX / DIGEST ----

func TestR5SetConditions(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ck"))
	// IFEQ on missing key: not created.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v", "IFEQ", "old")))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "r5ck")), 0)
	// IFNE on missing key: created.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v1", "IFNE", "other")), "OK")
	// IFEQ matching current value.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v2", "IFEQ", "v1")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5ck")), "v2")
	// IFEQ mismatch → nil, value untouched.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v3", "IFEQ", "nope")))
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5ck")), "v2")
	// IFNE mismatch (current == operand) → nil.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v4", "IFNE", "v2")))
	// Condition options are mutually exclusive.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v", "NX", "IFEQ", "v2")),
		"ERR syntax error")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v", "IFEQ", "a", "IFNE", "b")),
		"ERR syntax error")
	// IFEQ replaces expiration only on success.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5ck", "v5", "IFEQ", "v2", "EX", "100")), "OK")
	if ttl := testDB.Exec(nil, utils.ToCmdLine("TTL", "r5ck")); ttl.ToBytes()[0] != ':' {
		t.Errorf("expected int TTL reply, got %s", ttl.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ck"))
}

func TestR5SetDigestConditions(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5dk"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dk", "Hello world")), "OK")
	d := testDB.Exec(nil, utils.ToCmdLine("DIGEST", "r5dk"))
	asserts.AssertBulkReply(t, d, "b6acb9d84a38ff74")
	digest := string(d.(*protocol.BulkReply).Arg)

	// IFDEQ with the right digest sets; wrong digest does not.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dk", "next", "IFDEQ", digest)), "OK")
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dk", "x", "IFDEQ", digest)))
	// IFDNE: sets when digest differs (current value changed).
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dk", "y", "IFDNE", digest)), "OK")
	// IFDNE with matching digest → nil.
	cur := testDB.Exec(nil, utils.ToCmdLine("DIGEST", "r5dk"))
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dk", "z", "IFDNE", string(cur.(*protocol.BulkReply).Arg))))

	// DIGEST: missing key → nil; wrong type → WRONGTYPE.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("DIGEST", "r5missing")))
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5list", "a"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("DIGEST", "r5list")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5dk", "r5list"))
}

func TestR5DelEx(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5dx"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "r5dx", "target")), "OK")

	// Without options: plain delete.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5missing")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx")), 1)

	// IFEQ / IFNE.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5dx", "a"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFEQ", "b")), 0)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5dx")), "a")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFEQ", "a")), 1)
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5dx", "a"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFNE", "a")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFNE", "z")), 1)

	// IFDEQ / IFDNE against DIGEST.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5dx", "Hello world"))
	d := string(testDB.Exec(nil, utils.ToCmdLine("DIGEST", "r5dx")).(*protocol.BulkReply).Arg)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFDEQ", d)), 1)
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5dx", "Hello world"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFDNE", d)), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dx", "IFDNE", "deadbeefdeadbeef")), 1)

	// Non-string value with an option → WRONGTYPE; without an option → deleted.
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", "r5dxlist", "a"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dxlist", "IFEQ", "a")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("DELEX", "r5dxlist")), 1)
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5dx", "r5dxlist"))
}

// ---- R5-8 MSETEX ----

func TestR5MSetEX(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ma", "r5mb"))
	// Wrong arity.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("MSETEX", "2", "r5ma", "v")),
		"ERR wrong number of arguments for 'msetex' command")
	// Success with shared EX.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"MSETEX", "2", "r5ma", "va", "r5mb", "vb", "EX", "100")), 1)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5ma")), "va")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5mb")), "vb")
	ttlA := testDB.Exec(nil, utils.ToCmdLine("TTL", "r5ma"))
	if string(ttlA.ToBytes()) == ":-2" || string(ttlA.ToBytes()) == ":-1" {
		t.Errorf("expected TTL set on r5ma, got %s", ttlA.ToBytes())
	}
	// NX with any existing key → 0, nothing written.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"MSETEX", "2", "r5ma", "new", "r5mc", "new", "NX")), 0)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5ma")), "va")
	// XX with any missing key → 0.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"MSETEX", "2", "r5ma", "new", "r5mc", "new", "XX")), 0)
	// XX with all keys existing → 1.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"MSETEX", "2", "r5ma", "n1", "r5mb", "n2", "XX", "KEEPTTL")), 1)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "r5ma")), "n1")
	// Invalid expire.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"MSETEX", "1", "r5ma", "v", "EX", "0")), "ERR invalid expire time in 'msetex' command")
	// numkeys <= 0.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("MSETEX", "0", "r5ma", "v")),
		"ERR numkeys should be greater than 0")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ma", "r5mb"))
}

// ---- R5-9 script_runner command flag ----

func TestR5ScriptRunnerFlag(t *testing.T) {
	for _, name := range []string{"eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro"} {
		cmd, ok := cmdTable[name]
		if !ok {
			t.Fatalf("command %q not registered", name)
		}
		if cmd.extra == nil {
			t.Fatalf("command %q has no extra flags", name)
		}
		found := false
		for _, f := range cmd.extra.signs {
			if f == "script_runner" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("command %q missing script_runner flag: %v", name, cmd.extra.signs)
		}
	}
}

// ---- R5-10 FT.ALIASLIST ----

func TestR5FTAliasList(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ft:doc"))
	testDB.Exec(nil, utils.ToCmdLine("FT.DROPINDEX", "r5ft:idx", "DD"))
	testDB.Exec(nil, utils.ToCmdLine("FT.ALIASDEL", "r5ft:alias"))
	testDB.Exec(nil, utils.ToCmdLine("HSET", "r5ft:doc", "title", "hello"))
	if r := testDB.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "r5ft:idx", "ON", "HASH", "PREFIX", "1", "r5ft:", "SCHEMA", "title", "TEXT")); protocol.IsErrorReply(r) {
		t.Fatalf("FT.CREATE failed: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"FT.ALIASADD", "r5ft:alias", "r5ft:idx")), "OK")

	// List by real index name and by alias.
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("FT.ALIASLIST", "r5ft:idx")), []string{"r5ft:alias"})
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("FT.ALIASLIST", "r5ft:alias")), []string{"r5ft:alias"})
	// Unknown index → error.
	if r := testDB.Exec(nil, utils.ToCmdLine("FT.ALIASLIST", "r5ft:nope")); !protocol.IsErrorReply(r) {
		t.Errorf("expected error for unknown index, got %s", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("FT.ALIASDEL", "r5ft:alias"))
	testDB.Exec(nil, utils.ToCmdLine("FT.DROPINDEX", "r5ft:idx", "DD"))
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ft:doc"))
}

// ---- R5-11 LRM eviction ----

func TestR5LRMPolicyParsing(t *testing.T) {
	for _, s := range []string{"volatile-lrm", "allkeys-lrm"} {
		p, ok := memory.ParseEvictionPolicyStrict(s)
		if !ok {
			t.Fatalf("policy %q should parse", s)
		}
		if p.String() != s {
			t.Errorf("String() = %q, want %q", p.String(), s)
		}
	}
	if _, ok := memory.ParseEvictionPolicyStrict("volatile-lrmx"); ok {
		t.Error("typo policy should not parse")
	}
	// CONFIG SET now accepts what its own error message lists.
	if r := testServer.Exec(nil, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "volatile-lrm")); protocol.IsErrorReply(r) {
		t.Errorf("CONFIG SET maxmemory-policy volatile-lrm failed: %s", r.ToBytes())
	}
	testServer.Exec(nil, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "noeviction"))
}

func TestR5LRMEvictionPrefersOldestModified(t *testing.T) {
	db := makeTestDB()
	em := NewEvictionManager(db, memory.AllKeysLRM)
	db.SetEvictionManager(em)
	// Sampling is with replacement; a large sample count keeps the pick
	// deterministic with only three keys.
	em.SetSamples(50)
	for _, k := range []string{"fresh", "stale", "mid"} {
		db.PutEntity(k, &database.DataEntity{Data: []byte("v")})
	}
	em.Modify("fresh")
	em.SeedModify("stale", 3600)
	em.SeedModify("mid", 60)
	// Reads must not refresh LRM timestamps (unlike LRU).
	db.GetEntity("stale")
	db.GetEntity("stale")

	evicted := em.EvictKeys(1)
	if evicted != 1 {
		t.Fatalf("expected 1 eviction, got %d", evicted)
	}
	if _, ok := db.GetEntity("stale"); ok {
		t.Error("stale (oldest modified) should have been evicted first")
	}
	if _, ok := db.GetEntity("fresh"); !ok {
		t.Error("fresh should survive")
	}
}
