package database

// R5 第二波（Redis 8.4–8.10）测试：XREADGROUP CLAIM、XNACK、XADD IDMP、
// INCREX、时序增量、RETURN_STRICT 后处理截止。

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// assertRawFlat flattens a nested MultiRawReply (Int/Bulk leaves) into the
// expected token sequence — used where replies mix ints and bulk strings.
func assertRawFlat(t *testing.T, r redis.Reply, expected []string) {
	t.Helper()
	got := flattenReply(r)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("reply tokens = %v, want %v (wire %q)", got, expected, r.ToBytes())
	}
}

func flattenReply(r redis.Reply) []string {
	switch v := r.(type) {
	case *protocol.MultiRawReply:
		out := []string{}
		for _, sub := range v.Replies {
			out = append(out, flattenReply(sub)...)
		}
		return out
	case *protocol.SetReply:
		out := []string{}
		for _, sub := range v.Data {
			out = append(out, flattenReply(sub)...)
		}
		return out
	case *protocol.BulkReply:
		return []string{string(v.Arg)}
	case *protocol.IntReply:
		return []string{strconv.FormatInt(v.Code, 10)}
	default:
		return strings.Fields(string(r.ToBytes()))
	}
}

// ---- R5-13 XNACK ----

func r5w2SetupStream(t *testing.T, key string) {
	t.Helper()
	testDB.Exec(nil, utils.ToCmdLine("DEL", key))
	testDB.Exec(nil, utils.ToCmdLine("XADD", key, "1-1", "f", "v1"))
	testDB.Exec(nil, utils.ToCmdLine("XADD", key, "1-2", "f", "v2"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0")), "OK")
}

func TestR5W2XNack(t *testing.T) {
	r5w2SetupStream(t, "r5w2:xn")
	// Deliver both to consumer c1.
	r := testDB.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "r5w2:xn", ">"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("xreadgroup: %s", r.ToBytes())
	}

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xn", "g", "FAIL", "IDS", "2", "1-1", "1-2")), 2)
	// Not-pending ID → not counted.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xn", "g", "FAIL", "IDS", "1", "9-9")), 0)

	// Released entries show empty consumer and idle -1 in XPENDING detail.
	s2, _ := testDB.getAsStream("r5w2:xn")
	if s2 == nil {
		t.Fatal("stream missing")
	}
	gr, err := s2.GetGroup("g")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []stream.StreamID{{Timestamp: 1, Sequence: 1}, {Timestamp: 1, Sequence: 2}} {
		pe, ok := gr.Pending[id]
		if !ok {
			t.Fatalf("pending %s missing after XNACK", id.String())
		}
		if pe.Consumer != "" || !pe.DeliveryTime.IsZero() {
			t.Errorf("pending %s not released: consumer=%q zeroTime=%v", id.String(), pe.Consumer, pe.DeliveryTime.IsZero())
		}
	}

	// Wrong mode / bad arity.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("XNACK", "r5w2:xn", "g", "NOPE", "IDS", "1", "1-1")),
		"ERR Invalid XNACK mode. Must be SILENT, FAIL or FATAL")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("XNACK", "r5w2:xn", "g", "FAIL", "IDS", "2", "1-1")),
		"ERR wrong number of arguments for 'xnack' command")
	// Unknown group.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("XNACK", "r5w2:xn", "nog", "FAIL", "IDS", "1", "1-1")),
		"NOGROUP No such key 'r5w2:xn' or consumer group 'nog'")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:xn"))
}

func TestR5W2XNackModesAndForce(t *testing.T) {
	r5w2SetupStream(t, "r5w2:xnm")
	testDB.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "r5w2:xnm", ">"))
	s, _ := testDB.getAsStream("r5w2:xnm")
	gr, _ := s.GetGroup("g")

	// SILENT decrements the delivery counter (was 1 → 0).
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xnm", "g", "SILENT", "IDS", "1", "1-1")), 1)
	if pe := gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 1}]; pe.DeliveryCount != 0 {
		t.Errorf("SILENT: delivery count = %d, want 0", pe.DeliveryCount)
	}
	// RETRYCOUNT overrides the mode.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xnm", "g", "SILENT", "IDS", "1", "1-2", "RETRYCOUNT", "7")), 1)
	if pe := gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 2}]; pe.DeliveryCount != 7 {
		t.Errorf("RETRYCOUNT: delivery count = %d, want 7", pe.DeliveryCount)
	}
	// FATAL saturates.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xnm", "g", "FATAL", "IDS", "1", "1-2")), 1)
	if pe := gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 2}]; pe.DeliveryCount != math.MaxInt64 {
		t.Errorf("FATAL: delivery count = %d, want MaxInt64", pe.DeliveryCount)
	}

	// FORCE creates an unowned PEL entry for an in-stream, not-pending ID.
	testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:xnm", "2-1", "f", "v3"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XNACK", "r5w2:xnm", "g", "FAIL", "IDS", "1", "2-1", "FORCE")), 1)
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:xnm"))
}

// ---- R5-12 XREADGROUP CLAIM ----

func TestR5W2XReadGroupClaim(t *testing.T) {
	r5w2SetupStream(t, "r5w2:cl")
	// Deliver to c1.
	testDB.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "r5w2:cl", ">"))
	// Backdate the pending delivery so CLAIM min-idle is satisfied.
	s, _ := testDB.getAsStream("r5w2:cl")
	gr, _ := s.GetGroup("g")
	for _, pe := range gr.Pending {
		pe.DeliveryTime = time.Now().Add(-10 * time.Second)
	}

	// c2 claims with COUNT 1: one claimed entry, extended 4-field shape.
	r := testDB.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c2", "COUNT", "1", "CLAIM", "5000", "STREAMS", "r5w2:cl", ">"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("claim read: %s", r.ToBytes())
	}
	b := string(r.ToBytes())
	if !strings.Contains(b, "*4\r\n") {
		t.Errorf("claimed entry should use the extended 4-field shape: %q", b)
	}
	if !strings.Contains(b, "1-1") {
		t.Errorf("claimed entry 1-1 missing: %q", b)
	}
	// c2 now owns 1-1.
	if pe := gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 1}]; pe.Consumer != "c2" {
		t.Errorf("1-1 owner = %q, want c2", pe.Consumer)
	}
	if pe := gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 2}]; pe.Consumer != "c1" {
		t.Errorf("1-2 owner = %q, want c1 (COUNT 1)", pe.Consumer)
	}

	// CLAIM ignored for non-">" IDs (history read shape unchanged).
	r = testDB.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c2", "CLAIM", "0", "STREAMS", "r5w2:cl", "0"))
	b = string(r.ToBytes())
	if strings.Contains(b, "*4\r\n") {
		t.Errorf("history read must not use the claimed shape: %q", b)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:cl"))
}

func TestR5W2ClaimPrefersReleased(t *testing.T) {
	r5w2SetupStream(t, "r5w2:clr")
	testDB.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "r5w2:clr", ">"))
	// Release 1-2; backdate 1-1. The XNACK-released entry must be claimed first.
	testDB.Exec(nil, utils.ToCmdLine("XNACK", "r5w2:clr", "g", "FAIL", "IDS", "1", "1-2"))
	s, _ := testDB.getAsStream("r5w2:clr")
	gr, _ := s.GetGroup("g")
	gr.Pending[stream.StreamID{Timestamp: 1, Sequence: 1}].DeliveryTime = time.Now().Add(-10 * time.Second)

	r := testDB.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c2", "COUNT", "1", "CLAIM", "5000", "STREAMS", "r5w2:clr", ">"))
	b := string(r.ToBytes())
	if !strings.Contains(b, "1-2") || strings.Contains(b, "1-1") {
		t.Errorf("XNACK-released 1-2 should be claimed before idle 1-1: %q", b)
	}
	// The released entry's idle extra is -1.
	idx := strings.Index(b, "1-2")
	if idx >= 0 && !strings.Contains(b[idx:], ":-1\r\n") {
		t.Errorf("released entry idle should report -1: %q", b[idx:])
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:clr"))
}

// ---- R5-14 XADD IDMP / XCFGSET ----

func TestR5W2XAddIDMP(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:idmp"))
	id1 := testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMP", "p1", "m1", "*", "f", "v"))
	if protocol.IsErrorReply(id1) {
		t.Fatalf("xadd idmp: %s", id1.ToBytes())
	}
	// Same producer/idempotent ID → original entry, no new data.
	id2 := testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMP", "p1", "m1", "*", "f", "different"))
	if string(id1.ToBytes()) != string(id2.ToBytes()) {
		t.Errorf("IDMP dedup: %s vs %s", id1.ToBytes(), id2.ToBytes())
	}
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "r5w2:idmp")), 1)

	// Different idempotent ID → new entry.
	testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMP", "p1", "m2", "*", "f", "v"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "r5w2:idmp")), 2)

	// IDMPAUTO: same producer + same content dedups; different content does not.
	a1 := testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMPAUTO", "p2", "*", "f", "same"))
	a2 := testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMPAUTO", "p2", "*", "f", "same"))
	if string(a1.ToBytes()) != string(a2.ToBytes()) {
		t.Errorf("IDMPAUTO dedup: %s vs %s", a1.ToBytes(), a2.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("XADD", "r5w2:idmp", "IDMPAUTO", "p2", "*", "f", "other"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "r5w2:idmp")), 4)

	// IDMP requires an auto-generated ID.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XADD", "r5w2:idmp", "IDMP", "p1", "m3", "5-5", "f", "v")),
		"ERR IDMP can only be used with an auto-generated ID (*)")

	// XCFGSET per-stream retention.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XCFGSET", "r5w2:idmp", "IDMP-DURATION", "60", "IDMP-MAXSIZE", "10")), "OK")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"XCFGSET", "r5w2:idmp", "NOPE", "1")),
		"ERR Unsupported stream configuration argument: 'NOPE'")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:idmp"))
}

// ---- R5-15 INCREX ----

func TestR5W2IncrExInt(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:i1"))
	// Default increment by 1 from 0.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i1")), []string{"1", "1"})
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i1")), []string{"2", "1"})

	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:i2", "100"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i2", "BYINT", "5")), []string{"105", "5"})
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i2", "BYINT", "-10")), []string{"95", "-10"})

	// Out of bounds default: skipped, [current, 0], TTL untouched.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:i3", "99"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i3", "BYINT", "5", "UBOUND", "100")), []string{"99", "0"})
	// SATURATE caps and reports the saturated delta.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:i3", "99"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i3", "BYINT", "5", "UBOUND", "100", "SATURATE")), []string{"100", "1"})

	// Non-numeric value.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:i4", "abc"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i4")),
		"ERR value is not an integer or out of range")
	// BYINT over stored float.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:i5", "1.5"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i5", "BYINT", "1")),
		"ERR value is not an integer or out of range")
	// LBOUND > UBOUND.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:i1", "LBOUND", "10", "UBOUND", "5")),
		"ERR LBOUND can't be greater than UBOUND")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:i1", "r5w2:i2", "r5w2:i3", "r5w2:i4", "r5w2:i5"))
}

func TestR5W2IncrExFloatAndTTL(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:f1"))
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:f1", "1.5"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:f1", "BYFLOAT", "0.25")), []string{"1.75", "0.25"})

	// ENX: only sets TTL when none exists.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:f2", "10"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:f2", "BYINT", "1", "EX", "100", "ENX")), []string{"11", "1"})
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:f2", "10", "EX", "500"))
	testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:f2", "BYINT", "1", "EX", "10", "ENX"))
	ttl := testDB.Exec(nil, utils.ToCmdLine("TTL", "r5w2:f2"))
	ttlVal, terr := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(string(ttl.ToBytes()), ":")), 10, 64)
	if terr != nil || ttlVal < 400 || ttlVal > 500 {
		t.Errorf("ENX should keep the existing TTL (~500s), got %s", ttl.ToBytes())
	}

	// PERSIST removes TTL while incrementing.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w2:f3", "5", "EX", "1000"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:f3", "BYINT", "1", "PERSIST")), []string{"6", "1"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("TTL", "r5w2:f3")), -1)

	// ENX without expiry option → syntax error.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("INCREX", "r5w2:f3", "ENX")), "ERR syntax error")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:f1", "r5w2:f2", "r5w2:f3"))
}

// ---- R5-16 Time Series ----

func TestR5W2TSNaNAndCounts(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:ts1"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5w2:ts1")), "OK")
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5w2:ts1", "1000", "10"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5w2:ts1", "1100", "nan"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5w2:ts1", "1200", "30"))

	// count ignores NaN; countNaN / countAll account for it.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "r5w2:ts1", "-", "+", "AGGREGATION", "count", "1000")), []string{"1000", "2"})
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "r5w2:ts1", "-", "+", "AGGREGATION", "countNaN", "1000")), []string{"1000", "1"})
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "r5w2:ts1", "-", "+", "AGGREGATION", "countAll", "1000")), []string{"1000", "3"})
	// avg ignores NaN.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "r5w2:ts1", "-", "+", "AGGREGATION", "avg", "1000")), []string{"1000", "20"})

	// Multiple aggregators per command (8.8): [ts, avg, max].
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "r5w2:ts1", "-", "+", "AGGREGATION", "avg,max", "1000")), []string{"1000", "20", "30"})
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:ts1"))
}

func TestR5W2TSNRange(t *testing.T) {
	for _, k := range []string{"r5w2:n1", "r5w2:n2"} {
		testDB.Exec(nil, utils.ToCmdLine("DEL", k))
		asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", k)), "OK")
	}
	testDB.Exec(nil, utils.ToCmdLine("TS.MADD", "r5w2:n1", "1000", "10", "r5w2:n1", "2000", "12"))
	testDB.Exec(nil, utils.ToCmdLine("TS.MADD", "r5w2:n2", "1000", "20", "r5w2:n2", "3000", "25"))

	// Raw pivot (docs example): missing values as NaN.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NRANGE", "2", "r5w2:n1", "r5w2:n2", "-", "+")),
		[]string{"1000", "10", "20", "2000", "12", "NaN", "3000", "NaN", "25"})
	// Reverse.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NREVRANGE", "2", "r5w2:n1", "r5w2:n2", "-", "+")),
		[]string{"3000", "NaN", "25", "2000", "12", "NaN", "1000", "10", "20"})
	// COUNT keeps lowest timestamps.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NRANGE", "2", "r5w2:n1", "r5w2:n2", "-", "+", "COUNT", "1")),
		[]string{"1000", "10", "20"})

	// Per-key aggregation (docs example: avg and sum over 1000ms buckets).
	for _, k := range []string{"r5w2:n3", "r5w2:n4"} {
		testDB.Exec(nil, utils.ToCmdLine("DEL", k))
		testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", k))
	}
	testDB.Exec(nil, utils.ToCmdLine("TS.MADD", "r5w2:n3", "1000", "10", "r5w2:n3", "1100", "20", "r5w2:n3", "2000", "30"))
	testDB.Exec(nil, utils.ToCmdLine("TS.MADD", "r5w2:n4", "1000", "5", "r5w2:n4", "1100", "15", "r5w2:n4", "2000", "25"))
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NRANGE", "2", "r5w2:n3", "r5w2:n4", "-", "+", "AGGREGATION", "avg", "sum", "1000")),
		[]string{"1000", "15", "20", "2000", "30", "25"})
	// Comma-separated multi-aggregator per key.
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NRANGE", "2", "r5w2:n3", "r5w2:n4", "-", "+", "AGGREGATION", "avg,max", "sum", "1000")),
		[]string{"1000", "15", "20", "20", "2000", "30", "30", "25"})
	// Unsupported options are rejected explicitly, not ignored.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.NRANGE", "1", "r5w2:n1", "-", "+", "LATEST")),
		"TSDB: LATEST is not supported by this build")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:n1", "r5w2:n2", "r5w2:n3", "r5w2:n4"))
}

func TestR5W2TSQueryLabels(t *testing.T) {
	for _, k := range []string{"r5w2:q1", "r5w2:q2"} {
		testDB.Exec(nil, utils.ToCmdLine("DEL", k))
	}
	testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5w2:q1", "LABELS", "room", "study", "type", "temperature"))
	testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5w2:q2", "LABELS", "room", "kitchen", "type", "humidity"))

	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.QUERYLABELS", "LABELS", "FILTER", "room=kitchen")), []string{"room", "type"})
	assertRawFlat(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.QUERYLABELS", "VALUES", "type")), []string{"humidity", "temperature"})
	asserts.AssertMultiBulkReplySize(t, testDB.Exec(nil, utils.ToCmdLine(
		"TS.QUERYLABELS", "VALUES", "nosuch")), 0)
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:q1", "r5w2:q2"))
}

func TestR5W2TSMRangeExcludeEmpty(t *testing.T) {
	for _, k := range []string{"r5w2:m1", "r5w2:m2"} {
		testDB.Exec(nil, utils.ToCmdLine("DEL", k))
		testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", k, "LABELS", "g", "e"))
	}
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5w2:m1", "1000", "1"))
	// r5w2:m2 has no samples in range → excluded with EXCLUDEEMPTY.
	r := testDB.Exec(nil, utils.ToCmdLine(
		"TS.MRANGE", "-", "+", "EXCLUDEEMPTY", "FILTER", "g=e"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("mrange: %T", r)
	}
	if len(mb.Args) != 1 || !strings.Contains(string(mb.Args[0]), "r5w2:m1") {
		t.Errorf("EXCLUDEEMPTY should drop the empty series, got %s", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:m1", "r5w2:m2"))
}

// ---- R5-17 RETURN_STRICT ----

func TestR5W2ReturnStrictCutoff(t *testing.T) {
	if !ftStrictCutoff(time.Now().Add(-time.Second)) {
		// Only true under ON_TIMEOUT=RETURN_STRICT.
		t.Skip("requires ON_TIMEOUT=RETURN_STRICT")
	}
}

func TestR5W2ReturnStrictPolicy(t *testing.T) {
	// Set policy via FT.CONFIG SET, verify cutoff flips, restore.
	old := getFTConfigString("ON_TIMEOUT")
	defer func() {
		ftConfigMu.Lock()
		ftConfig["ON_TIMEOUT"] = old
		ftConfigMu.Unlock()
	}()

	setFTConfigStringForTest := func(v string) {
		ftConfigMu.Lock()
		ftConfig["ON_TIMEOUT"] = v
		ftConfigMu.Unlock()
	}

	setFTConfigStringForTest("RETURN_STRICT")
	if !ftStrictCutoff(time.Now().Add(-time.Second)) {
		t.Error("cutoff should fire past deadline under RETURN_STRICT")
	}
	if ftStrictCutoff(time.Now().Add(time.Hour)) {
		t.Error("cutoff must not fire before the deadline")
	}
	if ftStrictCutoff(time.Time{}) {
		t.Error("no timeout given → no cutoff")
	}

	setFTConfigStringForTest("RETURN")
	if ftStrictCutoff(time.Now().Add(-time.Second)) {
		t.Error("plain RETURN must not cut off post-processing")
	}

	// End to end: a search under RETURN_STRICT replies (partial or full),
	// never a timeout error.
	setFTConfigStringForTest("RETURN_STRICT")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:ftdoc"))
	testDB.Exec(nil, utils.ToCmdLine("HSET", "r5w2:ftdoc", "title", "hello world"))
	testDB.Exec(nil, utils.ToCmdLine("FT.CREATE", "r5w2:ftidx", "ON", "HASH", "PREFIX", "1", "r5w2:ft", "SCHEMA", "title", "TEXT"))
	defer func() {
		testDB.Exec(nil, utils.ToCmdLine("FT.DROPINDEX", "r5w2:ftidx", "DD"))
		testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w2:ftdoc"))
	}()
	r := testDB.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "r5w2:ftidx", "hello", "TIMEOUT", "1"))
	if protocol.IsErrorReply(r) {
		t.Errorf("RETURN_STRICT search should reply, got %s", r.ToBytes())
	}
}
