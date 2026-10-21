package database

// TS.READ（8.10，R5-16 收尾）测试：断言取自官方命令页示例。

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func tsReadWire(t *testing.T, args ...string) string {
	t.Helper()
	r := testDB.Exec(nil, utils.ToCmdLine2("TS.READ", args...))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TS.READ %v: %s", args, r.ToBytes())
	}
	return string(r.ToBytes())
}

func TestR5TSReadHistory(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:1"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5ts:1")), "OK")
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:1", "100", "1.0"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:1", "200", "2.0"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:1", "300", "3.0"))

	// Full read from 0 and from the - sentinel.
	if got := tsReadWire(t, "r5ts:1", "0"); !strings.Contains(got, ":100\r\n$1\r\n1\r\n") || !strings.Contains(got, ":300\r\n$1\r\n3\r\n") {
		t.Errorf("read 0: %q", got)
	}
	// MAX_COUNT paging (doc example).
	r := testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "-", "MAX_COUNT", "2"))
	if b, ok := r.(*protocol.MultiRawReply); !ok || len(b.Replies) != 2 {
		t.Errorf("page 1: %s", r.ToBytes())
	}
	if got := tsReadWire(t, "r5ts:1", "201", "MAX_COUNT", "2"); !strings.Contains(got, ":300\r\n") || strings.Contains(got, ":200\r\n") {
		t.Errorf("page 2: %q", got)
	}
	// + includes the latest sample.
	if got := tsReadWire(t, "r5ts:1", "+"); !strings.Contains(got, ":300\r\n") || strings.Contains(got, ":100\r\n") {
		t.Errorf("+ : %q", got)
	}
	// Beyond everything → empty array.
	if got := tsReadWire(t, "r5ts:1", "301"); got != "*0\r\n" {
		t.Errorf("empty: %q", got)
	}
	// Missing key → empty array.
	if got := tsReadWire(t, "r5ts:none", "-"); got != "*0\r\n" {
		t.Errorf("missing key: %q", got)
	}
	// Wrong type.
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5ts:str", "v"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:str", "0")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	// Validation.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "-1")),
		"TSDB: invalid timestamp")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "0", "BLOCK", "-1", "1")),
		"TSDB: BLOCK milliseconds must be a non-negative integer")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "0", "BLOCK", "100", "0")),
		"TSDB: BLOCK min_count must be a positive integer")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "0", "BLOCK", "100", "5", "MAX_COUNT", "2")),
		"TSDB: BLOCK min_count must be <= MAX_COUNT")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:1", "0", "NOPE")),
		"ERR wrong number of arguments for 'ts.read' command")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:1", "r5ts:str"))
}

func TestR5TSReadBlockTimeoutFlush(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:2"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5ts:2")), "OK")
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:2", "100", "1"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:2", "200", "2"))
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:2", "300", "3"))

	// Doc example: BLOCK 1000 10 cannot reach 10 → partial flush of the
	// two qualifying samples after the timeout.
	start := time.Now()
	r := testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:2", "101", "BLOCK", "500", "10"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("partial flush: %s", r.ToBytes())
	}
	if time.Since(start) < 400*time.Millisecond {
		t.Error("should have blocked ~500ms")
	}
	// Nothing qualifies → empty array on timeout.
	if got := tsReadWire(t, "r5ts:2", "301", "BLOCK", "100", "1"); got != "*0\r\n" {
		t.Errorf("timeout empty: %q", got)
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:2"))
}

func TestR5TSReadBlockWokenByAdd(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:3"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5ts:3")), "OK")
	testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:3", "100", "1"))

	// $ tail: only samples added after the call qualify.
	go func() {
		time.Sleep(150 * time.Millisecond)
		testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:3", "400", "4"))
	}()
	start := time.Now()
	r := testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:3", "$", "BLOCK", "3000", "1"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("woken read: %s", r.ToBytes())
	}
	pair := mr.Replies[0].(*protocol.MultiRawReply)
	if string(pair.Replies[0].ToBytes()) != ":400\r\n" {
		t.Errorf("woken sample: %s", pair.Replies[0].ToBytes())
	}
	if time.Since(start) > 2500*time.Millisecond {
		t.Error("should have unblocked on TS.ADD, not on timeout")
	}
	// min_count=2 waits for two samples (INCRBY also wakes; TIMESTAMP is a
	// named option).
	go func() {
		time.Sleep(150 * time.Millisecond)
		testDB.Exec(nil, utils.ToCmdLine("TS.ADD", "r5ts:3", "500", "5"))
		time.Sleep(100 * time.Millisecond)
		testDB.Exec(nil, utils.ToCmdLine("TS.INCRBY", "r5ts:3", "1", "TIMESTAMP", "600"))
	}()
	r = testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:3", "500", "BLOCK", "3000", "2"))
	if mr, ok = r.(*protocol.MultiRawReply); !ok || len(mr.Replies) != 2 {
		t.Fatalf("min_count 2: %s", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:3"))
}

func TestR5TSReadBlockKeyRemoval(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:4"))
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TS.CREATE", "r5ts:4")), "OK")

	go func() {
		time.Sleep(150 * time.Millisecond)
		testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ts:4"))
	}()
	start := time.Now()
	r := testDB.Exec(nil, utils.ToCmdLine("TS.READ", "r5ts:4", "$", "BLOCK", "5000", "1"))
	if got := string(r.ToBytes()); got != "*0\r\n" {
		t.Errorf("key removal should reply empty: %q", got)
	}
	if time.Since(start) > 2500*time.Millisecond {
		t.Error("should have unblocked on DEL, not on timeout")
	}
}
