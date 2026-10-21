package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func assertARScan(t *testing.T, r redisReply, want ...string) {
	t.Helper()
	var args [][]byte
	switch v := r.(type) {
	case *protocol.MultiBulkReply:
		args = v.Args
	case *protocol.EmptyMultiBulkReply:
	default:
		t.Fatalf("ARSCAN: %T %s", r, r.ToBytes())
	}
	if len(args) != len(want) {
		t.Fatalf("ARSCAN want %d entries, got %d: %s", len(want), len(args), r.ToBytes())
	}
	for i, w := range want {
		if got := string(args[i]); got != w {
			t.Fatalf("ARSCAN[%d]=%q want %q: %s", i, got, w, r.ToBytes())
		}
	}
}

func assertARScanErr(t *testing.T, r redisReply, contains string) {
	t.Helper()
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), contains) {
		t.Fatalf("want error containing %q, got %s", contains, r.ToBytes())
	}
}

func TestR5ArrayScan(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine("ARSET", "arscan", "0", "x")); protocol.IsErrorReply(r) {
		t.Fatalf("ARSET 0: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARSET", "arscan", "10", "y")); protocol.IsErrorReply(r) {
		t.Fatalf("ARSET 10: %s", r.ToBytes())
	}

	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "0", "10")), "0", "x", "10", "y")
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "10", "0")), "10", "y", "0", "x")
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "5", "5")))
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "0", "10", "LIMIT", "1")), "0", "x")
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "0", "100", "LIMIT", "10")), "0", "x", "10", "y")
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "999999", "1000000")))
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "100", "0")), "10", "y", "0", "x")
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "nosuch", "0", "0")))

	if r := db.Exec(nil, utils.ToCmdLine("ARDEL", "arscan", "0")); protocol.IsErrorReply(r) {
		t.Fatalf("ARDEL: %s", r.ToBytes())
	}
	assertARScan(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscan", "0", "10")), "10", "y")

	for _, tc := range []struct {
		args     []string
		contains string
	}{
		{[]string{"ARSCAN", "arscan", "-1", "2"}, "invalid array index"},
		{[]string{"ARSCAN", "arscan", "0", "-2"}, "invalid array index"},
		{[]string{"ARSCAN", "arscan", "x", "2"}, "invalid array index"},
		{[]string{"ARSCAN", "arscan", "0", "10", "LIMIT", "0"}, "LIMIT must be positive"},
		{[]string{"ARSCAN", "arscan", "0", "10", "LIMIT", "-1"}, "LIMIT must be positive"},
		{[]string{"ARSCAN", "arscan", "0", "10", "LIMIT", "abc"}, "integer or out of range"},
		{[]string{"ARSCAN", "arscan", "0", "10", "LIMIT"}, "wrong number"},
		{[]string{"ARSCAN", "arscan", "0", "10", "FOO"}, "wrong number"},
		{[]string{"ARSCAN", "arscan", "0"}, "wrong number"},
	} {
		assertARScanErr(t, db.Exec(nil, utils.ToCmdLine(tc.args...)), tc.contains)
	}

	if r := db.Exec(nil, utils.ToCmdLine("SET", "arscanstr", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("SET: %s", r.ToBytes())
	}
	assertARScanErr(t, db.Exec(nil, utils.ToCmdLine("ARSCAN", "arscanstr", "0", "0")), "WRONGTYPE")
}
