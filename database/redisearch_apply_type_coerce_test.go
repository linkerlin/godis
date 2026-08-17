package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateApplyTypeCoerce aligns APPLY numeric coerce + string type
// checks with Redis 8.6 QE (non-string upper→Null; strlen→Invalid type).
func TestFTAggregateApplyTypeCoerce(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "coerce", "ON", "HASH", "PREFIX", "1", "coerce:",
		"SCHEMA", "t", "TEXT", "SORTABLE", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "coerce:1", "t", "Hello", "n", "10"))

	ok := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "coerce", "*",
		"LOAD", "2", "@t", "@n",
		"APPLY", "floor('3.7')", "AS", "f",
		"APPLY", "ceil('3.2')", "AS", "c",
		"APPLY", "abs('-5')", "AS", "a",
		"APPLY", "upper(@t)", "AS", "u",
		"APPLY", "substr(@t,-6,2)", "AS", "s",
	))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("coerce ok path: %s", ok.ToBytes())
	}
	body := string(ok.ToBytes())
	for _, want := range []string{"f", "3", "c", "4", "a", "5", "u", "HELLO"} {
		if !strings.Contains(body, want) {
			t.Fatalf("want %q in reply: %s", want, body)
		}
	}
	// overshooting substr → empty field value (still present as key s)
	if !strings.Contains(body, "s") {
		t.Fatalf("want substr alias s: %s", body)
	}

	nullUpper := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "coerce", "*",
		"LOAD", "1", "@n",
		"APPLY", "upper(@n)", "AS", "u",
	))
	if protocol.IsErrorReply(nullUpper) {
		t.Fatalf("upper(@n) should be Null not ERR: %s", nullUpper.ToBytes())
	}
	nu := string(nullUpper.ToBytes())
	if !strings.Contains(nu, "$1\r\nu\r\n$-1") && !strings.Contains(nu, "u\r\n$-1") {
		t.Fatalf("upper(@n) want Null wire for u: %s", nu)
	}

	badStrlen := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "coerce", "*",
		"LOAD", "1", "@n",
		"APPLY", "strlen(@n)", "AS", "l",
	))
	if !protocol.IsErrorReply(badStrlen) ||
		!strings.Contains(string(badStrlen.ToBytes()), "Invalid type (1) for argument 0 in function 'strlen'") {
		t.Fatalf("strlen(@n): %s", badStrlen.ToBytes())
	}

	badSub := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "coerce", "*",
		"LOAD", "1", "@n",
		"APPLY", "substr(@n,0,1)", "AS", "s",
	))
	if !protocol.IsErrorReply(badSub) ||
		!strings.Contains(string(badSub.ToBytes()), "Invalid type for substr. Expected string") {
		t.Fatalf("substr(@n): %s", badSub.ToBytes())
	}
}
