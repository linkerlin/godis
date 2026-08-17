package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateApplyExistsAndGeo3 aligns APPLY exists(Null) + geodistance 3-arg
// and bool/num wire with Redis 8.x (ylf-e2e-redis).
func TestFTAggregateApplyExistsAndGeo3(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx73", "SCHEMA",
		"name", "TAG", "price", "NUMERIC", "loc", "GEO",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine(
		"HSET", "doc:a", "name", "alice", "price", "0", "loc", "13.36,38.11",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("hset a: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine(
		"HSET", "doc:b", "name", "bob", "price", "10",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("hset b: %s", r.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx73", "@name:{alice}",
		"LOAD", "1", "@price",
		"APPLY", "exists(@price)", "AS", "ep",
		"APPLY", "exists(0)", "AS", "ez",
	))
	body := string(r.ToBytes())
	if protocol.IsErrorReply(r) {
		t.Fatalf("exists present: %s", body)
	}
	if !strings.Contains(body, "ep") || !strings.Contains(body, "ez") {
		t.Fatalf("exists aliases missing: %s", body)
	}

	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx73", "@name:{bob}",
		"LOAD", "2", "@name", "@loc",
		"APPLY", "exists(@loc)", "AS", "el",
		"APPLY", `case(exists(@loc), @loc, "none")`, "AS", "c",
	))
	body = string(r.ToBytes())
	if protocol.IsErrorReply(r) {
		t.Fatalf("exists/case: %s", body)
	}
	if !strings.Contains(body, "none") {
		t.Fatalf("case missing loc want none: %s", body)
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx73", "@name:{bob}",
		"LOAD", "1", "@loc",
		"APPLY", "@loc", "AS", "L",
	))
	if !protocol.IsErrorReply(bad) ||
		!strings.Contains(string(bad.ToBytes()), "SEARCH_VALUE_NOT_FOUND") {
		t.Fatalf("bare @loc missing: %s", bad.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx73", "@name:{alice}",
		"LOAD", "1", "@loc",
		"APPLY", "geodistance(@loc,13.361389,38.115556)", "AS", "d1",
		"APPLY", "geodistance(13.361389,38.115556,@loc)", "AS", "d2",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("geo3: %s", r.ToBytes())
	}
	body = string(r.ToBytes())
	found := false
	for _, part := range strings.Split(body, "\r\n") {
		f, err := strconv.ParseFloat(part, 64)
		if err == nil && f > 600 && f < 700 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("geo3 want distance ~630 in reply: %s", body)
	}
}
