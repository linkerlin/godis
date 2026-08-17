package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateReduceValidation aligns REDUCE name/arity errors with Redis 8.x
// (no ERR prefix). Happy-path COUNT/AVG/FIRST_VALUE BY remain accepted.
func TestFTAggregateReduceValidation(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "rdv", "ON", "HASH", "PREFIX", "1", "rdv:",
		"SCHEMA", "title", "TEXT", "SORTABLE", "price", "NUMERIC", "SORTABLE", "cat", "TAG", "SORTABLE",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "rdv:1", "title", "Hello", "price", "10", "cat", "x"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "rdv:2", "title", "World", "price", "30", "cat", "x"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "rdv:3", "title", "Bye", "price", "20", "cat", "y"))

	assertErrContains := func(name string, reply redis.Reply, want string) {
		t.Helper()
		if !protocol.IsErrorReply(reply) {
			t.Fatalf("%s want ERR, got %s", name, reply.ToBytes())
		}
		got := string(reply.ToBytes())
		if !strings.Contains(got, want) {
			t.Fatalf("%s want %q in %s", name, want, got)
		}
		// Redis RediSearch omits the ERR prefix for these REDUCE errors.
		if strings.HasPrefix(strings.TrimPrefix(got, "-"), "ERR ") {
			t.Fatalf("%s should omit ERR prefix like Redis, got %s", name, got)
		}
	}

	assertErrContains("unknown", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "FOOBAR", "0", "AS", "x",
	)), "No such reducer: FOOBAR")

	assertErrContains("count nargs", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "COUNT", "1", "@x", "AS", "c",
	)), "Count accepts 0 values only")

	assertErrContains("sum missing", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "SUM", "0", "AS", "s",
	)), "Missing arguments for SUM")

	assertErrContains("avg missing", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "AVG", "0", "AS", "a",
	)), "Missing arguments for AVG")

	assertErrContains("quantile range", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "QUANTILE", "2", "@price", "1.5", "AS", "q",
	)), "Percentage must be between 0.0 and 1.0")

	assertErrContains("quantile bad", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "QUANTILE", "2", "@price", "abc", "AS", "q",
	)), "Bad arguments for QUANTILE: Could not convert argument to expected type")

	assertErrContains("quantile short", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "QUANTILE", "1", "@price", "AS", "q",
	)), "Bad arguments for QUANTILE: Could not convert argument to expected type")

	assertErrContains("rs missing size", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "RANDOM_SAMPLE", "1", "@title", "AS", "s",
	)), "Bad arguments for <sample size>: Expected an argument, but none provided")

	assertErrContains("rs neg", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "RANDOM_SAMPLE", "2", "@title", "-1", "AS", "s",
	)), "Bad arguments for <sample size>: Value is outside acceptable bounds")

	assertErrContains("rs abc", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "RANDOM_SAMPLE", "2", "@title", "abc", "AS", "s",
	)), "Bad arguments for <sample size>: Could not convert argument to expected type")

	assertErrContains("fv extra", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "FIRST_VALUE", "2", "@title", "@extra", "AS", "t",
	)), "Unknown argument `@extra` at position 1 for FIRST_VALUE")

	assertErrContains("fv by dir", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "FIRST_VALUE", "4", "@title", "BY", "@price", "FOO", "AS", "t",
	)), "Unknown argument `FOO` at position 3 for FIRST_VALUE")

	assertErrContains("fv by short", db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "FIRST_VALUE", "2", "@title", "BY", "AS", "t",
	)), "Missing arguments for FIRST_VALUE")

	// Happy paths (Redis 8.x).
	okCount := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "c",
	))
	if protocol.IsErrorReply(okCount) {
		t.Fatalf("COUNT 0: %s", okCount.ToBytes())
	}
	okAvg := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "0", "REDUCE", "AVG", "1", "@price", "AS", "a",
	))
	if protocol.IsErrorReply(okAvg) || !strings.Contains(string(okAvg.ToBytes()), "20") {
		t.Fatalf("AVG want 20: %s", okAvg.ToBytes())
	}
	okFV := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "rdv", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "FIRST_VALUE", "4", "@title", "BY", "@price", "DESC", "AS", "t",
	))
	if protocol.IsErrorReply(okFV) {
		t.Fatalf("FIRST_VALUE BY DESC: %s", okFV.ToBytes())
	}
	okBody := strings.ToLower(string(okFV.ToBytes()))
	if !strings.Contains(okBody, "world") {
		t.Fatalf("FIRST_VALUE BY DESC want World in cat:x, got %s", okFV.ToBytes())
	}
}
