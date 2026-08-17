package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTGeoFilterFilterSlopExpander aligns FT.SEARCH GEOFILTER / FILTER / SLOP /
// EXPANDER error paths with Redis 8.x QE (SEARCH_PARSE_ARGS / SEARCH_SYNTAX).
func TestFTGeoFilterFilterSlopExpander(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p81", "ON", "HASH", "PREFIX", "1", "p81:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC", "SORTABLE", "loc", "GEO",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p81:1", "t", "hello", "n", "5", "loc", "13.4,52.5"))

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"geo-lon", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "a", "2", "3", "km"}, "SEARCH_PARSE_ARGS Bad arguments for <lon>: Could not convert argument to expected type"},
		{"geo-lat", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "1", "b", "3", "km"}, "SEARCH_PARSE_ARGS Bad arguments for <lat>: Could not convert argument to expected type"},
		{"geo-rad", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "1", "2", "c", "km"}, "SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type"},
		{"geo-unit", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "1", "2", "3", "xx"}, "SEARCH_PARSE_ARGS Unknown distance unit xx"},
		{"geo-short", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "1", "2", "3"}, "SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments"},
		{"geo-bounds", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "181", "0", "1", "km"}, "SEARCH_SYNTAX Invalid GeoFilter lat/lon"},
		{"geo-lat91", []string{"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "0", "91", "1", "km"}, "SEARCH_SYNTAX Invalid GeoFilter lat/lon"},
		{"filt-lo", []string{"FT.SEARCH", "p81", "hello", "FILTER", "n", "a", "b"}, "SEARCH_PARSE_ARGS Bad lower range: a"},
		{"filt-hi", []string{"FT.SEARCH", "p81", "hello", "FILTER", "n", "1", "x"}, "SEARCH_PARSE_ARGS Bad upper range: x"},
		{"filt-short", []string{"FT.SEARCH", "p81", "hello", "FILTER", "n"}, "SEARCH_PARSE_ARGS FILTER requires 3 arguments"},
		{"slop-miss", []string{"FT.SEARCH", "p81", "hello", "SLOP"}, "SEARCH_PARSE_ARGS Bad arguments for SLOP: Expected an argument, but none provided"},
		{"slop-bad", []string{"FT.SEARCH", "p81", "hello", "SLOP", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for SLOP: Could not convert argument to expected type"},
		{"exp-miss", []string{"FT.SEARCH", "p81", "hello", "EXPANDER"}, "SEARCH_PARSE_ARGS Bad arguments for EXPANDER: Expected an argument, but none provided"},
		{"tanh-miss", []string{"FT.SEARCH", "p81", "hello", "BM25STD_TANH_FACTOR"}, "SEARCH_PARSE_ARGS Need an argument for BM25STD_TANH_FACTOR"},
		{"tanh-0", []string{"FT.SEARCH", "p81", "hello", "BM25STD_TANH_FACTOR", "0"}, "SEARCH_PARSE_ARGS BM25STD_TANH_FACTOR must be between 1 and 10000 inclusive"},
		{"tanh-frac", []string{"FT.SEARCH", "p81", "hello", "BM25STD_TANH_FACTOR", "1.5"}, "SEARCH_PARSE_ARGS BM25STD_TANH_FACTOR must be between 1 and 10000 inclusive"},
		{"apply-miss", []string{"FT.AGGREGATE", "p81", "*", "APPLY"}, "SEARCH_PARSE_ARGS Bad arguments for APPLY/FILTER: Expected an argument, but none provided"},
		{"apply-as", []string{"FT.AGGREGATE", "p81", "*", "APPLY", "@n", "AS"}, "SEARCH_PARSE_ARGS AS needs argument"},
		{"agg-filt-miss", []string{"FT.AGGREGATE", "p81", "*", "FILTER"}, "SEARCH_PARSE_ARGS Bad arguments for APPLY/FILTER: Expected an argument, but none provided"},
		{"groupby-miss", []string{"FT.AGGREGATE", "p81", "*", "GROUPBY"}, "SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided"},
		{"groupby-short", []string{"FT.AGGREGATE", "p81", "*", "GROUPBY", "1"}, "SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided"},
		{"groupby-bad", []string{"FT.AGGREGATE", "p81", "*", "GROUPBY", "abc", "@n"}, "SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Could not convert argument to expected type"},
		{"groupby-neg", []string{"FT.AGGREGATE", "p81", "*", "GROUPBY", "-1", "@n"}, "SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	// Redis accepts negative SLOP and arbitrary EXPANDER names.
	okSlop := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p81", "hello", "SLOP", "-1", "NOCONTENT"))
	if protocol.IsErrorReply(okSlop) {
		t.Fatalf("SLOP -1: %s", okSlop.ToBytes())
	}
	okExp := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p81", "hello", "EXPANDER", "STEM", "NOCONTENT"))
	if protocol.IsErrorReply(okExp) {
		t.Fatalf("EXPANDER STEM: %s", okExp.ToBytes())
	}
	okExp2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p81", "hello", "EXPANDER", "nosuch", "NOCONTENT"))
	if protocol.IsErrorReply(okExp2) {
		t.Fatalf("EXPANDER nosuch: %s", okExp2.ToBytes())
	}
	okGeo := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p81", "hello", "GEOFILTER", "loc", "13.4", "52.5", "100", "km", "NOCONTENT",
	))
	if protocol.IsErrorReply(okGeo) {
		t.Fatalf("GEOFILTER ok: %s", okGeo.ToBytes())
	}
	okFilt := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p81", "hello", "FILTER", "n", "1", "10", "NOCONTENT",
	))
	if protocol.IsErrorReply(okFilt) {
		t.Fatalf("FILTER ok: %s", okFilt.ToBytes())
	}
	okInf := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p81", "hello", "FILTER", "n", "-inf", "+inf", "NOCONTENT",
	))
	if protocol.IsErrorReply(okInf) {
		t.Fatalf("FILTER -inf +inf: %s", okInf.ToBytes())
	}
	// Redis accepts integer factors >10000 despite the error-message wording.
	okTanh := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p81", "hello", "BM25STD_TANH_FACTOR", "10001", "NOCONTENT",
	))
	if protocol.IsErrorReply(okTanh) {
		t.Fatalf("TANH_FACTOR 10001: %s", okTanh.ToBytes())
	}
	// APPLY without AS: alias defaults to expression text (SORTABLE field in pipeline).
	okApply := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p81", "*", "APPLY", "@n*1", "AS", "x"))
	if protocol.IsErrorReply(okApply) {
		t.Fatalf("APPLY @n*1 AS x: %s", okApply.ToBytes())
	}
	bareApply := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p81", "*", "APPLY", "@n+0"))
	if protocol.IsErrorReply(bareApply) {
		t.Fatalf("APPLY @n+0 (no AS): %s", bareApply.ToBytes())
	}
	okGB := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p81", "*", "GROUPBY", "1", "@n", "REDUCE", "COUNT", "0", "AS", "c",
	))
	if protocol.IsErrorReply(okGB) {
		t.Fatalf("GROUPBY 1 @n: %s", okGB.ToBytes())
	}
}

