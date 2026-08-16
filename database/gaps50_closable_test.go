package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps50LolwutAndLatencyDoctor(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "0")), "OK")
	doc := srv.Exec(c, utils.ToCmdLine("LATENCY", "DOCTOR"))
	bulk, ok := doc.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "Latency monitoring is disabled") {
		t.Fatalf("LATENCY DOCTOR disabled: %T %s", doc, doc.ToBytes())
	}

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "1")), "OK")
	doc = srv.Exec(c, utils.ToCmdLine("LATENCY", "DOCTOR"))
	bulk, ok = doc.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "no latency spike was observed") {
		t.Fatalf("LATENCY DOCTOR enabled empty: %T %s", doc, doc.ToBytes())
	}
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "0")), "OK")

	it := srv.Exec(c, utils.ToCmdLine("LOLWUT", "IT"))
	ib, ok := it.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(ib.Arg), "CAPELLI") {
		t.Fatalf("LOLWUT IT: %T %s", it, it.ToBytes())
	}

	short := srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "-1"))
	sb, ok := short.(*protocol.BulkReply)
	if !ok || string(sb.Arg) != "Godis ver. redis-compat" {
		t.Fatalf("LOLWUT VERSION -1 want short line, got %T %q", short, short.ToBytes())
	}
	short = srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "100"))
	sb, ok = short.(*protocol.BulkReply)
	if !ok || string(sb.Arg) != "Godis ver. redis-compat" {
		t.Fatalf("LOLWUT VERSION 100 want short line, got %q", short.ToBytes())
	}

	styled := srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "6"))
	st, ok := styled.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(st.Arg), "style 6") {
		t.Fatalf("LOLWUT VERSION 6: %s", styled.ToBytes())
	}
}
