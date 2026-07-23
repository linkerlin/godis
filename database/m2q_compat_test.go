package database

import (
	"bytes"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2qPSubscribePublish(t *testing.T) {
	hub := pubsub.MakeHub()
	c := connection.NewFakeConn()
	_ = pubsub.PSubscribe(hub, c, [][]byte{[]byte("news.*")})
	if c.PSubsCount() != 1 {
		t.Fatalf("psubs count=%d", c.PSubsCount())
	}
	asserts.AssertIntReply(t, pubsub.Publish(hub, [][]byte{[]byte("news.a"), []byte("hi")}), 1)
	out := string(c.Bytes())
	if !strings.Contains(out, "pmessage") || !strings.Contains(out, "news.a") {
		t.Fatalf("expected pmessage, got %q", out)
	}
	if hub.NumPat() != 1 {
		t.Fatalf("NumPat=%d", hub.NumPat())
	}
}

func TestM2qBFReserveTopKOpts(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"BF.RESERVE", "bf", "0.01", "100", "EXPANSION", "4", "NONSCALING",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TOPK.RESERVE", "tk", "5", "10", "3", "0.8",
	)), "OK")
	info := db.Exec(nil, utils.ToCmdLine("TOPK.INFO", "tk"))
	if protocol.IsErrorReply(info) {
		t.Fatalf("TOPK.INFO: %s", info.ToBytes())
	}
}

func TestM2qPFDebugDecodeFTConfigShutdown(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("PFADD", "h", "a"))
	dec := db.Exec(nil, utils.ToCmdLine("PFDEBUG", "DECODE", "h"))
	bulk, ok := dec.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "dense") {
		t.Fatalf("PFDEBUG DECODE: %T %s", dec, dec.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "TIMEOUT", "100")), "OK")
	got := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "TIMEOUT"))
	multi, ok := got.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 || string(multi.Args[1]) != "100" {
		t.Fatalf("FT.CONFIG GET: %s", got.ToBytes())
	}

	r := execShutdown(nil, nil)
	if _, ok := r.(*protocol.NoReply); !ok {
		t.Fatalf("SHUTDOWN: %T", r)
	}
}

func TestM2qBlobErrorParse(t *testing.T) {
	payload := []byte("!5\r\nERR x\r\n")
	ch := parser.ParseStream(bytes.NewReader(payload))
	p := <-ch
	if p.Err != nil {
		t.Fatalf("parse err: %v", p.Err)
	}
	errReply, ok := p.Data.(*protocol.StandardErrReply)
	if !ok || errReply.Error() != "ERR x" {
		t.Fatalf("blob error: %#v", p.Data)
	}
}

func TestM2qTSAddOnDuplicate(t *testing.T) {
	db := makeTestDB()
	ok := db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "*", "1.0", "ON_DUPLICATE", "LAST"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("TS.ADD ON_DUPLICATE: %s", ok.ToBytes())
	}
	bad := db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts2", "*", "1.0", "ON_DUPLICATE", "NOPE"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("bad policy should err")
	}
}
