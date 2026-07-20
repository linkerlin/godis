package database

import (
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestBLPopWokenByLPush(t *testing.T) {
	testDB.Flush()
	key := "bl:wake"
	var wg sync.WaitGroup
	wg.Add(1)
	var result redis.Reply
	go func() {
		defer wg.Done()
		result = testDB.Exec(nil, utils.ToCmdLine("BLPOP", key, "2"))
	}()
	time.Sleep(50 * time.Millisecond)
	testDB.Exec(nil, utils.ToCmdLine("LPUSH", key, "hello"))
	wg.Wait()
	mb, ok := result.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 2 {
		t.Fatalf("BLPOP: expected [key value], got %T %v", result, result)
	}
	if string(mb.Args[0]) != key || string(mb.Args[1]) != "hello" {
		t.Fatalf("BLPOP unexpected: %q %q", mb.Args[0], mb.Args[1])
	}
}

func TestBLPopTimeout(t *testing.T) {
	testDB.Flush()
	start := time.Now()
	result := testDB.Exec(nil, utils.ToCmdLine("BLPOP", "bl:empty", "0.2"))
	asserts.AssertNullBulk(t, result)
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("BLPOP returned too early")
	}
}

func TestBZPopMinWokenByZAdd(t *testing.T) {
	testDB.Flush()
	key := "bz:wake"
	var wg sync.WaitGroup
	wg.Add(1)
	var result redis.Reply
	go func() {
		defer wg.Done()
		result = testDB.Exec(nil, utils.ToCmdLine("BZPOPMIN", key, "2"))
	}()
	time.Sleep(50 * time.Millisecond)
	testDB.Exec(nil, utils.ToCmdLine("ZADD", key, "1.5", "m"))
	wg.Wait()
	mb, ok := result.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 3 {
		t.Fatalf("BZPOPMIN: expected [key member score], got %T %v", result, result)
	}
	if string(mb.Args[1]) != "m" {
		t.Fatalf("member: %q", mb.Args[1])
	}
}

func TestBitmapMSBFirstViaCommands(t *testing.T) {
	testDB.Flush()
	key := "bm:msb"
	testDB.Exec(nil, utils.ToCmdLine("SETBIT", key, "0", "1"))
	got := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	bulk, ok := got.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) < 1 || bulk.Arg[0] != 0x80 {
		t.Fatalf("SETBIT 0 should yield 0x80, got %v", got)
	}
}
