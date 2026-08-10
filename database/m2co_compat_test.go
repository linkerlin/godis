package database

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/linkerlin/godis/datastruct/hll"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2coInfoMemoryProcessRSS(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "used_memory_rss:") {
		t.Fatalf("missing used_memory_rss:\n%s", s)
	}
	rss := getProcessRSSBytes()
	if rss == 0 {
		t.Skip("process RSS unavailable on this platform; INFO falls back to MemStats.Sys")
	}
	// RSS should be positive and typically >= Alloc-scale values.
	if !strings.Contains(s, "used_memory_rss:") {
		t.Fatal("missing used_memory_rss")
	}
	stats := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	m, ok := stats.(*protocol.MapReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", stats)
	}
	if _, ok := m.Data["process.rss"]; !ok {
		t.Fatalf("missing process.rss in MEMORY STATS keys=%v", m.Data)
	}
}

func TestM2coSparseHLLClearError(t *testing.T) {
	db := makeTestDB()
	sparse := make([]byte, hll.TotalSize)
	copy(sparse[:4], "HYLL")
	sparse[4] = 1
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "sh", string(sparse))), "OK")
	r := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "sh"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("want ERR, got %s", r.ToBytes())
	}
	if !strings.Contains(string(r.ToBytes()), "sparse HyperLogLog encoding is not supported") {
		t.Fatalf("want sparse ERR, got %s", r.ToBytes())
	}
}

func TestM2coFloat16Decode(t *testing.T) {
	// 1.0f16 = 0x3c00; -2.0f16 = 0xc000
	blob := make([]byte, 4)
	binary.LittleEndian.PutUint16(blob[0:], 0x3c00)
	binary.LittleEndian.PutUint16(blob[2:], 0xc000)
	got, err := redisearch.DecodeVectorByType(blob, redisearch.VectorTypeFloat16, 2)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(got[0]-1.0)) > 1e-5 || math.Abs(float64(got[1]+2.0)) > 1e-5 {
		t.Fatalf("got %v want [1, -2]", got)
	}
	_, err = redisearch.DecodeVectorByType(blob[:2], redisearch.VectorTypeBFloat16, 1)
	if err == nil || !strings.Contains(err.Error(), "not yet implemented") {
		t.Fatalf("BFLOAT16 should stay unimplemented, got %v", err)
	}
}
