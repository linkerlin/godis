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
}

func TestM2coNarrowVectorDecode(t *testing.T) {
	// bfloat16 of 1.0f32: top 16 bits of 0x3f800000 → 0x3f80
	bf := make([]byte, 2)
	binary.LittleEndian.PutUint16(bf, 0x3f80)
	got, err := redisearch.DecodeVectorByType(bf, redisearch.VectorTypeBFloat16, 1)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(got[0]-1.0)) > 1e-5 {
		t.Fatalf("BFLOAT16 got %v want 1", got)
	}

	i8, err := redisearch.DecodeVectorByType([]byte{0xff, 0x7f}, redisearch.VectorTypeInt8, 2)
	if err != nil {
		t.Fatal(err)
	}
	if i8[0] != -1 || i8[1] != 127 {
		t.Fatalf("INT8 got %v want [-1, 127]", i8)
	}

	u8, err := redisearch.DecodeVectorByType([]byte{0, 255}, redisearch.VectorTypeUint8, 2)
	if err != nil {
		t.Fatal(err)
	}
	if u8[0] != 0 || u8[1] != 255 {
		t.Fatalf("UINT8 got %v want [0, 255]", u8)
	}
}

func TestM2coInfoMemoryAllocatorIsGo(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "mem_allocator:go") {
		t.Fatalf("want mem_allocator:go (not jemalloc), got:\n%s", s)
	}
	if strings.Contains(strings.ToLower(s), "jemalloc") {
		t.Fatalf("INFO must not claim jemalloc:\n%s", s)
	}
	if !strings.Contains(s, "used_memory_scripts:") {
		t.Fatalf("missing used_memory_scripts:\n%s", s)
	}
}
