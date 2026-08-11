package aof

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	List "github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/datastruct/set"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
)

func TestEntityToCmdTypes(t *testing.T) {
	if EntityToCmd("k", nil) != nil {
		t.Fatal("nil entity")
	}

	strCmd := EntityToCmd("k", &database.DataEntity{Data: []byte("v")})
	if string(strCmd.Args[0]) != "SET" || string(strCmd.Args[1]) != "k" || string(strCmd.Args[2]) != "v" {
		t.Fatalf("string cmd=%v", argsAsStrings(strCmd.Args))
	}

	lst := List.Make([]byte("a"), []byte("b"))
	listCmd := EntityToCmd("L", &database.DataEntity{Data: lst})
	if string(listCmd.Args[0]) != "RPUSH" || len(listCmd.Args) != 4 {
		t.Fatalf("list cmd=%v", argsAsStrings(listCmd.Args))
	}

	st := set.Make("x", "y")
	setCmd := EntityToCmd("S", &database.DataEntity{Data: st})
	if string(setCmd.Args[0]) != "SADD" || len(setCmd.Args) != 4 {
		t.Fatalf("set cmd=%v", argsAsStrings(setCmd.Args))
	}

	h := dict.MakeSimple()
	h.Put("f", []byte("1"))
	hashCmd := EntityToCmd("H", &database.DataEntity{Data: h})
	if string(hashCmd.Args[0]) != "HMSET" || string(hashCmd.Args[2]) != "f" {
		t.Fatalf("hash cmd=%v", argsAsStrings(hashCmd.Args))
	}

	z := SortedSet.Make()
	z.Add("m", 1.5)
	zCmd := EntityToCmd("Z", &database.DataEntity{Data: z})
	if string(zCmd.Args[0]) != "ZADD" || string(zCmd.Args[2]) != "1.5" || string(zCmd.Args[3]) != "m" {
		t.Fatalf("zset cmd=%v", argsAsStrings(zCmd.Args))
	}

	eng := redisearch.NewRediSearchEngine(&redisearch.EngineConfig{Name: "idx"})
	eng.SetCreateArgs([][]byte{
		[]byte("idx"), []byte("ON"), []byte("HASH"), []byte("SKIPINITIALSCAN"),
		[]byte("SCHEMA"), []byte("t"), []byte("TEXT"),
	})
	ftCmd := EntityToCmd("idx", &database.DataEntity{Data: eng})
	if ftCmd == nil || string(ftCmd.Args[0]) != "FT.CREATE" {
		t.Fatalf("ft cmd=%v", ftCmd)
	}
	joined := strings.Join(argsAsStrings(ftCmd.Args), " ")
	if strings.Contains(strings.ToUpper(joined), "SKIPINITIALSCAN") {
		t.Fatalf("SKIPINITIALSCAN should be stripped: %s", joined)
	}
	emptyEng := redisearch.NewRediSearchEngine(&redisearch.EngineConfig{Name: "empty"})
	if EntityToCmd("idx", &database.DataEntity{Data: emptyEng}) != nil {
		t.Fatal("empty CreateArgs should yield nil")
	}
}

func TestMakeExpireCmdAndOpaqueIsPayload(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	cmd := MakeExpireCmd("k", at)
	if string(cmd.Args[0]) != "PEXPIREAT" || string(cmd.Args[1]) != "k" {
		t.Fatalf("%v", argsAsStrings(cmd.Args))
	}
	ed := dict.NewExpireDict(4)
	ed.Put("f", []byte("v"))
	ed.Expire("f", time.Now().Add(time.Hour))
	payload, ok := EncodeOpaque(&database.DataEntity{Data: ed})
	if !ok {
		t.Fatal("encode expire dict")
	}
	if !IsOpaquePayload(payload) {
		t.Fatal("IsOpaquePayload")
	}
	if IsOpaquePayload([]byte("nope")) || IsOpaquePayload(nil) {
		t.Fatal("false positive")
	}
	// ExpireDict also satisfies dict.Dict, so EntityToCmd prefers HMSET rewrite path.
	hashish := EntityToCmd("hk", &database.DataEntity{Data: ed})
	if hashish == nil || string(hashish.Args[0]) != "HMSET" {
		t.Fatalf("expire-dict EntityToCmd=%v", hashish)
	}
	entity, ok := DecodeOpaque(payload)
	if !ok {
		t.Fatal("decode")
	}
	got, ok := entity.Data.(*dict.ExpireDict)
	if !ok {
		t.Fatalf("%T", entity.Data)
	}
	val, exists := got.Get("f")
	if !exists || !bytes.Equal(val.([]byte), []byte("v")) {
		t.Fatalf("field missing: %v %v", val, exists)
	}
}

func TestPersisterStatsNilAndWriteExpireDict(t *testing.T) {
	var p *Persister
	st := p.Stats()
	if st["enabled"] != false {
		t.Fatalf("%v", st)
	}
	p = &Persister{aofFilename: "appendonly.aof", aofFsync: FsyncEverySec}
	st = p.Stats()
	if st["enabled"] != false {
		t.Fatalf("no file: %v", st)
	}
	p.SetFsync(FsyncAlways)
	if p.getFsync() != FsyncAlways {
		t.Fatal(p.getFsync())
	}

	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "tmp.aof"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ed := dict.NewExpireDict(4)
	ed.Put("a", []byte("1"))
	ed.Expire("a", time.Now().Add(time.Hour))
	writeExpireDictToAof(f, "hk", ed)
	raw, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if !strings.Contains(s, "HMSET") || !strings.Contains(s, "HPEXPIREAT") || !strings.Contains(s, "FIELDS") {
		t.Fatalf("rewrite bytes=%q", s)
	}
}

func argsAsStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}
