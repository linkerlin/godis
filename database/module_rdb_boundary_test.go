package database

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	rdb "github.com/hdt3213/rdb/parser"
	"github.com/linkerlin/godis/config"
)

// encodeRDBLength encodes an integer the way Redis RDB length fields do
// (6/14/32/64-bit). Used only for synthetic module fixtures.
func encodeRDBLength(value uint64) []byte {
	const (
		maxUint6  = 1<<6 - 1
		maxUint14 = 1<<14 - 1
		len32Bit  = 0x80
		len64Bit  = 0x81
		len14Mask = 0x40
	)
	if value <= maxUint6 {
		return []byte{byte(value)}
	}
	if value <= maxUint14 {
		return []byte{byte(value>>8) | len14Mask, byte(value)}
	}
	if value <= ^uint64(0)>>32 {
		buf := make([]byte, 5)
		buf[0] = len32Bit
		binary.BigEndian.PutUint32(buf[1:], uint32(value))
		return buf
	}
	buf := make([]byte, 9)
	buf[0] = len64Bit
	binary.BigEndian.PutUint64(buf[1:], value)
	return buf
}

// moduleTypeID packs a 9-char module type name + encVersion into a Redis
// module type id (same layout as Redis / hdt3213/rdb).
func moduleTypeID(name string, encVersion uint64) uint64 {
	const cset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	if len(name) != 9 {
		panic("module type name must be 9 chars")
	}
	charCode := func(c byte) uint64 {
		for i := 0; i < len(cset); i++ {
			if cset[i] == c {
				return uint64(i)
			}
		}
		panic("invalid module type char")
	}
	var id uint64
	for i := 0; i < 9; i++ {
		id |= charCode(name[i])
		id <<= 6
	}
	id <<= 4
	id |= encVersion & 1023
	return id
}

// buildMinimalModule2RDB produces a synthetic REDIS0011 dump with one
// typeModule2 key whose module value is immediately OpcodeEOF.
func buildMinimalModule2RDB(key, moduleName string, encVersion uint64) []byte {
	var buf bytes.Buffer
	buf.WriteString("REDIS0011")
	buf.WriteByte(0xFE) // SELECTDB
	buf.Write(encodeRDBLength(0))
	buf.WriteByte(0xFB) // RESIZEDB
	buf.Write(encodeRDBLength(1))
	buf.Write(encodeRDBLength(0))
	buf.WriteByte(rdbTypeModule2)
	buf.Write(encodeRDBLength(uint64(len(key))))
	buf.WriteString(key)
	buf.Write(encodeRDBLength(moduleTypeID(moduleName, encVersion)))
	buf.Write(encodeRDBLength(0)) // ModuleOpcodeEOF
	buf.WriteByte(0xFF)           // EOF
	buf.Write(make([]byte, 8))    // file CRC ignored by decoder
	return buf.Bytes()
}

// TestLoadRDBRejectsOfficialModuleType verifies LoadRDB fails with a clear
// error on official module RDB (typeModule2), instead of silently skipping.
func TestLoadRDBRejectsOfficialModuleType(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 1}
	t.Cleanup(func() { config.Properties = old })

	raw := buildMinimalModule2RDB("doc:1", "ReJSON-RL", 0)
	dec := rdb.NewDecoder(bytes.NewReader(raw))
	server := MakeAuxiliaryServer()
	err := server.LoadRDB(dec)
	if err == nil {
		t.Fatal("LoadRDB must reject official module RDB, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "module type") {
		t.Fatalf("want module-type error, got %v", err)
	}
	if !strings.Contains(msg, "ReJSON-RL") {
		t.Fatalf("want module name in error, got %v", err)
	}
	if !strings.Contains(msg, "doc:1") {
		t.Fatalf("want key in error, got %v", err)
	}
	// Must not leave the module key loaded.
	db, _ := server.selectDBSafe(0)
	if _, ok := db.GetEntity("doc:1"); ok {
		t.Fatal("module key must not be present after rejected LoadRDB")
	}
}

func TestModuleTypeIDRoundTripCharset(t *testing.T) {
	// Sanity: ReJSON-RL and search-ft encode without panic (9 chars).
	_ = moduleTypeID("ReJSON-RL", 0)
	_ = moduleTypeID("search-ft", 1)
}
