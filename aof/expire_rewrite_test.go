package aof

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
)

// TestWriteExpireDictToAofEmitsFields ensures pure-AOF rewrite serializes
// HPEXPIREAT with Redis-compatible FIELDS numfields syntax.
func TestWriteExpireDictToAofEmitsFields(t *testing.T) {
	ed := dict.NewExpireDict(4)
	ed.Put("f1", []byte("v1"))
	expireAt := time.Now().Add(time.Hour)
	if !ed.Expire("f1", expireAt) {
		t.Fatal("Expire f1")
	}

	tmp, err := os.CreateTemp(t.TempDir(), "hexpire-*.aof")
	if err != nil {
		t.Fatal(err)
	}
	defer tmp.Close()

	writeExpireDictToAof(tmp, "hk", ed)
	if _, err := tmp.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("HPEXPIREAT")) {
		t.Fatalf("missing HPEXPIREAT in rewrite output: %q", raw)
	}
	if !bytes.Contains(raw, []byte("FIELDS")) {
		t.Fatalf("HPEXPIREAT rewrite must include FIELDS keyword, got: %q", raw)
	}
	// Bare form was "HPEXPIREAT key ms field" — ensure "FIELDS" appears before the field name.
	if !bytes.Contains(raw, []byte("FIELDS\r\n$1\r\n1\r\n")) &&
		!bytes.Contains(raw, []byte("$6\r\nFIELDS\r\n")) {
		t.Fatalf("expected FIELDS bulk token in HPEXPIREAT line, got: %q", raw)
	}
}
