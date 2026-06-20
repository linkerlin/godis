package parser

import (
	"bytes"
	"testing"
	"time"
)

const fuzzMaxInput = 1 << 20 // 1 MiB

func addParseSeeds(f *testing.F) {
	f.Add([]byte("+OK\r\n"))
	f.Add([]byte("-ERR unknown\r\n"))
	f.Add([]byte(":42\r\n"))
	f.Add([]byte("$-1\r\n"))
	f.Add([]byte("*0\r\n"))
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("SET k v"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	f.Add([]byte("$5\r\nhello\r\n"))
	f.Add([]byte("*2\r\n$-1\r\n$3\r\nfoo\r\n"))
}

func FuzzParseOne(f *testing.F) {
	addParseSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInput {
			t.Skip()
		}
		parseOneWithTimeout(t, data, 2*time.Second)
	})
}

func FuzzParseBytes(f *testing.F) {
	addParseSeeds(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInput {
			t.Skip()
		}
		parseBytesWithTimeout(t, data, 2*time.Second)
	})
}

func FuzzParseV2(f *testing.F) {
	addParseSeeds(f)
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInput {
			t.Skip()
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ParseV2 panic on input len=%d: %v", len(data), r)
			}
		}()
		_, _ = ParseV2(bytes.NewReader(data))
	})
}

func parseOneWithTimeout(t *testing.T, data []byte, timeout time.Duration) {
	t.Helper()
	type parseResult struct {
		reply interface{}
		err   error
	}
	done := make(chan parseResult, 1)
	go func() {
		r, err := ParseOne(data)
		done <- parseResult{reply: r, err: err}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("ParseOne did not finish within timeout (possible hang)")
	}
}

func parseBytesWithTimeout(t *testing.T, data []byte, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_, _ = ParseBytes(data)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("ParseBytes did not finish within timeout (possible hang)")
	}
}

// TestFuzzSeedsRun ensures seed corpus parses without hang (non-fuzz CI path).
func TestFuzzSeedsRun(t *testing.T) {
	seeds := [][]byte{
		[]byte("+OK\r\n"),
		[]byte("*1\r\n$4\r\nPING\r\n"),
		[]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
	}
	for _, data := range seeds {
		parseOneWithTimeout(t, data, time.Second)
		parseBytesWithTimeout(t, data, time.Second)
		_, err := ParseV2(bytes.NewReader(data))
		if err != nil && len(data) > 0 && data[0] == '*' {
			// ParseV2 only supports bulk strings in arrays; some seeds may error.
			continue
		}
	}
}
