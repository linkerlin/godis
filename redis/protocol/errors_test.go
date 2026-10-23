package protocol

import (
	"testing"
)

// R5-1 CR/LF stripping in error replies, pinned to Redis 8.10 output:
// each of \r and \n maps to one space (redis:8.10-alpine probe:
// "bad\r\ninjection" → "bad  injection", two spaces).
func TestSanitizeErrText(t *testing.T) {
	if got := SanitizeErrText("bad\r\ninjection"); got != "bad  injection" {
		t.Errorf("CRLF: %q", got)
	}
	if got := SanitizeErrText("only\nlf"); got != "only lf" {
		t.Errorf("LF: %q", got)
	}
	if got := SanitizeErrText("only\rcr"); got != "only cr" {
		t.Errorf("CR: %q", got)
	}
	if got := SanitizeErrText("no newline"); got != "no newline" {
		t.Errorf("plain: %q", got)
	}
	// Wire form stays one line; only the trailing CRLF terminator remains.
	frame := string((&StandardErrReply{Status: "bad\r\ninjection"}).ToBytes())
	if frame != "-bad  injection\r\n" {
		t.Errorf("frame: %q", frame)
	}
}
