package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"appendonly yes\n" +
		"peers a,b"
	p, err := parse(strings.NewReader(src))
	if err != nil {
		t.Error("parse failed:", err)
		return
	}
	if p == nil {
		t.Error("cannot get result")
		return
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if !p.AppendOnly {
		t.Error("bool parse failed")
	}
}
