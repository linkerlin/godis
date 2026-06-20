package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"appendonly yes\n" +
		"search-backend sqlite\n" +
		"vector-backend sqlite\n" +
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
	if p.SearchBackend != "sqlite" {
		t.Error("search-backend parse failed")
	}
	if p.VectorBackend != "sqlite" {
		t.Error("vector-backend parse failed")
	}
}

func TestParseMaxClientsAndMetricsAddr(t *testing.T) {
	src := "maxclients 128\nmetrics-addr 127.0.0.1:9090\n"
	p, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxClients != 128 {
		t.Fatalf("maxclients: got %d", p.MaxClients)
	}
	if p.MetricsAddr != "127.0.0.1:9090" {
		t.Fatalf("metrics-addr: got %q", p.MetricsAddr)
	}
}
