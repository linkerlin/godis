package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/tcp"
)

func TestMetricsServerEndpoints(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status: got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "OK\n" {
		t.Fatalf("unexpected health body: %q", body)
	}

	resp, err = http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("unexpected content type: %s", ct)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "godis_connected_clients") {
		t.Fatalf("metrics missing connected_clients:\n%s", body)
	}
}

func TestStartEmptyAddr(t *testing.T) {
	if err := Start(""); err != nil {
		t.Fatalf("Start with empty addr should no-op: %v", err)
	}
}

func TestWritePrometheusRejectedConnectionsValue(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		tcp.ClientCounter = 0
		tcp.RejectedConnections = 0
	}()

	_ = tcp.TryAcceptClient()
	_ = tcp.TryAcceptClient()

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), "godis_rejected_connections_total 1") {
		t.Fatalf("expected rejected counter 1 in:\n%s", rec.Body.String())
	}
}
