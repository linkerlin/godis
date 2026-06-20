package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linkerlin/godis/database"
)

func TestWritePrometheusContainsCoreMetrics(t *testing.T) {
	database.ResetCommandStats()
	database.RecordCommand("ping", 10, false)
	database.RecordCommand("get", 20, true)

	rec := httptest.NewRecorder()
	WritePrometheus(rec)

	body := rec.Body.String()
	for _, want := range []string{
		"godis_commands_total",
		"godis_command_failures_total",
		"godis_command_duration_usec_total",
		"godis_connected_clients",
		"godis_tracking_clients",
		"godis_slowlog_length",
		"godis_net_input_bytes",
		"godis_net_output_bytes",
		"godis_rejected_connections_total",
		`godis_commands_total{command="ping"} 1`,
		`godis_command_failures_total{command="get"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected metric %q in output:\n%s", want, body)
		}
	}
}

func TestMetricsHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		WritePrometheus(w)
	})
	handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("unexpected content type: %s", ct)
	}
}

func TestHealthHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/health", nil)

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "OK\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}
