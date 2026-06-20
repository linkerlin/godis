package metrics

import (
	"net/http"

	"github.com/cockroachdb/errors"
)

// Handler returns the HTTP handler for /metrics and /health.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		WritePrometheus(w)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	return mux
}

// Start serves Prometheus metrics on addr until the server exits.
func Start(addr string) error {
	if addr == "" {
		return nil
	}
	server := &http.Server{Addr: addr, Handler: Handler()}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.Wrap(err, "metrics server failed")
	}
	return nil
}
