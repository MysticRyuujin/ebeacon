package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsHandlerFor_DisablesCompression(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ebeacon_metrics_handler_test",
		Help: "test metric",
	})
	registry.MustRegister(gauge)
	gauge.Set(1)

	handler := metricsHandlerFor(registry, registry)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("content-encoding: got %q want empty", got)
	}
	if body := rec.Body.String(); !strings.Contains(body, "ebeacon_metrics_handler_test") {
		t.Fatalf("response body missing test metric: %q", body)
	}
}
