package network

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Per-network request and cache metrics. Each Network curries the network
// label once in New.
var (
	metricReqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_requests_total",
		Help: "Proxy requests by upstream and HTTP status class",
	}, []string{"network", "upstream", "status_class"})
	metricReqDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ebeacon_request_duration_seconds",
		Help:    "End-to-end proxy request duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"network", "upstream"})
	metricReqDurationByMethod = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ebeacon_request_duration_by_method_seconds",
		Help:    "End-to-end proxy request duration by HTTP method",
		Buckets: prometheus.DefBuckets,
	}, []string{"network", "method"})
	metricReqDurationByPath = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ebeacon_request_duration_by_path_seconds",
		Help:    "End-to-end proxy request duration by normalized Beacon API path",
		Buckets: prometheus.DefBuckets,
	}, []string{"network", "upstream", "api_path"})
	metricReqByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_requests_by_method_total",
		Help: "Proxy requests by HTTP method and status class",
	}, []string{"network", "method", "status_class"})
	metricReqByPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_requests_by_path_total",
		Help: "Proxy requests by normalized Beacon API path and status class",
	}, []string{"network", "api_path", "status_class"})
	metricReqByAPIKey = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_requests_by_api_key_total",
		Help: "Proxy requests by API key, HTTP method, and status class",
	}, []string{"network", "api_key", "method", "status_class"})
	metricReqByAPIKeyPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_requests_by_api_key_path_total",
		Help: "Proxy requests by API key, normalized Beacon API path, and status class",
	}, []string{"network", "api_key", "api_path", "status_class"})
	metricCacheByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_cache_requests_by_method_total",
		Help: "Cache outcome by HTTP method (hit, miss, bypass_method, bypass_policy)",
	}, []string{"network", "method", "cache_result"})
	metricCacheByPath = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_cache_requests_by_path_total",
		Help: "Cache outcome by normalized Beacon API path",
	}, []string{"network", "api_path", "cache_result"})
	metricCacheServed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_cache_served_total",
		Help: "Responses served from cache",
	}, []string{"network"})
	metricMultiplexedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ebeacon_multiplexed_total",
		Help: "Requests served via deduplication",
	}, []string{"network"})
)
