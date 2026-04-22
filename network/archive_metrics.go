package network

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metricArchivePromotion counts how often requests are routed to archive
// upstreams due to archive-aware routing. The "reason" label distinguishes
// between proactive classification (before any upstream was tried) and
// error-driven fallthrough (after a pruning-shaped 404 from a pruned upstream).
var metricArchivePromotion = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ebeacon_archive_promotion_total",
	Help: "Requests routed to archive upstreams by promotion reason",
}, []string{"network", "reason"})

// metricPruningErrorNoArchive counts pruning-shaped 404 responses that could
// not be retried because no upstream is marked archive-capable. A sustained
// non-zero rate here is a signal that configuring an archive upstream would
// reduce visible errors for historical-data requests.
var metricPruningErrorNoArchive = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ebeacon_pruning_error_no_archive_total",
	Help: "Pruning-shaped 404 responses returned to clients because no archive upstream is configured",
}, []string{"network"})

const (
	archivePromotionProactive = "proactive"
	archivePromotionOnError   = "pruning_error"
)
