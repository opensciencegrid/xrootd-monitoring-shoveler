package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// enrichmentQueueDropped counts records dropped because the enrichment queue was full.
	enrichmentQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_enrichment_queue_dropped",
		Help: "Total number of enrichment records dropped because the bounded queue was at capacity",
	})

	// enrichmentQueueSize tracks the current number of pending records in the enrichment queue.
	enrichmentQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_enrichment_queue_size",
		Help: "Current number of pending records in the enrichment queue",
	})
)
