package shoveler

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	PacketsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_packets_received",
		Help: "The total number of packets received",
	})

	ValidationsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_validations_failed",
		Help: "The total number of packets that failed validation",
	})

	RabbitmqReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_rabbitmq_reconnects",
		Help: "The total number of reconnections to rabbitmq bus",
	})

	QueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_queue_size",
		Help: "The number of messages in the queue",
	})

	// Collector mode metrics
	PacketsParsedOK = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_packets_parsed_ok",
		Help: "The total number of packets parsed successfully",
	})

	ParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_parse_errors",
		Help: "The total number of parse errors by reason",
	}, []string{"reason"})

	TTLEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_ttl_evictions",
		Help: "The total number of state entries evicted due to TTL",
	})

	StateCollisions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_state_collisions",
		Help: "The total number of state key collisions",
	})

	StateSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_state_size",
		Help: "Current number of entries in the state map",
	})

	RecordsEmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_records_emitted",
		Help: "The total number of collector records emitted",
	})

	ParseTimeMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "shoveler_parse_time_ms",
		Help:    "Packet parsing time in milliseconds",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	})

	RequestLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "shoveler_request_latency_ms",
		Help:    "Request latency in milliseconds (collector mode)",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15),
	})
)

func StartMetrics(metricsPort int) {

	// Listen to the metrics requests in a separate thread
	go func() {
		listenAddress := ":" + strconv.Itoa(metricsPort)
		log.Debugln("Starting metrics at " + listenAddress + "/metrics")
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(listenAddress, nil)
		if err != nil {
			log.Errorln("Failed to listen and serve metrics:", err)
			return
		}
	}()

}
