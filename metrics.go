package shoveler

import (
	"net/http"
	"strconv"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
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

	MetricsProcessFailures = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "metrics_process_failure",
		Help: "The number of packets that failed to be processed by the metrics library",
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

func ProcessMetricsPacket(bytes []byte) {
	err := metrics.HandlePacket(bytes)
	if err != nil {
		log.Errorln("Failed to process metrics packet:", err)
		MetricsProcessFailures.Inc()
		return
	}
}
