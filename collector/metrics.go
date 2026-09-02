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

	// packetsPerServerTotal counts successfully parsed monitoring packets broken
	// down by the upstream server IP and the XRootD stream / frame type.  Use
	// this to verify that each XRootD node is still sending and to see which
	// streams (fstat, dict, user, eainfo, map, gstream, …) are active.
	//
	// Opt-in via metrics.per_server; exports no series unless enabled.
	// See Correlator.countByServer for why.
	packetsPerServerTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_packets_by_server_total",
		Help: "Total number of successfully parsed monitoring packets per upstream server IP and XRootD stream type (opt-in: metrics.per_server)",
	}, []string{"server_ip", "packet_type"})

	// standaloneCloseRecordsTotal counts file close records for which no matching
	// open was found in stateMap. These records are emitted via createStandaloneCloseRecord
	// and are degraded: they lack LFN, open-time, and full user context. A sustained
	// rise here indicates the correlator is missing opens (e.g. due to packet loss,
	// TTL expiry, or shoveler restart mid-session).
	standaloneCloseRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_standalone_close_records_total",
		Help: "Total number of file close records emitted without a matching open in stateMap (degraded records missing LFN, open-time, and full user context)",
	})

	// fileOpenRecordsTotal counts FileOpen records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	// Each packet can carry multiple records.
	// Opt-in via metrics.per_server; exports no series unless enabled.
	fileOpenRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_open_records_total",
		Help: "Total number of file open records received in f-stream and t-stream packets per upstream server IP (opt-in: metrics.per_server)",
	}, []string{"server_ip"})

	// fileCloseRecordsTotal counts FileClose records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	// Opt-in via metrics.per_server; exports no series unless enabled.
	fileCloseRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_close_records_total",
		Help: "Total number of file close records received in f-stream and t-stream packets per upstream server IP (opt-in: metrics.per_server)",
	}, []string{"server_ip"})

	// fileTimeRecordsTotal counts FileTime (TOD) records parsed from file-record packets
	// (both f-stream/fstat and t-stream/trace), broken down by upstream server IP.
	// Opt-in via metrics.per_server; exports no series unless enabled.
	fileTimeRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_file_time_records_total",
		Help: "Total number of file time (TOD) records received in f-stream and t-stream packets per upstream server IP (opt-in: metrics.per_server)",
	}, []string{"server_ip"})
)
