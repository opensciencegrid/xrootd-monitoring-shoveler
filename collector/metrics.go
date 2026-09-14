package collector

import (
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CountParsedPacket records a successfully parsed packet's upstream server and
// type. Call after setting RemoteAddr and before dispatch, so packets dropped by
// the gstream queue are included. Processing methods do not count packets again.
func (c *Correlator) CountParsedPacket(packet *parser.Packet) {
	if !c.perServerMetrics || packet == nil {
		return
	}
	packetType := PacketTypeName(packet.PacketType)
	if packet.IsXML {
		packetType = "xml"
	}
	c.countByServer(packetsPerServerTotal, canonicalServerIP(packet.RemoteAddr), packetType)
}

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
	// siteRegistryDomains reports how many domain suffixes the currently loaded
	// CRIC domains map knows about. A value of 0 means every host resolves to
	// unknown_domain (the "domain" resolution method is effectively off).
	siteRegistryDomains = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_registry_domains",
		Help: "Number of domain suffixes in the currently loaded CRIC domains map",
	})

	// siteRegistryHosts reports how many unique hosts the currently loaded CRIC
	// SE endpoint map knows about. A value of 0 means the "hostname" method is
	// effectively off.
	siteRegistryHosts = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_registry_hosts",
		Help: "Number of unique hosts in the currently loaded CRIC SE endpoint map",
	})

	// siteRegistryReloadFailures counts failed fetches of the CRIC domains URL,
	// including the first one at startup. The current map is kept. If it keeps
	// rising, the URL is unreachable or returning bad data.
	siteRegistryReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_registry_reload_failures",
		Help: "Total number of failed CRIC domains map fetches from a URL source, including the first at startup",
	})

	// siteHostnameReloadFailures counts failed fetches of the CRIC SE URL,
	// including the first one at startup. The current host map is kept.
	siteHostnameReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_hostname_reload_failures",
		Help: "Total number of failed CRIC SE endpoint map fetches from a URL source, including the first at startup",
	})

	// siteUnresolved counts transfer endpoints that no configured resolution
	// method could map to an RCSite, broken down by role (src/dst) and reason
	// (ambiguous, unknown_domain, no_host, unknown_ip, no_applicable_method).
	// This is the primary "how many unknowns" signal; a resolved endpoint is not counted.
	siteUnresolved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_site_unresolved",
		Help: "Total number of transfer endpoints not resolved to an RCSite, by role and reason",
	}, []string{"role", "reason"})

	// siteIPRoutes reports how many CIDR blocks the currently loaded CRIC
	// netroutes table holds. 0 means the "ip" method is effectively off.
	siteIPRoutes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_site_ip_routes",
		Help: "Number of CIDR blocks in the currently loaded CRIC netroutes table",
	})

	// siteIPReloadFailures counts failed fetches of the CRIC netroutes URL,
	// including the first one at startup. The current route table is kept.
	siteIPReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_site_ip_reload_failures",
		Help: "Total number of failed CRIC netroutes table fetches from a URL source, including the first at startup",
	})

	// siteResolvedByMethod counts endpoints resolved to an RCSite by role
	// (src/dst) and by which step of site.resolution_order produced the answer
	// (config, hostname, ip, domain). hostname is an exact CRIC SE endpoint match;
	// domain is a suffix match. It shows what each step actually contributes,
	// which is what tuning the order needs.
	siteResolvedByMethod = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_site_resolved_by_method",
		Help: "Total number of transfer endpoints resolved to an RCSite, by role and resolution method",
	}, []string{"role", "method"})

	// trafficScopeTotal counts classified records by network scope (LAN, WAN,
	// UNKNOWN). UNKNOWN is the topology-resolution failure signal for whole
	// records; the per-endpoint reasons stay in shoveler_site_unresolved.
	trafficScopeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_traffic_scope",
		Help: "Total number of WLCG records classified by network scope (LAN, WAN, UNKNOWN)",
	}, []string{"scope"})

	// trafficXRootDInternal counts records classified as generated by XRootD
	// itself, by the first signal that matched (internal_user, job_agent,
	// replication, storage_to_storage). Records with no matching signal are not
	// counted here; subtract from shoveler_traffic_scope for the user-generated
	// share.
	trafficXRootDInternal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_traffic_xrootd_internal",
		Help: "Total number of WLCG records classified as XRootD-internal traffic, by the signal that matched",
	}, []string{"signal"})

	// scitagsRegistryExperiments reports how many experiments the currently loaded
	// SciTags registry knows about. A value of 0 means id->name resolution is off.
	scitagsRegistryExperiments = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_scitags_registry_experiments",
		Help: "Number of experiments in the currently loaded SciTags registry",
	})

	// scitagsRegistryActivities reports how many (experiment, activity) pairs the
	// currently loaded SciTags registry knows about.
	scitagsRegistryActivities = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_scitags_registry_activities",
		Help: "Number of (experiment, activity) pairs in the currently loaded SciTags registry",
	})

	// scitagsRegistryReloadFailures counts failed fetches of a SciTags URL,
	// including the first one at startup; the current registry is kept. A steady
	// rise means the URL is down or serving bad data.
	scitagsRegistryReloadFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_scitags_registry_reload_failures",
		Help: "Total number of failed SciTags registry URL fetches",
	})

	// scitagsUnmappedIDs counts U-stream experiment/activity ids that could
	// not be resolved to a name against the loaded registry, broken down by which
	// id was missing. A sustained rise usually means the registry snapshot is
	// stale relative to what the servers are tagging.
	scitagsUnmappedIDs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_scitags_unmapped_ids",
		Help: "Total number of SciTags ids that could not be resolved to a name",
	}, []string{"kind"})
)
