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
	// replication). Records with no matching signal are not counted here;
	// subtract from shoveler_traffic_scope for the user-generated share.
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

// Correlation state lifecycle. These say how entries leave the state other
// than by being used: the session and open-file lifetimes and the server
// reaping that replaced the single entry TTL.
var (
	// expiredUsersTotal counts sessions removed from the state, by reason:
	// "disconnected" is the grace period after a reported disconnect running
	// out (the mechanism working); "idle" is a session with no disconnect
	// that went quiet for state.user_idle_ttl. A climbing "idle" with live
	// sessions behind it means user_idle_ttl is too short.
	expiredUsersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_expired_users_total",
		Help: "Total number of client sessions removed from the correlation state, by reason (disconnected, idle)",
	}, []string{"reason"})

	// staleOpensTotal counts open files dropped without their close, by how
	// that was found out: "disconnect" (still open when the session ended) or
	// "file_ttl" (not heard of for state.file_ttl: silent, on a server that
	// sends XFR; that old, elsewhere).
	staleOpensTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shoveler_stale_opens_total",
		Help: "Total number of open files dropped from the correlation state without a close, by reason (disconnect, file_ttl)",
	}, []string{"reason"})

	// reapedServersTotal counts server incarnations dropped after
	// state.server_ttl of silence, with everything they owned.
	reapedServersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_reaped_servers_total",
		Help: "Total number of server incarnations dropped from the correlation state after state.server_ttl without a packet",
	})

	// fileXFRRecordsTotal counts in-flight XFR records. Zero means no server
	// reporting here has "xfr" on its xrootd.monitor line, and state.file_ttl
	// then counts from each open (with state.max_memory set, it has no effect).
	fileXFRRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_file_xfr_records_total",
		Help: "Total number of in-flight XFR records received in f-stream packets",
	})

	// fileDisconnectRecordsTotal counts session disconnect records, which start
	// a session's grace period.
	fileDisconnectRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_file_disconnect_records_total",
		Help: "Total number of session disconnect records received in f-stream packets",
	})
)

// Correlation state memory bound (state.max_memory, state.max_entries).
var (
	// stateEvictedTotal counts entries evicted, least recently used first,
	// because the state was over its byte budget or its entry cap. Evicted
	// entries are lost correlation: a steady rate means the budget is binding.
	stateEvictedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_state_evicted_total",
		Help: "Total number of correlation entries evicted (least recently used first) to stay within state.max_memory or state.max_entries",
	})

	// stateEvictPinnedTotal counts how often eviction passed over an entry
	// it may not evict: a session with files open, or its dictid entry.
	stateEvictPinnedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_state_evict_pinned_total",
		Help: "Total number of times LRU eviction passed over an entry of a session that has files open (its session or dictid entry)",
	})

	// stateEntries is the number of entries in the correlation maps (open
	// files, sessions, dictionary records) under the LRU bound.
	stateEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_state_entries",
		Help: "Current number of correlation entries (open files, sessions, dictionary records)",
	})

	// stateBudgetUtilization is how full the state's byte budget is, 0 to 1;
	// 0 when unbounded. A ratio on purpose: the charged bytes behind it are an
	// estimate, and must not be read as the process's memory.
	stateBudgetUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_state_budget_utilization",
		Help: "Fraction of the correlation state's byte budget in use (0 when state.max_memory is off)",
	})

	// stateMaxMemoryBytes publishes state.max_memory, so a dashboard can draw
	// the cap next to the process memory it bounds.
	stateMaxMemoryBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shoveler_state_max_memory_bytes",
		Help: "The state.max_memory process memory cap in force (0 = unbounded)",
	})

	// stateMemoryFlooredTotal counts control ticks spent over state.max_memory
	// with the state budget already at its floor and nothing left to evict:
	// the memory is somewhere other than the correlation state.
	stateMemoryFlooredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shoveler_state_memory_floored_total",
		Help: "Total number of memory control ticks over state.max_memory with the correlation state already at its floor",
	})
)
