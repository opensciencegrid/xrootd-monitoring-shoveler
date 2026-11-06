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
)
