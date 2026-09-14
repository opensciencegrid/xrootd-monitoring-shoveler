package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/sirupsen/logrus"
)

// embeddedCRICDomains is a snapshot of the CRIC domain->RCSite map compiled into
// the binary. It is the fallback source so src/dst site resolution works
// offline and even when no source is configured; a configured file/URL source
// overrides it at runtime.
//
// Snapshot of the CRIC "domains" preset with besttier=1 (which collapses noisy
// low-tier/test sites, e.g. cern.ch -> CERN-PROD instead of [BOINC, CERN-PROD]):
//
//	https://wlcg-cric.cern.ch/api/core/rcsite/query/?json&preset=domains&besttier=1
//
//go:embed cric_domains.json
var embeddedCRICDomains []byte

// Site resolution status values recorded per endpoint. They make the UNKNOWN
// rate measurable end to end and are also exported via the
// shoveler_site_unresolved metric (labelled by role and reason).
const (
	SiteStatusResolved         = "resolved"          // exactly one RCSite matched by domain suffix in the domains map
	SiteStatusResolvedConfig   = "resolved_config"   // taken from the configured local site (site.local_site)
	SiteStatusResolvedOverride = "resolved_override" // pinned by an operator entry in site.overrides
	SiteStatusAmbiguous        = "ambiguous"         // matched >1 RCSite even after besttier=1; the site is a first-match guess
	SiteStatusUnknown          = "unknown_domain"    // a host name was available but no domain suffix matched
	SiteStatusNoHost           = "no_host"           // no usable host name (missing, still an IP literal, or DNS failed)
)

// The methods that can map one endpoint to an RCSite, named as they appear in
// site.resolution_order. Each uses a different identifier and the first hit
// wins, so the order is a statement of which identifier we trust most.
const (
	SiteMethodConfig   = "config"   // the site this collector runs at, from site.local_site
	SiteMethodHostname = "hostname" // the full host name, as an exact CRIC SE protocol endpoint
	SiteMethodIP       = "ip"       // the endpoint address, by CRIC netroutes CIDR containment
	SiteMethodDomain   = "domain"   // the host name's longest matching domain suffix in the domains map
)

// SiteMethodOverride labels an endpoint settled by site.overrides. It is NOT a
// resolution_order method: pins are not an inference to be ranked against the
// others, so they cannot be ordered, left out of a custom order, or switched off
// there. They are consulted before every CRIC lookup and win outright. The label
// exists so pinned endpoints are visible in shoveler_site_resolved_by_method
// alongside the rest.
const SiteMethodOverride = "override"

// DefaultSiteResolutionOrder is the order used when site.resolution_order is not
// set: the operator's own declaration of where the collector runs first (nothing
// we can infer beats it, and it needs no DNS), then the identifiers from most to
// least specific — an exact host name, then the declared IP range, then the
// domain suffix, which is the broadest and so the easiest to over-match.
var DefaultSiteResolutionOrder = []string{SiteMethodConfig, SiteMethodHostname, SiteMethodIP, SiteMethodDomain}

// NormalizeSiteResolutionOrder cleans a configured order: it lowercases the
// method names, drops unknown ones and repeats (both logged), and falls back to
// DefaultSiteResolutionOrder when nothing usable is left, so a typo in the config
// degrades to the default instead of disabling site resolution.
func NormalizeSiteResolutionOrder(order []string, logger *logrus.Logger) []string {
	if logger == nil {
		logger = logrus.New()
	}

	known := map[string]bool{
		SiteMethodConfig:   true,
		SiteMethodHostname: true,
		SiteMethodIP:       true,
		SiteMethodDomain:   true,
	}

	normalized := make([]string, 0, len(order))
	seen := make(map[string]bool, len(order))
	for _, method := range order {
		m := strings.ToLower(strings.TrimSpace(method))
		if m == "" {
			continue
		}
		if !known[m] {
			logger.Warnf("site: ignoring unknown resolution method %q (known: %s)",
				method, strings.Join(DefaultSiteResolutionOrder, ", "))
			continue
		}
		if seen[m] {
			logger.Warnf("site: ignoring repeated resolution method %q", m)
			continue
		}
		seen[m] = true
		normalized = append(normalized, m)
	}

	if len(normalized) == 0 {
		return append([]string(nil), DefaultSiteResolutionOrder...)
	}
	return normalized
}

// SiteRegistry maps a fully-qualified host name to its WLCG RCSite name using the
// CRIC "domains" map: a flat object of domain suffix -> list of RCSite names.
// Resolution is a longest-suffix match; a suffix that still maps to more than one
// site (even after besttier=1) is treated as ambiguous and left unresolved.
//
// It is safe for concurrent use: readers take a read lock while a background
// refresh swaps in a freshly fetched map under the write lock.
type SiteRegistry struct {
	logger *logrus.Logger

	mu      sync.RWMutex
	domains map[string][]string // domain suffix -> RCSite name(s)
}

// NewSiteRegistry returns a registry seeded from the embedded CRIC domains
// snapshot. It never fails: a bad snapshot only yields an empty registry (every
// host resolves to unknown_domain), which is logged.
func NewSiteRegistry(logger *logrus.Logger) *SiteRegistry {
	if logger == nil {
		logger = logrus.New()
	}
	r := &SiteRegistry{logger: logger, domains: map[string][]string{}}
	if err := r.Load(embeddedCRICDomains); err != nil {
		logger.Warnf("site: failed to load embedded CRIC domains snapshot: %v", err)
	}
	return r
}

// Load parses a CRIC domains document and atomically swaps in the new map. A
// parse error, or a document that yields no usable entries, leaves the current
// registry untouched so a transient bad fetch never wipes a good in-memory map.
//
// Entries are sanitized as they are read: a blank suffix, a blank site name, and
// a suffix whose site list ends up empty all carry no resolvable information and
// are dropped. That keeps the map free of entries resolveHost could match but not
// answer from, and makes the shoveler_site_registry_domains count mean "suffixes
// that can actually resolve".
func (r *SiteRegistry) Load(data []byte) error {
	var doc map[string][]string
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse CRIC domains: %w", err)
	}

	domains := make(map[string][]string, len(doc))
	for suffix, sites := range doc {
		key := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(suffix), "."))
		if key == "" {
			continue
		}
		cleaned := make([]string, 0, len(sites))
		for _, site := range sites {
			if s := strings.TrimSpace(site); s != "" {
				cleaned = append(cleaned, s)
			}
		}
		if len(cleaned) == 0 {
			continue
		}
		domains[key] = cleaned
	}

	// Emptiness is checked *after* normalization, not before. A document that is
	// non-empty as JSON but whose every entry drops out here (blank keys, empty
	// site lists) is exactly as unusable as an empty one, and swapping it in would
	// wipe a good map — the opposite of the fail-open contract above.
	if len(domains) == 0 {
		return fmt.Errorf("CRIC domains map has no usable entries")
	}

	r.mu.Lock()
	r.domains = domains
	r.mu.Unlock()

	siteRegistryDomains.Set(float64(len(domains)))
	r.logger.Infof("site: loaded CRIC domains map: %d domains", len(domains))
	return nil
}

// ResolveHost maps a host name to an RCSite via longest domain-suffix match. It
// returns the site and a status classifying the outcome for measurement. The
// site is authoritative only when the status is SiteStatusResolved: on
// SiteStatusAmbiguous it is the first of the several sites CRIC lists for that
// domain, which consumers must treat as a guess (see resolveHost).
func (r *SiteRegistry) ResolveHost(host string) (site string, status string) {
	return r.resolveHost(host)
}

// resolveHost walks the host's suffixes from most to least specific, taking the
// first hit. The walk starts at the full host name, so an exact key in the
// domains map is matched here too — the "hostname" method matches CRIC SE
// endpoints instead (see HostSiteRegistry).
func (r *SiteRegistry) resolveHost(host string) (site string, status string) {
	h := normalizeHost(host)
	if h == "" {
		return "", SiteStatusNoHost
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Walk suffixes from most specific (full host) to least specific, taking the
	// first hit -> the longest matching suffix. "n01.gla.scotgrid.ac.uk" matches
	// "gla.scotgrid.ac.uk" before the broader (and ambiguous) "ac.uk".
	labels := strings.Split(h, ".")
	for i := 0; i < len(labels)-1; i++ {
		suffix := strings.Join(labels[i:], ".")
		sites, ok := r.domains[suffix]
		// An empty site list is treated as no match at all, so the walk continues
		// to a broader suffix. Load drops such entries, so this is defence in
		// depth — but the package installs no recover(), so indexing sites[0] on a
		// zero-length list would take the whole process down from an enrichment
		// worker, and a configured site.source is operator-supplied data.
		if !ok || len(sites) == 0 {
			continue
		}
		if len(sites) == 1 {
			return sites[0], SiteStatusResolved
		}
		// Longest suffix matched but is shared by several RCSites. We return the
		// first one CRIC lists so downstream has something to work with, flagged
		// SiteStatusAmbiguous so it is never mistaken for a definite answer — a
		// consumer that cannot accept a guess filters on the status. Stop here
		// rather than falling through to an even broader (and no less ambiguous)
		// suffix.
		return sites[0], SiteStatusAmbiguous
	}
	return "", SiteStatusUnknown
}

// Candidates returns every RCSite the longest matching suffix lists for host, or
// nil when nothing matches. It exists for reporting an ambiguous match: callers
// resolving a site must use ResolveHost, which applies the status rules.
func (r *SiteRegistry) Candidates(host string) []string {
	h := normalizeHost(host)
	if h == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	labels := strings.Split(h, ".")
	for i := 0; i < len(labels)-1; i++ {
		if sites, ok := r.domains[strings.Join(labels[i:], ".")]; ok && len(sites) > 0 {
			return append([]string(nil), sites...)
		}
	}
	return nil
}

// normalizeHost lowercases a host, strips a trailing dot, and returns "" for a
// host that cannot be matched against the domain map: empty, the sentinel
// "unknown", an IP literal (incl. bracketed/zoned IPv6), or a single-label name.
// IP literals are intentionally rejected here; matching them to a site is the job
// of the IP/CIDR resolver, not the domain map.
func normalizeHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimSuffix(h, ".")
	if h == "" || h == "unknown" {
		return ""
	}

	stripped := strings.Trim(h, "[]")
	if zoneIdx := strings.LastIndex(stripped, "%"); zoneIdx >= 0 {
		stripped = stripped[:zoneIdx]
	}
	if net.ParseIP(stripped) != nil {
		return ""
	}

	// A single-label name (no dot) cannot match any domain suffix.
	if !strings.Contains(h, ".") {
		return ""
	}
	return h
}

// isURL reports whether src looks like an http(s) URL rather than a file path.
func isURL(src string) bool {
	return strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://")
}

// LoadSource fetches and loads the CRIC domains map from a source that is either
// a local file path or an http(s):// URL.
func (r *SiteRegistry) LoadSource(ctx context.Context, src string) error {
	data, err := fetchSiteSource(ctx, src)
	if err != nil {
		return err
	}
	return r.Load(data)
}

// fetchSiteSource reads a CRIC document from a file path or http(s) URL.
func fetchSiteSource(ctx context.Context, src string) ([]byte, error) {
	if !isURL(src) {
		data, err := os.ReadFile(src)
		if err != nil {
			return nil, fmt.Errorf("read CRIC file %q: %w", src, err)
		}
		return data, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return nil, fmt.Errorf("build CRIC request: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch CRIC url %q: %w", src, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch CRIC url %q: unexpected status %s", src, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // cap at 8 MiB
	if err != nil {
		return nil, fmt.Errorf("read CRIC url body %q: %w", src, err)
	}
	return data, nil
}

// StartRefresh periodically re-fetches a URL source and reloads the domains map
// until ctx is cancelled. It is a no-op for file sources or when interval <= 0.
// A failed refresh is logged and the previous map is retained.
func (r *SiteRegistry) StartRefresh(ctx context.Context, src string, interval time.Duration) {
	if src == "" || !isURL(src) || interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.LoadSource(ctx, src); err != nil {
					siteRegistryReloadFailures.Inc()
					r.logger.Warnf("site: CRIC domains refresh failed, keeping previous data: %v", err)
				}
			}
		}
	}()
}

// siteRecordEnricher resolves the src/dst RCSite of each record from the
// resolved server and client endpoints, ordered by data-flow direction. It must
// run AFTER dnsRecordEnricher so the resolved host names are already populated.
//
// Each endpoint is resolved by trying the methods in order and taking the first
// hit (see DefaultSiteResolutionOrder). hosts is optional (nil drops the
// "hostname" method), ips is optional (nil drops the "ip" method), and so is
// localSite (empty drops the "config" method).
type siteRecordEnricher struct {
	domains   *SiteRegistry
	hosts     *HostSiteRegistry
	ips       *IPSiteRegistry
	overrides *SiteOverrides // operator pins from site.overrides; nil disables overriding

	// ambig names ambiguous hosts in the log, once each, so they can be pinned.
	// nil disables that reporting (the metric still counts them).
	ambig *ambiguityReporter

	// wlcgOnly gates the whole resolution. The resolved sites are emitted on the
	// WLCG record only, so for a record that will not be converted the work — up
	// to two map walks and a route-table scan — would be thrown away. The
	// correlator wires this to its WLCG routing predicate; nil resolves every
	// record, which is what the resolver's own unit tests want.
	wlcgOnly func(*CollectorRecord) bool

	// localSite is the RCSite this collector runs at (site.local_site). It
	// resolves the reporting server outright: the server whose UDP stream reaches
	// this collector sits at the collector's own site, which the operator knows
	// for certain and no lookup can beat. Empty disables the "config" method.
	localSite string
	// localSiteLANClients extends localSite to clients on a private, loopback or
	// link-local address. Those are at the reporting server's site by definition
	// and CRIC does not declare worker-node ranges, so without this they stay
	// unresolved.
	localSiteLANClients bool
	// order is the method order to try, first hit wins; empty means
	// DefaultSiteResolutionOrder.
	order []string
}

// siteResolution is the outcome for one endpoint: the RCSite (empty unless
// resolved), the status stamped on the record, and which method produced it
// (empty when nothing resolved).
type siteResolution struct {
	site   string
	status string
	method string
}

// siteEndpoint is one end of a transfer as the resolver sees it.
type siteEndpoint struct {
	host string // the best known host name; "" when we only have an address
	ip   net.IP // the address when the endpoint was reported as one, else nil
	// atLocalSite marks an endpoint known to sit at the collector's own site,
	// which is what makes localSite usable for it: the reporting server always
	// does, a client only when it is on a local/private address.
	atLocalSite bool
}

func (s *siteRecordEnricher) Name() string { return "site" }

func (s *siteRecordEnricher) Enrich(ctx context.Context, record *CollectorRecord) {
	if record == nil || s.domains == nil {
		return
	}

	// Only WLCG-bound records carry the site fields, so anything else is skipped
	// before any lookup happens.
	if s.wlcgOnly != nil && !s.wlcgOnly(record) {
		return
	}

	// The server IP is always known (from the packet source address). A client
	// IP is only available when the client was reported as an IP literal; when it
	// was a name we have no address to fall back on, so IP resolution is skipped.
	clientIP := endpointIP(record.Host)
	server := s.resolveEndpoint(siteEndpoint{
		host:        record.ServerHostname,
		ip:          endpointIP(record.ServerIP),
		atLocalSite: true,
	})
	client := s.resolveEndpoint(siteEndpoint{
		host:        record.clientHost(),
		ip:          clientIP,
		atLocalSite: s.localSiteLANClients && isLocalClientAddress(clientIP),
	})

	// Order endpoints by data-flow direction. On read the data source is the
	// server; on write it is the client. Unknown operations keep server as source
	// by convention so the fields are still populated deterministically.
	src, dst := server, client
	if deriveOperation(record) == "write" {
		src, dst = client, server
	}
	record.srcSite, record.srcSiteStatus = src.site, src.status
	record.dstSite, record.dstSiteStatus = dst.site, dst.status

	recordSiteMetric("src", src)
	recordSiteMetric("dst", dst)
}

// isLocalClientAddress reports whether an address is one that puts the client at
// the reporting server's own site without a lookup: any-local, loopback,
// multicast, link-local or private (IsPrivate covers RFC1918 and the IPv6
// unique-local range).
func isLocalClientAddress(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate()
}

// resolveEndpoint tries each configured method in order and returns the first
// hit. When every method misses it returns the empty site with the most
// informative failure reason any of them gave (see worseSiteFailure), which is
// what the UNKNOWN-rate metric is keyed on.
func (s *siteRecordEnricher) resolveEndpoint(ep siteEndpoint) siteResolution {
	order := s.order
	if len(order) == 0 {
		order = DefaultSiteResolutionOrder
	}

	// The operator's own answer comes first and wins outright: a pin corrects CRIC
	// as well as settling something CRIC reports ambiguously. It is checked here
	// rather than as a step in the order because it is not an inference to be
	// ranked against the others — see SiteOverrides.
	if site, ok := s.overrides.Resolve(ep); ok {
		return siteResolution{site: site, status: SiteStatusResolvedOverride, method: SiteMethodOverride}
	}

	var failure siteResolution
	for _, method := range order {
		site, status := s.resolveBy(method, ep)
		switch status {
		case SiteStatusResolved, SiteStatusResolvedConfig, SiteStatusResolvedOverride, SiteStatusResolvedHostname, SiteStatusResolvedIP:
			return siteResolution{site: site, status: status, method: method}
		default:
			// Carries the site along with the reason, because an ambiguous match
			// still names a candidate. method stays empty: nothing here resolved,
			// so this must not count towards the resolved-by-method totals.
			failure = worseSiteFailure(failure, siteResolution{site: site, status: status})
		}
	}
	if failure.status == "" {
		failure.status = SiteStatusNoHost
	}
	// Nothing CRIC knows settled this endpoint, and no pin covered it. Name it once
	// so the operator learns which key to add to site.overrides.
	if failure.status == SiteStatusAmbiguous && s.ambig != nil {
		s.ambig.report(ep, failure.site, s.ambiguousCandidates(ep))
	}
	return failure
}

// resolveBy runs a single resolution method. A method that cannot apply to this
// endpoint (not configured, or the endpoint lacks the identifier it needs)
// returns an empty status, so it contributes no failure reason.
func (s *siteRecordEnricher) resolveBy(method string, ep siteEndpoint) (site string, status string) {
	switch method {
	case SiteMethodConfig:
		if s.localSite == "" || !ep.atLocalSite {
			return "", ""
		}
		return s.localSite, SiteStatusResolvedConfig
	case SiteMethodHostname:
		if s.hosts == nil {
			return "", ""
		}
		return s.hosts.ResolveHost(ep.host)
	case SiteMethodIP:
		if s.ips == nil || ep.ip == nil {
			return "", ""
		}
		return s.ips.ResolveIP(ep.ip)
	case SiteMethodDomain:
		if s.domains == nil {
			return "", ""
		}
		return s.domains.ResolveHost(ep.host)
	default:
		return "", ""
	}
}

// worseSiteFailure keeps the more informative of two failed resolutions, along
// with whatever site it named, so the reported reason does not depend on where a
// method sits in the order: "ambiguous" (we found the endpoint but can only
// guess between its sites) outranks "unknown_domain", which outranks "no_host",
// which outranks "unknown_ip". The name-side reasons come first because they
// describe why the endpoint could not be identified at all, which is what
// no_host has always meant: no usable name and no matching IP range either.
// Ranking ambiguous highest is also what carries a guessed site through: it is
// the only failure that has one.
func worseSiteFailure(current, candidate siteResolution) siteResolution {
	rank := func(status string) int {
		switch status {
		case SiteStatusAmbiguous:
			return 4
		case SiteStatusUnknown:
			return 3
		case SiteStatusNoHost:
			return 2
		case SiteStatusUnknownIP:
			return 1
		default:
			return 0
		}
	}
	if rank(candidate.status) > rank(current.status) {
		return candidate
	}
	return current
}

// recordSiteMetric increments the observability counters for one resolved
// direction: every success is counted by the method that produced it (so the
// configured order can be tuned on evidence), IP hits keep their own counter,
// and any non-resolved outcome is counted by reason.
func recordSiteMetric(role string, res siteResolution) {
	if res.method != "" {
		siteResolvedByMethod.WithLabelValues(role, res.method).Inc()
	}
	switch res.status {
	case SiteStatusResolved, SiteStatusResolvedConfig, SiteStatusResolvedOverride, SiteStatusResolvedHostname:
		// resolved by the operator's declaration, an SE hostname, or the domain
		// map; not counted here (siteResolvedByMethod already breaks these down).
	case SiteStatusResolvedIP:
		siteResolvedByIP.WithLabelValues(role).Inc()
	default:
		siteUnresolved.WithLabelValues(role, res.status).Inc()
	}
}

// endpointIP parses the IP of an endpoint from a host string that may be an IP
// literal (bracketed/zoned/IPv4-mapped forms included). It returns nil for a
// name, the sentinel "unknown", or anything not parseable as an IP.
func endpointIP(host string) net.IP {
	if host == "" || host == "unknown" {
		return nil
	}
	return net.ParseIP(extractIPFromHost(host))
}
