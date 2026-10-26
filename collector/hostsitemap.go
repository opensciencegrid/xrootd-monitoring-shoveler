package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/sirupsen/logrus"
)

// embeddedCRICSE is a snapshot of CRIC storage-element services compiled into
// the binary as the offline default for hostname-based resolution. It is the
// CRIC service/query/?json&type=SE response
// (https://wlcg-cric.cern.ch/api/core/service/query/?json&type=SE) reduced to
// the only fields this file reads — each SE's rcsite and protocol endpoints —
// so a full, unreduced response parses identically. Refresh it (or point
// site.hostname_source at the live endpoint) to keep coverage current. See the
// README for how to regenerate it.
//
//go:embed cric_se.json
var embeddedCRICSE []byte

// SiteStatusResolvedHostname is the status stamped when the hostname method
// matched a CRIC SE endpoint exactly. It is distinct from SiteStatusResolved
// (domain-suffix match) so a consumer can tell an exact storage-host hit from a
// broader domain guess.
const SiteStatusResolvedHostname = "resolved_hostname"

// The following types mirror only the fields of CRIC's service/query/?json&type=SE
// response that we need: a top-level object keyed by SE name, each carrying an
// rcsite and protocols whose endpoint is a URL (scheme://host:port). Every other
// field in the (large) CRIC document is ignored by the JSON decoder.
//
// "aprotocols" is deliberately absent. It looks like a second set of protocols
// but is an access-pattern index — {"read_wan": ["<protocol name>", ...]} — whose
// values are lists of names already present in "protocols", so it yields no
// endpoint of its own. Declaring it as protocols-shaped is worse than useless:
// unmarshalling a list into a struct fails, and because Load rejects a document
// it cannot parse, one such entry anywhere in the response discards the whole
// thing and silently pins the collector to the embedded snapshot.
//
// The lifecycle fields ("state", "status") are absent for the same reason: we
// read these endpoints for host->site geography, not for whether a service is
// usable, and a machine does not change site because a door is retired. Dropping
// such an endpoint would only lose coverage for a host whose site we know. In the
// live response every "state" is ACTIVE anyway, so filtering on it would be code
// that never runs.
type cricSEProtocol struct {
	Endpoint string `json:"endpoint"`
}

type cricSE struct {
	RCSite    string                    `json:"rcsite"`
	Endpoint  string                    `json:"endpoint"`
	Protocols map[string]cricSEProtocol `json:"protocols"`
}

// HostSiteRegistry maps a fully-qualified host name to its WLCG RCSite by exact
// match against the host part of CRIC SE protocol endpoints (scheme and port
// stripped). It backs the "hostname" resolution method: storage servers such as
// xrootd.aglt2.org are listed as endpoints, so an exact hit is authoritative and
// carries no risk of over-matching a domain suffix.
//
// Safe for concurrent use: readers take a read lock while a background refresh
// swaps in a fresh host map under the write lock, mirroring SiteRegistry.
type HostSiteRegistry struct {
	logger *logrus.Logger

	mu    sync.RWMutex
	hosts map[string][]string // lowercased host -> RCSite name(s)
}

// NewHostSiteRegistry returns a registry seeded from the embedded CRIC SE
// snapshot. It never fails: a bad snapshot only yields an empty registry (every
// host misses), which is logged.
func NewHostSiteRegistry(logger *logrus.Logger) *HostSiteRegistry {
	if logger == nil {
		logger = logrus.New()
	}
	r := &HostSiteRegistry{logger: logger, hosts: map[string][]string{}}
	if err := r.Load(embeddedCRICSE); err != nil {
		logger.Warnf("site: failed to load embedded CRIC SE snapshot: %v", err)
	}
	return r
}

// Load parses a CRIC service/query (type=SE) document, extracts the host from
// every protocol endpoint, and atomically swaps in the host->RCSite map. A parse
// error or a map with no usable hosts leaves the current registry untouched so a
// transient bad fetch never wipes a good in-memory map.
func (r *HostSiteRegistry) Load(data []byte) error {
	var doc map[string]cricSE
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse CRIC SE: %w", err)
	}

	collected := map[string]map[string]struct{}{}
	var skipped int
	for _, se := range doc {
		site := strings.TrimSpace(se.RCSite)
		if site == "" {
			skipped++
			continue
		}
		addSEEndpoint(collected, site, se.Endpoint)
		for _, p := range se.Protocols {
			addSEEndpoint(collected, site, p.Endpoint)
		}
	}

	hosts := make(map[string][]string, len(collected))
	for host, sites := range collected {
		list := make([]string, 0, len(sites))
		for s := range sites {
			list = append(list, s)
		}
		sort.Strings(list)
		hosts[host] = list
	}
	if len(hosts) == 0 {
		return fmt.Errorf("CRIC SE document contained no usable endpoints")
	}

	r.mu.Lock()
	r.hosts = hosts
	r.mu.Unlock()

	siteRegistryHosts.Set(float64(len(hosts)))
	r.logger.Infof("site: loaded CRIC SE endpoints: %d hosts (%d services without an rcsite skipped)", len(hosts), skipped)
	return nil
}

func addSEEndpoint(into map[string]map[string]struct{}, site, endpoint string) {
	host := hostFromSEEndpoint(endpoint)
	if host == "" {
		return
	}
	sites := into[host]
	if sites == nil {
		sites = map[string]struct{}{}
		into[host] = sites
	}
	sites[site] = struct{}{}
}

// hostFromSEEndpoint returns the lowercased host of a CRIC protocol endpoint
// URL, stripping scheme and port. IP literals and unusable values yield "".
func hostFromSEEndpoint(endpoint string) string {
	ep := strings.TrimSpace(endpoint)
	if ep == "" || strings.HasPrefix(ep, "/") {
		return ""
	}
	if !strings.Contains(ep, "://") {
		ep = "dummy://" + ep
	}
	u, err := url.Parse(ep)
	if err != nil {
		return ""
	}
	return normalizeHost(u.Hostname())
}

// ResolveHost maps a host name to an RCSite only when a CRIC SE endpoint lists
// that exact host. It returns SiteStatusResolvedHostname when exactly one site
// owns the host, SiteStatusAmbiguous when several do (naming the
// lexicographically first as a guess), SiteStatusUnknown when the name is
// usable but not listed, and SiteStatusNoHost for an unusable host.
func (r *HostSiteRegistry) ResolveHost(host string) (site string, status string) {
	h := normalizeHost(host)
	if h == "" {
		return "", SiteStatusNoHost
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	sites, ok := r.hosts[h]
	if !ok || len(sites) == 0 {
		return "", SiteStatusUnknown
	}
	if len(sites) == 1 {
		return sites[0], SiteStatusResolvedHostname
	}
	return sites[0], SiteStatusAmbiguous
}

// Candidates returns every RCSite that lists host as an SE endpoint, or nil when
// none does. It exists for reporting an ambiguous match; use ResolveHost to
// resolve, which applies the status rules.
func (r *HostSiteRegistry) Candidates(host string) []string {
	h := normalizeHost(host)
	if h == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if sites, ok := r.hosts[h]; ok && len(sites) > 0 {
		return append([]string(nil), sites...)
	}
	return nil
}

// LoadSource fetches and loads a CRIC SE document from a file path or an
// http(s):// URL. It reuses the shared fetch helper from sitemap.go.
func (r *HostSiteRegistry) LoadSource(ctx context.Context, src string) error {
	data, err := fetchSiteSource(ctx, src)
	if err != nil {
		return err
	}
	return r.Load(data)
}

// StartRefresh periodically re-fetches a URL source and reloads the host map
// until ctx is cancelled. It is a no-op for file sources or when interval <= 0.
// A failed refresh is logged and the previous map is retained.
func (r *HostSiteRegistry) StartRefresh(ctx context.Context, src string, interval time.Duration) {
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
					siteHostnameReloadFailures.Inc()
					r.logger.Warnf("site: CRIC SE refresh failed, keeping previous data: %v", err)
				}
			}
		}
	}()
}
