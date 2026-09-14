package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/sirupsen/logrus"
)

// embeddedCRICNetroutes is a snapshot of the CRIC network-route CIDR table
// compiled into the binary as the offline default for IP-based resolution. It is
// the CRIC rcsite/query response
// (https://wlcg-cric.cern.ch/api/core/rcsite/query/?json) reduced to the only
// fields this file reads — the per-site netroutes — so a full, unreduced response
// parses identically. Refresh it (or point site.ip_source at the live endpoint)
// to keep coverage current. See the README for how to regenerate it.
//
//go:embed cric_netroutes.json
var embeddedCRICNetroutes []byte

// Additional resolution statuses used by the "ip" resolution method. The shared
// no_host/ambiguous statuses from sitemap.go are reused where they apply.
const (
	SiteStatusResolvedIP = "resolved_ip" // resolved via CRIC netroutes CIDR containment
	SiteStatusUnknownIP  = "unknown_ip"  // an IP was available but no declared CIDR block contained it
)

// ipRoute is one declared CIDR block and the RCSite that owns it.
type ipRoute struct {
	net  *net.IPNet
	site string
}

// The following types mirror only the fields of CRIC's rcsite/query/?json
// response that we need: a top-level object keyed by RCSite name, each carrying
// netroutes whose networks hold the declared ipv4/ipv6 CIDR blocks. Every other
// field in the (large) CRIC document is ignored by the JSON decoder.
type cricNetworks struct {
	IPv4 []string `json:"ipv4"`
	IPv6 []string `json:"ipv6"`
}

type cricNetroute struct {
	Networks cricNetworks `json:"networks"`
}

type cricRCSite struct {
	Netroutes map[string]cricNetroute `json:"netroutes"`
}

// IPSiteRegistry maps an IP address to its WLCG RCSite by longest-prefix
// containment over the CIDR blocks each site declares in its CRIC network
// routes. It backs the "ip" resolution method, which reaches endpoints the
// domain map cannot (clients with no useful PTR, ambiguous or unknown domains).
// CRIC's own resolution is nothing more than this containment test, so we
// replicate it locally to avoid any per-record network calls.
//
// Safe for concurrent use: readers take a read lock while a background refresh
// swaps in a fresh route table under the write lock, mirroring SiteRegistry.
type IPSiteRegistry struct {
	logger *logrus.Logger

	mu     sync.RWMutex
	routes []ipRoute
}

// NewIPSiteRegistry returns a registry seeded from the embedded CRIC netroutes
// snapshot. It never fails: a bad snapshot only yields an empty table (every IP
// resolves to unknown_ip), which is logged.
func NewIPSiteRegistry(logger *logrus.Logger) *IPSiteRegistry {
	if logger == nil {
		logger = logrus.New()
	}
	r := &IPSiteRegistry{logger: logger}
	if err := r.Load(embeddedCRICNetroutes); err != nil {
		logger.Warnf("site: failed to load embedded CRIC netroutes snapshot: %v", err)
	}
	return r
}

// Load parses a CRIC rcsite/query document, flattens every site's declared
// ipv4/ipv6 CIDR blocks into a route table, and atomically swaps it in. A parse
// error or a table with no usable CIDRs leaves the current table untouched so a
// transient bad fetch never wipes a good in-memory table.
func (r *IPSiteRegistry) Load(data []byte) error {
	var doc map[string]cricRCSite
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse CRIC netroutes: %w", err)
	}

	var routes []ipRoute
	var bad int
	for site, rc := range doc {
		for _, nr := range rc.Netroutes {
			for _, cidrs := range [][]string{nr.Networks.IPv4, nr.Networks.IPv6} {
				for _, cidr := range cidrs {
					_, ipnet, err := net.ParseCIDR(strings.TrimSpace(cidr))
					if err != nil || ipnet == nil {
						bad++
						continue
					}
					routes = append(routes, ipRoute{net: ipnet, site: site})
				}
			}
		}
	}
	if len(routes) == 0 {
		return fmt.Errorf("CRIC netroutes contained no usable CIDR blocks")
	}

	r.mu.Lock()
	r.routes = routes
	r.mu.Unlock()

	siteIPRoutes.Set(float64(len(routes)))
	r.logger.Infof("site: loaded CRIC netroutes: %d CIDR blocks (%d unparseable skipped)", len(routes), bad)
	return nil
}

// ResolveIP maps an IP to an RCSite by longest-prefix containment. It returns
// SiteStatusResolvedIP with the owning site when exactly one site owns the
// most-specific containing block, SiteStatusAmbiguous when two different sites
// tie at that most-specific length, SiteStatusUnknownIP when no block contains
// the IP, and SiteStatusNoHost for a nil IP. An ambiguous result still names a
// site — the lexicographically first of the tied ones — which the status marks
// as a guess.
func (r *IPSiteRegistry) ResolveIP(ip net.IP) (site string, status string) {
	if ip == nil {
		return "", SiteStatusNoHost
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	bestLen := -1
	winner := ""
	ambiguous := false
	for _, rt := range r.routes {
		if !rt.net.Contains(ip) {
			continue
		}
		ones, _ := rt.net.Mask.Size()
		switch {
		case ones > bestLen:
			// A strictly more specific block wins outright and clears any earlier tie.
			bestLen = ones
			winner = rt.site
			ambiguous = false
		case ones == bestLen && rt.site != winner:
			// Two different sites declare an equally specific block: shared range.
			// Keep the lexicographically first as the guess. The route table is
			// built by ranging a map, so its order is not stable across restarts;
			// picking by name keeps the reported site reproducible.
			ambiguous = true
			if rt.site < winner {
				winner = rt.site
			}
		}
	}

	switch {
	case bestLen < 0:
		return "", SiteStatusUnknownIP
	case ambiguous:
		return winner, SiteStatusAmbiguous
	default:
		return winner, SiteStatusResolvedIP
	}
}

// Candidates returns every RCSite that ties at the most-specific block containing
// ip, or nil when fewer than two do. It exists for reporting an ambiguous match;
// use ResolveIP to resolve, which applies the status rules.
func (r *IPSiteRegistry) Candidates(ip net.IP) []string {
	if ip == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	bestLen := -1
	var sites []string
	for _, rt := range r.routes {
		if !rt.net.Contains(ip) {
			continue
		}
		ones, _ := rt.net.Mask.Size()
		switch {
		case ones > bestLen:
			bestLen = ones
			sites = []string{rt.site}
		case ones == bestLen:
			sites = append(sites, rt.site)
		}
	}
	if len(sites) < 2 {
		return nil
	}

	seen := make(map[string]struct{}, len(sites))
	unique := make([]string, 0, len(sites))
	for _, site := range sites {
		if _, dup := seen[site]; dup {
			continue
		}
		seen[site] = struct{}{}
		unique = append(unique, site)
	}
	if len(unique) < 2 {
		return nil
	}
	sort.Strings(unique)
	return unique
}

// LoadSource fetches and loads a CRIC netroutes document from a file path or an
// http(s):// URL. It reuses the shared fetch helper from sitemap.go.
func (r *IPSiteRegistry) LoadSource(ctx context.Context, src string) error {
	data, err := fetchSiteSource(ctx, src)
	if err != nil {
		return err
	}
	return r.Load(data)
}

// StartRefresh periodically re-fetches a URL source and reloads the route table
// until ctx is cancelled. It is a no-op for file sources or when interval <= 0.
// A failed refresh is logged and the previous table is retained.
func (r *IPSiteRegistry) StartRefresh(ctx context.Context, src string, interval time.Duration) {
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
					siteIPReloadFailures.Inc()
					r.logger.Warnf("site: CRIC netroutes refresh failed, keeping previous data: %v", err)
				}
			}
		}
	}()
}
