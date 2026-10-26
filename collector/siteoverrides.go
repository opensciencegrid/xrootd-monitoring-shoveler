package collector

import (
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// maxReportedAmbiguousHosts bounds the set of endpoints the ambiguity reporter
// remembers. The set exists to log each one once rather than once per record, so
// it must not grow with traffic: a collector seeing a long tail of ambiguous
// endpoints stops reporting new ones rather than retaining every name it saw.
const maxReportedAmbiguousHosts = 256

// SiteOverrides is the site's own answer for an endpoint, declared in
// site.overrides. It is consulted before any CRIC lookup and is used, so
// it corrects CRIC as well as settling a host CRIC reports ambiguously.
//
// A key is matched by what it parses as:
//
//	xrootd.example.org   an exact host name
//	example.org          a domain suffix — everything under it
//	192.0.2.7            one address
//	192.0.2.0/24         an address range
//
// Host keys use the domains-map walk (full host first, then broader suffixes,
// longest key is used); address keys use longest-prefix containment. Host keys are
// tried first, so a name overridden explicitly beats a range that merely contains it.
type SiteOverrides struct {
	hosts  *SiteRegistry // host and suffix keys, reusing the domain-suffix walk
	routes []ipRoute     // address and CIDR keys, longest prefix wins
}

// NewSiteOverrides builds the overrides from site.overrides. It returns nil when
// nothing usable is declared, which disables overriding entirely.
//
// The registry behind host keys is built directly rather than through Load: this
// is config, not a fetched CRIC document, so it must not touch the domains gauge,
// must not be refreshed, and an empty map is a legitimate "no overrides" rather
// than an error.
func NewSiteOverrides(overrides map[string]string, logger *logrus.Logger) *SiteOverrides {
	if logger == nil {
		logger = logrus.New()
	}

	hosts := map[string][]string{}
	var routes []ipRoute
	for key, site := range overrides {
		k := strings.TrimSpace(key)
		s := strings.TrimSpace(site)
		if k == "" || s == "" {
			logger.Warnf("site: ignoring override with an empty key or site (%q -> %q)", key, site)
			continue
		}

		if ipnet := parseOverrideNetwork(k); ipnet != nil {
			routes = append(routes, ipRoute{net: ipnet, site: s})
			continue
		}

		h := strings.ToLower(strings.TrimSuffix(k, "."))
		if !strings.Contains(h, ".") {
			logger.Warnf("site: ignoring override key %q: not an address, a CIDR, or a dotted host name", key)
			continue
		}
		// One site per key by construction, so a pin can never itself be
		// ambiguous — that is the whole point of declaring it.
		hosts[h] = []string{s}
	}

	if len(hosts) == 0 && len(routes) == 0 {
		return nil
	}

	// Most specific first, so the walk can stop at the first containing block.
	sort.Slice(routes, func(i, j int) bool {
		a, _ := routes[i].net.Mask.Size()
		b, _ := routes[j].net.Mask.Size()
		return a > b
	})

	keys := make([]string, 0, len(hosts)+len(routes))
	for k := range hosts {
		keys = append(keys, k)
	}
	for _, r := range routes {
		keys = append(keys, r.net.String())
	}
	sort.Strings(keys)
	logger.Infof("site: %d resolution override(s) configured: %s", len(keys), strings.Join(keys, ", "))

	o := &SiteOverrides{routes: routes}
	if len(hosts) > 0 {
		o.hosts = &SiteRegistry{logger: logger, domains: hosts}
	}
	return o
}

// parseOverrideNetwork returns the network an address or CIDR key denotes, or
// nil when the key is not one. A bare address becomes a single-address block so
// both forms share one longest-prefix walk.
func parseOverrideNetwork(key string) *net.IPNet {
	if _, ipnet, err := net.ParseCIDR(key); err == nil && ipnet != nil {
		return ipnet
	}
	ip := net.ParseIP(strings.Trim(key, "[]"))
	if ip == nil {
		return nil
	}
	bits := 8 * net.IPv6len
	if v4 := ip.To4(); v4 != nil {
		ip, bits = v4, 8*net.IPv4len
	}
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}
}

// Resolve returns the site an operator pinned for the endpoint, if any. The host
// name is tried first, then the address, so an explicitly named host beats a
// range that merely contains it.
func (o *SiteOverrides) Resolve(ep siteEndpoint) (string, bool) {
	if o == nil {
		return "", false
	}
	if o.hosts != nil {
		if site, status := o.hosts.ResolveHost(ep.host); status == SiteStatusResolved {
			return site, true
		}
	}
	if ep.ip != nil {
		// routes are sorted most-specific first, so the first hit is the longest
		// matching prefix.
		for _, r := range o.routes {
			if r.net.Contains(ep.ip) {
				return r.site, true
			}
		}
	}
	return "", false
}

// ambiguityReporter names the endpoints that resolved ambiguously, once each, so
// an operator can pin them. The ambiguous count in shoveler_site_unresolved says
// how often it happens but never which endpoint, and that cannot go in a metric
// label without unbounded cardinality — so it goes in the log instead, with the
// candidates and the config line that settles it.
type ambiguityReporter struct {
	logger *logrus.Logger

	mu       sync.Mutex
	reported map[string]struct{}
	capped   bool
}

func newAmbiguityReporter(logger *logrus.Logger) *ambiguityReporter {
	if logger == nil {
		logger = logrus.New()
	}
	return &ambiguityReporter{logger: logger, reported: map[string]struct{}{}}
}

// report logs an ambiguous endpoint once, naming every site it matched and the
// guess that was used. An endpoint with no usable host name is keyed by its
// address, which site.overrides accepts as a key too, so both forms get an
// actionable line. candidates may be empty when the match came from a source that
// no longer lists the endpoint, in which case only the guess is reported.
func (r *ambiguityReporter) report(ep siteEndpoint, guess string, candidates []string) {
	key := normalizeHost(ep.host)
	if key == "" {
		if ep.ip == nil {
			return
		}
		key = ep.ip.String()
	}

	r.mu.Lock()
	if _, seen := r.reported[key]; seen {
		r.mu.Unlock()
		return
	}
	if len(r.reported) >= maxReportedAmbiguousHosts {
		first := !r.capped
		r.capped = true
		r.mu.Unlock()
		if first {
			r.logger.Warnf("site: more than %d distinct ambiguous endpoints seen; "+
				"no longer naming new ones (shoveler_site_unresolved still counts them)",
				maxReportedAmbiguousHosts)
		}
		return
	}
	r.reported[key] = struct{}{}
	r.mu.Unlock()

	between := ""
	if len(candidates) > 1 {
		between = " between " + strings.Join(candidates, ", ")
	}
	r.logger.Warnf("site: %q is ambiguous%s; using %q as a guess. "+
		"Pin it with site.overrides: {%q: %q}", key, between, guess, key, guess)
}

// ambiguousCandidates returns every RCSite the configured sources associate with
// the endpoint, for reporting only. Sources are asked in resolution order, so the
// candidates named are the ones from the method whose guess actually survived.
func (s *siteRecordEnricher) ambiguousCandidates(ep siteEndpoint) []string {
	if s.hosts != nil {
		if sites := s.hosts.Candidates(ep.host); len(sites) > 1 {
			return sites
		}
	}
	if s.domains != nil {
		if sites := s.domains.Candidates(ep.host); len(sites) > 1 {
			return sites
		}
	}
	if s.ips != nil {
		if sites := s.ips.Candidates(ep.ip); len(sites) > 1 {
			return sites
		}
	}
	return nil
}
