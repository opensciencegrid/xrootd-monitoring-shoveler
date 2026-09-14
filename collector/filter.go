package collector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Default WLCG matching values, as used by the upstream OSG collector.
// These are the rule when wlcg.enabled is off.
var (
	defaultWLCGVOs          = []string{"cms"}
	defaultWLCGPathPrefixes = []string{"/store", "/user/dteam"}
)

// recordsDropped counts records silently dropped by the filter before publishing.
// The "reason" label is either "vo" or "path_prefix".
var recordsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "shoveler_records_dropped",
	Help: "The total number of records dropped by the filter before publishing, labeled by reason (vo or path_prefix)",
}, []string{"reason"})

// wlcgRouting decides which records are converted to WLCG format.
//
// When Enabled is false the other fields are ignored and the rule is the
// upstream one: VO "cms", or a path under /store or /user/dteam.
//
// When it is true, records are converted unless VOs/PathPrefixes narrow that,
// and the Exclude lists then take records back out.
type wlcgRouting struct {
	Enabled bool

	// Which records to convert. Leaving both empty converts everything, which is
	// the default. Set either one to narrow it. Used only when Enabled is true.
	VOs          []string // case-insensitive exact match
	PathPrefixes []string // HasPrefix match

	// Which of those to leave out again. Used only when Enabled is true.
	ExcludeVOs          []string // case-insensitive exact match
	ExcludePathPrefixes []string // HasPrefix match
}

// eligible reports whether one record should be converted to WLCG format.
func (r wlcgRouting) eligible(vo, path string) bool {
	if !r.Enabled {
		return matchesVOOrPath(vo, path, defaultWLCGVOs, defaultWLCGPathPrefixes)
	}

	// With no lists set, everything is converted. That is what a WLCG site wants,
	// and it is the only rule that covers records with no VO. Most are in that
	// position: a VO only shows up when the auth/token stream sent one.
	if len(r.VOs) > 0 || len(r.PathPrefixes) > 0 {
		if !matchesVOOrPath(vo, path, r.VOs, r.PathPrefixes) {
			return false
		}
	}

	return !matchesVOOrPath(vo, path, r.ExcludeVOs, r.ExcludePathPrefixes)
}

// matchesWLCG reports whether the record should be converted to WLCG format and
// sent to the WLCG exchange.
func (c *Correlator) matchesWLCG(record *CollectorRecord) bool {
	return c.wlcgRouting.eligible(record.routingVO(), record.Filename)
}

// matchesVOOrPath reports whether the VO is in vos (case-insensitive) or the
// path starts with one of pathPrefixes. Empty lists match nothing. A record with
// no VO can only be matched by its path.
func matchesVOOrPath(vo, path string, vos, pathPrefixes []string) bool {
	recordVO := strings.ToLower(strings.TrimSpace(vo))
	if recordVO != "" {
		for _, candidate := range vos {
			if strings.ToLower(strings.TrimSpace(candidate)) == recordVO {
				return true
			}
		}
	}

	filename := strings.TrimSpace(path)
	for _, prefix := range pathPrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" && strings.HasPrefix(filename, prefix) {
			return true
		}
	}

	return false
}

// shouldDrop returns (true, reason) when the record matches the drop filter and
// must be discarded before any publish. Reason is "vo" or "path_prefix".
// An empty filter (nil or empty slices) never drops anything.
//
// It uses the resolved VO, the same one the WLCG rules use, so every decision
// about a record is made on the same value. When wlcg.enabled is off nothing is
// resolved and this is the VO from the packet, as upstream does.
func (c *Correlator) shouldDrop(record *CollectorRecord) (bool, string) {
	recordVO := strings.ToLower(strings.TrimSpace(record.routingVO()))
	if recordVO != "" {
		for _, vo := range c.dropVOs {
			if vo != "" && strings.ToLower(strings.TrimSpace(vo)) == recordVO {
				return true, "vo"
			}
		}
	}

	filename := strings.TrimSpace(record.Filename)
	for _, prefix := range c.dropPathPrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" && strings.HasPrefix(filename, prefix) {
			return true, "path_prefix"
		}
	}

	return false, ""
}
