package collector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Default WLCG matching values — preserved from the original hardcoded logic.
var (
	defaultWLCGVOs          = []string{"cms"}
	defaultWLCGPathPrefixes = []string{"/store", "/user/dteam"}
)

// recordsDropped counts records silently dropped by the filter before publishing.
// The "reason" label is either "vo" or "path_prefix".
var recordsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "shoveler_records_dropped_total",
	Help: "Total number of records dropped by the filter before publishing, labeled by reason (vo or path_prefix)",
}, []string{"reason"})

// matchesWLCG returns true when the record should be routed to the WLCG exchange.
// It checks c.wlcgVOs (case-insensitive exact match) and c.wlcgPathPrefixes (HasPrefix).
func (c *Correlator) matchesWLCG(record *CollectorRecord) bool {
	recordVO := strings.ToLower(strings.TrimSpace(record.VO))
	if recordVO != "" {
		for _, vo := range c.wlcgVOs {
			if strings.ToLower(strings.TrimSpace(vo)) == recordVO {
				return true
			}
		}
	}

	filename := strings.TrimSpace(record.Filename)
	for _, prefix := range c.wlcgPathPrefixes {
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
func (c *Correlator) shouldDrop(record *CollectorRecord) (bool, string) {
	recordVO := strings.ToLower(strings.TrimSpace(record.VO))
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
