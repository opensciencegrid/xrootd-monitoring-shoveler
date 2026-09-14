package collector

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// The three places a VO can come from, tried in the order set by wlcg.vo_order.
// The first one with a value wins, so the order says which we trust most.
const (
	VOSourceRecord  = "record"  // the auth/token stream, published as record_vo
	VOSourceScitags = "scitags" // the SciTags experiment name, published as scitags_vo
	VOSourceConfig  = "config"  // the VO this collector serves, from wlcg.vo
)

// DefaultVOOrder is used when wlcg.vo_order is not set: what the record said,
// then its SciTags marking, then the configured VO. Config goes last because it
// is the same for every record, so anything the record carries is more specific.
var DefaultVOOrder = []string{VOSourceRecord, VOSourceScitags, VOSourceConfig}

// NormalizeVOOrder tidies up a configured order: it lowercases the names, drops
// unknown ones and repeats (logging both), and falls back to DefaultVOOrder if
// nothing usable is left, so a typo does not leave records without a VO.
//
// Leaving a source out on purpose works: an order without "config" never uses
// the configured VO, and one without "scitags" keeps SciTags out of "vo" while
// still publishing scitags_vo.
func NormalizeVOOrder(order []string, logger *logrus.Logger) []string {
	if logger == nil {
		logger = logrus.New()
	}

	known := map[string]bool{
		VOSourceRecord:  true,
		VOSourceScitags: true,
		VOSourceConfig:  true,
	}

	normalized := make([]string, 0, len(order))
	seen := make(map[string]bool, len(order))
	for _, source := range order {
		src := strings.ToLower(strings.TrimSpace(source))
		if src == "" {
			continue
		}
		if !known[src] {
			logger.Warnf("wlcg: ignoring unknown vo source %q (known: %s)",
				source, strings.Join(DefaultVOOrder, ", "))
			continue
		}
		if seen[src] {
			logger.Warnf("wlcg: ignoring repeated vo source %q", src)
			continue
		}
		seen[src] = true
		normalized = append(normalized, src)
	}

	if len(normalized) == 0 {
		return append([]string(nil), DefaultVOOrder...)
	}
	return normalized
}

// resolveScitagsNames turns the 'U'-stream experiment/activity ids into the
// names from the SciTags registry, and returns the experiment name as the VO.
// A nil registry or a record with no experiment id resolves nothing. Ids the
// registry does not know are counted in scitagsUnmappedIDsTotal.
func resolveScitagsNames(record *CollectorRecord, scitags *ScitagsRegistry) (experiment, activity, scitagsVO string) {
	if scitags == nil || record.ExperimentID == 0 {
		return "", "", ""
	}

	experiment = scitags.ExperimentName(record.ExperimentID)
	if experiment == "" {
		// The experiment id is unknown to the registry. Activity ids are namespaced
		// per experiment, so the activity lookup cannot succeed either; skip it
		// rather than counting the same unknown experiment a second time under
		// kind="activity".
		scitagsUnmappedIDsTotal.WithLabelValues("experiment").Inc()
		return "", "", ""
	}

	// The experiment name is the VO this flow belongs to. It gets its own field
	// and feeds into the resolved VO through wlcg.vo_order.
	scitagsVO = experiment

	if record.ActivityID != 0 {
		activity = scitags.ActivityName(record.ExperimentID, record.ActivityID)
		if activity == "" {
			scitagsUnmappedIDsTotal.WithLabelValues("activity").Inc()
		}
	}

	return experiment, activity, scitagsVO
}

// resolveVOWithSource returns the first VO it finds, trying the sources in
// order, plus the name of the source it came from. Both are empty if no source
// had one, which is normal: most records have no VO and no SciTags marking, and
// wlcg.vo is unset by default.
func resolveVOWithSource(order []string, recordVO, scitagsVO, configVO string) (vo, source string) {
	if len(order) == 0 {
		order = DefaultVOOrder
	}

	for _, src := range order {
		switch src {
		case VOSourceRecord:
			if recordVO != "" {
				return recordVO, VOSourceRecord
			}
		case VOSourceScitags:
			if scitagsVO != "" {
				return scitagsVO, VOSourceScitags
			}
		case VOSourceConfig:
			if configVO != "" {
				return configVO, VOSourceConfig
			}
		}
	}

	return "", ""
}

// resolveRecordVO works out the record's VO and stores it on the record, along
// with the SciTags names it may have come from. It runs before every rule that
// looks at a VO (the drop filter, wlcg.vos, wlcg.exclude_vos) so they all see
// the same value. Most records have no VO from the auth stream, so without this
// those rules would have almost nothing to match.
//
// It does nothing when wlcg.enabled is off. routingVO then falls back to the
// packet's VO, and ConvertToWLCG looks up the SciTags names itself, as upstream
// does.
func (c *Correlator) resolveRecordVO(record *CollectorRecord) {
	if record == nil || !c.wlcgMetadata.VOResolution.Enabled {
		return
	}

	record.experiment, record.activity, record.scitagsVO = resolveScitagsNames(record, c.scitags)
	record.resolvedVO, record.voSource = resolveVOWithSource(
		c.wlcgMetadata.VOResolution.Order,
		record.VO,
		record.scitagsVO,
		c.wlcgMetadata.VOResolution.VO,
	)
	record.voResolved = true
}
