package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/sirupsen/logrus"
)

// embeddedScitagsAPI is a snapshot of the SciTags registry
// (https://scitags.docs.cern.ch/api.json) compiled into the binary. It is the
// fallback source so activity/experiment resolution works offline and even when
// no source is configured; a configured file/URL source overrides it at runtime.
//
//go:embed scitags_api.json
var embeddedScitagsAPI []byte

// scitagsDoc mirrors the SciTags api.json schema: a top-level "experiments"
// array of {expId, expName, activities:[{activityId, activityName}]}. Activity
// ids are namespaced per experiment (activityId 3 is "Cache" for one experiment
// and something else for another), so an activity is only meaningful together
// with its experiment id.
type scitagsDoc struct {
	Experiments []struct {
		ExpName    string `json:"expName"`
		ExpID      int    `json:"expId"`
		Activities []struct {
			ActivityName string `json:"activityName"`
			ActivityID   int    `json:"activityId"`
		} `json:"activities"`
	} `json:"experiments"`
	Version  int    `json:"version"`
	Modified string `json:"modified"`
}

// scitagsDefaultExperiment is the registry's name for flows tagged without a
// specific experiment (expId 1). It is a real registry entry, but not a VO.
const scitagsDefaultExperiment = "default"

// activityKey packs an experiment id and activity id into a single map key.
// Activity ids are only unique within an experiment, so both are needed. This
// mirrors the ((expId<<32)|actId) key used by the xrootd collector (PR #2855).
func activityKey(expID, actID int) int64 {
	return int64(expID)<<32 | int64(actID)
}

// ScitagsRegistry resolves numeric SciTags experiment/activity ids (carried on
// the 'U'/MAPUEAC monitoring stream as &Ec=/&Ac=) to their human names. It is
// safe for concurrent use: readers take a read lock while a background refresh
// swaps in a freshly fetched registry under the write lock.
type ScitagsRegistry struct {
	logger *logrus.Logger
	client *http.Client // fetches URL sources; nil trusts only the OS CA certificates

	mu          sync.RWMutex
	experiments map[int]string   // expId -> expName
	activities  map[int64]string // activityKey(expId,actId) -> activityName
	version     int
	modified    string
}

// NewScitagsRegistry returns a registry seeded from the embedded api.json
// snapshot. It never fails: a bad embedded snapshot only yields an empty
// registry (ids pass through unresolved), which is logged.
func NewScitagsRegistry(logger *logrus.Logger) *ScitagsRegistry {
	if logger == nil {
		logger = logrus.New()
	}
	r := &ScitagsRegistry{
		logger:      logger,
		experiments: map[int]string{},
		activities:  map[int64]string{},
	}
	if err := r.Load(embeddedScitagsAPI); err != nil {
		logger.Warnf("scitags: failed to load embedded registry snapshot: %v", err)
	}
	return r
}

// Load parses api.json bytes and swaps in the new mappings. If the data is bad,
// it returns an error and keeps the current registry.
//
// Entries need an id above 0 and a name; others are skipped (0 means "unset" on
// the 'U' stream).
func (r *ScitagsRegistry) Load(data []byte) error {
	var doc scitagsDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse scitags registry: %w", err)
	}

	experiments := make(map[int]string, len(doc.Experiments))
	activities := make(map[int64]string)
	for _, e := range doc.Experiments {
		name := strings.TrimSpace(e.ExpName)
		if e.ExpID <= 0 || name == "" {
			continue
		}
		experiments[e.ExpID] = name
		for _, a := range e.Activities {
			actName := strings.TrimSpace(a.ActivityName)
			if a.ActivityID <= 0 || actName == "" {
				continue
			}
			activities[activityKey(e.ExpID, a.ActivityID)] = actName
		}
	}

	// Check what is left after skipping bad entries, so a file of only bad
	// entries keeps the current registry.
	if len(experiments) == 0 {
		return fmt.Errorf("scitags registry has no usable experiments")
	}

	r.mu.Lock()
	r.experiments = experiments
	r.activities = activities
	r.version = doc.Version
	r.modified = doc.Modified
	r.mu.Unlock()

	scitagsRegistryExperiments.Set(float64(len(experiments)))
	scitagsRegistryActivities.Set(float64(len(activities)))
	r.logger.Infof("scitags: loaded registry v%d (modified %s): %d experiments, %d activities",
		doc.Version, doc.Modified, len(experiments), len(activities))
	return nil
}

// ExperimentName returns the experiment name for an experiment id, or "" if the
// id is 0 or not present in the current registry.
func (r *ScitagsRegistry) ExperimentName(expID int) string {
	if expID == 0 {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.experiments[expID]
}

// ActivityName returns the activity name for an (experiment, activity) id pair.
// Because activity ids are namespaced per experiment, both ids are required;
// it returns "" when either id is 0 or the pair is not in the registry.
func (r *ScitagsRegistry) ActivityName(expID, actID int) string {
	if expID == 0 || actID == 0 {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.activities[activityKey(expID, actID)]
}

// LoadSource fetches and loads a registry from a source that is either a local
// file path or an http(s):// URL.
func (r *ScitagsRegistry) LoadSource(ctx context.Context, src string) error {
	data, err := fetchSource(ctx, r.client, src, "SciTags")
	if err != nil {
		return err
	}
	return r.Load(data)
}

// StartRefresh keeps the registry up to date from a URL, in the background. It
// fetches right away, then again every interval (only once if interval is 0).
// If a fetch fails, the current registry is kept and the error is logged. It
// does nothing for a file; use LoadSource for that.
func (r *ScitagsRegistry) StartRefresh(ctx context.Context, src string, interval time.Duration) {
	if !isURL(src) {
		return
	}
	go refreshLoop(ctx, interval,
		func(ctx context.Context) error { return r.LoadSource(ctx, src) },
		func(err error) {
			scitagsRegistryReloadFailures.Inc()
			r.logger.Warnf("scitags: registry fetch failed, keeping current data: %v", err)
		})
}

// SetHTTPClient sets the client used to fetch URL sources. Call it before
// LoadSource and StartRefresh. A nil client trusts only the OS CA certificates.
func (r *ScitagsRegistry) SetHTTPClient(client *http.Client) {
	r.client = client
}
