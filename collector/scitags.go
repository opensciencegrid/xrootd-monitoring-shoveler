package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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

// Load parses api.json bytes and atomically swaps in the new mappings. A parse
// error or an empty experiments list leaves the current registry untouched so a
// transient bad fetch never wipes a good in-memory registry.
func (r *ScitagsRegistry) Load(data []byte) error {
	var doc scitagsDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse scitags registry: %w", err)
	}
	if len(doc.Experiments) == 0 {
		return fmt.Errorf("scitags registry has no experiments")
	}

	experiments := make(map[int]string, len(doc.Experiments))
	activities := make(map[int64]string)
	for _, e := range doc.Experiments {
		if e.ExpID == 0 {
			continue
		}
		experiments[e.ExpID] = e.ExpName
		for _, a := range e.Activities {
			activities[activityKey(e.ExpID, a.ActivityID)] = a.ActivityName
		}
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
	data, err := fetchScitagsSource(ctx, src)
	if err != nil {
		return err
	}
	return r.Load(data)
}

// fetchScitagsSource reads a registry document from a file path or http(s) URL.
func fetchScitagsSource(ctx context.Context, src string) ([]byte, error) {
	if !isURL(src) {
		data, err := os.ReadFile(src)
		if err != nil {
			return nil, fmt.Errorf("read scitags file %q: %w", src, err)
		}
		return data, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return nil, fmt.Errorf("build scitags request: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch scitags url %q: %w", src, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch scitags url %q: unexpected status %s", src, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // cap at 8 MiB
	if err != nil {
		return nil, fmt.Errorf("read scitags url body %q: %w", src, err)
	}
	return data, nil
}

// StartRefresh periodically re-fetches a URL source and reloads the registry
// until ctx is cancelled. It is a no-op for file sources or when interval <= 0.
// A failed refresh is logged and the previous registry is retained.
func (r *ScitagsRegistry) StartRefresh(ctx context.Context, src string, interval time.Duration) {
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
					scitagsRegistryReloadFailures.Inc()
					r.logger.Warnf("scitags: registry refresh failed, keeping previous data: %v", err)
				}
			}
		}
	}()
}
