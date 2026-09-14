package collector

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// newTestCorrelator creates a minimal correlator suitable for filter tests.
func newTestCorrelator(cfg CorrelatorConfig) *Correlator {
	return NewCorrelatorWithConfig(cfg)
}

// ---- WLCG match tests -------------------------------------------------------

func TestMatchesWLCG_DefaultConfig(t *testing.T) {
	// An empty WLCG config falls back to the upstream defaults and behaves the
	// same as IsWLCGPacket.
	c := newTestCorrelator(CorrelatorConfig{
		TTL:    time.Minute,
		Logger: nil,
		// WLCGVOs and WLCGPathPrefixes intentionally left unset → defaults apply
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{"default VO cms", &CollectorRecord{VO: "cms", Filename: "/other/path"}, true},
		{"default VO cms case-insensitive upper", &CollectorRecord{VO: "CMS", Filename: "/other/path"}, true},
		{"default path /store", &CollectorRecord{VO: "atlas", Filename: "/store/data/file.root"}, true},
		{"default path /user/dteam", &CollectorRecord{VO: "other", Filename: "/user/dteam/test.dat"}, true},
		{"no match", &CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"}, false},
		{"empty VO and filename", &CollectorRecord{VO: "", Filename: ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.matchesWLCG(tt.record); got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMatchesWLCG_ConfigSlicesAreCopied(t *testing.T) {
	vos := []string{"atlas"}
	prefixes := []string{"/eos/atlas"}

	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		WLCGEnabled:      true,
		WLCGVOs:          vos,
		WLCGPathPrefixes: prefixes,
	})
	defer c.Stop()

	// Changing the caller's slices should not reach the correlator's rules.
	vos[0] = "cms"
	prefixes[0] = "/store"

	if !c.matchesWLCG(&CollectorRecord{VO: "atlas", Filename: "/other"}) {
		t.Error("the correlator must keep the values it was configured with")
	}
	if c.matchesWLCG(&CollectorRecord{VO: "cms", Filename: "/other"}) {
		t.Error("mutating the caller's slice must not change the correlator")
	}

	// The package defaults should be untouched.
	if defaultWLCGVOs[0] != "cms" {
		t.Errorf("defaultWLCGVOs was mutated: got %q, want %q", defaultWLCGVOs[0], "cms")
	}
	if defaultWLCGPathPrefixes[0] != "/store" {
		t.Errorf("defaultWLCGPathPrefixes was mutated: got %q, want %q", defaultWLCGPathPrefixes[0], "/store")
	}
}

// TestMatchesWLCG_NoInclusionRuleIncludesEverything checks the default once
// wlcg.enabled is on: with neither list set, every record converts, including
// the ones no VO or path list could have picked up.
func TestMatchesWLCG_NoInclusionRuleIncludesEverything(t *testing.T) {
	// Unset and explicitly empty should behave the same.
	for _, cfg := range []CorrelatorConfig{
		{TTL: time.Minute, WLCGEnabled: true},
		{TTL: time.Minute, WLCGEnabled: true, WLCGVOs: []string{}, WLCGPathPrefixes: []string{}},
	} {
		runNoInclusionRule(t, cfg)
	}
}

func runNoInclusionRule(t *testing.T, cfg CorrelatorConfig) {
	t.Helper()

	c := newTestCorrelator(cfg)
	defer c.Stop()

	// No inclusion rule at all: everything converts, including the records no VO
	// or path list could ever have selected.
	for _, record := range []*CollectorRecord{
		{VO: "cms", Filename: "/store/data/f.root"},
		{VO: "alice", Filename: "/alice/sim/g.root"},
		{VO: "", Filename: "/anything/at/all"},
		{VO: "", Filename: "relative/path"},
		{VO: "", Filename: ""},
	} {
		if !c.matchesWLCG(record) {
			t.Errorf("matchesWLCG(%+v) = false, want true with no inclusion rule", record)
		}
	}
}

// TestMatchesWLCG_OneListEmptyStillFilters checks that clearing one list only
// removes that half of the rule.
func TestMatchesWLCG_OneListEmptyStillFilters(t *testing.T) {
	t.Run("vos empty, paths still filter", func(t *testing.T) {
		c := newTestCorrelator(CorrelatorConfig{
			TTL:              time.Minute,
			WLCGEnabled:      true,
			WLCGVOs:          []string{},
			WLCGPathPrefixes: []string{"/store"},
		})
		defer c.Stop()

		if !c.matchesWLCG(&CollectorRecord{VO: "", Filename: "/store/f"}) {
			t.Error("a matching path should still convert")
		}
		if c.matchesWLCG(&CollectorRecord{VO: "cms", Filename: "/other/f"}) {
			t.Error("a non-matching path should not convert, even for a known VO")
		}
	})

	t.Run("paths empty, vos still filter", func(t *testing.T) {
		c := newTestCorrelator(CorrelatorConfig{
			TTL:              time.Minute,
			WLCGEnabled:      true,
			WLCGVOs:          []string{"alice"},
			WLCGPathPrefixes: []string{},
		})
		defer c.Stop()

		if !c.matchesWLCG(&CollectorRecord{VO: "alice", Filename: "/other/f"}) {
			t.Error("a matching VO should still convert")
		}
		if c.matchesWLCG(&CollectorRecord{VO: "", Filename: "/store/f"}) {
			t.Error("a non-matching VO should not convert, even on a /store path")
		}
	})
}

// TestMatchesWLCG_NoInclusionRuleStillExcludes checks the layering: with no
// list set, the exclusions are all that is left. That pairing is what a site
// serving several VOs wants.
func TestMatchesWLCG_NoInclusionRuleStillExcludes(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:                     time.Minute,
		WLCGEnabled:             true,
		WLCGExcludeVOs:          []string{"dune"},
		WLCGExcludePathPrefixes: []string{"/pnfs/dune"},
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{"included", &CollectorRecord{VO: "alice", Filename: "/alice/f"}, true},
		{"excluded by VO", &CollectorRecord{VO: "dune", Filename: "/alice/f"}, false},
		{"excluded by path", &CollectorRecord{VO: "alice", Filename: "/pnfs/dune/f"}, false},
		{"empty VO still included", &CollectorRecord{VO: "", Filename: "/x/f"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.matchesWLCG(tt.record); got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMatchesWLCG_CustomConfig(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		WLCGEnabled:      true,
		WLCGVOs:          []string{"atlas", "lhcb"},
		WLCGPathPrefixes: []string{"/eos/atlas "},
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{"VO atlas hit", &CollectorRecord{VO: "atlas", Filename: "/other"}, true},
		{"VO atlas case-insensitive", &CollectorRecord{VO: "ATLAS", Filename: "/other"}, true},
		{"VO lhcb hit", &CollectorRecord{VO: "lhcb", Filename: "/other"}, true},
		{"path /eos/atlas hit", &CollectorRecord{VO: "cms", Filename: "/eos/atlas/file"}, true},
		{"VO cms no longer matches (not in custom list)", &CollectorRecord{VO: "cms", Filename: "/other/path"}, false},
		{"path /store no longer matches (not in custom list)", &CollectorRecord{VO: "osg", Filename: "/store/data/file.root"}, false},
		{"no match", &CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.matchesWLCG(tt.record); got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMatchesWLCG_DisabledIgnoresEverything guards the OSG deployments that
// share this collector. wlcg.enabled is off by default, and with it off the
// other fields do nothing even when set: routing is the upstream rule.
func TestMatchesWLCG_DisabledIgnoresEverything(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL: time.Minute,
		// Every field set, but the switch left off.
		WLCGVOs:                 []string{"atlas"},
		WLCGPathPrefixes:        []string{"/eos/atlas"},
		WLCGExcludeVOs:          []string{"cms"},
		WLCGExcludePathPrefixes: []string{"/store"},
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		// A record outside the CMS rule stays out.
		{"outside the CMS rule", &CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"}, false},
		// The exclusions are ignored, so records the upstream rule picks up still go.
		{"vo exclusion is ignored", &CollectorRecord{VO: "cms", Filename: "/other/path"}, true},
		{"path exclusion is ignored", &CollectorRecord{VO: "osg", Filename: "/store/data/f.root"}, true},
		// The configured lists are ignored too.
		{"configured vos are ignored", &CollectorRecord{VO: "atlas", Filename: "/other/path"}, false},
		{"configured path prefixes are ignored", &CollectorRecord{VO: "osg", Filename: "/eos/atlas/f.root"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.matchesWLCG(tt.record); got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMatchesWLCG_ExclusionsRemoveIncluded covers the second stage: exclusions
// only take records out, never add them.
func TestMatchesWLCG_ExclusionsRemoveIncluded(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:                     time.Minute,
		WLCGEnabled:             true,
		WLCGExcludeVOs:          []string{"dune", " belle2 "},
		WLCGExcludePathPrefixes: []string{"/pnfs/dune", "/skao "},
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{"included and not excluded", &CollectorRecord{VO: "cms", Filename: "/store/f"}, true},
		{"excluded by VO", &CollectorRecord{VO: "dune", Filename: "/store/f"}, false},
		{"excluded by VO, case-insensitive", &CollectorRecord{VO: "DUNE", Filename: "/store/f"}, false},
		{"excluded by VO, surrounding space in config", &CollectorRecord{VO: "belle2", Filename: "/store/f"}, false},
		{"excluded by path", &CollectorRecord{VO: "cms", Filename: "/pnfs/dune/f"}, false},
		{"excluded by path, surrounding space in config", &CollectorRecord{VO: "cms", Filename: "/skao/f"}, false},
		{"VO exclusion is exact, not a prefix", &CollectorRecord{VO: "dunes", Filename: "/store/f"}, true},
		{"path exclusion must match at the start", &CollectorRecord{VO: "cms", Filename: "/x/pnfs/dune/f"}, true},
		{"empty VO is never excluded by VO", &CollectorRecord{VO: "", Filename: "/store/f"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.matchesWLCG(tt.record); got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMatchesWLCG_ExclusionsNeverAdd checks the ordering: a record the lists
// never picked up stays out, whatever the exclusions say.
func TestMatchesWLCG_ExclusionsNeverAdd(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:                     time.Minute,
		WLCGEnabled:             true,
		WLCGVOs:                 []string{"cms"},
		WLCGPathPrefixes:        []string{"/store"},
		WLCGExcludeVOs:          []string{"dune"},
		WLCGExcludePathPrefixes: []string{"/pnfs/dune"},
	})
	defer c.Stop()

	if c.matchesWLCG(&CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"}) {
		t.Error("a record outside the inclusion lists must stay out")
	}
}

// TestMatchesWLCG_ExclusionsDefaultToNothing pins that adding the feature changes
// no existing deployment: with no exclusions configured, the upstream defaults
// decide alone.
func TestMatchesWLCG_ExclusionsDefaultToNothing(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{TTL: time.Minute})
	defer c.Stop()

	for _, record := range []*CollectorRecord{
		{VO: "cms", Filename: "/other/path"},
		{VO: "dune", Filename: "/store/data/f.root"},
	} {
		if !c.matchesWLCG(record) {
			t.Errorf("matchesWLCG(%+v) = false, want true", record)
		}
	}
}

// ---- Drop filter tests ------------------------------------------------------

func TestShouldDrop_EmptyConfig(t *testing.T) {
	// An unconfigured filter must never drop anything.
	c := newTestCorrelator(CorrelatorConfig{
		TTL: time.Minute,
		// DropVOs and DropPathPrefixes intentionally left nil
	})
	defer c.Stop()

	records := []*CollectorRecord{
		{VO: "cms", Filename: "/store/data/file.root"},
		{VO: "atlas", Filename: "/eos/atlas/data"},
		{VO: "", Filename: ""},
	}

	for _, r := range records {
		drop, _ := c.shouldDrop(r)
		if drop {
			t.Errorf("shouldDrop() returned true for %+v with empty config", r)
		}
	}
}

func TestShouldDrop_ByVO(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:     time.Minute,
		DropVOs: []string{"atlas"},
	})
	defer c.Stop()

	tests := []struct {
		name       string
		record     *CollectorRecord
		wantDrop   bool
		wantReason string
	}{
		{
			name:       "drop atlas",
			record:     &CollectorRecord{VO: "atlas", Filename: "/eos/atlas/data"},
			wantDrop:   true,
			wantReason: "vo",
		},
		{
			name:       "drop atlas case-insensitive",
			record:     &CollectorRecord{VO: "ATLAS", Filename: "/eos/atlas/data"},
			wantDrop:   true,
			wantReason: "vo",
		},
		{
			name:     "cms not dropped",
			record:   &CollectorRecord{VO: "cms", Filename: "/store/data/file.root"},
			wantDrop: false,
		},
		{
			name:     "empty VO not dropped",
			record:   &CollectorRecord{VO: "", Filename: "/store/data/file.root"},
			wantDrop: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drop, reason := c.shouldDrop(tt.record)
			if drop != tt.wantDrop {
				t.Errorf("shouldDrop() drop = %v, want %v", drop, tt.wantDrop)
			}
			if tt.wantDrop && reason != tt.wantReason {
				t.Errorf("shouldDrop() reason = %q, want %q", reason, tt.wantReason)
			}
		})
	}
}

func TestShouldDrop_ByPathPrefix(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		DropPathPrefixes: []string{"/eos "},
	})
	defer c.Stop()

	tests := []struct {
		name       string
		record     *CollectorRecord
		wantDrop   bool
		wantReason string
	}{
		{
			name:       "drop /eos path",
			record:     &CollectorRecord{VO: "atlas", Filename: "/eos/atlas/data"},
			wantDrop:   true,
			wantReason: "path_prefix",
		},
		{
			name:       "drop /eos/cms path",
			record:     &CollectorRecord{VO: "cms", Filename: "/eos/cms/run3/file.root"},
			wantDrop:   true,
			wantReason: "path_prefix",
		},
		{
			name:     "/store not dropped",
			record:   &CollectorRecord{VO: "cms", Filename: "/store/data/file.root"},
			wantDrop: false,
		},
		{
			name:     "empty filename not dropped",
			record:   &CollectorRecord{VO: "atlas", Filename: ""},
			wantDrop: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drop, reason := c.shouldDrop(tt.record)
			if drop != tt.wantDrop {
				t.Errorf("shouldDrop() drop = %v, want %v", drop, tt.wantDrop)
			}
			if tt.wantDrop && reason != tt.wantReason {
				t.Errorf("shouldDrop() reason = %q, want %q", reason, tt.wantReason)
			}
		})
	}
}

// ---- Integration: drop filter blocks publish --------------------------------

// TestDropFilterBlocksPublish verifies that a record matching the drop filter
// is sent to neither the main exchange nor the WLCG exchange.
func TestDropFilterBlocksPublish(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		DropPathPrefixes: []string{"/eos"},
		// No WLCG exclusions, so the record *would* be converted if not dropped.
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	// This record matches the drop filter (/eos prefix) and would also match
	// the WLCG VO (cms). Drop must win.
	record := &CollectorRecord{
		VO:       "cms",
		Filename: "/eos/cms/run3/file.root",
	}

	req := enrichmentRequest{record: record, destination: dest}
	c.processEnrichmentRequest(req)

	// Nothing should be sent to the result channel.
	select {
	case got := <-resultCh:
		t.Errorf("expected no publish but got record on exchange %q", got.Exchange)
	default:
		// correct: nothing published
	}
}

// TestDropPrecedenceOverWLCG verifies that a record matching both the drop
// filter (by VO) and the WLCG filter (by path) is dropped, not published.
func TestDropPrecedenceOverWLCG(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:     time.Minute,
		DropVOs: []string{"cms"},
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	record := &CollectorRecord{
		VO:       "cms",      // matches drop filter
		Filename: "/store/x", // would match WLCG path prefix
	}

	req := enrichmentRequest{record: record, destination: dest}
	c.processEnrichmentRequest(req)

	select {
	case got := <-resultCh:
		t.Errorf("expected drop but record was published to exchange %q", got.Exchange)
	default:
		// correct
	}
}

// TestNonDroppedRecordIsPublished verifies that a record that does NOT match
// the drop filter is still published (to the main or WLCG exchange).
func TestNonDroppedRecordIsPublished(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		DropPathPrefixes: []string{"/eos"},
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	// Not excluded from WLCG, and the path does not match the drop filter
	record := &CollectorRecord{
		VO:       "cms",
		Filename: "/store/data/file.root",
		Site:     "T2_US_Nebraska",
	}

	req := enrichmentRequest{record: record, destination: dest}
	c.processEnrichmentRequest(req)

	select {
	case got := <-resultCh:
		if got.Exchange != "wlcg-exchange" {
			t.Errorf("expected wlcg-exchange, got %q", got.Exchange)
		}
		var parsed map[string]interface{}
		if err := json.Unmarshal(got.Payload, &parsed); err != nil {
			t.Fatalf("payload is not valid JSON: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published record")
	}
}

// TestDropFilterWithEnrichmentPipeline verifies drop via the full async
// enrichment pipeline (EnqueueForEnrichment), not just processEnrichmentRequest.
func TestDropFilterWithEnrichmentPipeline(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:               time.Minute,
		EnrichmentWorkers: 1,
		DropVOs:           []string{"droppedvo"},
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	record := &CollectorRecord{VO: "droppedvo", Filename: "/some/path"}
	c.EnqueueForEnrichment(record, dest)

	// Give workers time to process.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case got := <-resultCh:
		t.Errorf("dropped record was published to exchange %q", got.Exchange)
	case <-ctx.Done():
		// correct: nothing published within timeout
	}
}
