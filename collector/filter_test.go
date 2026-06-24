package collector

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// newTestCorrelator creates a minimal correlator suitable for filter tests.
// It starts no enrichment workers so processEnrichmentRequest can be called
// synchronously via a direct call.
func newTestCorrelator(cfg CorrelatorConfig) *Correlator {
	// Ensure logger is set
	c := NewCorrelatorWithConfig(cfg)
	return c
}

// ---- WLCG match tests -------------------------------------------------------

func TestMatchesWLCG_DefaultConfig(t *testing.T) {
	// A correlator with empty WLCG config must fall back to the defaults
	// and behave identically to the hardcoded IsWLCGPacket function.
	c := newTestCorrelator(CorrelatorConfig{
		TTL:    time.Minute,
		Logger: nil,
		// WLCGVOs and WLCGPathPrefixes intentionally left empty → defaults apply
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{
			name:     "default VO cms",
			record:   &CollectorRecord{VO: "cms", Filename: "/other/path"},
			expected: true,
		},
		{
			name:     "default VO cms case-insensitive upper",
			record:   &CollectorRecord{VO: "CMS", Filename: "/other/path"},
			expected: true,
		},
		{
			name:     "default path /store",
			record:   &CollectorRecord{VO: "atlas", Filename: "/store/data/file.root"},
			expected: true,
		},
		{
			name:     "default path /user/dteam",
			record:   &CollectorRecord{VO: "other", Filename: "/user/dteam/test.dat"},
			expected: true,
		},
		{
			name:     "no match",
			record:   &CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"},
			expected: false,
		},
		{
			name:     "empty VO and filename",
			record:   &CollectorRecord{VO: "", Filename: ""},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.matchesWLCG(tt.record)
			if got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMatchesWLCG_CustomConfig(t *testing.T) {
	c := newTestCorrelator(CorrelatorConfig{
		TTL:              time.Minute,
		WLCGVOs:          []string{"atlas", "lhcb"},
		WLCGPathPrefixes: []string{"/eos/atlas"},
	})
	defer c.Stop()

	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{
			name:     "VO atlas hit",
			record:   &CollectorRecord{VO: "atlas", Filename: "/other"},
			expected: true,
		},
		{
			name:     "VO atlas case-insensitive",
			record:   &CollectorRecord{VO: "ATLAS", Filename: "/other"},
			expected: true,
		},
		{
			name:     "VO lhcb hit",
			record:   &CollectorRecord{VO: "lhcb", Filename: "/other"},
			expected: true,
		},
		{
			name:     "path /eos/atlas hit",
			record:   &CollectorRecord{VO: "cms", Filename: "/eos/atlas/file"},
			expected: true,
		},
		{
			name:     "VO cms no longer matches (not in custom list)",
			record:   &CollectorRecord{VO: "cms", Filename: "/other/path"},
			expected: false,
		},
		{
			name:     "path /store no longer matches (not in custom list)",
			record:   &CollectorRecord{VO: "osg", Filename: "/store/data/file.root"},
			expected: false,
		},
		{
			name:     "no match",
			record:   &CollectorRecord{VO: "osg", Filename: "/ospool/data.txt"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.matchesWLCG(tt.record)
			if got != tt.expected {
				t.Errorf("matchesWLCG() = %v, want %v", got, tt.expected)
			}
		})
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
		DropPathPrefixes: []string{"/eos"},
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
		// Also configure WLCG so the record *would* match it via VO if not dropped
		WLCGVOs: []string{"cms"},
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
		TTL:              time.Minute,
		WLCGPathPrefixes: []string{"/store"},
		DropVOs:          []string{"cms"},
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	record := &CollectorRecord{
		VO:       "cms",       // matches drop filter
		Filename: "/store/x",  // would match WLCG path prefix
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
		WLCGVOs:          []string{"cms"},
	})
	defer c.Stop()

	resultCh := make(chan EnrichedRecord, 1)
	dest := EnrichmentDestination{
		Results:      resultCh,
		WLCGExchange: "wlcg-exchange",
	}

	// cms VO matches WLCG, path does not match drop filter
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
		TTL:              time.Minute,
		EnrichmentWorkers: 1,
		DropVOs:          []string{"droppedvo"},
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
