package collector

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// TestResolvedVOFallback covers the default resolution order for the WLCG
// record's "vo" field: what the record itself reported, then its SciTags
// marking, then the operator's declaration. The two derived-from fields are
// always published unmodified alongside the answer.
// enabledVOConfig is a correlator with wlcg.enabled on and the VO settings
// wired the way the binary wires them.
func enabledVOConfig(configVO string, order []string) CorrelatorConfig {
	return CorrelatorConfig{
		TTL:         5 * time.Second,
		WLCGEnabled: true,
		WLCGMetadata: WLCGMetadata{
			Producer: "cms", Type: "aaa-ng",
			VOResolution: VOResolution{Enabled: true, VO: configVO, Order: order},
		},
	}
}

// resolveAndConvert runs the real order, resolve then convert, so the tests see
// what the emitted record actually carries.
func resolveAndConvert(t *testing.T, cfg CorrelatorConfig, record *CollectorRecord) *WLCGRecord {
	t.Helper()

	c := NewCorrelatorWithConfig(cfg)
	defer c.Stop()

	c.resolveRecordVO(record)

	wlcg, err := ConvertToWLCG(record, c.wlcgMetadata, c.scitags)
	if err != nil {
		t.Fatalf("ConvertToWLCG() error = %v", err)
	}
	return wlcg
}

// TestResolvedVOFallback covers the default order on the emitted record: what
// the record said, then its SciTags marking, then the configured VO. record_vo
// and scitags_vo are always published as-is, and vo_source says which one won.
func TestResolvedVOFallback(t *testing.T) {
	// Experiment id 2 is "atlas" in the embedded registry. Checked here so the
	// fallback cases are not quietly comparing empty strings.
	if got := NewScitagsRegistry(nil).ExperimentName(2); got != "atlas" {
		t.Fatalf("precondition: ExperimentName(2) = %q, expected atlas", got)
	}

	for _, tc := range []struct {
		name          string
		configured    string
		packetVO      string
		experimentID  int
		wantVO        string
		wantSource    string
		wantRecordVO  string
		wantScitagsVO string
	}{
		{
			name:   "nothing known leaves every VO field empty",
			wantVO: "", wantSource: "", wantRecordVO: "", wantScitagsVO: "",
		},
		{
			name:     "record_vo alone answers",
			packetVO: "cms",
			wantVO:   "cms", wantSource: VOSourceRecord, wantRecordVO: "cms", wantScitagsVO: "",
		},
		{
			name:         "scitags_vo answers when record_vo is absent",
			experimentID: 2,
			wantVO:       "atlas", wantSource: VOSourceScitags, wantRecordVO: "", wantScitagsVO: "atlas",
		},
		{
			name:     "record_vo beats scitags_vo",
			packetVO: "cms", experimentID: 2,
			wantVO: "cms", wantSource: VOSourceRecord, wantRecordVO: "cms", wantScitagsVO: "atlas",
		},
		{
			name:       "record_vo beats the configured VO",
			configured: "alice", packetVO: "cms",
			wantVO: "cms", wantSource: VOSourceRecord, wantRecordVO: "cms", wantScitagsVO: "",
		},
		{
			name:       "scitags_vo beats the configured VO",
			configured: "alice", experimentID: 2,
			wantVO: "atlas", wantSource: VOSourceScitags, wantRecordVO: "", wantScitagsVO: "atlas",
		},
		{
			name:       "the configured VO answers when the record knows nothing",
			configured: "alice",
			wantVO:     "alice", wantSource: VOSourceConfig, wantRecordVO: "", wantScitagsVO: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := &CollectorRecord{
				VO:           tc.packetVO,
				ExperimentID: tc.experimentID,
				Filename:     "/store/data/file.root",
			}

			wlcg := resolveAndConvert(t, enabledVOConfig(tc.configured, nil), record)

			if wlcg.VO != tc.wantVO {
				t.Errorf("vo = %q, expected %q", wlcg.VO, tc.wantVO)
			}
			if wlcg.VOSource != tc.wantSource {
				t.Errorf("vo_source = %q, expected %q", wlcg.VOSource, tc.wantSource)
			}
			if wlcg.RecordVO != tc.wantRecordVO {
				t.Errorf("record_vo = %q, expected %q", wlcg.RecordVO, tc.wantRecordVO)
			}
			if wlcg.ScitagsVO != tc.wantScitagsVO {
				t.Errorf("scitags_vo = %q, expected %q", wlcg.ScitagsVO, tc.wantScitagsVO)
			}

			// MONIT checks the metadata block against a fixed schema, so no VO there.
			if _, ok := wlcg.Metadata["vo"]; ok {
				t.Errorf("metadata should carry no vo field, got %v", wlcg.Metadata["vo"])
			}

			// The source record is what goes to the main exchange, so resolving must
			// not rewrite its VO.
			if record.VO != tc.packetVO {
				t.Errorf("source record VO = %q, expected the packet VO %q", record.VO, tc.packetVO)
			}
		})
	}
}

// TestResolvedVOConfiguredOrder covers wlcg.vo_order: the same three sources
// give a different answer when reordered, and a source left out is not used even
// when it has a value.
func TestResolvedVOConfiguredOrder(t *testing.T) {
	// A record that knows all three: record_vo cms, scitags_vo atlas, config alice.
	record := func() *CollectorRecord {
		return &CollectorRecord{VO: "cms", ExperimentID: 2, Filename: "/store/data/file.root"}
	}

	for _, tc := range []struct {
		name       string
		order      []string
		wantVO     string
		wantSource string
	}{
		{"nil order uses the default", nil, "cms", VOSourceRecord},
		{"default order", DefaultVOOrder, "cms", VOSourceRecord},
		{"config first wins", []string{VOSourceConfig, VOSourceRecord, VOSourceScitags}, "alice", VOSourceConfig},
		{"scitags first wins", []string{VOSourceScitags, VOSourceRecord, VOSourceConfig}, "atlas", VOSourceScitags},
		{"a source left out is not consulted", []string{VOSourceScitags, VOSourceConfig}, "atlas", VOSourceScitags},
		{"omitting record ignores a present record_vo", []string{VOSourceConfig}, "alice", VOSourceConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wlcg := resolveAndConvert(t, enabledVOConfig("alice", tc.order), record())

			if wlcg.VO != tc.wantVO {
				t.Errorf("vo = %q, expected %q", wlcg.VO, tc.wantVO)
			}
			if wlcg.VOSource != tc.wantSource {
				t.Errorf("vo_source = %q, expected %q", wlcg.VOSource, tc.wantSource)
			}

			// record_vo and scitags_vo are published as-is whatever the order.
			if wlcg.RecordVO != "cms" {
				t.Errorf("record_vo = %q, expected cms", wlcg.RecordVO)
			}
			if wlcg.ScitagsVO != "atlas" {
				t.Errorf("scitags_vo = %q, expected atlas", wlcg.ScitagsVO)
			}
		})
	}
}

// TestResolvedVOFeedsWLCGRouting is why we resolve before routing: the rules
// match the resolved VO, so a record whose auth stream said nothing can still be
// picked up or left out by VO.
func TestResolvedVOFeedsWLCGRouting(t *testing.T) {
	// No auth VO and a non-CMS path, so without resolution no rule can match it.
	newRecord := func() *CollectorRecord {
		return &CollectorRecord{Filename: "/alice/data/file.root"}
	}

	t.Run("the resolved VO is excluded", func(t *testing.T) {
		cfg := enabledVOConfig("alice", nil)
		cfg.WLCGVOs = []string{}
		cfg.WLCGPathPrefixes = []string{}
		cfg.WLCGExcludeVOs = []string{"alice"}

		c := NewCorrelatorWithConfig(cfg)
		defer c.Stop()

		record := newRecord()
		c.resolveRecordVO(record)

		if record.resolvedVO != "alice" {
			t.Fatalf("resolvedVO = %q, expected alice", record.resolvedVO)
		}
		if c.matchesWLCG(record) {
			t.Error("the exclusion should match the resolved VO")
		}
	})

	t.Run("the resolved VO is included", func(t *testing.T) {
		cfg := enabledVOConfig("alice", nil)
		cfg.WLCGVOs = []string{"alice"}
		cfg.WLCGPathPrefixes = []string{}

		c := NewCorrelatorWithConfig(cfg)
		defer c.Stop()

		record := newRecord()
		c.resolveRecordVO(record)

		if !c.matchesWLCG(record) {
			t.Error("the inclusion list should match the resolved VO")
		}
	})

	t.Run("scitags can supply the VO the rules match on", func(t *testing.T) {
		cfg := enabledVOConfig("", nil)
		cfg.WLCGVOs = []string{}
		cfg.WLCGPathPrefixes = []string{}
		cfg.WLCGExcludeVOs = []string{"atlas"}

		c := NewCorrelatorWithConfig(cfg)
		defer c.Stop()

		// Experiment id 2 is atlas; the auth stream said nothing.
		record := &CollectorRecord{ExperimentID: 2, Filename: "/other/f.root"}
		c.resolveRecordVO(record)

		if record.voSource != VOSourceScitags {
			t.Fatalf("vo_source = %q, expected scitags", record.voSource)
		}
		if c.matchesWLCG(record) {
			t.Error("the exclusion should match a VO that came from SciTags")
		}
	})

	// filter.drop_vos drops on the VO wherever it came from: the auth/token
	// stream, the SciTags marking, or the config.
	for _, tc := range []struct {
		name         string
		packetVO     string
		experimentID int
		configVO     string
		wantSource   string
	}{
		{name: "from the packet", packetVO: "atlas", wantSource: VOSourceRecord},
		{name: "from the scitags stream", experimentID: 2, wantSource: VOSourceScitags},
		{name: "from the config", configVO: "atlas", wantSource: VOSourceConfig},
	} {
		t.Run("drop_vos drops a VO "+tc.name, func(t *testing.T) {
			cfg := enabledVOConfig(tc.configVO, nil)
			cfg.WLCGVOs = []string{}
			cfg.WLCGPathPrefixes = []string{}
			cfg.DropVOs = []string{"atlas"}

			c := NewCorrelatorWithConfig(cfg)
			defer c.Stop()

			// Experiment id 2 is "atlas", so all three cases reach the same VO by a
			// different route.
			record := &CollectorRecord{
				VO:           tc.packetVO,
				ExperimentID: tc.experimentID,
				Filename:     "/alice/data/file.root",
			}

			// Before resolution only the packet case has a VO to match on.
			if drop, _ := c.shouldDrop(record); drop != (tc.packetVO != "") {
				t.Fatalf("precondition: unresolved drop = %v", drop)
			}

			c.resolveRecordVO(record)

			if record.resolvedVO != "atlas" {
				t.Fatalf("resolvedVO = %q, expected atlas", record.resolvedVO)
			}
			if record.voSource != tc.wantSource {
				t.Fatalf("vo_source = %q, expected %q", record.voSource, tc.wantSource)
			}

			drop, reason := c.shouldDrop(record)
			if !drop {
				t.Error("the record should be dropped whatever source supplied its VO")
			}
			if reason != "vo" {
				t.Errorf("reason = %q, expected vo", reason)
			}
		})
	}

	t.Run("a dropped record never reaches the WLCG rules", func(t *testing.T) {
		cfg := enabledVOConfig("alice", nil)
		cfg.WLCGVOs = []string{}
		cfg.WLCGPathPrefixes = []string{}
		cfg.DropVOs = []string{"alice"}

		c := NewCorrelatorWithConfig(cfg)
		defer c.Stop()

		results := make(chan EnrichedRecord, 1)
		c.processEnrichmentRequest(enrichmentRequest{
			record:      newRecord(),
			destination: EnrichmentDestination{Results: results, WLCGExchange: "wlcg-exchange"},
		})

		select {
		case got := <-results:
			t.Errorf("expected the record to be dropped, got a publish to %q", got.Exchange)
		default:
		}
	})
}

// TestNormalizeVOOrder covers the tidy-up rules, which match
// NormalizeSiteResolutionOrder: unknown and repeated names are dropped, case and
// whitespace do not matter, and an unusable order falls back to the default.
func TestNormalizeVOOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order []string
		want  []string
	}{
		{"nil falls back to the default", nil, DefaultVOOrder},
		{"empty falls back to the default", []string{}, DefaultVOOrder},
		{"only unknown names falls back to the default", []string{"nonsense", "vo"}, DefaultVOOrder},
		{"case and whitespace are forgiven", []string{" Record ", "SCITAGS"}, []string{VOSourceRecord, VOSourceScitags}},
		{"unknown names are dropped", []string{VOSourceRecord, "nonsense", VOSourceConfig}, []string{VOSourceRecord, VOSourceConfig}},
		{"repeats are dropped", []string{VOSourceRecord, VOSourceRecord, VOSourceConfig}, []string{VOSourceRecord, VOSourceConfig}},
		{"a deliberate subset is kept", []string{VOSourceRecord}, []string{VOSourceRecord}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeVOOrder(tc.order, newVOTestLogger())
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("NormalizeVOOrder(%v) = %v, want %v", tc.order, got, tc.want)
			}
		})
	}
}

// newVOTestLogger keeps the "ignoring unknown/repeated" warnings out of the test
// output. What is being tested is the returned order, not the logging.
func newVOTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	return logger
}

// TestResolvedVOSerialisation checks the wire shape: empty fields are left out,
// and vo_source says where "vo" came from.
func TestResolvedVOSerialisation(t *testing.T) {
	record := &CollectorRecord{VO: "cms", Filename: "/store/data/file.root"}

	wlcg := resolveAndConvert(t, enabledVOConfig("", nil), record)

	encoded, err := wlcg.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error = %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	for field, want := range map[string]interface{}{
		"vo":        "cms",
		"record_vo": "cms",
		"vo_source": VOSourceRecord,
	} {
		if decoded[field] != want {
			t.Errorf("%s = %v, expected %v", field, decoded[field], want)
		}
	}
	if _, ok := decoded["scitags_vo"]; ok {
		t.Error("scitags_vo should be omitted when empty")
	}
}

// TestConfiguredVOLeavesCollectorRecordsAlone checks the scope: records that are
// not WLCG-bound never reach a converter, so the configured VO cannot show up on
// the main exchange.
func TestConfiguredVOLeavesCollectorRecordsAlone(t *testing.T) {
	correlator := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:          5 * time.Second,
		MaxEntries:   0,
		WLCGMetadata: WLCGMetadata{Producer: "cms", Type: "aaa-ng", VOResolution: VOResolution{Enabled: true, VO: "alice"}},
	})
	defer correlator.Stop()

	// No VO and a path outside the default inclusion rules: the record the raw
	// feed receives.
	record := &CollectorRecord{Filename: "/other/data/file.root"}

	enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
	if err != nil {
		t.Fatalf("buildEnrichedRecord() error = %v", err)
	}

	if enriched.Exchange != "" {
		t.Fatalf("Exchange = %q, expected the main exchange", enriched.Exchange)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(enriched.Payload, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if vo, ok := decoded["vo"]; ok && vo != "" {
		t.Errorf("collector record vo = %v, expected the configured VO not to reach it", vo)
	}
}

// TestWLCGDisabledLeavesRecordUnchanged guards the OSG deployments that share
// this collector. With wlcg.enabled off, "vo" is whatever the packet said,
// record_vo is not written, and the other settings do nothing even when set.
func TestWLCGDisabledLeavesRecordUnchanged(t *testing.T) {
	registry := NewScitagsRegistry(nil)

	for _, tc := range []struct {
		name     string
		packetVO string
		// The record also carries a SciTags marking, which must not reach "vo".
		experimentID int
		wantVO       string
	}{
		{"packet VO passes through", "cms", 0, "cms"},
		{"no packet VO leaves vo empty", "", 0, ""},
		{"scitags does not fill in for a missing VO", "", 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := &CollectorRecord{
				VO:           tc.packetVO,
				ExperimentID: tc.experimentID,
				Filename:     "/store/data/file.root",
			}

			// Fields set but Enabled left false, so none of it should apply.
			meta := WLCGMetadata{
				Producer: "cms", Type: "aaa-ng",
				VOResolution: VOResolution{VO: "alice", Order: []string{VOSourceConfig}},
			}

			wlcg, err := ConvertToWLCG(record, meta, registry)
			if err != nil {
				t.Fatalf("ConvertToWLCG() error = %v", err)
			}

			if wlcg.VO != tc.wantVO {
				t.Errorf("vo = %q, expected the packet's %q", wlcg.VO, tc.wantVO)
			}
			if wlcg.RecordVO != "" {
				t.Errorf("record_vo = %q, expected it not to be set while disabled", wlcg.RecordVO)
			}
			if wlcg.VOSource != "" {
				t.Errorf("vo_source = %q, expected it not to be set while disabled", wlcg.VOSource)
			}

			// Routing still sees the packet's VO, since nothing was resolved.
			if record.voResolved {
				t.Error("the record must not be marked resolved while disabled")
			}
			if record.routingVO() != tc.packetVO {
				t.Errorf("routingVO() = %q, expected the packet's %q", record.routingVO(), tc.packetVO)
			}

			// record_vo should not appear in the JSON at all, so the wire format is
			// unchanged for consumers that do not know about it.
			encoded, err := wlcg.ToJSON()
			if err != nil {
				t.Fatalf("ToJSON() error = %v", err)
			}
			var decoded map[string]interface{}
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			for _, field := range []string{"record_vo", "vo_source"} {
				if _, ok := decoded[field]; ok {
					t.Errorf("%s must be absent from the JSON while disabled", field)
				}
			}
		})
	}
}

// TestWLCGDisabledLeavesDropFilterOnPacketVO is the other half of that guard.
// The drop filter matches the resolved VO, but nothing is resolved while
// wlcg.enabled is off, so it still sees the packet's VO and a configured VO
// cannot make it drop anything.
func TestWLCGDisabledLeavesDropFilterOnPacketVO(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:     5 * time.Second,
		DropVOs: []string{"alice", "cms"},
		// Populated but not enabled: it must not supply a VO to drop on.
		WLCGMetadata: WLCGMetadata{VOResolution: VOResolution{VO: "alice"}},
	})
	defer c.Stop()

	// No packet VO, and with the switch off nothing is resolved, so there is
	// nothing to match even though "alice" is configured and on the drop list.
	record := &CollectorRecord{Filename: "/store/data/f.root"}
	c.resolveRecordVO(record)

	if record.voResolved {
		t.Error("nothing must resolve while WLCG mode is off")
	}
	if drop, reason := c.shouldDrop(record); drop {
		t.Errorf("record dropped by %q; the configured VO must not reach the drop filter", reason)
	}

	// A record that does have a VO is still dropped, as upstream.
	withVO := &CollectorRecord{VO: "cms", Filename: "/store/data/f.root"}
	c.resolveRecordVO(withVO)

	drop, reason := c.shouldDrop(withVO)
	if !drop || reason != "vo" {
		t.Errorf("shouldDrop() = %v/%q, expected a vo drop on the packet VO", drop, reason)
	}
}
