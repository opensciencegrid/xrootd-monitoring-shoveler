package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// TestScitagsEmbeddedRegistry verifies the embedded snapshot loads and resolves
// known ids, and that activity ids are correctly namespaced per experiment.
func TestScitagsEmbeddedRegistry(t *testing.T) {
	r := NewScitagsRegistry(newTestLogger())

	assert.Equal(t, "atlas", r.ExperimentName(2))
	assert.Equal(t, "cms", r.ExperimentName(3))
	assert.Equal(t, "lhcb", r.ExperimentName(4))

	// Activity 3 means different things depending on the experiment.
	assert.Equal(t, "Data Brokering", r.ActivityName(2, 3)) // atlas
	assert.Equal(t, "Cache", r.ActivityName(3, 3))          // cms
	assert.Equal(t, "Cache", r.ActivityName(4, 3))          // lhcb

	// Well-known per-experiment activities.
	assert.Equal(t, "Production Input", r.ActivityName(2, 15)) // atlas
	assert.Equal(t, "Analysis Input", r.ActivityName(3, 14))   // cms
}

// TestScitagsUnknownAndZeroIDs verifies graceful handling of the inconsistencies
// the 'U' stream can produce: id 0 (unset) and ids absent from the registry.
func TestScitagsUnknownAndZeroIDs(t *testing.T) {
	r := NewScitagsRegistry(newTestLogger())

	assert.Empty(t, r.ExperimentName(0), "id 0 is unset")
	assert.Empty(t, r.ActivityName(0, 0), "id 0 is unset")
	assert.Empty(t, r.ActivityName(2, 0), "activity 0 is unset even with a valid experiment")
	assert.Empty(t, r.ExperimentName(99999), "unknown experiment id")
	assert.Empty(t, r.ActivityName(2, 99999), "unknown activity id")
	// An activity id valid in one experiment but not another must not leak.
	assert.Empty(t, r.ActivityName(12, 15), "juno only defines activity 1")
}

// TestScitagsLoadRejectsBadData verifies that a bad load leaves the previous
// registry intact (fail-open on refresh).
func TestScitagsLoadRejectsBadData(t *testing.T) {
	r := NewScitagsRegistry(newTestLogger())

	require.Error(t, r.Load([]byte("not json")))
	require.Error(t, r.Load([]byte(`{"experiments": []}`)))

	// Previous (embedded) data still resolves.
	assert.Equal(t, "atlas", r.ExperimentName(2))
}

// TestScitagsLoadReplaces verifies a good load atomically swaps in new mappings.
func TestScitagsLoadReplaces(t *testing.T) {
	r := NewScitagsRegistry(newTestLogger())

	doc := `{"experiments":[{"expName":"testexp","expId":42,"activities":[{"activityName":"testact","activityId":7}]}],"version":9,"modified":"2026-01-01T00:00:00Z"}`
	require.NoError(t, r.Load([]byte(doc)))

	assert.Equal(t, "testexp", r.ExperimentName(42))
	assert.Equal(t, "testact", r.ActivityName(42, 7))
	// Old embedded ids are gone after a full replace.
	assert.Empty(t, r.ExperimentName(2))
}

// TestScitagsLoadSourceFile verifies loading from a local file path.
func TestScitagsLoadSourceFile(t *testing.T) {
	r := NewScitagsRegistry(newTestLogger())

	dir := t.TempDir()
	path := filepath.Join(dir, "api.json")
	doc := `{"experiments":[{"expName":"fileexp","expId":50,"activities":[{"activityName":"fileact","activityId":3}]}],"version":1,"modified":"2026-01-01T00:00:00Z"}`
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))

	require.NoError(t, r.LoadSource(context.Background(), path))
	assert.Equal(t, "fileexp", r.ExperimentName(50))
	assert.Equal(t, "fileact", r.ActivityName(50, 3))
}

// TestScitagsLoadSourceURL verifies loading from an http(s) URL.
func TestScitagsLoadSourceURL(t *testing.T) {
	doc := `{"experiments":[{"expName":"urlexp","expId":60,"activities":[{"activityName":"urlact","activityId":2}]}],"version":1,"modified":"2026-01-01T00:00:00Z"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(doc))
	}))
	defer srv.Close()

	r := NewScitagsRegistry(newTestLogger())
	require.NoError(t, r.LoadSource(context.Background(), srv.URL))
	assert.Equal(t, "urlexp", r.ExperimentName(60))
	assert.Equal(t, "urlact", r.ActivityName(60, 2))
}

// TestScitagsLoadSourceURLBadStatus verifies a non-200 URL response is an error.
func TestScitagsLoadSourceURLBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	r := NewScitagsRegistry(newTestLogger())
	require.Error(t, r.LoadSource(context.Background(), srv.URL))
}
