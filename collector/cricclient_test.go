package collector

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newPrivateCATLSServer serves body over TLS with a certificate no system trust
// store carries, standing in for CRIC's CERN Grid CA, and writes that
// certificate as PEM into dir.
func newPrivateCATLSServer(t *testing.T, dir, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestFetchSiteSourceUntrustedCAHintsCAPath(t *testing.T) {
	srv := newPrivateCATLSServer(t, t.TempDir(), "{}")

	_, err := fetchSiteSource(context.Background(), nil, srv.URL)
	if err == nil {
		t.Fatal("fetch from an untrusted CA succeeded")
	}
	if !strings.Contains(err.Error(), "site.ca_path") {
		t.Errorf("error does not point at site.ca_path: %v", err)
	}
}

func TestNewCRICHTTPClientTrustsCAPath(t *testing.T) {
	dir := t.TempDir()
	srv := newPrivateCATLSServer(t, dir, `{"ok":1}`)
	// Grid CA directories also hold non-certificate files; they must not break loading.
	if err := os.WriteFile(filepath.Join(dir, "ca.signing_policy"), []byte("access_id_CA"), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, path := range map[string]string{
		"file":      filepath.Join(dir, "ca.pem"),
		"directory": dir,
	} {
		t.Run(name, func(t *testing.T) {
			client, err := NewCRICHTTPClient(path)
			if err != nil {
				t.Fatalf("NewCRICHTTPClient(%q): %v", path, err)
			}
			data, err := fetchSiteSource(context.Background(), client, srv.URL)
			if err != nil {
				t.Fatalf("fetch with site.ca_path trusted: %v", err)
			}
			if string(data) != `{"ok":1}` {
				t.Errorf("body = %q", data)
			}
		})
	}
}

func TestNewCRICHTTPClientRejectsBadCAPath(t *testing.T) {
	empty := t.TempDir()
	if err := os.WriteFile(filepath.Join(empty, "README"), []byte("no certs here"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"missing":   filepath.Join(empty, "nope.pem"),
		"no certs":  empty,
		"not a PEM": filepath.Join(empty, "README"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewCRICHTTPClient(path); err == nil {
				t.Errorf("NewCRICHTTPClient(%q) accepted a path with no certificates", path)
			}
		})
	}
}
