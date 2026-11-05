package collector

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// cricFetchTimeout bounds a single CRIC fetch.
const cricFetchTimeout = 30 * time.Second

// NewCRICHTTPClient returns the HTTP client used to fetch CRIC URL sources.
// wlcg-cric.cern.ch serves a certificate issued by the CERN Grid CA, which
// the OS's trusted CA certificates do not include, so a fetch from a non-CERN
// host fails verification unless that CA is supplied. caPath is a PEM file or a
// directory of them (e.g. /etc/grid-security/certificates); its certificates are
// trusted in addition to the OS ones, not instead of them. An empty caPath
// trusts only the OS CA certificates.
func NewCRICHTTPClient(caPath string) (*http.Client, error) {
	client := &http.Client{Timeout: cricFetchTimeout}
	if caPath == "" {
		return client, nil
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	added, err := appendCertsFromPath(pool, caPath)
	if err != nil {
		return nil, err
	}
	if added == 0 {
		return nil, fmt.Errorf("site.ca_path %q contains no PEM certificates", caPath)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	client.Transport = transport
	return client, nil
}

// appendCertsFromPath adds the PEM certificates in a file, or in every regular
// file of a directory, to pool and returns how many files contributed at least
// one. Non-certificate files (grid CA directories also hold signing policies,
// namespaces and CRLs) contribute nothing and are skipped silently.
func appendCertsFromPath(pool *x509.CertPool, path string) (int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("site.ca_path: %w", err)
	}
	files := []string{path}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return 0, fmt.Errorf("site.ca_path: %w", err)
		}
		files = files[:0]
		for _, e := range entries {
			if !e.IsDir() {
				files = append(files, filepath.Join(path, e.Name()))
			}
		}
	}

	added := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue // a dangling link or unreadable file should not void the rest
		}
		if pool.AppendCertsFromPEM(data) {
			added++
		}
	}
	return added, nil
}

// withCAHint adds a pointer to site.ca_path to a fetch error caused by an
// untrusted certificate, which is what a CRIC fetch from a host without the CERN
// Grid CA fails with.
func withCAHint(err error) error {
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return fmt.Errorf("%w (CRIC's certificate is issued by the CERN Grid CA; "+
			"set site.ca_path to a file or directory holding it, e.g. /etc/grid-security/certificates)", err)
	}
	return err
}
