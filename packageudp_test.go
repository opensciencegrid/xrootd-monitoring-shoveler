package shoveler

import (
	"encoding/json"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageUdp(t *testing.T) {
	// No mapping enabled
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.7"), Port: 12345}
	config := Config{}
	packaged := PackageUdp([]byte("asdf"), &ip, &config)
	assert.NotEmpty(t, packaged)
	// Parse back the json
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, ip.String(), pkg.Remote, "Remote IP should be the same")
	assert.Equal(t, "YXNkZg==", pkg.Data, "Data should be base64 encoded")
}

func TestPackageUdp_Mapping(t *testing.T) {
	// Mapping enabled
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.8"), Port: 12345}
	config := Config{IpMapAll: "172.0.0.9"}
	packaged := PackageUdp([]byte("asdf"), &ip, &config)
	assert.NotEmpty(t, packaged)
	// Parse back the json
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, "172.0.0.9:12345", pkg.Remote, "Remote IP should be the same")
	assert.Equal(t, "YXNkZg==", pkg.Data, "Data should be base64 encoded")
}

func TestPackageUdp_MappingMultiple(t *testing.T) {
	// Mapping enabled
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.8"), Port: 12345}
	config := Config{}
	config.IpMap = make(map[string]string)
	config.IpMap["192.168.0.8"] = "172.0.0.10"
	config.IpMap["192.168.0.9"] = "172.0.0.11"
	packaged := PackageUdp([]byte("asdf"), &ip, &config)
	assert.NotEmpty(t, packaged)
	// Parse back the json
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, "172.0.0.10:12345", pkg.Remote, "Remote IP should be the same")
	assert.Equal(t, "YXNkZg==", pkg.Data, "Data should be base64 encoded")
}

func TestPackageUdp_IPv6(t *testing.T) {
	// Genuine IPv6 address must be wrapped in brackets so that
	// net.SplitHostPort can parse the remote address on the collector side.
	ip := net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 12345}
	config := Config{}
	packaged := PackageUdp([]byte("asdf"), &ip, &config)
	assert.NotEmpty(t, packaged)
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:12345", pkg.Remote)

	// Verify that the result can be parsed back by net.SplitHostPort
	host, port, splitErr := net.SplitHostPort(pkg.Remote)
	assert.NoError(t, splitErr, "net.SplitHostPort must succeed for the DNS enrichment path")
	assert.Equal(t, "2001:db8::1", host)
	assert.Equal(t, "12345", port)
}
