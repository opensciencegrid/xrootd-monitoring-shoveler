package shoveler

import (
	"encoding/json"
	"net"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPackageUdp(t *testing.T) {
	log = logrus.New()
	
	// No mapping enabled
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.7"), Port: 12345}
	packaged := PackageUdp([]byte("asdf"), &ip)
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
	mapAll = "172.0.0.9"
	packaged := PackageUdp([]byte("asdf"), &ip)
	assert.NotEmpty(t, packaged)
	// Parse back the json
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, "172.0.0.9:12345", pkg.Remote, "Remote IP should be the same")
	assert.Equal(t, "YXNkZg==", pkg.Data, "Data should be base64 encoded")
	mapAll = ""
}

func TestPackageUdp_MappingMultiple(t *testing.T) {
	// Mapping enabled
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.8"), Port: 12345}
	ipMap = make(map[string]string)
	defer func() {
		ipMap = nil
	}()
	ipMap["192.168.0.8"] = "172.0.0.10"
	ipMap["192.168.0.9"] = "172.0.0.11"
	packaged := PackageUdp([]byte("asdf"), &ip)
	assert.NotEmpty(t, packaged)
	// Parse back the json
	var pkg Message
	err := json.Unmarshal(packaged, &pkg)
	assert.NoError(t, err)
	assert.Equal(t, "172.0.0.10:12345", pkg.Remote, "Remote IP should be the same")
	assert.Equal(t, "YXNkZg==", pkg.Data, "Data should be base64 encoded")
}
