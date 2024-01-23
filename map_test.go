package shoveler

import (
	"bytes"
	"net"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/stretchr/testify/assert"
)

func TestSingleIp(t *testing.T) {
	log = logrus.New()
	ip := net.UDPAddr{IP: net.ParseIP("192.168.0.5"), Port: 514}

	// If the map is not set
	config := Config{}
	config.ReadConfig()
	ConfigureMap()
	ipStr := mapIp(&ip)
	assert.Equal(t, "192.168.0.5", ipStr, "Test when map is not set")

	// If the map is set by environment variable
	err := os.Setenv("SHOVELER_MAP_ALL", "172.168.0.5")
	assert.NoError(t, err, "Failed to set environment variable SHOVELER_MAP_ALL")
	config.ReadConfig()
	ConfigureMap()
	ipStr = mapIp(&ip)
	assert.Equal(t, "172.168.0.5", ipStr, "Test when map is set by environment variable")

	// If the map is set by config file
	err = os.Unsetenv("SHOVELER_MAP_ALL")
	assert.NoError(t, err, "Failed to unset SHOVELER_MAP_ALL")
	// any approach to require this configuration into your program.
	var yamlExample = []byte(`
map:
  192.168.1.5: 172.168.1.6
  172.168.2.7: 129.93.10.5
`)
	err = viper.ReadConfig(bytes.NewBuffer(yamlExample))
	defer viper.Reset()
	assert.NoError(t, err, "Failed to read config file")
	ConfigureMap()
	defer func() {
		ipMap = nil
		mapAll = ""
	}()
	ip = net.UDPAddr{IP: net.ParseIP("192.168.1.5"), Port: 514}
	assert.Equal(t, "172.168.1.6", mapIp(&ip), "Test when map is set by config file")
	ip = net.UDPAddr{IP: net.ParseIP("172.168.2.7"), Port: 514}
	assert.Equal(t, "129.93.10.5", mapIp(&ip), "Test when map is set by config file")
}
