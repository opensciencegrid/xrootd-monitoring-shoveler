package main

import (
	"net"

	"github.com/spf13/viper"
)

var (
	mapAll string
	ipMap  map[string]string
)

// configureMap sets the mapping configuration
func configureMap() {
	// First, check for the map environment variable
	mapAll = viper.GetString("map.all")

	// If the map is not set
	ipMap = viper.GetStringMapString("map")

}

// mapIp returns the mapped IP address
func mapIp(remote *net.UDPAddr) string {

	if mapAll != "" {
		return mapAll
	}
	if ipMap == nil || len(ipMap) == 0 {
		return remote.IP.String()
	}
	return ipMap[remote.IP.String()]
}
