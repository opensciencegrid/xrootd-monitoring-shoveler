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
	// When run inside Kubernetes, there may be multiple layers of IP mapping.
	// Hence, we allow overriding these values based on XRDHOST; XRDHOST is
	// also the current env var name used to override the advertise hostname =by
	// the cmsd in the OSG-shipped patches.
	viper.BindEnv("map.all", "XRDHOST");

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
	if len(ipMap) == 0 {
		return remote.IP.String()
	}
	if ip, ok := ipMap[remote.IP.String()]; ok {
		return ip
	}
	return remote.IP.String()
}
