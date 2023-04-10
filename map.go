package shoveler

import (
	"net"

	"github.com/spf13/viper"
)

var (
	mapAll string
	ipMap  map[string]string
)

// ConfigureMap sets the mapping configuration
func ConfigureMap() {
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
