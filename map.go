package shoveler

import (
	"net"
)

// mapIp returns the mapped IP address
func mapIp(remote *net.UDPAddr, config *Config) string {

	if config.IpMapAll != "" {
		return config.IpMapAll
	}
	if len(config.IpMap) == 0 {
		return remote.IP.String()
	}
	if ip, ok := config.IpMap[remote.IP.String()]; ok {
		return ip
	}
	return remote.IP.String()
}
