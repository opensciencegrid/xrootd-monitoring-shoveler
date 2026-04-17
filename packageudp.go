package shoveler

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"strconv"
)

type Message struct {
	Remote          string `json:"remote"`
	ShovelerVersion string `json:"version"`
	Data            string `json:"data"`
}

func PackageUdp(packet []byte, remote *net.UDPAddr, config *Config) []byte {
	msg := Message{}
	// Base64 encode the packet
	str := base64.StdEncoding.EncodeToString(packet)
	msg.Data = str

	// Use net.JoinHostPort so IPv6 addresses are wrapped in brackets,
	// producing a valid "host:port" string that net.SplitHostPort can parse.
	msg.Remote = net.JoinHostPort(mapIp(remote, config), strconv.Itoa(remote.Port))

	msg.ShovelerVersion = ShovelerVersion

	b, err := json.Marshal(msg)

	if err != nil {
		log.Errorln("Failed to Marshal the msg to json:", err)
	}
	return b
}
