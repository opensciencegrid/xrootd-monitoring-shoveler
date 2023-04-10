package shoveler

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type Message struct {
	Remote          string `json:"remote"`
	ShovelerVersion string `json:"version"`
	Data            string `json:"data"`
}

func PackageUdp(packet []byte, remote *net.UDPAddr) []byte {
	msg := Message{}
	// Base64 encode the packet
	str := base64.StdEncoding.EncodeToString(packet)
	msg.Data = str

	// add the remote
	msg.Remote = mapIp(remote)
	msg.Remote += ":" + strconv.Itoa(remote.Port)

	msg.ShovelerVersion = ShovelerVersion

	b, err := json.Marshal(msg)

	if err != nil {
		log.Errorln("Failed to Marshal the msg to json:", err)
	}
	return b
}
