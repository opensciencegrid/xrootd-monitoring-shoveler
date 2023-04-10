package shoveler

import (
	"bytes"
	"encoding/binary"

	log "github.com/sirupsen/logrus"
)

// Header is the XRootD structure
// 1 + 1 + 2 + 4 = 8 bytes
type Header struct {
	Code        byte
	Pseq        uint8
	Plen        uint16
	ServerStart uint32
}

// verifyPacket will verify the packet matches the expected
// format from XRootD
func VerifyPacket(packet []byte) bool {
	// Try reading in the header, which is 8 bytes
	if len(packet) < 8 {
		// If it is less than 8 bytes, then it can't have the header, and discard it
		log.Infoln("Packet not large enough for XRootD header of 8 bytes, dropping.")
		return false
	}
	header := Header{}
	buffer := bytes.NewBuffer(packet[:8])
	err := binary.Read(buffer, binary.BigEndian, &header)
	if err != nil {
		log.Warningln("Failed to read the binary packet into header structure:", err)
		return false
	}
	// If the beginning of the packet doesn't match some expectations, then continue
	if len(packet) != int(header.Plen) {
		log.Warningln("Packet length does not match header.  Packet:", len(packet), "Header:", int(header.Plen))
		return false
	}
	return true
}
