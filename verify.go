package shoveler

import (
	"encoding/binary"
)

// Header is the XRootD structure
// 1 + 1 + 2 + 4 = 8 bytes
type Header struct {
	Code        byte
	Pseq        uint8
	Plen        uint16
	ServerStart int32
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

	// XML '<' character indicates a summary packet
	if len(packet) > 0 && packet[0] == '<' {
		return true
	}

	header := Header{}
	header.Code = packet[0]
	header.Pseq = packet[1]
	header.Plen = binary.BigEndian.Uint16(packet[2:4])
	header.ServerStart = int32(binary.BigEndian.Uint32(packet[4:8]))

	// If the beginning of the packet doesn't match some expectations, then continue
	if len(packet) != int(header.Plen) {
		log.Warningln("Packet length does not match header.  Packet:", len(packet), "Header:", int(header.Plen))
		return false
	}
	return true
}
