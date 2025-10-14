package shoveler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
)

// Header is the XRootD structure
// 1 + 1 + 2 + 4 = 8 bytes
type Header struct {
	Code        byte
	Pseq        uint8
	Plen        uint16
	ServerStart int32
}

// VerifyPacket will verify the packet matches the expected format from XRootD
// and return a routing key for use with RabbitMQ consistent hashing exchange.
// Returns: routingKey (string), error
// - For valid XRootD packets: returns server startup time as routing key
// - For summary packets (starting with '<' or '{'): returns random routing key
// - For invalid packets: returns empty string and error
func VerifyPacket(packet []byte) (string, error) {
	// Try reading in the header, which is 8 bytes
	if len(packet) < 8 {
		// If it is less than 8 bytes, then it can't have the header, and discard it
		log.Infoln("Packet not large enough for XRootD header of 8 bytes, dropping.")
		return "", errors.New("packet too small")
	}

	// XML '<' character indicates a summary packet
	// JSON '{' character could also indicate a special packet
	if len(packet) > 0 && (packet[0] == '<' || packet[0] == '{') {
		// Return a random routing key for summary/special packets
		randomKey := rand.Int31()
		return fmt.Sprintf("%d", randomKey), nil
	}

	header := Header{}
	header.Code = packet[0]
	header.Pseq = packet[1]
	header.Plen = binary.BigEndian.Uint16(packet[2:4])
	header.ServerStart = int32(binary.BigEndian.Uint32(packet[4:8]))

	// If the beginning of the packet doesn't match some expectations, then continue
	if len(packet) != int(header.Plen) {
		log.Warningln("Packet length does not match header.  Packet:", len(packet), "Header:", int(header.Plen))
		return "", errors.New("packet length mismatch")
	}
	
	// Return the server startup time as the routing key
	return fmt.Sprintf("%d", header.ServerStart), nil
}
