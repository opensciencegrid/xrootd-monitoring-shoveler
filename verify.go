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

// VerifyPacket will verify the packet matches the expected
// format from XRootD and return a routing key for RabbitMQ.
// Returns the routing key (ServerStart time for valid packets, random for special packets)
// and an error if the packet is invalid.
func VerifyPacket(packet []byte) (string, error) {
	// Try reading in the header, which is 8 bytes
	if len(packet) < 8 {
		// If it is less than 8 bytes, then it can't have the header, and discard it
		log.Infoln("Packet not large enough for XRootD header of 8 bytes, dropping.")
		return "", errors.New("packet too small for XRootD header")
	}

	// XML '<' character indicates a summary packet - use random routing key
	if len(packet) > 0 && packet[0] == '<' {
		// Generate a random routing key for summary packets
		routingKey := fmt.Sprintf("summary-%d", rand.Int31())
		return routingKey, nil
	}

	// JSON '{' character indicates a special packet - use random routing key
	if len(packet) > 0 && packet[0] == '{' {
		// Generate a random routing key for JSON packets
		routingKey := fmt.Sprintf("json-%d", rand.Int31())
		return routingKey, nil
	}

	header := Header{}
	header.Code = packet[0]
	header.Pseq = packet[1]
	header.Plen = binary.BigEndian.Uint16(packet[2:4])
	header.ServerStart = int32(binary.BigEndian.Uint32(packet[4:8]))

	// If the beginning of the packet doesn't match some expectations, then return error
	if len(packet) != int(header.Plen) {
		log.Warningln("Packet length does not match header.  Packet:", len(packet), "Header:", int(header.Plen))
		return "", errors.New("packet length mismatch")
	}

	// Use ServerStart time as the routing key for consistent hashing
	routingKey := fmt.Sprintf("%d", header.ServerStart)
	return routingKey, nil
}
