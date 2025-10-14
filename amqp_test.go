package shoveler

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExtractRoutingKeyFromMessage_ValidXRootDPacket tests routing key extraction from a valid XRootD packet
func TestExtractRoutingKeyFromMessage_ValidXRootDPacket(t *testing.T) {
	// Create a valid XRootD packet with ServerStart = 1234567890
	header := Header{
		Code:        0,
		Pseq:        0,
		Plen:        16,
		ServerStart: 1234567890,
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &header)
	assert.NoError(t, err, "Failed to write header")
	err = binary.Write(buf, binary.BigEndian, []byte("12345678"))
	assert.NoError(t, err, "Failed to write body")

	// Package it like PackageUdp does
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:9993")
	config := Config{}
	msg := PackageUdp(buf.Bytes(), addr, &config)

	// Extract the routing key
	routingKey := extractRoutingKeyFromMessage(msg)
	
	// Should return the ServerStart value as string
	assert.Equal(t, "1234567890", routingKey, "Routing key should match ServerStart")
}

// TestExtractRoutingKeyFromMessage_SummaryPacket tests routing key extraction from XML summary packet
func TestExtractRoutingKeyFromMessage_SummaryPacket(t *testing.T) {
	summaryPacket := []byte("<statistics>test</statistics>")
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:9993")
	config := Config{}
	msg := PackageUdp(summaryPacket, addr, &config)

	// Extract the routing key
	routingKey := extractRoutingKeyFromMessage(msg)
	
	// Should return a random key (not empty)
	assert.NotEmpty(t, routingKey, "Routing key should not be empty for summary packet")
	// Should be numeric
	assert.Regexp(t, "^[0-9]+$", routingKey, "Routing key should be numeric")
}

// TestExtractRoutingKeyFromMessage_JSONPacket tests routing key extraction from JSON packet
func TestExtractRoutingKeyFromMessage_JSONPacket(t *testing.T) {
	jsonPacket := []byte(`{"type":"test"}`)
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:9993")
	config := Config{}
	msg := PackageUdp(jsonPacket, addr, &config)

	// Extract the routing key
	routingKey := extractRoutingKeyFromMessage(msg)
	
	// Should return a random key (not empty)
	assert.NotEmpty(t, routingKey, "Routing key should not be empty for JSON packet")
	// Should be numeric
	assert.Regexp(t, "^[0-9]+$", routingKey, "Routing key should be numeric")
}

// TestExtractRoutingKeyFromMessage_SmallPacket tests routing key extraction from a small packet
func TestExtractRoutingKeyFromMessage_SmallPacket(t *testing.T) {
	smallPacket := []byte("short")
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:9993")
	config := Config{}
	msg := PackageUdp(smallPacket, addr, &config)

	// Extract the routing key
	routingKey := extractRoutingKeyFromMessage(msg)
	
	// Should return a random key (not empty)
	assert.NotEmpty(t, routingKey, "Routing key should not be empty for small packet")
	// Should be numeric
	assert.Regexp(t, "^[0-9]+$", routingKey, "Routing key should be numeric")
}

// TestExtractRoutingKeyFromMessage_InvalidJSON tests routing key extraction from invalid JSON
func TestExtractRoutingKeyFromMessage_InvalidJSON(t *testing.T) {
	invalidMsg := []byte("not valid json")

	// Extract the routing key
	routingKey := extractRoutingKeyFromMessage(invalidMsg)
	
	// Should return a random key (not empty)
	assert.NotEmpty(t, routingKey, "Routing key should not be empty for invalid JSON")
	// Should be numeric
	assert.Regexp(t, "^[0-9]+$", routingKey, "Routing key should be numeric")
}
