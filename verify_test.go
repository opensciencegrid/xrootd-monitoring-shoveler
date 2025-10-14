package shoveler

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGoodVerify tests the good validation
func TestGoodVerify(t *testing.T) {
	goodHeader := Header{}
	goodHeader.Plen = 16
	goodHeader.ServerStart = 12345
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &goodHeader)
	assert.NoError(t, err, "Failed to write to binary buffer")

	// Generate 8 random bytes
	token := make([]byte, 8)
	_, err = rand.Read(token)
	assert.NoError(t, err, "Failed to create random bytes")
	err = binary.Write(buf, binary.BigEndian, token)
	assert.NoError(t, err, "Failed to write random to binary buffer")

	routingKey, err := VerifyPacket(buf.Bytes())
	assert.NoError(t, err, "Failed to verify packet")
	assert.Equal(t, "12345", routingKey, "Routing key should match ServerStart time")

}

func TestVerifySummaryPacket(t *testing.T) {
	summaryPacket := `<statistics  
     tod="int64" ver="chars" src=”chars” tos=”int64”
     pgm=”chars” ins=”chars” pid=”int” site=”chars”>
	</statistics>
	`

	routingKey, err := VerifyPacket([]byte(summaryPacket))
	assert.NoError(t, err, "Failed to verify packet")
	assert.Contains(t, routingKey, "summary-", "Routing key should start with 'summary-' for XML packets")
}

// TestBadVerify tests the validation if the packets are not good (random bits)
func TestBadVerify(t *testing.T) {
	badHeader := Header{}
	badHeader.Plen = 17
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &badHeader)
	assert.NoError(t, err, "Failed to write to binary buffer")

	// Generate 8 random bytes
	token := make([]byte, 8)
	_, err = rand.Read(token)
	assert.NoError(t, err, "Failed to create random bytes")
	err = binary.Write(buf, binary.BigEndian, token)
	assert.NoError(t, err, "Failed to write random to binary buffer")

	_, err = VerifyPacket(buf.Bytes())
	assert.Error(t, err, "Should return error for invalid packet")
}

// TestVerifyJsonPacket tests verification of JSON packets
func TestVerifyJsonPacket(t *testing.T) {
	jsonPacket := `{"test": "data", "some": "json"}`

	routingKey, err := VerifyPacket([]byte(jsonPacket))
	assert.NoError(t, err, "Failed to verify JSON packet")
	assert.Contains(t, routingKey, "json-", "Routing key should start with 'json-' for JSON packets")
}

// TestVerifyTooSmallPacket tests verification of packets that are too small
func TestVerifyTooSmallPacket(t *testing.T) {
	tooSmall := []byte("small")

	_, err := VerifyPacket(tooSmall)
	assert.Error(t, err, "Should return error for packet too small")
}
