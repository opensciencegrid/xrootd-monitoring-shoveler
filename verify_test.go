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
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, &goodHeader)
	assert.NoError(t, err, "Failed to write to binary buffer")

	// Generate 8 random bytes
	token := make([]byte, 8)
	_, err = rand.Read(token)
	assert.NoError(t, err, "Failed to create random bytes")
	err = binary.Write(buf, binary.BigEndian, token)
	assert.NoError(t, err, "Failed to write random to binary buffer")

	assert.True(t, VerifyPacket(buf.Bytes()), "Failed to verify packet")

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

	assert.False(t, VerifyPacket(buf.Bytes()), "Failed to verify packet")
}
