package main

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
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
	rand.Seed(time.Now().UnixNano())
	rand.Read(token)
	err = binary.Write(buf, binary.BigEndian, token)
	assert.NoError(t, err, "Failed to write random to binary buffer")

	assert.True(t, verifyPacket(buf.Bytes()), "Failed to verify packet")

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
	rand.Seed(time.Now().UnixNano())
	rand.Read(token)
	err = binary.Write(buf, binary.BigEndian, token)
	assert.NoError(t, err, "Failed to write random to binary buffer")

	assert.False(t, verifyPacket(buf.Bytes()), "Failed to verify packet")
}
