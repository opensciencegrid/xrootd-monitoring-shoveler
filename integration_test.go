// +build integration

package shoveler

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/collector"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndFileOperation tests the complete flow from packet to collector record
func TestEndToEndFileOperation(t *testing.T) {
	// Create correlator
	correlator := collector.NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Simulate file open packet
	openPacket := createFileOpenPacket(123, 456, 1024, "/test/file.txt")
	packet, err := parser.ParsePacket(openPacket)
	require.NoError(t, err)
	
	record, err := correlator.ProcessPacket(packet)
	require.NoError(t, err)
	assert.Nil(t, record) // Open should not produce a record yet

	// Simulate file close packet
	closePacket := createFileClosePacket(123, 456, 2048, 512, 256)
	packet, err = parser.ParsePacket(closePacket)
	require.NoError(t, err)
	
	record, err = correlator.ProcessPacket(packet)
	require.NoError(t, err)
	require.NotNil(t, record) // Close should produce a correlated record

	// Verify the correlated record
	assert.Equal(t, int64(2048), record.Read)
	assert.Equal(t, int64(512), record.Readv)
	assert.Equal(t, int64(256), record.Write)
	assert.Equal(t, int64(1024), record.Filesize)
	assert.Equal(t, "/test/file.txt", record.Filename)
	assert.Equal(t, 1, record.HasFileCloseMsg)
}

// createFileOpenPacket creates a file open packet for testing
func createFileOpenPacket(fileId, userId uint32, fileSize int64, filename string) []byte {
	lfnBytes := []byte(filename)
	recSize := 16 + 8 + 4 + len(lfnBytes) // file header + filesize + user + lfn
	plen := 8 + recSize // packet header + record
	
	packet := make([]byte, plen)
	
	// Packet header
	packet[0] = parser.PacketTypeFOpen
	packet[1] = 1 // sequence
	binary.BigEndian.PutUint16(packet[2:4], uint16(plen))
	binary.BigEndian.PutUint32(packet[4:8], 1000) // server start
	
	// File header
	offset := 8
	packet[offset] = parser.RecTypeOpen
	packet[offset+1] = 0 // flags
	binary.BigEndian.PutUint16(packet[offset+2:offset+4], uint16(recSize))
	binary.BigEndian.PutUint32(packet[offset+4:offset+8], fileId)
	binary.BigEndian.PutUint32(packet[offset+8:offset+12], userId)
	
	// File size
	offset += 16
	binary.BigEndian.PutUint64(packet[offset:offset+8], uint64(fileSize))
	
	// User
	offset += 8
	binary.BigEndian.PutUint32(packet[offset:offset+4], userId)
	
	// Filename
	offset += 4
	copy(packet[offset:], lfnBytes)
	
	return packet
}

// createFileClosePacket creates a file close packet for testing
func createFileClosePacket(fileId, userId uint32, readBytes, readvBytes, writeBytes int64) []byte {
	recSize := 40 // 16 (header) + 24 (xfr stats)
	plen := 8 + recSize
	
	packet := make([]byte, plen)
	
	// Packet header
	packet[0] = parser.PacketTypeFClose
	packet[1] = 2 // sequence
	binary.BigEndian.PutUint16(packet[2:4], uint16(plen))
	binary.BigEndian.PutUint32(packet[4:8], 1000) // server start
	
	// File header
	offset := 8
	packet[offset] = parser.RecTypeClose
	packet[offset+1] = 0 // flags
	binary.BigEndian.PutUint16(packet[offset+2:offset+4], uint16(recSize))
	binary.BigEndian.PutUint32(packet[offset+4:offset+8], fileId)
	binary.BigEndian.PutUint32(packet[offset+8:offset+12], userId)
	
	// XFR stats
	offset += 16
	binary.BigEndian.PutUint64(packet[offset:offset+8], uint64(readBytes))
	binary.BigEndian.PutUint64(packet[offset+8:offset+16], uint64(readvBytes))
	binary.BigEndian.PutUint64(packet[offset+16:offset+24], uint64(writeBytes))
	
	return packet
}

// TestPacketVerificationFlow tests the verification flow in shoveling mode
func TestPacketVerificationFlow(t *testing.T) {
	// Create a valid XRootD packet
	validPacket := make([]byte, 12)
	validPacket[0] = parser.PacketTypeMap
	validPacket[1] = 1
	binary.BigEndian.PutUint16(validPacket[2:4], 12)
	binary.BigEndian.PutUint32(validPacket[4:8], 1000)
	
	// Verify it passes
	assert.True(t, VerifyPacket(validPacket))
	
	// Create an invalid packet (length mismatch)
	invalidPacket := make([]byte, 10)
	invalidPacket[0] = parser.PacketTypeMap
	invalidPacket[1] = 1
	binary.BigEndian.PutUint16(invalidPacket[2:4], 20) // Claims 20 but only 10 bytes
	
	// Verify it fails
	assert.False(t, VerifyPacket(invalidPacket))
}
