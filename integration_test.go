//go:build integration
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
	correlator := collector.NewCorrelator(5*time.Second, 0, nil)
	defer correlator.Stop()

	// Simulate an f-stream packet carrying a file open
	openPacket := createFStreamPacket(1, 1000, 1030, createFileOpenRecord(123, 456, 1024, "/test/file.txt"))
	packet, err := parser.ParsePacket(openPacket)
	require.NoError(t, err)

	open := requireSingleFileRecord[parser.FileOpenRecord](t, packet)
	assert.Equal(t, uint32(123), open.Header.FileId)
	assert.Equal(t, uint32(456), open.User)
	assert.Equal(t, int64(1024), open.FileSize)
	assert.Equal(t, "/test/file.txt", string(open.Lfn))

	records, err := correlator.ProcessPacket(packet)
	require.NoError(t, err)
	assert.Empty(t, records) // Open should not produce a record yet

	// Simulate an f-stream packet carrying the matching file close
	closePacket := createFStreamPacket(2, 1060, 1090, createFileCloseRecord(123, 2048, 512, 256))
	packet, err = parser.ParsePacket(closePacket)
	require.NoError(t, err)

	closeRec := requireSingleFileRecord[parser.FileCloseRecord](t, packet)
	assert.Equal(t, uint32(123), closeRec.Header.FileId)

	records, err = correlator.ProcessPacket(packet)
	require.NoError(t, err)
	require.Len(t, records, 1) // Close should produce a correlated record

	// Verify the correlated record
	record := records[0]
	assert.Equal(t, int64(2048), record.Read)
	assert.Equal(t, int64(512), record.Readv)
	assert.Equal(t, int64(256), record.Write)
	assert.Equal(t, int64(1024), record.Filesize)
	assert.Equal(t, "/test/file.txt", record.Filename)
	assert.Equal(t, 1, record.HasFileCloseMsg)
}

// requireSingleFileRecord returns the only file record of type T in packet.
// The leading XrdXrootdMonFileTOD may or may not be included in FileRecords,
// so it is ignored here.
func requireSingleFileRecord[T any](t *testing.T, packet *parser.Packet) T {
	t.Helper()

	var found []T
	for _, rec := range packet.FileRecords {
		switch r := rec.(type) {
		case parser.FileTimeRecord:
			continue
		case T:
			found = append(found, r)
		default:
			t.Fatalf("unexpected file record %T", rec)
		}
	}
	require.Len(t, found, 1)
	return found[0]
}

// createFStreamPacket creates an f-stream packet: the 8-byte packet header,
// the 24-byte XrdXrootdMonFileTOD bounding the window [tBeg, tEnd], then the
// given file records.
func createFStreamPacket(pseq uint8, tBeg, tEnd uint32, records ...[]byte) []byte {
	plen := 8 + 24
	for _, rec := range records {
		plen += len(rec)
	}

	// Packet header
	b := []byte{parser.PacketTypeFStat, pseq}
	b = binary.BigEndian.AppendUint16(b, uint16(plen))
	b = binary.BigEndian.AppendUint32(b, 1000) // server start

	// FileTOD: recType, recFlag, recSize, nRecs[0] (xfr records), nRecs[1] (total records), tBeg, tEnd, sid
	b = append(b, parser.RecTypeTime, 0)
	b = binary.BigEndian.AppendUint16(b, 24)
	b = binary.BigEndian.AppendUint16(b, 0)
	b = binary.BigEndian.AppendUint16(b, uint16(len(records)))
	b = binary.BigEndian.AppendUint32(b, tBeg)
	b = binary.BigEndian.AppendUint32(b, tEnd)
	b = binary.BigEndian.AppendUint64(b, 42) // sid

	for _, rec := range records {
		b = append(b, rec...)
	}
	return b
}

// createFileOpenRecord creates an open record with the hasLFN flag set: the
// 8-byte file header, the file size, then the user dictid and the
// null-terminated filename, padded to a 4-byte boundary.
func createFileOpenRecord(fileId, userId uint32, fileSize int64, filename string) []byte {
	lfn := append([]byte(filename), 0)
	for len(lfn)%4 != 0 {
		lfn = append(lfn, 0)
	}
	recSize := 8 + 8 + 4 + len(lfn)

	b := []byte{parser.RecTypeOpen, 0x01} // hasLFN
	b = binary.BigEndian.AppendUint16(b, uint16(recSize))
	b = binary.BigEndian.AppendUint32(b, fileId)
	b = binary.BigEndian.AppendUint64(b, uint64(fileSize))
	b = binary.BigEndian.AppendUint32(b, userId)
	return append(b, lfn...)
}

// createFileCloseRecord creates a close record: the 8-byte file header
// followed by the read, readv and write byte counts.
func createFileCloseRecord(fileId uint32, readBytes, readvBytes, writeBytes int64) []byte {
	b := []byte{parser.RecTypeClose, 0}
	b = binary.BigEndian.AppendUint16(b, 32) // record size
	b = binary.BigEndian.AppendUint32(b, fileId)
	b = binary.BigEndian.AppendUint64(b, uint64(readBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(readvBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(writeBytes))
	return b
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
