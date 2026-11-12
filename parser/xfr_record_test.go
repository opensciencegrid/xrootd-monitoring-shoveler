package parser

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An in-flight XFR record is decoded rather than skipped, and the record after
// it is still found. The leading FileTOD is the first record.
func TestParsePacket_FileXFRRecord(t *testing.T) {
	// 8 byte header + 24 byte FileTOD + 32 byte XFR + 32 byte close
	data := createHeader(PacketTypeFStat, 96)

	tod := make([]byte, 24)
	tod[0] = RecTypeTime
	binary.BigEndian.PutUint16(tod[2:4], 24)
	data = append(data, tod...)

	xfr := make([]byte, 32)
	xfr[0] = RecTypeXFR
	binary.BigEndian.PutUint16(xfr[2:4], 32)
	binary.BigEndian.PutUint32(xfr[4:8], 222)     // file id
	binary.BigEndian.PutUint64(xfr[8:16], 4096)   // read bytes so far
	binary.BigEndian.PutUint64(xfr[16:24], 8192)  // readv bytes so far
	binary.BigEndian.PutUint64(xfr[24:32], 16384) // write bytes so far
	data = append(data, xfr...)

	cls := make([]byte, 32)
	cls[0] = RecTypeClose
	binary.BigEndian.PutUint16(cls[2:4], 32)
	binary.BigEndian.PutUint32(cls[4:8], 222)
	data = append(data, cls...)

	packet, err := ParsePacket(data)
	require.NoError(t, err)
	require.Len(t, packet.FileRecords, 3)

	x, ok := packet.FileRecords[1].(FileXFRRecord)
	require.True(t, ok, "first record is the XFR snapshot")
	assert.Equal(t, uint32(222), x.Header.FileId)
	assert.Equal(t, int64(4096), x.Xfr.Read)
	assert.Equal(t, int64(8192), x.Xfr.Readv)
	assert.Equal(t, int64(16384), x.Xfr.Write)

	_, ok = packet.FileRecords[2].(FileCloseRecord)
	assert.True(t, ok, "the close after it is still parsed")
}
