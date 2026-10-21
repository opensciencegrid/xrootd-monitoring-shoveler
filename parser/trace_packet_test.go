package parser

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTracePacket assembles a t-stream packet: the common 8-byte header
// followed by the given fixed 16-byte XrdXrootdMonTrace entries.
func buildTracePacket(t *testing.T, entries ...[16]byte) []byte {
	t.Helper()

	plen := 8 + 16*len(entries)
	b := make([]byte, 0, plen)
	b = append(b, PacketTypeTrace, 0x01) // code 't', pseq
	b = binary.BigEndian.AppendUint16(b, uint16(plen))
	b = binary.BigEndian.AppendUint32(b, 1782994817) // stod (server start)
	for _, e := range entries {
		b = append(b, e[:]...)
	}
	return b
}

// traceReadEntry builds a read/write XrdXrootdMonTrace entry: a 64-bit file
// offset, a 32-bit transfer length, and the dictid of the file.
func traceReadEntry(offset uint64, length, dictid uint32) [16]byte {
	var e [16]byte
	binary.BigEndian.PutUint64(e[0:8], offset)
	binary.BigEndian.PutUint32(e[8:12], length)
	binary.BigEndian.PutUint32(e[12:16], dictid)
	return e
}

// TestTracePacketProducesNoFileRecords verifies that t-stream packets are
// accepted but never fed through the f-stream record walker. The t-stream
// consists of fixed 16-byte entries with no recType/recFlag/recSize header;
// misaligned parsing of those entries used to fabricate file-close records
// with garbage 64-bit byte counters (observed in production as PB/EiB-scale
// transfer spikes on records with filename "unknown").
//
// See issue #100 (opensciencegrid/xrootd-monitoring-shoveler).
func TestTracePacketProducesNoFileRecords(t *testing.T) {
	// Window mark entry (0xe0): the usual first entry of a trace buffer.
	window := [16]byte{0: 0xe0}
	binary.BigEndian.PutUint32(window[8:12], 1782994800)  // prev window end
	binary.BigEndian.PutUint32(window[12:16], 1782994830) // this window start

	// Close entry (0xc0): rRshift, wRshift, pad, rTot, wTot, dictid.
	closeEntry := [16]byte{0: 0xc0, 1: 0x02, 2: 0x01} // non-zero wRshift at byte 2:
	// bytes 2-3 previously misread as an f-stream recSize, starting the
	// misaligned walk that fabricated close records.
	binary.BigEndian.PutUint32(closeEntry[4:8], 0x2FCBE0C0)  // rTot
	binary.BigEndian.PutUint32(closeEntry[8:12], 0x2FC71A80) // wTot
	binary.BigEndian.PutUint32(closeEntry[12:16], 12235)     // dictid

	// Zero-heavy entry: a stray 0x00 first byte previously matched
	// RecTypeClose and became a fabricated 64-bit counter record.
	zeroHeavy := [16]byte{2: 0x00, 3: 0x20} // bytes 2-3 = 32, a "valid" recSize
	binary.BigEndian.PutUint32(zeroHeavy[8:12], 0x05002FD2)
	binary.BigEndian.PutUint32(zeroHeavy[12:16], 0x28C00000)

	// The two constructions below are the ones that actually fabricate records
	// when t-stream data is fed to the f-stream walker. Both are verified to
	// produce a bogus FileCloseRecord against the pre-fix parser, so they fail
	// if the 't' case is ever routed back through parseFileRecords.

	// A close trace entry (0xc0) whose bytes 2-3 read as an f-stream recSize of
	// 24. The walker does not recognise recType 0xc0, so its "unknown type"
	// branch seeks 24 bytes from the record start -- landing mid-entry, on the
	// following entry's 32-bit length field rather than on an entry boundary.
	misalignClose := [16]byte{0: 0xc0}
	binary.BigEndian.PutUint16(misalignClose[2:4], 24)
	binary.BigEndian.PutUint32(misalignClose[4:8], 0x2FCBE0C0)
	binary.BigEndian.PutUint32(misalignClose[8:12], 0x2FC71A80)
	binary.BigEndian.PutUint32(misalignClose[12:16], 12235)

	// Ordinary 56-byte reads. Nothing about them is unusual, and that is the
	// point: the top byte of a 64-bit offset is 0x00, which equals RecTypeClose,
	// and the low 16 bits of the length (0x0038 = 56) are an in-range recSize.
	// A misaligned cursor landing here therefore reads a "close record" whose
	// 64-bit byte counters are splices of offsets and dictids -- the PB/EiB-scale
	// transfer spikes reported in production.
	smallRead1 := traceReadEntry(0x0000000012345678, 56, 12235)
	smallRead2 := traceReadEntry(0x00000000abcdef01, 56, 12235)
	smallRead3 := traceReadEntry(0x0000000000112233, 56, 12235)
	smallRead4 := traceReadEntry(0x00000000deadbeef, 56, 12235)

	// A read at a genuinely large offset, where bits 47:32 of the offset land
	// in the accepted recSize range on their own -- no preceding misalignment
	// needed for the very first entry to be mistaken for a close record.
	largeOffsetRead := traceReadEntry(0x0000003800001000, 65536, 12235)

	cases := []struct {
		name    string
		entries [][16]byte
	}{
		{"window_only", [][16]byte{window}},
		{"window_and_close", [][16]byte{window, closeEntry}},
		{"misparse_bait", [][16]byte{closeEntry, zeroHeavy, closeEntry, zeroHeavy}},
		{"misaligned_close_then_reads", [][16]byte{
			misalignClose, smallRead1, smallRead2, smallRead3, smallRead4, smallRead1,
		}},
		{"read_at_large_offset", [][16]byte{
			largeOffsetRead, smallRead1, smallRead2, smallRead3, smallRead4, smallRead1,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := buildTracePacket(t, tc.entries...)

			packet, err := ParsePacket(raw)
			require.NoError(t, err, "t-stream packets must still be accepted (shoveling mode)")
			require.NotNil(t, packet)

			assert.Equal(t, PacketTypeTrace, packet.PacketType)
			assert.Empty(t, packet.FileRecords,
				"t-stream packets must not produce file records; the f-stream walker fabricates garbage counters from 16-byte trace entries")
			assert.Equal(t, raw, packet.RawData, "raw data must be preserved for shoveling")
		})
	}
}
