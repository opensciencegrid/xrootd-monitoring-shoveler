package collector

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadCapturedPackets returns the given lines of tests/use_case — f-stream
// packets captured from production XRootD servers, one base64 JSON message per
// line — parsed with parser.ParsePacket and stamped with the message's remote
// address, as the collector's message-bus input does.
func loadCapturedPackets(t *testing.T, lineNums ...int) []*parser.Packet {
	t.Helper()

	raw, err := os.ReadFile("../tests/use_case")
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))

	packets := make([]*parser.Packet, 0, len(lineNums))
	for _, n := range lineNums {
		require.Greater(t, len(lines), n)

		var msg struct {
			Data   string `json:"data"`
			Remote string `json:"remote"`
		}
		require.NoError(t, json.Unmarshal(lines[n], &msg))
		data, err := base64.StdEncoding.DecodeString(msg.Data)
		require.NoError(t, err)

		packet, err := parser.ParsePacket(data)
		require.NoError(t, err)
		packet.RemoteAddr = msg.Remote
		packets = append(packets, packet)
	}
	return packets
}

// TestFileTODWindowFromCapturedPackets runs two consecutive f-stream packets
// from one XRootD server (tests/use_case lines 1 and 2, December 2021) through
// ParsePacket and the Correlator. Each packet leads with an XrdXrootdMonFileTOD
// whose tBeg/tEnd bound when its file events happened; start_time and end_time
// must come from those windows, not from when the collector processed the
// packet. Regression test for collector v2.4.0, where the parser dropped the
// leading FileTOD and every record fell back to time.Now() (operation_time 0).
func TestFileTODWindowFromCapturedPackets(t *testing.T) {
	const (
		win1Beg = int64(1639615464) // line 1 FileTOD tBeg
		win1End = int64(1639615519) // line 1 FileTOD tEnd
		win2Beg = int64(1639615700) // line 2 FileTOD tBeg
		win2End = int64(1639615759) // line 2 FileTOD tEnd
	)

	correlator := NewCorrelator(5*time.Minute, 0, nil)
	defer correlator.Stop()

	packets := loadCapturedPackets(t, 1, 2)

	// Line 1: four files opened and closed within the same window.
	first := packets[0]
	require.NotEmpty(t, first.FileRecords)
	tod, ok := first.FileRecords[0].(parser.FileTimeRecord)
	require.True(t, ok, "first f-stream record should be the leading FileTOD, got %T", first.FileRecords[0])
	assert.Equal(t, int32(win1Beg), tod.TBeg)
	assert.Equal(t, int32(win1End), tod.TEnd)
	assert.Equal(t, int64(30659922307477), tod.SID)
	assert.Equal(t, int16(15), tod.Header.NRecs1, "nRecs[1] is the total record count")

	recs, err := correlator.ProcessPacket(first)
	require.NoError(t, err)
	require.Len(t, recs, 4)
	for _, rec := range recs {
		assert.Equal(t, win1Beg, rec.StartTime, "start_time of %s", rec.Filename)
		assert.Equal(t, win1End, rec.EndTime, "end_time of %s", rec.Filename)
		assert.Equal(t, win1End-win1Beg, rec.OperationTime, "operation_time of %s", rec.Filename)
	}

	// Line 2: the close of a file opened in line 1 (fileID 37295), two files
	// opened and closed in this window, and three closes whose opens predate
	// the capture.
	const crossPacketLfn = "/store/mc/HC/GenericTTbar/AODSIM/CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/00000/64558671-1776-E711-806B-FA163E995805.root"
	recs, err = correlator.ProcessPacket(packets[1])
	require.NoError(t, err)
	require.Len(t, recs, 6)

	var crossPacket, standalone int
	for _, rec := range recs {
		assert.Equal(t, win2End, rec.EndTime, "end_time of %s", rec.Filename)
		switch rec.Filename {
		case crossPacketLfn:
			// Opened in line 1's window, closed in line 2's.
			crossPacket++
			assert.Equal(t, win1Beg, rec.StartTime)
			assert.Equal(t, win2End-win1Beg, rec.OperationTime)
		case "unknown":
			// No open seen: the close window is the best bound available.
			standalone++
			assert.Equal(t, win2Beg, rec.StartTime)
			assert.Equal(t, win2End-win2Beg, rec.OperationTime)
		default:
			assert.Equal(t, win2Beg, rec.StartTime, "start_time of %s", rec.Filename)
			assert.Equal(t, win2End-win2Beg, rec.OperationTime, "operation_time of %s", rec.Filename)
		}
	}
	assert.Equal(t, 1, crossPacket, "one close should correlate with its open from the previous packet")
	assert.Equal(t, 3, standalone, "three closes have no matching open")
}
