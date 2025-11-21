package collector

import (
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCorrelator_FileOpenClose(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Create an open record
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  123,
			UserId:  456,
		},
		FileSize: 1024,
		User:     456,
		Lfn:      []byte("/path/to/file.txt"),
	}

	openPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{openRec},
	}

	// Process open - should not return a record
	rec, err := correlator.ProcessPacket(openPacket)
	require.NoError(t, err)
	assert.Nil(t, rec)

	// Verify state was stored
	assert.Equal(t, 1, correlator.GetStateSize())

	// Create a close record
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  123,
			UserId:  456,
		},
		Xfr: parser.StatXFR{
			Read:  2048,
			Readv: 512,
			Write: 256,
		},
		Ops: parser.StatOPS{
			Read:  10,
			Readv: 2,
			Write: 1,
			RdMin: 100,
			RdMax: 500,
		},
	}

	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{closeRec},
	}

	// Process close - should return a correlated record
	rec, err = correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify record fields
	assert.Equal(t, int64(2048), rec.Read)
	assert.Equal(t, int64(512), rec.Readv)
	assert.Equal(t, int64(256), rec.Write)
	assert.Equal(t, int32(10), rec.ReadOperations)
	assert.Equal(t, int32(2), rec.ReadVectorOperations)
	assert.Equal(t, int32(1), rec.WriteOperations)
	assert.Equal(t, int64(1024), rec.Filesize)
	assert.Equal(t, "/path/to/file.txt", rec.Filename)
	assert.Equal(t, 1, rec.HasFileCloseMsg)

	// State should be removed after correlation
	assert.Equal(t, 0, correlator.GetStateSize())
}

func TestCorrelator_CloseWithoutOpen(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Create a close record without a prior open
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  999,
			UserId:  777,
		},
		Xfr: parser.StatXFR{
			Read:  1000,
			Readv: 200,
			Write: 100,
		},
	}

	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{closeRec},
	}

	// Process close - should return a standalone record
	rec, err := correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify record was created
	assert.Equal(t, int64(1000), rec.Read)
	assert.Equal(t, "unknown", rec.Filename)
	assert.Equal(t, 1, rec.HasFileCloseMsg)
}

func TestCorrelator_TimeRecord(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	timeRec := parser.FileTimeRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeTime,
			FileId:  111,
			UserId:  222,
		},
		TBeg: 1000,
		TEnd: 2000,
		SID:  333,
	}

	timePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{timeRec},
	}

	// Process time record
	rec, err := correlator.ProcessPacket(timePacket)
	require.NoError(t, err)
	assert.Nil(t, rec) // Time records don't produce output immediately

	// State should be stored
	assert.Equal(t, 1, correlator.GetStateSize())
}

func TestCorrelator_XMLPacket(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	xmlPacket := &parser.Packet{
		IsXML:   true,
		RawData: []byte("<stats>test</stats>"),
	}

	rec, err := correlator.ProcessPacket(xmlPacket)
	require.NoError(t, err)
	assert.Nil(t, rec) // XML packets are not correlated
}

func TestCorrelator_RecordAverages(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Create open
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			FileId: 123,
			UserId: 456,
		},
		FileSize: 1024,
		User:     456,
		Lfn:      []byte("/test.txt"),
	}
	openPacket := &parser.Packet{
		Header:      parser.Header{ServerStart: 1000},
		FileRecords: []interface{}{openRec},
	}
	correlator.ProcessPacket(openPacket)

	// Create close with specific values for average calculation
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			FileId: 123,
			UserId: 456,
		},
		Xfr: parser.StatXFR{
			Read:  1000,
			Readv: 600,
			Write: 300,
		},
		Ops: parser.StatOPS{
			Read:  10, // 1000 / 10 = 100 average
			Readv: 3,  // 600 / 3 = 200 average
			Write: 2,  // 300 / 2 = 150 average
			Rsegs: 15, // 15 / 3 = 5 segments per readv
		},
	}
	closePacket := &parser.Packet{
		Header:      parser.Header{ServerStart: 1000},
		FileRecords: []interface{}{closeRec},
	}

	rec, err := correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Check calculated averages
	assert.Equal(t, int64(100), rec.ReadAverage)
	assert.Equal(t, int64(200), rec.ReadVectorAverage)
	assert.Equal(t, int64(150), rec.WriteAverage)
	assert.Equal(t, float64(5), rec.ReadVectorCountAverage)
}

func TestCollectorRecord_ToJSON(t *testing.T) {
	record := &CollectorRecord{
		StartTime:       1000,
		EndTime:         2000,
		Read:            1024,
		Write:           512,
		Filename:        "/test.txt",
		HasFileCloseMsg: 1,
	}

	data, err := record.ToJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), "start_time")
	assert.Contains(t, string(data), "1024")
	assert.Contains(t, string(data), "/test.txt")
}

func TestCorrelator_UserRecord(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Create a user record
	userRec := &parser.UserRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: 1000,
		},
		DictId: 456, // This matches the UserID in file operations
		UserInfo: parser.UserInfo{
			Protocol: "xrootd",
			Username: "testuser",
			Pid:      12345,
			Sid:      67890,
			Host:     "client.example.com",
		},
		AuthInfo: parser.AuthInfo{
			AuthProtocol: "gsi",
			DN:           "/DC=org/DC=example/CN=testuser",
			Hostname:     "client.example.com",
			Org:          "ExampleOrg",
			Role:         "production",
			InetVersion:  "4",
		},
	}

	userPacket := &parser.Packet{
		Header:     parser.Header{ServerStart: 1000},
		UserRecord: userRec,
	}

	// Process user packet
	rec, err := correlator.ProcessPacket(userPacket)
	require.NoError(t, err)
	assert.Nil(t, rec) // User packets don't produce output

	// Verify user map was populated
	assert.Equal(t, 1, correlator.GetUserMapSize())

	// Now create file open and close with this user ID
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  123,
			UserId:  456, // Matches dictId from user record
		},
		FileSize: 1024,
		User:     456,
		Lfn:      []byte("/path/to/file.txt"),
	}

	openPacket := &parser.Packet{
		Header:      parser.Header{ServerStart: 1000},
		FileRecords: []interface{}{openRec},
	}

	rec, err = correlator.ProcessPacket(openPacket)
	require.NoError(t, err)
	assert.Nil(t, rec)

	// Create close record
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  123,
			UserId:  456,
		},
		Xfr: parser.StatXFR{
			Read:  2048,
			Readv: 512,
			Write: 256,
		},
		Ops: parser.StatOPS{
			Read:  10,
			Readv: 2,
			Write: 1,
		},
	}

	closePacket := &parser.Packet{
		Header:      parser.Header{ServerStart: 1000},
		FileRecords: []interface{}{closeRec},
	}

	// Process close - should return a correlated record with user info
	rec, err = correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify record has user information
	assert.Equal(t, "testuser", rec.User)
	assert.Equal(t, "/DC=org/DC=example/CN=testuser", rec.UserDN)
	assert.Equal(t, "client.example.com", rec.Host)
	assert.Equal(t, "xrootd", rec.Protocol)
	assert.False(t, rec.IPv6)
}

func TestCorrelator_UserRecordWithIPv6(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0)
	defer correlator.Stop()

	// Create a user record with IPv6
	userRec := &parser.UserRecord{
		Header: parser.Header{
			ServerStart: 1000, // Must match the SID used in file operations
		},
		DictId: 999,
		UserInfo: parser.UserInfo{
			Username: "ipv6user",
			Host:     "2001:db8::1",
		},
		AuthInfo: parser.AuthInfo{
			InetVersion: "6",
		},
	}

	serverID := "1000#127.0.0.1:9930"
	correlator.handleUserRecord(userRec, serverID)

	// Create and process a close with this user
	state := &FileState{
		FileID:   1,
		UserID:   999,
		OpenTime: 1000,
		Filename: "/test.txt",
		ServerID: serverID, // Include serverID in the state
	}

	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			FileId: 1,
			UserId: 999,
		},
	}

	packet := &parser.Packet{
		Header:     parser.Header{ServerStart: 1000},
		RemoteAddr: "127.0.0.1:9930", // Set RemoteAddr to match serverID
	}

	record := correlator.createCorrelatedRecord(state, closeRec, packet)

	// Verify IPv6 flag is set
	assert.True(t, record.IPv6)
	assert.Equal(t, "ipv6user", record.User)
	assert.Equal(t, "2001:db8::1", record.Host)
}

func TestExtractDirnames(t *testing.T) {
	tests := []struct {
		name            string
		filename        string
		expectedDir1    string
		expectedDir2    string
		expectedLogical string
	}{
		{
			name:            "user path",
			filename:        "/user/johndoe/data/file.txt",
			expectedDir1:    "/user",
			expectedDir2:    "/user/johndoe",
			expectedLogical: "/user/johndoe",
		},
		{
			name:            "osgconnect public",
			filename:        "/osgconnect/public/user/project/file.txt",
			expectedDir1:    "/osgconnect",
			expectedDir2:    "/osgconnect/public",
			expectedLogical: "/osgconnect/public/user",
		},
		{
			name:            "ospool path",
			filename:        "/ospool/ap21/data/username/file.txt",
			expectedDir1:    "/ospool",
			expectedDir2:    "/ospool/ap21",
			expectedLogical: "/ospool/ap21/data/username",
		},
		{
			name:            "path-facility",
			filename:        "/path-facility/data/username/file.txt",
			expectedDir1:    "/path-facility",
			expectedDir2:    "/path-facility/data",
			expectedLogical: "/path-facility/data/username",
		},
		{
			name:            "hcc path",
			filename:        "/hcc/part1/part2/part3/part4/part5/file.txt",
			expectedDir1:    "/hcc",
			expectedDir2:    "/hcc/part1",
			expectedLogical: "/hcc/part1/part2/part3/part4",
		},
		{
			name:            "pnfs fnal",
			filename:        "/pnfs/fnal.gov/usr/dir1/dir2/file.txt",
			expectedDir1:    "/pnfs",
			expectedDir2:    "/pnfs/fnal.gov",
			expectedLogical: "/pnfs/fnal.gov/usr/dir1",
		},
		{
			name:            "gwdata",
			filename:        "/gwdata/project/file.txt",
			expectedDir1:    "/gwdata",
			expectedDir2:    "/gwdata/project",
			expectedLogical: "/gwdata/project",
		},
		{
			name:            "chtc path",
			filename:        "/chtc/data/file.txt",
			expectedDir1:    "/chtc",
			expectedDir2:    "/chtc/data",
			expectedLogical: "/chtc",
		},
		{
			name:            "icecube path",
			filename:        "/icecube/data/file.txt",
			expectedDir1:    "/icecube",
			expectedDir2:    "/icecube/data",
			expectedLogical: "/icecube",
		},
		{
			name:            "igwn path",
			filename:        "/igwn/ligo/data/file.txt",
			expectedDir1:    "/igwn",
			expectedDir2:    "/igwn/ligo",
			expectedLogical: "/igwn/ligo/data",
		},
		{
			name:            "store path (CMS)",
			filename:        "/store/user/data/file.txt",
			expectedDir1:    "/store",
			expectedDir2:    "/store/user",
			expectedLogical: "/store/user",
		},
		{
			name:            "unknown path",
			filename:        "/some/random/path/file.txt",
			expectedDir1:    "/some",
			expectedDir2:    "/some/random",
			expectedLogical: "unknown directory",
		},
		{
			name:            "empty filename",
			filename:        "",
			expectedDir1:    "unknown directory",
			expectedDir2:    "unknown directory",
			expectedLogical: "unknown directory",
		},
		{
			name:            "root only",
			filename:        "/",
			expectedDir1:    "unknown directory",
			expectedDir2:    "unknown directory",
			expectedLogical: "unknown directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir1, dir2, logical := extractDirnames(tt.filename)
			assert.Equal(t, tt.expectedDir1, dir1, "dirname1 mismatch")
			assert.Equal(t, tt.expectedDir2, dir2, "dirname2 mismatch")
			assert.Equal(t, tt.expectedLogical, logical, "logical_dirname mismatch")
		})
	}
}
