package collector

import (
	"fmt"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCorrelator_FileOpenClose(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
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
	recs, err := correlator.ProcessPacket(openPacket)
	require.NoError(t, err)
	assert.Nil(t, recs)

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
	recs, err = correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, recs)
	require.Len(t, recs, 1)
	rec := recs[0]

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
	correlator := NewCorrelator(5*time.Second, 0, nil)
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
	recs, err := correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, recs)
	require.Len(t, recs, 1)
	rec := recs[0]

	// Verify record was created
	assert.Equal(t, int64(1000), rec.Read)
	assert.Equal(t, "unknown", rec.Filename)
	assert.Equal(t, 1, rec.HasFileCloseMsg)
}

func TestCorrelator_TimeRecord(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
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
	recs, err := correlator.ProcessPacket(timePacket)
	require.NoError(t, err)
	assert.Nil(t, recs) // Time records don't produce output immediately

	// State should be stored
	assert.Equal(t, 1, correlator.GetStateSize())
}

func TestCorrelator_XMLPacket(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
	defer correlator.Stop()

	xmlPacket := &parser.Packet{
		IsXML:   true,
		RawData: []byte("<stats>test</stats>"),
	}

	recs, err := correlator.ProcessPacket(xmlPacket)
	require.NoError(t, err)
	assert.Nil(t, recs) // XML packets are not correlated
}

func TestCorrelator_RecordAverages(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
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
	if _, err := correlator.ProcessPacket(openPacket); err != nil {
		t.Fatalf("Failed to process open packet: %v", err)
	}

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

	recs, err := correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, recs)
	require.Len(t, recs, 1)
	rec := recs[0]

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
	correlator := NewCorrelator(5*time.Second, 0, nil)
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
	recs, err := correlator.ProcessPacket(userPacket)
	require.NoError(t, err)
	assert.Nil(t, recs) // User packets don't produce output

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

	recs, err = correlator.ProcessPacket(openPacket)
	require.NoError(t, err)
	assert.Nil(t, recs)

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
	recs, err = correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.NotNil(t, recs)
	require.Len(t, recs, 1)
	rec := recs[0]

	// Verify record has user information
	assert.Equal(t, "testuser", rec.User)
	assert.Equal(t, "/DC=org/DC=example/CN=testuser", rec.UserDN)
	assert.Equal(t, "client.example.com", rec.Host)
	assert.Equal(t, "xrootd", rec.Protocol)
	assert.False(t, rec.IPv6)
}

func TestCorrelator_UserRecordWithIPv6(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
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

func TestCorrelator_UserDomainFromIP(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
	defer correlator.Stop()

	// Test with a well-known IP that should resolve (Google DNS)
	// This test might be flaky depending on network, so we'll test both success and failure cases
	userRec := &parser.UserRecord{
		Header: parser.Header{
			ServerStart: 1000,
		},
		DictId: 999,
		UserInfo: parser.UserInfo{
			Username: "testuser",
			Host:     "[::8.8.8.8]", // Google DNS in bracket format
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
		ServerID: serverID,
	}

	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			FileId: 1,
			UserId: 999,
		},
	}

	packet := &parser.Packet{
		Header:     parser.Header{ServerStart: 1000},
		RemoteAddr: "127.0.0.1:9930",
	}

	record := correlator.createCorrelatedRecord(state, closeRec, packet)

	// Verify the record was created
	assert.Equal(t, "testuser", record.User)
	assert.Equal(t, "[::8.8.8.8]", record.Host)
	// UserDomain might be set if reverse DNS succeeds, or empty if it fails
	// We just verify the code doesn't crash
	t.Logf("UserDomain: %s", record.UserDomain)
}

func TestExtractIPFromHost(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		expected string
	}{
		{
			name:     "IPv6 with brackets",
			host:     "[::1234:5678]",
			expected: "1234:5678",
		},
		{
			name:     "IPv6 with brackets and colons",
			host:     "[::192.168.1.1]",
			expected: "192.168.1.1",
		},
		{
			name:     "IPv4",
			host:     "192.168.1.1",
			expected: "192.168.1.1",
		},
		{
			name:     "hostname",
			host:     "example.com",
			expected: "example.com",
		},
		{
			name:     "empty",
			host:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractIPFromHost(tt.host)
			assert.Equal(t, tt.expected, result)
		})
	}
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

func TestCorrelator_ServerInfo(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
	defer correlator.Stop()

	// Create a server info packet ('=' type)
	serverInfoBytes := []byte("&site=TEST_SITE&port=1094&inst=test-instance&pgm=xrootd&ver=5.0.0")
	userInfoBytes := []byte("xrootd/testuser.1234:5678@testhost.example.com")

	// Combine userInfo and serverInfo with newline separator
	mapInfo := append(userInfoBytes, '\n')
	mapInfo = append(mapInfo, serverInfoBytes...)

	serverInfoPacket := &parser.Packet{
		Header: parser.Header{
			Code:        '=',
			ServerStart: 1000,
		},
		PacketType: '=',
		MapRecord: &parser.MapRecord{
			DictId: 999,
			Info:   mapInfo,
		},
		ServerInfo: &parser.ServerInfo{
			Site:     "TEST_SITE",
			Port:     "1094",
			Instance: "test-instance",
			Program:  "xrootd",
			Version:  "5.0.0",
		},
		RemoteAddr: "127.0.0.1:9930",
	}

	// Process the server info packet
	recs, err := correlator.ProcessPacket(serverInfoPacket)
	require.NoError(t, err)
	assert.Nil(t, recs) // Server info packets don't produce records

	// Verify server info was stored
	serverID := "1000#127.0.0.1:9930"
	val, exists := correlator.serverMap.Get(serverID)
	require.True(t, exists, "Server info should be stored")
	serverInfo, ok := val.(*parser.ServerInfo)
	require.True(t, ok, "Server info should be correct type")
	assert.Equal(t, "TEST_SITE", serverInfo.Site)
	assert.Equal(t, "1094", serverInfo.Port)
	assert.Equal(t, "test-instance", serverInfo.Instance)
	assert.Equal(t, "xrootd", serverInfo.Program)
	assert.Equal(t, "5.0.0", serverInfo.Version)

	// Now create a file open/close sequence and verify site is included
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  123,
			UserId:  456,
		},
		FileSize: 1024,
		Lfn:      []byte("/test/file.txt"),
	}

	openPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{openRec},
		RemoteAddr:  "127.0.0.1:9930",
	}

	recs, err = correlator.ProcessPacket(openPacket)
	require.NoError(t, err)
	assert.Nil(t, recs)

	// Create close record
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  123,
			UserId:  456,
		},
		Xfr: parser.StatXFR{
			Read:  1000,
			Readv: 500,
			Write: 0,
		},
		Ops: parser.StatOPS{
			Read:  10,
			Readv: 5,
			Write: 0,
		},
	}

	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 1000,
		},
		FileRecords: []interface{}{closeRec},
		RemoteAddr:  "127.0.0.1:9930",
	}

	recs, err = correlator.ProcessPacket(closePacket)
	require.NoError(t, err)
	require.Len(t, recs, 1)

	// Verify the site was included in the correlated record
	assert.Equal(t, "TEST_SITE", recs[0].Site)
}

func TestCorrelator_ServerInfoTTL(t *testing.T) {
	// Use a very short TTL for testing
	ttl := 200 * time.Millisecond
	correlator := NewCorrelator(ttl, 0, nil)
	defer correlator.Stop()

	serverID := "2000#192.168.1.1:1094"
	serverInfo := &parser.ServerInfo{
		Site:     "TEST_SITE_TTL",
		Port:     "1094",
		Instance: "ttl-test",
		Program:  "xrootd",
		Version:  "5.0.0",
	}

	// Store initial server info
	correlator.handleServerInfo(serverInfo, serverID)

	// Verify it's stored
	val, exists := correlator.serverMap.Get(serverID)
	require.True(t, exists, "Server info should be stored initially")

	// Wait a bit but not long enough for expiry
	time.Sleep(100 * time.Millisecond)

	// Send another server info packet (simulating periodic updates)
	// This should reset the TTL
	correlator.handleServerInfo(serverInfo, serverID)

	// Wait another 150ms (total 250ms from first insert, but only 150ms from refresh)
	time.Sleep(150 * time.Millisecond)

	// Server info should still exist because TTL was reset
	val, exists = correlator.serverMap.Get(serverID)
	assert.True(t, exists, "Server info should still exist after TTL reset")
	if exists {
		info, ok := val.(*parser.ServerInfo)
		require.True(t, ok)
		assert.Equal(t, "TEST_SITE_TTL", info.Site)
	}

	// Wait for TTL to expire (another 100ms, making it 250ms since last refresh)
	time.Sleep(100 * time.Millisecond)

	// Now it should be expired
	_, exists = correlator.serverMap.Get(serverID)
	assert.False(t, exists, "Server info should be expired after TTL without refresh")
}

func TestCorrelator_TokenAugmentsUser(t *testing.T) {
	correlator := NewCorrelator(5*time.Second, 0, nil)
	defer correlator.Stop()

	// First, create a regular user record
	userRec := &parser.UserRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: 1000,
		},
		DictId: 456,
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
			Org:          "ExampleOrg",
		},
	}

	userPacket := &parser.Packet{
		Header:     parser.Header{ServerStart: 1000},
		UserRecord: userRec,
		RemoteAddr: "server.example.com:1094",
	}

	// Process the user packet
	recs, err := correlator.ProcessPacket(userPacket)
	require.NoError(t, err)
	assert.Nil(t, recs)

	// Verify user was stored
	serverID := correlator.getServerID(userPacket)
	userInfoKey := fmt.Sprintf("%s-userinfo-%s", serverID, userInfoString(userRec.UserInfo))
	val, exists := correlator.userMap.Get(userInfoKey)
	require.True(t, exists, "User should be in userMap")
	userState, ok := val.(*UserState)
	require.True(t, ok, "Value should be UserState")
	assert.Equal(t, "", userState.TokenInfo.Subject, "TokenInfo should be empty initially")

	// Now send a token record that references this user
	tokenRec := &parser.UserRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeToken,
			ServerStart: 1000,
		},
		DictId: 999, // Different dictID - token has its own
		TokenInfo: parser.TokenInfo{
			UserDictID: 456, // References the original user's DictId
			Subject:    "CN=testuser,OU=People,DC=example,DC=org",
			Username:   "mappeduser",
			Org:        "TokenOrg",
			Role:       "tokenrole",
			Groups:     "group1 group2",
		},
	}

	tokenPacket := &parser.Packet{
		Header:     parser.Header{ServerStart: 1000},
		UserRecord: tokenRec,
		RemoteAddr: "server.example.com:1094",
	}

	// Process the token packet
	recs, err = correlator.ProcessPacket(tokenPacket)
	require.NoError(t, err)
	assert.Nil(t, recs) // Token packets don't produce output either

	// Verify the original user was augmented with token info
	val, exists = correlator.userMap.Get(userInfoKey)
	require.True(t, exists, "User should still be in userMap")
	userState, ok = val.(*UserState)
	require.True(t, ok, "Value should still be UserState")

	// Check that token info was added
	assert.Equal(t, uint32(456), userState.TokenInfo.UserDictID)
	assert.Equal(t, "CN=testuser,OU=People,DC=example,DC=org", userState.TokenInfo.Subject)
	assert.Equal(t, "mappeduser", userState.TokenInfo.Username)
	assert.Equal(t, "TokenOrg", userState.TokenInfo.Org)
	assert.Equal(t, "tokenrole", userState.TokenInfo.Role)
	assert.Equal(t, "group1 group2", userState.TokenInfo.Groups)

	// Verify original user info wasn't changed
	assert.Equal(t, "testuser", userState.UserInfo.Username)
	assert.Equal(t, "ExampleOrg", userState.AuthInfo.Org)
}
