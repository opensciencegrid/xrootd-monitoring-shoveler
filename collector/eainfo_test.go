package collector

import (
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseEAInfo tests the parseEAInfo function. The 'U' (MAPUEAC) stream
// carries numeric SciTags ids in Ec/Ac (not names), so parseEAInfo returns ints
// and treats empty/non-numeric values as 0 ("unset").
func TestParseEAInfo(t *testing.T) {
	tests := []struct {
		name         string
		eaInfo       string
		expectedUdid uint32
		expectedExp  int
		expectedAct  int
	}{
		{
			name:         "Complete eainfo",
			eaInfo:       "&Uc=1234&Ec=2&Ac=15",
			expectedUdid: 1234,
			expectedExp:  2,
			expectedAct:  15,
		},
		{
			name:         "Missing activity",
			eaInfo:       "&Uc=5678&Ec=3",
			expectedUdid: 5678,
			expectedExp:  3,
			expectedAct:  0,
		},
		{
			name:         "Empty values",
			eaInfo:       "&Uc=9999&Ec=&Ac=",
			expectedUdid: 9999,
			expectedExp:  0,
			expectedAct:  0,
		},
		{
			name:         "No leading ampersand",
			eaInfo:       "Uc=111&Ec=5&Ac=4",
			expectedUdid: 111,
			expectedExp:  5,
			expectedAct:  4,
		},
		{
			name:         "Non-numeric ids resolve to zero",
			eaInfo:       "&Uc=222&Ec=ATLAS&Ac=production",
			expectedUdid: 222,
			expectedExp:  0,
			expectedAct:  0,
		},
		{
			name:         "Invalid format",
			eaInfo:       "invalid",
			expectedUdid: 0,
			expectedExp:  0,
			expectedAct:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			udid, exp, act := parseEAInfo(tt.eaInfo)
			assert.Equal(t, tt.expectedUdid, udid, "udid mismatch")
			assert.Equal(t, tt.expectedExp, exp, "experiment id mismatch")
			assert.Equal(t, tt.expectedAct, act, "activity id mismatch")
		})
	}
}

// TestCorrelator_EAInfoPacket tests the handling of eainfo packets
func TestCorrelator_EAInfoPacket(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, logger)
	defer correlator.Stop()

	serverID := "1639505770#192.168.1.100:1094"

	// First, create a user record (simulating a 'u' packet)
	userRec := &parser.UserRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: 1639505770,
		},
		DictId: 100,
		UserInfo: parser.UserInfo{
			Protocol: "root",
			Username: "testuser",
			Pid:      1234,
			Sid:      5678,
			Host:     "client.example.com",
		},
		AuthInfo: parser.AuthInfo{
			DN:  "/DC=org/DC=example/OU=People/CN=Test User",
			Org: "cms",
		},
	}

	// Process user record
	correlator.handleUserRecord(userRec, serverID)

	// Verify user is stored
	userKey := "1639505770#192.168.1.100:1094-userinfo-root/testuser.1234:5678@client.example.com"
	val, exists := correlator.userMap.Get(userKey)
	require.True(t, exists, "User should be stored")
	userState := val.(*UserState)
	assert.Equal(t, "testuser", userState.UserInfo.Username)
	assert.Zero(t, userState.ExperimentID, "Experiment id should be zero initially")
	assert.Zero(t, userState.ActivityID, "Activity id should be zero initially")

	// Now send an eainfo packet (type 'U')
	// Format: userid\neainfo. Ec=2 (atlas), Ac=15 (Production Input).
	eaInfoBytes := []byte("root/testuser.1234:5678@client.example.com\n&Uc=100&Ec=2&Ac=15")
	mapRec := &parser.MapRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeEAInfo,
			ServerStart: 1639505770,
		},
		DictId: 999, // This is the new record's dictid, not the user's
		Info:   eaInfoBytes,
	}

	// Process eainfo record
	correlator.handleDictIDRecord(mapRec, serverID, parser.PacketTypeEAInfo)

	// Verify the user state was updated with experiment and activity ids
	val, exists = correlator.userMap.Get(userKey)
	require.True(t, exists, "User should still exist")
	updatedUserState := val.(*UserState)
	assert.Equal(t, 2, updatedUserState.ExperimentID, "Experiment id should be set")
	assert.Equal(t, 15, updatedUserState.ActivityID, "Activity id should be set")
}

// TestCorrelator_EAInfoWithoutExistingUser tests eainfo packet when user doesn't exist yet
func TestCorrelator_EAInfoWithoutExistingUser(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, logger)
	defer correlator.Stop()

	serverID := "1639505770#192.168.1.100:1094"

	// Send eainfo packet without prior user record. Ec=3 (cms), Ac=14 (Analysis Input).
	eaInfoBytes := []byte("root/newuser.9999:8888@newclient.example.com\n&Uc=200&Ec=3&Ac=14")
	mapRec := &parser.MapRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeEAInfo,
			ServerStart: 1639505770,
		},
		DictId: 555,
		Info:   eaInfoBytes,
	}

	// Process eainfo record
	correlator.handleDictIDRecord(mapRec, serverID, parser.PacketTypeEAInfo)

	// Verify new user state was created with the dictID mapping
	dictKey := "1639505770#192.168.1.100:1094-dictid-200"
	val, exists := correlator.dictMap.Get(dictKey)
	require.True(t, exists, "DictID mapping should exist")
	userInfo := val.(parser.UserInfo)
	assert.Equal(t, "newuser", userInfo.Username)

	// Verify user state was created
	userKey := "1639505770#192.168.1.100:1094-userinfo-root/newuser.9999:8888@newclient.example.com"
	val, exists = correlator.userMap.Get(userKey)
	require.True(t, exists, "User state should be created")
	userState := val.(*UserState)
	assert.Equal(t, 3, userState.ExperimentID)
	assert.Equal(t, 14, userState.ActivityID)
	assert.Equal(t, uint32(200), userState.UserID)
}

// TestCorrelator_EAInfoInCorrelatedRecord tests that the resolved experiment and
// activity names (plus their numeric ids) appear in the final record.
func TestCorrelator_EAInfoInCorrelatedRecord(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, logger)
	defer correlator.Stop()

	serverStart := int32(1639505770)
	remoteAddr := "192.168.1.100:1094"
	serverID := "1639505770#192.168.1.100:1094"

	// Create user record
	userRec := &parser.UserRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: serverStart,
		},
		DictId: 100,
		UserInfo: parser.UserInfo{
			Protocol: "root",
			Username: "testuser",
			Pid:      1234,
			Sid:      5678,
			Host:     "client.example.com",
		},
		AuthInfo: parser.AuthInfo{
			DN:  "/DC=org/DC=example/OU=People/CN=Test User",
			Org: "cms",
		},
	}
	correlator.handleUserRecord(userRec, serverID)

	// Add eainfo. Ec=4 (lhcb), Ac=3 (Cache) per the embedded SciTags registry.
	eaInfoBytes := []byte("root/testuser.1234:5678@client.example.com\n&Uc=100&Ec=4&Ac=3")
	mapRec := &parser.MapRecord{
		Header: parser.Header{
			Code:        parser.PacketTypeEAInfo,
			ServerStart: serverStart,
		},
		DictId: 999,
		Info:   eaInfoBytes,
	}
	correlator.handleDictIDRecord(mapRec, serverID, parser.PacketTypeEAInfo)

	// Create file open
	openRec := parser.FileOpenRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeOpen,
			FileId:  1,
			UserId:  100,
		},
		FileSize: 1024,
		Lfn:      []byte("/test/file.root"),
	}
	openPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr: remoteAddr,
	}
	_, err := correlator.handleFileOpen(openRec, openPacket, serverID, 1639505700)
	require.NoError(t, err)

	// Create file close
	closeRec := parser.FileCloseRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeClose,
			FileId:  1,
			UserId:  100,
		},
		Xfr: parser.StatXFR{
			Read:  1000,
			Write: 0,
		},
		Ops: parser.StatOPS{
			Read: 10,
		},
	}
	closePacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: serverStart,
		},
		RemoteAddr: remoteAddr,
	}

	record, err := correlator.handleFileClose(closeRec, closePacket, serverID, 1639505800, 1639505900)
	require.NoError(t, err)
	require.NotNil(t, record)

	// Verify resolved names and raw ids are in the record.
	assert.Equal(t, "lhcb", record.Experiment, "Experiment name should be resolved")
	assert.Equal(t, "Cache", record.Activity, "Activity name should be resolved")
	assert.Equal(t, 4, record.ExperimentID, "Experiment id should be in record")
	assert.Equal(t, 3, record.ActivityID, "Activity id should be in record")
	// The SciTags-derived VO mirrors the experiment name and must not clobber
	// the auth-derived VO (Org "cms" on the user record above).
	assert.Equal(t, "lhcb", record.ScitagsVO, "SciTags VO should be the experiment name")
	assert.Equal(t, "cms", record.VO, "Auth-derived VO should be untouched")
	assert.Equal(t, "testuser", record.User)
	assert.Equal(t, int64(1000), record.Read)
}
