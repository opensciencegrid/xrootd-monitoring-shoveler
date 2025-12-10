package collector

import (
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseEAInfo tests the parseEAInfo function
func TestParseEAInfo(t *testing.T) {
	tests := []struct {
		name             string
		eaInfo           string
		expectedUdid     uint32
		expectedExpr     string
		expectedActivity string
	}{
		{
			name:             "Complete eainfo",
			eaInfo:           "&Uc=1234&Ec=ATLAS&Ac=production",
			expectedUdid:     1234,
			expectedExpr:     "ATLAS",
			expectedActivity: "production",
		},
		{
			name:             "Missing activity",
			eaInfo:           "&Uc=5678&Ec=CMS",
			expectedUdid:     5678,
			expectedExpr:     "CMS",
			expectedActivity: "",
		},
		{
			name:             "Empty values",
			eaInfo:           "&Uc=9999&Ec=&Ac=",
			expectedUdid:     9999,
			expectedExpr:     "",
			expectedActivity: "",
		},
		{
			name:             "No leading ampersand",
			eaInfo:           "Uc=111&Ec=ALICE&Ac=test",
			expectedUdid:     111,
			expectedExpr:     "ALICE",
			expectedActivity: "test",
		},
		{
			name:             "Invalid format",
			eaInfo:           "invalid",
			expectedUdid:     0,
			expectedExpr:     "",
			expectedActivity: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			udid, expr, activity := parseEAInfo(tt.eaInfo)
			assert.Equal(t, tt.expectedUdid, udid, "udid mismatch")
			assert.Equal(t, tt.expectedExpr, expr, "experiment code mismatch")
			assert.Equal(t, tt.expectedActivity, activity, "activity code mismatch")
		})
	}
}

// TestCorrelator_EAInfoPacket tests the handling of eainfo packets
func TestCorrelator_EAInfoPacket(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, true, logger)
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
	assert.Empty(t, userState.ExperimentCode, "Experiment code should be empty initially")
	assert.Empty(t, userState.ActivityCode, "Activity code should be empty initially")

	// Now send an eainfo packet (type 'U')
	// Format: userid\neainfo
	eaInfoBytes := []byte("root/testuser.1234:5678@client.example.com\n&Uc=100&Ec=ATLAS&Ac=production")
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

	// Verify the user state was updated with experiment and activity codes
	val, exists = correlator.userMap.Get(userKey)
	require.True(t, exists, "User should still exist")
	updatedUserState := val.(*UserState)
	assert.Equal(t, "ATLAS", updatedUserState.ExperimentCode, "Experiment code should be set")
	assert.Equal(t, "production", updatedUserState.ActivityCode, "Activity code should be set")
}

// TestCorrelator_EAInfoWithoutExistingUser tests eainfo packet when user doesn't exist yet
func TestCorrelator_EAInfoWithoutExistingUser(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, true, logger)
	defer correlator.Stop()

	serverID := "1639505770#192.168.1.100:1094"

	// Send eainfo packet without prior user record
	eaInfoBytes := []byte("root/newuser.9999:8888@newclient.example.com\n&Uc=200&Ec=CMS&Ac=analysis")
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
	assert.Equal(t, "CMS", userState.ExperimentCode)
	assert.Equal(t, "analysis", userState.ActivityCode)
	assert.Equal(t, uint32(200), userState.UserID)
}

// TestCorrelator_EAInfoInCorrelatedRecord tests that experiment and activity appear in final record
func TestCorrelator_EAInfoInCorrelatedRecord(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	correlator := NewCorrelator(5*time.Minute, 0, true, logger)
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

	// Add eainfo
	eaInfoBytes := []byte("root/testuser.1234:5678@client.example.com\n&Uc=100&Ec=LHCb&Ac=simulation")
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
	_, err := correlator.handleFileOpen(openRec, openPacket, serverID)
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

	record, err := correlator.handleFileClose(closeRec, closePacket, serverID)
	require.NoError(t, err)
	require.NotNil(t, record)

	// Verify experiment and activity are in the record
	assert.Equal(t, "LHCb", record.Experiment, "Experiment code should be in record")
	assert.Equal(t, "simulation", record.Activity, "Activity code should be in record")
	assert.Equal(t, "testuser", record.User)
	assert.Equal(t, int64(1000), record.Read)
}
