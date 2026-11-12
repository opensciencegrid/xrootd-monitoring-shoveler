package collector

import (
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

func TestHandleDisconnect(t *testing.T) {
	// A disconnect starts the session's grace period (UserTTL) rather than
	// deleting it, so a close that arrives after it still finds its client.
	correlator := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:        300 * time.Second,
		MaxEntries: 10000,
		UserTTL:    100 * time.Millisecond,
	})
	defer correlator.Stop()
	serverID := "12345#host:1094"

	// Create and store a user record first
	userRec := &parser.UserRecord{
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

	// Handle the user record to populate the maps
	correlator.handleUserRecord(userRec, serverID)

	// Verify user is in the maps
	dictKey := "12345#host:1094-dictid-100"
	userInfoKey := BuildUserInfoKey(serverID, userRec.UserInfo)

	val, exists := correlator.dictMap.Get(dictKey)
	if !exists {
		t.Errorf("User should be in dictMap after handleUserRecord (key: %s)", dictKey)
	} else {
		t.Logf("dictMap entry found: %+v", val)
	}

	val2, exists2 := correlator.userMap.Get(userInfoKey)
	if !exists2 {
		t.Errorf("User should be in userMap after handleUserRecord (key: %s)", userInfoKey)
	} else {
		t.Logf("userMap entry found: %+v", val2)
	}

	// Create a disconnect record
	discRec := parser.FileDisconnectRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeDisc,
		},
		UserID: 100, // dictid of the user
	}

	// Handle the disconnect
	correlator.handleDisconnect(discRec, serverID)

	// Still there during the grace period
	if _, exists := correlator.dictMap.Get(dictKey); !exists {
		t.Error("User should stay in dictMap during the grace period")
	}
	if _, exists := correlator.userMap.Get(userInfoKey); !exists {
		t.Error("User should stay in userMap during the grace period")
	}

	time.Sleep(150 * time.Millisecond)

	// Gone from both maps once it is over
	if _, exists := correlator.dictMap.Get(dictKey); exists {
		t.Error("User should be removed from dictMap after the grace period")
	}
	if _, exists := correlator.userMap.Get(userInfoKey); exists {
		t.Error("User should be removed from userMap after the grace period")
	}
}

func TestHandleDisconnect_UserNotFound(t *testing.T) {
	// Create correlator
	correlator := NewCorrelator(300*time.Second, 10000, nil)
	serverID := "12345#host:1094"

	// Create a disconnect record for a non-existent user
	discRec := parser.FileDisconnectRecord{
		Header: parser.FileHeader{
			RecType: parser.RecTypeDisc,
		},
		UserID: 999, // Non-existent dictid
	}

	// This should not panic or error
	correlator.handleDisconnect(discRec, serverID)

	// Test passes if no panic occurred
}

func TestProcessPacket_Disconnect(t *testing.T) {
	// Create correlator
	correlator := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:        300 * time.Second,
		MaxEntries: 10000,
		UserTTL:    100 * time.Millisecond,
	})
	defer correlator.Stop()

	// Create a user record and process it
	userPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeUser,
			ServerStart: 12345,
		},
		PacketType: parser.PacketTypeUser,
		UserRecord: &parser.UserRecord{
			DictId: 200,
			UserInfo: parser.UserInfo{
				Protocol: "root",
				Username: "testuser2",
				Pid:      2222,
				Sid:      3333,
				Host:     "client2.example.com",
			},
		},
		RemoteAddr: "server.example.com:1094",
	}

	_, err := correlator.ProcessPacket(userPacket)
	if err != nil {
		t.Fatalf("ProcessPacket failed for user record: %v", err)
	}

	// Verify user is stored
	dictKey := "12345#server.example.com:1094-dictid-200"
	if _, exists := correlator.dictMap.Get(dictKey); !exists {
		t.Error("User should be in dictMap after ProcessPacket")
	}

	// Create a packet with disconnect record
	discPacket := &parser.Packet{
		Header: parser.Header{
			Code:        parser.PacketTypeFStat,
			ServerStart: 12345,
		},
		PacketType: parser.PacketTypeFStat,
		FileRecords: []interface{}{
			parser.FileDisconnectRecord{
				Header: parser.FileHeader{
					RecType: parser.RecTypeDisc,
				},
				UserID: 200,
			},
		},
		RemoteAddr: "server.example.com:1094",
	}

	// Process the disconnect packet
	records, err := correlator.ProcessPacket(discPacket)
	if err != nil {
		t.Fatalf("ProcessPacket failed for disconnect: %v", err)
	}

	// Disconnect should not return a record
	if len(records) > 0 {
		t.Error("Disconnect should not return a collector record")
	}

	// Verify user is removed once the grace period is over
	if _, exists := correlator.dictMap.Get(dictKey); !exists {
		t.Error("User should stay in dictMap during the grace period")
	}
	time.Sleep(150 * time.Millisecond)
	if _, exists := correlator.dictMap.Get(dictKey); exists {
		t.Error("User should be removed from dictMap after the grace period")
	}
}
