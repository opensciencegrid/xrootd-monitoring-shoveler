package parser

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MessageBusPacket represents a packet as it would come from a message bus
type MessageBusPacket struct {
	Data    string `json:"data"`
	Remote  string `json:"remote"`
	Version string `json:"version"`
}

// TestRealWorldPackets tests parsing of real-world packet data from message bus
// These are actual packets provided as example data that should parse successfully
func TestRealWorldPackets(t *testing.T) {
	// Example packets from message bus (base64-encoded binary)
	packets := []string{
		`{"data":"de4AeWg9+SQAgULWaHR0cHMvdW5rbm93bi4zMjQxODM6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQomcD1odHRwcyZuPSZoPVs6OmZmZmY6MTk4LjEyNC4yMzkuMTBdJm89JnI9Jmc9Jm09Jkk9NA==","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"ae8AV2g9+SQAgULXaHR0cHMvdW5rbm93bi4zMjQxODM6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQp4cmRjbC1wZWxpY2FuLzEuMi4x","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"dfAAeWg9+SQAgULYaHR0cHMvdW5rbm93bi4zMjQxODQ6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQomcD1odHRwcyZuPSZoPVs6OmZmZmY6MTk4LjEyNC4yMzkuMTBdJm89JnI9Jmc9Jm09Jkk9NA==","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"afEAV2g9+SQAgULZaHR0cHMvdW5rbm93bi4zMjQxODQ6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQp4cmRjbC1wZWxpY2FuLzEuMi4x","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"ZPIBDGg9+SQAgULaaHR0cHMvdW5rbm93bi4zMjQxODQ6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQovbmNhci9yZGEvZDY1MTAwNy9iLmUxMy5CSElTVEM1Lm5lMTIwX3QxMi5jZXNtLWloZXNwLWhpcmVzMS4wLjMwLTE5MjAtMjAwNS4wMDMvYXRtL3Byb2MvdHNlcmllcy9ob3VyXzYvYi5lMTMuQkhJU1RDNS5uZTEyMF90MTIuY2VzbS1paGVzcC1oaXJlczEuMC4zMC0xOTIwLTIwMDUuMDAzLmNhbS5oMi5VNTAwLjE5NzYwMTAxMDAtMTk3NzAxMDEwMC5uYw==","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"ZPMBDGg9+SQAgULbaHR0cHMvdW5rbm93bi4zMjQxODM6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQovbmNhci9yZGEvZDY1MTAwNy9iLmUxMy5CSElTVEM1Lm5lMTIwX3QxMi5jZXNtLWloZXNwLWhpcmVzMS4wLjMwLTE5MjAtMjAwNS4wMDMvYXRtL3Byb2MvdHNlcmllcy9ob3VyXzYvYi5lMTMuQkhJU1RDNS5uZTEyMF90MTIuY2VzbS1paGVzcC1oaXJlczEuMC4zMC0xOTIwLTIwMDUuMDAzLmNhbS5oMi5VNTAwLjE5NzYwMTAxMDAtMTk3NzAxMDEwMC5uYw==","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`,
		`{"data":"dfAAtmg3XSMASV/CeHJvb3QvY21zcGx0MDEuMjcwNTcyMzoyMDIxMjk2NTYzNDc3NTVAYjlwMjBwNzI4MS5jZXJuLmNoCiZwPWdzaSZuPWNtc3V4eHh4Jmg9YjlwMjBwNzI4MS5jZXJuLmNoJm89Y21zIGNtcyZyPU5VTEwgTlVMTCZnPS9jbXMgL2Ntcy91c2NtcyZtPSZSPXVua25vd24meD1weXRob24zJnk9Jkk9NiZJPTY=","remote":"2603:4000:c00:0:c:ffff:120c:1b1:51729","version":"1.4.0"}`,
		`{"data":"dfEAsWg3XSMASV/DeHJvb3QvY21zdTA3MjUuMjcwNTczNDoyMDIxMjk2NTYzNDc3NTVAdDJiYXQwNDQ1CiZwPWdzaSZuPWNtc3V4eHh4Jmg9dDJiYXQwNDQ1LmNtc2FmLm1pdC5lZHUmbz1jbXMgY21zJnI9TlVMTCBOVUxMJmc9L2NtcyAvY21zL3VzY21zJm09JlI9dW5rbm93biZ4PWNtc1J1biZ5PSZJPTQmST00","remote":"2603:4000:c00:0:c:ffff:120c:1b1:51729","version":"1.4.0"}`,
		`{"data":"dTwAe2g3V9EAHnHvaHR0cHMvdW5rbm93bi40OTY2MzI6MjQ3NTA4ODczNjA1Mzk1QFs6OjE5OC4yMDIuMTAwLjEzNV0KJnA9aHR0cHMmbj0maD1bOjpmZmZmOjE5OC4yMDIuMTAwLjEzNV0mbz0mcj0mZz0mbT0mST00","remote":"127.0.0.1:41636","version":"Pelican-7.16.5"}`,
		`{"data":"aT0AV2g3V9EAHnHwaHR0cHMvdW5rbm93bi40OTY2MzI6MjQ3NTA4ODczNjA1Mzk1QFs6OjE5OC4yMDIuMTAwLjEzNV0KR28taHR0cC1jbGllbnQvMS4x","remote":"127.0.0.1:41636","version":"Pelican-7.16.5"}`,
		`{"data":"VD4Ag2g3V9EAHnHxaHR0cHMvdW5rbm93bi40OTY2MzI6MjQ3NTA4ODczNjA1Mzk1QFs6OjE5OC4yMDIuMTAwLjEzNV0KJlVjPTE5OTUyNDcmcz1qYWNvYi5sYW5nZSZuPSZvPWh0dHBzOi8vb3NkZi5pZ3duLm9yZy9jaXQmcj0mZz0=","remote":"127.0.0.1:41636","version":"Pelican-7.16.5"}`,
	}

	for i, packetJSON := range packets {
		t.Run(fmt.Sprintf("packet_%d", i), func(t *testing.T) {
			// Unmarshal the JSON
			var mbPacket MessageBusPacket
			err := json.Unmarshal([]byte(packetJSON), &mbPacket)
			require.NoError(t, err, "Failed to unmarshal packet JSON")

			// Decode base64
			packetData, err := base64.StdEncoding.DecodeString(mbPacket.Data)
			require.NoError(t, err, "Failed to decode base64 data")

			// Parse the packet
			packet, err := ParsePacket(packetData)
			require.NoError(t, err, "Failed to parse packet")
			assert.NotNil(t, packet, "Packet should not be nil")

			// Log packet type for debugging
			t.Logf("Packet %d: Type=%c (0x%02x), Length=%d bytes, Remote=%s",
				i, packet.PacketType, packet.PacketType, len(packetData), mbPacket.Remote)

			// Verify packet type is recognized
			validTypes := []byte{
				PacketTypeMap, PacketTypeDictID, PacketTypeFStat, PacketTypeGStream,
				PacketTypeInfo, PacketTypePurg, PacketTypeRedir, PacketTypeTrace,
				PacketTypeToken, PacketTypeUser, PacketTypeXFR,
			}
			assert.Contains(t, validTypes, packet.PacketType,
				"Packet type should be recognized: %c (0x%02x)", packet.PacketType, packet.PacketType)

			// Additional validation based on packet type
			switch packet.PacketType {
			case PacketTypeUser:
				assert.NotNil(t, packet.UserRecord, "User packet should have UserRecord")
				t.Logf("  User: %s@%s (protocol=%s)",
					packet.UserRecord.UserInfo.Username,
					packet.UserRecord.UserInfo.Host,
					packet.UserRecord.UserInfo.Protocol)
			case PacketTypeMap, PacketTypeDictID:
				assert.NotNil(t, packet.MapRecord, "Map/Dict packet should have MapRecord")
				t.Logf("  Map/Dict: DictID=%d, Info length=%d",
					packet.MapRecord.DictId, len(packet.MapRecord.Info))
			case PacketTypeFStat, PacketTypeTrace:
				assert.NotEmpty(t, packet.FileRecords, "File packet should have FileRecords")
				t.Logf("  File records: %d", len(packet.FileRecords))
			}
		})
	}
}

// TestMessageBusPacketFlow tests the complete flow of packets from message bus
func TestMessageBusPacketFlow(t *testing.T) {
	// This test validates that packets can flow through the entire pipeline:
	// 1. Receive from message bus (JSON with base64 data)
	// 2. Decode base64
	// 3. Parse XRootD packet
	// 4. Extract useful information

	packetJSON := `{"data":"de4AeWg9+SQAgULWaHR0cHMvdW5rbm93bi4zMjQxODM6MjU4MzA1OTk3MjQ1MTg0QFs6OjE5OC4xMjQuMjM5LjEwXQomcD1odHRwcyZuPSZoPVs6OmZmZmY6MTk4LjEyNC4yMzkuMTBdJm89JnI9Jmc9Jm09Jkk9NA==","remote":"127.0.0.1:37029","version":"Pelican-7.16.5"}`

	// Step 1: Unmarshal JSON
	var mbPacket MessageBusPacket
	err := json.Unmarshal([]byte(packetJSON), &mbPacket)
	require.NoError(t, err)

	// Step 2: Decode base64
	packetData, err := base64.StdEncoding.DecodeString(mbPacket.Data)
	require.NoError(t, err)

	// Step 3: Parse packet
	packet, err := ParsePacket(packetData)
	require.NoError(t, err)
	assert.NotNil(t, packet)

	// Step 4: Verify packet contents
	assert.Equal(t, PacketTypeUser, packet.PacketType)
	assert.NotNil(t, packet.UserRecord)

	// Verify user info was parsed correctly
	assert.Equal(t, "https", packet.UserRecord.UserInfo.Protocol)
	assert.Equal(t, "unknown", packet.UserRecord.UserInfo.Username)
	assert.Equal(t, "[::198.124.239.10]", packet.UserRecord.UserInfo.Host)

	// Verify auth info
	assert.Equal(t, "https", packet.UserRecord.AuthInfo.AuthProtocol)
	assert.Equal(t, "[::ffff:198.124.239.10]", packet.UserRecord.AuthInfo.Hostname)
	assert.Equal(t, "4", packet.UserRecord.AuthInfo.InetVersion)
}
