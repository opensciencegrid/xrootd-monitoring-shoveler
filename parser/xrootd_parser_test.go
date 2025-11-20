package parser

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a minimal valid header
func createHeader(code byte, plen uint16) []byte {
	header := make([]byte, 8)
	header[0] = code
	header[1] = 1 // Sequence
	binary.BigEndian.PutUint16(header[2:4], plen)
	binary.BigEndian.PutUint32(header[4:8], 1234567890) // Server start time
	return header
}

func TestParsePacket_XMLPacket(t *testing.T) {
	xmlData := []byte("<stats>test</stats>")
	packet, err := ParsePacket(xmlData)
	
	require.NoError(t, err)
	assert.True(t, packet.IsXML)
	assert.Equal(t, xmlData, packet.RawData)
}

func TestParsePacket_TooShort(t *testing.T) {
	shortData := []byte{0x01, 0x02}
	_, err := ParsePacket(shortData)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestParsePacket_LengthMismatch(t *testing.T) {
	header := createHeader(PacketTypeMap, 100)
	// Only provide header (8 bytes) but header says 100 bytes
	
	_, err := ParsePacket(header)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "length mismatch")
}

func TestParsePacket_MapRecord(t *testing.T) {
	// Create a map record packet
	plen := uint16(20) // 8 byte header + 4 byte dictid + 8 byte info
	data := createHeader(PacketTypeMap, plen)
	
	// Add dict id (4 bytes)
	dictId := make([]byte, 4)
	binary.BigEndian.PutUint32(dictId, 12345)
	data = append(data, dictId...)
	
	// Add some info (8 bytes to match plen)
	info := []byte("testinfo")
	data = append(data, info...)
	
	packet, err := ParsePacket(data)
	
	require.NoError(t, err)
	assert.Equal(t, PacketTypeMap, packet.PacketType)
	assert.NotNil(t, packet.MapRecord)
	assert.Equal(t, uint32(12345), packet.MapRecord.DictId)
	assert.Equal(t, info, packet.MapRecord.Info)
}

func TestParsePacket_FileTimeRecord(t *testing.T) {
	// Create a time record packet
	// 8 byte main header + 16 byte file header + 16 byte time data
	plen := uint16(40)
	data := createHeader(PacketTypeTime, plen)
	
	// Add file header (16 bytes)
	fileHeader := make([]byte, 16)
	fileHeader[0] = RecTypeTime
	fileHeader[1] = 0 // flags
	binary.BigEndian.PutUint16(fileHeader[2:4], 32) // record size
	binary.BigEndian.PutUint32(fileHeader[4:8], 99)   // file id
	binary.BigEndian.PutUint32(fileHeader[8:12], 123) // user id
	binary.BigEndian.PutUint16(fileHeader[12:14], 0)  // nrecs0
	binary.BigEndian.PutUint16(fileHeader[14:16], 0)  // nrecs1
	data = append(data, fileHeader...)
	
	// Add time data (16 bytes)
	timeData := make([]byte, 16)
	binary.BigEndian.PutUint32(timeData[0:4], 1000)   // TBeg
	binary.BigEndian.PutUint32(timeData[4:8], 2000)   // TEnd
	binary.BigEndian.PutUint64(timeData[8:16], 54321) // SID
	data = append(data, timeData...)
	
	packet, err := ParsePacket(data)
	
	require.NoError(t, err)
	assert.Equal(t, PacketTypeTime, packet.PacketType)
	assert.Len(t, packet.FileRecords, 1)
	
	timeRec, ok := packet.FileRecords[0].(FileTimeRecord)
	require.True(t, ok)
	assert.Equal(t, int32(1000), timeRec.TBeg)
	assert.Equal(t, int32(2000), timeRec.TEnd)
	assert.Equal(t, int64(54321), timeRec.SID)
}

func TestParsePacket_FileCloseRecord(t *testing.T) {
	// Create a close record packet with XFR stats only
	// 8 byte main header + 16 byte file header + 24 byte xfr stats
	plen := uint16(48)
	data := createHeader(PacketTypeFClose, plen)
	
	// Add file header (16 bytes)
	fileHeader := make([]byte, 16)
	fileHeader[0] = RecTypeClose
	fileHeader[1] = 0 // flags
	binary.BigEndian.PutUint16(fileHeader[2:4], 40) // record size
	binary.BigEndian.PutUint32(fileHeader[4:8], 111)   // file id
	binary.BigEndian.PutUint32(fileHeader[8:12], 222) // user id
	binary.BigEndian.PutUint16(fileHeader[12:14], 0)  // nrecs0
	binary.BigEndian.PutUint16(fileHeader[14:16], 0)  // nrecs1
	data = append(data, fileHeader...)
	
	// Add XFR stats (24 bytes)
	xfrData := make([]byte, 24)
	binary.BigEndian.PutUint64(xfrData[0:8], 1000)   // Read bytes
	binary.BigEndian.PutUint64(xfrData[8:16], 2000)  // Readv bytes
	binary.BigEndian.PutUint64(xfrData[16:24], 500)  // Write bytes
	data = append(data, xfrData...)
	
	packet, err := ParsePacket(data)
	
	require.NoError(t, err)
	assert.Equal(t, PacketTypeFClose, packet.PacketType)
	assert.Len(t, packet.FileRecords, 1)
	
	closeRec, ok := packet.FileRecords[0].(FileCloseRecord)
	require.True(t, ok)
	assert.Equal(t, int64(1000), closeRec.Xfr.Read)
	assert.Equal(t, int64(2000), closeRec.Xfr.Readv)
	assert.Equal(t, int64(500), closeRec.Xfr.Write)
}

func TestParsePacket_UnknownType(t *testing.T) {
	plen := uint16(8)
	data := createHeader('Z', plen) // Unknown type
	
	_, err := ParsePacket(data)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown packet type")
}

func TestGetRequestID(t *testing.T) {
	// Test map record ID
	mapPacket := &Packet{
		MapRecord: &MapRecord{
			DictId: 12345,
		},
	}
	assert.Equal(t, "map-12345", mapPacket.GetRequestID())
	
	// Test time record ID
	timePacket := &Packet{
		FileRecords: []interface{}{
			FileTimeRecord{
				Header: FileHeader{FileId: 99},
				SID:    54321,
			},
		},
	}
	assert.Equal(t, "time-99-54321", timePacket.GetRequestID())
	
	// Test close record ID
	closePacket := &Packet{
		FileRecords: []interface{}{
			FileCloseRecord{
				Header: FileHeader{FileId: 111, UserId: 222},
			},
		},
	}
	assert.Equal(t, "close-111-222", closePacket.GetRequestID())
}

func TestParsePacket_EmptyPacket(t *testing.T) {
	_, err := ParsePacket([]byte{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestParsePacket_UserRecord(t *testing.T) {
	// Create a user record packet
	// Format: header (8) + dictid (4) + userInfo + \n + authInfo
	userInfo := "xrootd/user123.12345:67890@host.example.com"
	authInfo := "p=gsi&n=/DC=org/DC=example/CN=user&h=host.example.com&o=Example&r=production&g=group1&m=info&x=xrootd&y=mon&I=4"
	
	info := userInfo + "\n" + authInfo
	plen := uint16(8 + 4 + len(info))
	
	data := createHeader(PacketTypeUser, plen)
	
	// Add dict id (4 bytes)
	dictId := make([]byte, 4)
	binary.BigEndian.PutUint32(dictId, 99999)
	data = append(data, dictId...)
	
	// Add info
	data = append(data, []byte(info)...)
	
	packet, err := ParsePacket(data)
	
	require.NoError(t, err)
	assert.Equal(t, PacketTypeUser, packet.PacketType)
	require.NotNil(t, packet.UserRecord)
	assert.Equal(t, uint32(99999), packet.UserRecord.DictId)
	
	// Verify userInfo parsing
	assert.Equal(t, "xrootd", packet.UserRecord.UserInfo.Protocol)
	assert.Equal(t, "user123", packet.UserRecord.UserInfo.Username)
	assert.Equal(t, 12345, packet.UserRecord.UserInfo.Pid)
	assert.Equal(t, 67890, packet.UserRecord.UserInfo.Sid)
	assert.Equal(t, "host.example.com", packet.UserRecord.UserInfo.Host)
	
	// Verify authInfo parsing
	assert.Equal(t, "gsi", packet.UserRecord.AuthInfo.AuthProtocol)
	assert.Equal(t, "/DC=org/DC=example/CN=user", packet.UserRecord.AuthInfo.DN)
	assert.Equal(t, "host.example.com", packet.UserRecord.AuthInfo.Hostname)
	assert.Equal(t, "Example", packet.UserRecord.AuthInfo.Org)
	assert.Equal(t, "production", packet.UserRecord.AuthInfo.Role)
	assert.Equal(t, "group1", packet.UserRecord.AuthInfo.Groups)
	assert.Equal(t, "4", packet.UserRecord.AuthInfo.InetVersion)
}

func TestParseUserInfo(t *testing.T) {
	// Test with protocol
	userInfo, err := parseUserInfo([]byte("xrootd/testuser.1234:5678@host.com"))
	require.NoError(t, err)
	assert.Equal(t, "xrootd", userInfo.Protocol)
	assert.Equal(t, "testuser", userInfo.Username)
	assert.Equal(t, 1234, userInfo.Pid)
	assert.Equal(t, 5678, userInfo.Sid)
	assert.Equal(t, "host.com", userInfo.Host)
	
	// Test without protocol
	userInfo, err = parseUserInfo([]byte("user.999:111@example.org"))
	require.NoError(t, err)
	assert.Equal(t, "", userInfo.Protocol)
	assert.Equal(t, "user", userInfo.Username)
	assert.Equal(t, 999, userInfo.Pid)
	assert.Equal(t, 111, userInfo.Sid)
	assert.Equal(t, "example.org", userInfo.Host)
	
	// Test error cases
	_, err = parseUserInfo([]byte(""))
	assert.Error(t, err)
	
	_, err = parseUserInfo([]byte("invalid"))
	assert.Error(t, err)
}

func TestParseAuthInfo(t *testing.T) {
	authInfo := parseAuthInfo([]byte("p=gsi&n=/DC=org/CN=test&h=host&o=Org&r=role&g=groups&m=info&x=exec&y=mon&I=6"))
	
	assert.Equal(t, "gsi", authInfo.AuthProtocol)
	assert.Equal(t, "/DC=org/CN=test", authInfo.DN)
	assert.Equal(t, "host", authInfo.Hostname)
	assert.Equal(t, "Org", authInfo.Org)
	assert.Equal(t, "role", authInfo.Role)
	assert.Equal(t, "groups", authInfo.Groups)
	assert.Equal(t, "info", authInfo.Info)
	assert.Equal(t, "exec", authInfo.ExecName)
	assert.Equal(t, "mon", authInfo.MonInfo)
	assert.Equal(t, "6", authInfo.InetVersion)
	
	// Test empty auth info
	emptyAuth := parseAuthInfo([]byte(""))
	assert.Equal(t, "", emptyAuth.AuthProtocol)
	assert.Equal(t, "", emptyAuth.DN)
}
