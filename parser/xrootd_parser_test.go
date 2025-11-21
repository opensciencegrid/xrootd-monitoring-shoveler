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
	// f-stream packets contain file records including FileTOD (time records)
	// 8 byte main header + 24 byte FileTOD record
	plen := uint16(32)
	data := createHeader(PacketTypeFStat, plen)

	// Add FileTOD record (24 bytes): RecType(1) + RecFlag(1) + RecSize(2) + isXfr_recs(2) + total_recs(2) + tBeg(4) + tEnd(4) + sid(8)
	fileHeader := make([]byte, 24)
	fileHeader[0] = RecTypeTime
	fileHeader[1] = 0                                    // flags
	binary.BigEndian.PutUint16(fileHeader[2:4], 24)      // record size
	binary.BigEndian.PutUint16(fileHeader[4:6], 0)       // isXfr_recs
	binary.BigEndian.PutUint16(fileHeader[6:8], 0)       // total_recs
	binary.BigEndian.PutUint32(fileHeader[8:12], 1000)   // TBeg
	binary.BigEndian.PutUint32(fileHeader[12:16], 2000)  // TEnd
	binary.BigEndian.PutUint64(fileHeader[16:24], 54321) // SID
	data = append(data, fileHeader...)

	packet, err := ParsePacket(data)

	require.NoError(t, err)
	assert.Equal(t, PacketTypeFStat, packet.PacketType)
	// FileTOD records are skipped, so no records should be returned
	assert.Len(t, packet.FileRecords, 0)
}

func TestParsePacket_FileCloseRecord(t *testing.T) {
	// f-stream packet with FileTOD + close record
	// 8 byte main header + 24 byte FileTOD + 32 byte close record
	plen := uint16(64)
	data := createHeader(PacketTypeFStat, plen)

	// Add FileTOD record (24 bytes) - will be skipped by parser
	fileTOD := make([]byte, 24)
	fileTOD[0] = RecTypeTime
	fileTOD[1] = 0
	binary.BigEndian.PutUint16(fileTOD[2:4], 24) // record size
	data = append(data, fileTOD...)

	// Add close record header (8 bytes): RecType(1) + RecFlag(1) + RecSize(2) + FileID(4)
	fileHeader := make([]byte, 8)
	fileHeader[0] = RecTypeClose
	fileHeader[1] = 0                                // flags
	binary.BigEndian.PutUint16(fileHeader[2:4], 32)  // record size
	binary.BigEndian.PutUint32(fileHeader[4:8], 111) // file id
	data = append(data, fileHeader...)

	// Add XFR stats (24 bytes): Read(8) + Readv(8) + Write(8)
	xfrData := make([]byte, 24)
	binary.BigEndian.PutUint64(xfrData[0:8], 1000)  // Read bytes
	binary.BigEndian.PutUint64(xfrData[8:16], 2000) // Readv bytes
	binary.BigEndian.PutUint64(xfrData[16:24], 500) // Write bytes
	data = append(data, xfrData...)

	packet, err := ParsePacket(data)

	require.NoError(t, err)
	assert.Equal(t, PacketTypeFStat, packet.PacketType)
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

func TestParseTokenInfo(t *testing.T) {
	// Test full token info
	tokenData := []byte("&Uc=12345&s=subject&n=username&o=org&r=role&g=group1 group2")
	tokenInfo := parseTokenInfo(tokenData)

	assert.Equal(t, uint32(12345), tokenInfo.UserDictID)
	assert.Equal(t, "subject", tokenInfo.Subject)
	assert.Equal(t, "username", tokenInfo.Username)
	assert.Equal(t, "org", tokenInfo.Org)
	assert.Equal(t, "role", tokenInfo.Role)
	assert.Equal(t, "group1 group2", tokenInfo.Groups)

	// Test partial token info (optional fields)
	partialData := []byte("&Uc=999&s=subj")
	partialInfo := parseTokenInfo(partialData)
	assert.Equal(t, uint32(999), partialInfo.UserDictID)
	assert.Equal(t, "subj", partialInfo.Subject)
	assert.Equal(t, "", partialInfo.Username)
	assert.Equal(t, "", partialInfo.Org)

	// Test empty token info
	emptyToken := parseTokenInfo([]byte(""))
	assert.Equal(t, uint32(0), emptyToken.UserDictID)
	assert.Equal(t, "", emptyToken.Subject)
}

func TestParsePacket_TokenRecord(t *testing.T) {
	// Create a token record packet ('T' type)
	// Format: header + dictid (4 bytes) + userinfo + '\n' + tokeninfo
	userInfo := "testuser.123:456@host"
	tokenInfo := "&Uc=789&s=tokensubj&n=tokenuser&o=tokenorg"
	info := userInfo + "\n" + tokenInfo

	plen := uint16(8 + 4 + len(info))
	packet := createHeader(PacketTypeToken, plen)
	packet = append(packet, 0, 0, 0, 100) // dictid = 100
	packet = append(packet, []byte(info)...)

	parsed, err := ParsePacket(packet)

	require.NoError(t, err)
	assert.Equal(t, PacketTypeToken, parsed.PacketType)
	assert.NotNil(t, parsed.UserRecord)
	assert.Equal(t, uint32(100), parsed.UserRecord.DictId)

	// Check userInfo was parsed
	assert.Equal(t, "testuser", parsed.UserRecord.UserInfo.Username)
	assert.Equal(t, "host", parsed.UserRecord.UserInfo.Host)

	// Check tokenInfo was parsed
	assert.Equal(t, uint32(789), parsed.UserRecord.TokenInfo.UserDictID)
	assert.Equal(t, "tokensubj", parsed.UserRecord.TokenInfo.Subject)
	assert.Equal(t, "tokenuser", parsed.UserRecord.TokenInfo.Username)
	assert.Equal(t, "tokenorg", parsed.UserRecord.TokenInfo.Org)
}
