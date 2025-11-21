package parser

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// XRootD monitoring packet types as defined in:
// https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498
const (
	PacketTypeMap     byte = '=' // Server identification
	PacketTypeDictID  byte = 'd' // Dictionary ID for user/path
	PacketTypeFStat   byte = 'f' // File stat stream (contains file records)
	PacketTypeGStream byte = 'g' // g-stream
	PacketTypeInfo    byte = 'i' // Dictionary ID for information
	PacketTypePurg    byte = 'p' // Purge (FRM only)
	PacketTypeRedir   byte = 'r' // Redirect
	PacketTypeTrace   byte = 't' // Trace stream (files, io, iov)
	PacketTypeToken   byte = 'T' // Token information
	PacketTypeUser    byte = 'u' // User login/auth
	PacketTypeXFR     byte = 'x' // Transfer (FRM only)
)

// Record types for file records
// Per Python reference: RecType=0 is close, RecType=1 is open
const (
	RecTypeClose byte = 0 // Close record (was incorrectly 1)
	RecTypeOpen  byte = 1 // Open record (was incorrectly 0)
	RecTypeTime  byte = 2
	RecTypeXFR   byte = 3
	RecTypeDisc  byte = 4
)

// Header represents the XRootD monitoring packet header (8 bytes)
// Ref: https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498
type Header struct {
	Code        byte   // Packet type code
	Pseq        uint8  // Packet sequence number
	Plen        uint16 // Packet length
	ServerStart int32  // Unix time at server start
}

// Binary struct definitions for direct reading with encoding/binary
type binaryHeader struct {
	Code        uint8
	Pseq        uint8
	Plen        uint16
	ServerStart uint32
}

type binaryFileHeaderCommon struct {
	RecType uint8
	RecFlag uint8
	RecSize uint16
	FileId  uint32
}

type binaryFileTimeRecord struct {
	RecType uint8
	RecFlag uint8
	RecSize uint16
	FileId  uint32
	NRecs0  uint16
	NRecs1  uint16
	TBeg    uint32
	TEnd    uint32
	SID     uint64
}

type binaryFileCloseRecordHeader struct {
	RecType uint8
	RecFlag uint8
	RecSize uint16
	FileId  uint32
	Read    uint64
	Readv   uint64
	Write   uint64
}

type binaryFileOpenRecordHeader struct {
	RecType  uint8
	RecFlag  uint8
	RecSize  uint16
	FileId   uint32
	FileSize uint64
}

type binaryStatOPS struct {
	Read  uint32
	Readv uint32
	Write uint32
	RsMin uint16
	RsMax uint16
	Rsegs uint64
	RdMin uint32
	RdMax uint32
	RvMin uint32
	RvMax uint32
	WrMin uint32
	WrMax uint32
}

// MapRecord represents a dictionary mapping packet
type MapRecord struct {
	Header Header
	DictId uint32
	Info   []byte
}

// FileHeader represents the file operation record header
type FileHeader struct {
	RecType byte   // Record type (open, close, etc.)
	RecFlag byte   // Record flags
	RecSize uint16 // Record size (unsigned to prevent negative values)
	FileId  uint32 // File identifier
	UserId  uint32 // User identifier
	NRecs0  int16  // Number of records (part 0)
	NRecs1  int16  // Number of records (part 1)
}

// FileTimeRecord represents a TOD (time of day) record
type FileTimeRecord struct {
	Header FileHeader
	TBeg   int32 // Begin time
	TEnd   int32 // End time
	SID    int64 // Stream/Session ID
}

// StatXFR represents transfer statistics
type StatXFR struct {
	Read  int64 // Bytes read from file using read()
	Readv int64 // Bytes read from file using readv()
	Write int64 // Bytes written to file
}

// StatOPS represents operation statistics (48 bytes)
type StatOPS struct {
	Read  int32 // Number of read() calls
	Readv int32 // Number of readv() calls
	Write int32 // Number of write() calls
	RsMin int16 // Smallest readv() segment count
	RsMax int16 // Largest readv() segment count
	Rsegs int64 // Number of readv() segments
	RdMin int32 // Smallest read() request size
	RdMax int32 // Largest read() request size
	RvMin int32 // Smallest readv() request size
	RvMax int32 // Largest readv() request size
	WrMin int32 // Smallest write() request size
	WrMax int32 // Largest write() request size
}

// FileCloseRecord represents a file close event with statistics
type FileCloseRecord struct {
	Header FileHeader
	Xfr    StatXFR
	Ops    StatOPS // Optional, depends on record size
}

// FileOpenRecord represents a file open event
type FileOpenRecord struct {
	Header   FileHeader
	FileSize int64
	User     uint32
	Lfn      []byte // Logical file name
}

// UserInfo represents parsed user information from the userInfo field
// Format: [protocol/]username.pid:sid@host
type UserInfo struct {
	Protocol string
	Username string
	Pid      int
	Sid      int
	Host     string
}

// AuthInfo represents authorization information from user packets
type AuthInfo struct {
	AuthProtocol string // p= authentication protocol
	DN           string // n= distinguished name
	Hostname     string // h= hostname
	Org          string // o= organization
	Role         string // r= role
	Groups       string // g= groups
	Info         string // m= miscellaneous info
	ExecName     string // x= executable name
	MonInfo      string // y= monitoring info
	InetVersion  string // I= inet version (4 or 6)
}

// TokenInfo represents token information from 'T' message type
// Format: &Uc=udid&s=subj&n=[un]&o=[on]&r=[rn]&g=[gn]
type TokenInfo struct {
	UserDictID uint32 // Uc= user's dictionary id (from previous 'u' record)
	Subject    string // s= token's subject name
	Username   string // n= token's (possibly mapped) username
	Org        string // o= client's organization name
	Role       string // r= client's role name
	Groups     string // g= client's group names (space-separated list)
}

// UserRecord represents a user packet (type 'u')
type UserRecord struct {
	Header    Header
	DictId    uint32
	UserInfo  UserInfo
	AuthInfo  AuthInfo
	TokenInfo TokenInfo
}

// GStreamRecord represents a gstream packet (type 'g')
// GStream packets contain monitoring events for cache, TCP, or TPC activities
type GStreamRecord struct {
	Begin      uint32                   // Beginning timestamp
	End        uint32                   // Ending timestamp
	Ident      uint64                   // Identifier with stream type in top 8 bits
	StreamType byte                     // Stream type: 'T'=TCP, 'C'=Cache, 'P'=TPC
	Events     []map[string]interface{} // Parsed JSON events
}

// Packet is a parsed XRootD monitoring packet
type Packet struct {
	Header        Header
	PacketType    byte
	IsXML         bool
	MapRecord     *MapRecord
	UserRecord    *UserRecord
	GStreamRecord *GStreamRecord
	FileRecords   []interface{} // Can contain FileTimeRecord, FileCloseRecord, FileOpenRecord
	RawData       []byte        // Original packet data
	RemoteAddr    string        // Remote address (host:port) for server ID calculation
}

// ParsePacket parses a raw XRootD monitoring packet
func ParsePacket(b []byte) (*Packet, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("packet too short: %d bytes", len(b))
	}

	// Check for XML summary packet
	if b[0] == '<' {
		return &Packet{
			IsXML:   true,
			RawData: b,
		}, nil
	}

	// Parse binary packet header (8 bytes minimum)
	if len(b) < 8 {
		return nil, fmt.Errorf("packet too short for binary header: %d bytes", len(b))
	}

	// Use binary.Read to parse header
	var binHeader binaryHeader
	reader := bytes.NewReader(b)
	if err := binary.Read(reader, binary.BigEndian, &binHeader); err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", err)
	}

	header := Header{
		Code:        byte(binHeader.Code),
		Pseq:        binHeader.Pseq,
		Plen:        binHeader.Plen,
		ServerStart: int32(binHeader.ServerStart),
	}

	// Validate packet length
	if len(b) != int(header.Plen) {
		return nil, fmt.Errorf("packet length mismatch: got %d, expected %d", len(b), header.Plen)
	}

	packet := &Packet{
		Header:     header,
		PacketType: header.Code,
		RawData:    b,
	}

	// Parse based on packet type
	switch header.Code {
	case PacketTypeMap:
		mapRec, err := parseMapRecord(header, b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map record: %w", err)
		}
		packet.MapRecord = mapRec
	case PacketTypeUser:
		userRec, err := parseUserRecord(header, b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse user record: %w", err)
		}
		packet.UserRecord = userRec
	case PacketTypeToken:
		// 'T' packets contain token information
		userRec, err := parseTokenRecord(header, b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse token record: %w", err)
		}
		packet.UserRecord = userRec
	case PacketTypeFStat, PacketTypeTrace:
		// f-stream (fstat) and t-stream (files/io/iov) contain file records
		// These have embedded file records with recType determining the record type
		fileRecs, err := parseFileRecords(header, b, header.Code)
		if err != nil {
			// Don't fail the entire packet if we can't parse file records
			// Just log and continue - this allows shoveling mode to work
			packet.FileRecords = nil
		} else {
			packet.FileRecords = fileRecs
		}
	case PacketTypeDictID:
		// 'd' packets are dictionary mappings for paths
		// Parse as map record for use in correlator
		mapRec, err := parseMapRecord(header, b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse dict record: %w", err)
		}
		packet.MapRecord = mapRec
	case PacketTypeXFR:
		// FRM (File Residency Manager) xfr packets
		// These are handled as pass-through
	case PacketTypeGStream:
		// 'g' packets are gstream events (cache, TCP, TPC)
		gstreamRec, err := parseGStreamRecord(b[8:])
		if err != nil {
			return nil, fmt.Errorf("failed to parse gstream record: %w", err)
		}
		packet.GStreamRecord = gstreamRec
	case PacketTypeInfo, PacketTypePurg, PacketTypeRedir:
		// These packet types are supported but not fully parsed yet
		// They will be handled in shoveling mode as pass-through
	default:
		return nil, fmt.Errorf("unknown packet type: %c (0x%02x)", header.Code, header.Code)
	}

	return packet, nil
}

// parseMapRecord parses a dictionary mapping packet
func parseMapRecord(header Header, b []byte) (*MapRecord, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("map record too short: %d bytes", len(b))
	}

	dictId := binary.BigEndian.Uint32(b[8:12])
	info := b[12:]

	return &MapRecord{
		Header: header,
		DictId: dictId,
		Info:   info,
	}, nil
}

// parseUserRecord parses a user packet (type 'u')
// Ref: https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498
func parseUserRecord(header Header, b []byte) (*UserRecord, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("user record too short: %d bytes", len(b))
	}

	dictId := binary.BigEndian.Uint32(b[8:12])
	info := b[12:]

	// Split userInfo and authInfo by newline
	var userInfoBytes, authInfoBytes []byte
	newlineIdx := -1
	for i, b := range info {
		if b == '\n' {
			newlineIdx = i
			break
		}
	}

	if newlineIdx >= 0 {
		userInfoBytes = info[:newlineIdx]
		authInfoBytes = info[newlineIdx+1:]
	} else {
		// No newline found, entire info is userInfo
		userInfoBytes = info
		authInfoBytes = []byte{}
	}

	// Parse userInfo
	userInfo, err := parseUserInfo(userInfoBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user info: %w", err)
	}

	// Parse authInfo
	authInfo := parseAuthInfo(authInfoBytes)

	return &UserRecord{
		Header:   header,
		DictId:   dictId,
		UserInfo: userInfo,
		AuthInfo: authInfo,
	}, nil
}

// parseUserInfo parses the userInfo field
// Format: [protocol/]username.pid:sid@host
func parseUserInfo(b []byte) (UserInfo, error) {
	if len(b) == 0 {
		return UserInfo{}, fmt.Errorf("empty user info")
	}

	info := UserInfo{}
	s := string(b)

	// Check for protocol
	if idx := bytesIndexByte(b, '/'); idx >= 0 {
		info.Protocol = string(b[:idx])
		s = string(b[idx+1:])
	}

	// Find the @ for host
	atIdx := bytesLastIndexByte([]byte(s), '@')
	if atIdx < 0 {
		return info, fmt.Errorf("missing @ in user info: %s", s)
	}
	info.Host = s[atIdx+1:]
	s = s[:atIdx]

	// Split by : for sid
	colonIdx := bytesIndexByte([]byte(s), ':')
	if colonIdx < 0 {
		return info, fmt.Errorf("missing : in user info: %s", s)
	}
	sidStr := s[colonIdx+1:]
	s = s[:colonIdx]

	// Split by . for pid
	dotIdx := bytesLastIndexByte([]byte(s), '.')
	if dotIdx < 0 {
		return info, fmt.Errorf("missing . in user info: %s", s)
	}
	pidStr := s[dotIdx+1:]
	info.Username = s[:dotIdx]

	// Parse pid and sid
	var pid, sid int
	if _, err := fmt.Sscanf(pidStr, "%d", &pid); err != nil {
		return info, fmt.Errorf("invalid pid: %s", pidStr)
	}
	if _, err := fmt.Sscanf(sidStr, "%d", &sid); err != nil {
		return info, fmt.Errorf("invalid sid: %s", sidStr)
	}

	info.Pid = pid
	info.Sid = sid

	return info, nil
}

// parseAuthInfo parses the authorization info field
// Format: &p=ap&n=[dn]&h=[hn]&o=[on]&r=[rn]&g=[gn]&m=[info]&x=[xeqname]&y=[minfo]&I={4|6}
func parseAuthInfo(b []byte) AuthInfo {
	info := AuthInfo{}
	if len(b) == 0 {
		return info
	}

	// Split by & and parse key=value pairs
	parts := bytesplit(b, '&')
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		// Find first = to split key and value (value may contain =)
		eqIdx := bytesIndexByte(part, '=')
		if eqIdx < 0 || eqIdx >= len(part)-1 {
			continue
		}

		key := string(part[:eqIdx])
		value := string(part[eqIdx+1:])

		switch key {
		case "p":
			info.AuthProtocol = value
		case "n":
			info.DN = value
		case "h":
			info.Hostname = value
		case "o":
			info.Org = value
		case "r":
			info.Role = value
		case "g":
			info.Groups = value
		case "m":
			info.Info = value
		case "x":
			info.ExecName = value
		case "y":
			info.MonInfo = value
		case "I":
			info.InetVersion = value
		}
	}

	return info
}

// parseTokenRecord parses a token packet (type 'T')
// Format: userid\ntokeninfo
// tokeninfo format: &Uc=udid&s=subj&n=[un]&o=[on]&r=[rn]&g=[gn]
func parseTokenRecord(header Header, b []byte) (*UserRecord, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("token record too short: %d bytes", len(b))
	}

	dictId := binary.BigEndian.Uint32(b[8:12])
	info := b[12:]

	// Split userInfo and tokenInfo by newline
	var userInfoBytes, tokenInfoBytes []byte
	newlineIdx := -1
	for i, b := range info {
		if b == '\n' {
			newlineIdx = i
			break
		}
	}

	if newlineIdx >= 0 {
		userInfoBytes = info[:newlineIdx]
		tokenInfoBytes = info[newlineIdx+1:]
	} else {
		// No newline found, entire info is userInfo
		userInfoBytes = info
		tokenInfoBytes = []byte{}
	}

	// Parse userInfo
	userInfo, err := parseUserInfo(userInfoBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user info: %w", err)
	}

	// Parse tokenInfo
	tokenInfo := parseTokenInfo(tokenInfoBytes)

	return &UserRecord{
		Header:    header,
		DictId:    dictId,
		UserInfo:  userInfo,
		TokenInfo: tokenInfo,
	}, nil
}

// parseTokenInfo parses the tokeninfo field from 'T' packets
// Format: &Uc=udid&s=subj&n=[un]&o=[on]&r=[rn]&g=[gn]
func parseTokenInfo(b []byte) TokenInfo {
	info := TokenInfo{}
	if len(b) == 0 {
		return info
	}

	// Split by & and parse key=value pairs
	parts := bytesplit(b, '&')
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		// Find first = to split key and value
		eqIdx := bytesIndexByte(part, '=')
		if eqIdx < 0 || eqIdx >= len(part)-1 {
			continue
		}

		key := string(part[:eqIdx])
		value := string(part[eqIdx+1:])

		switch key {
		case "Uc":
			// Parse user dictionary ID as uint32
			var udid uint32
			fmt.Sscanf(value, "%d", &udid)
			info.UserDictID = udid
		case "s":
			info.Subject = value
		case "n":
			info.Username = value
		case "o":
			info.Org = value
		case "r":
			info.Role = value
		case "g":
			info.Groups = value
		}
	}

	return info
}

// Helper functions for byte slice operations
func bytesIndexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

func bytesLastIndexByte(b []byte, c byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func bytesplit(b []byte, sep byte) [][]byte {
	var parts [][]byte
	start := 0
	for i, v := range b {
		if v == sep {
			parts = append(parts, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		parts = append(parts, b[start:])
	}
	return parts
}

// parseGStreamRecord parses a gstream packet (type 'g')
// Format: begin (uint32), end (uint32), ident (uint64), events (JSON string)
// The top 8 bits of ident contain the stream type: 'T'=TCP, 'C'=Cache, 'P'=TPC
func parseGStreamRecord(b []byte) (*GStreamRecord, error) {
	// Minimum size: 4 + 4 + 8 = 16 bytes header
	if len(b) < 16 {
		return nil, fmt.Errorf("gstream packet too short: %d bytes", len(b))
	}

	gstream := &GStreamRecord{}

	// Parse header fields (big endian)
	gstream.Begin = binary.BigEndian.Uint32(b[0:4])
	gstream.End = binary.BigEndian.Uint32(b[4:8])
	gstream.Ident = binary.BigEndian.Uint64(b[8:16])

	// Extract stream type from top 8 bits of ident
	gstream.StreamType = byte(gstream.Ident >> 56)

	// Rest is JSON events string (null-terminated)
	if len(b) > 16 {
		eventData := b[16:]
		// Strip null termination
		eventData = bytes.TrimRight(eventData, "\x00")

		if len(eventData) > 0 {
			// Events are newline-separated JSON objects
			eventStr := string(eventData)
			lines := strings.Split(eventStr, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				var event map[string]interface{}
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					return nil, fmt.Errorf("failed to parse gstream event JSON: %w", err)
				}
				gstream.Events = append(gstream.Events, event)
			}
		}
	}

	return gstream, nil
}

// parseFileRecords parses file operation records from a packet
func parseFileRecords(header Header, b []byte, packetType byte) ([]interface{}, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("file record packet too short: %d bytes", len(b))
	}

	// Skip the main packet header (8 bytes)
	reader := bytes.NewReader(b[8:])

	// f-stream packets always start with a FileTOD structure (RecType=isTime=2)
	// We need to skip this first record before processing actual file records
	if reader.Len() >= 8 {
		// Peek at the first record to check if it's a time record
		var firstHeader binaryFileHeaderCommon
		pos, _ := reader.Seek(0, io.SeekCurrent)
		if err := binary.Read(reader, binary.BigEndian, &firstHeader); err == nil {
			if firstHeader.RecType == RecTypeTime {
				// Skip to the end of this record
				reader.Seek(pos+int64(firstHeader.RecSize), io.SeekStart)
			} else {
				// Reset to the start of this record
				reader.Seek(pos, io.SeekStart)
			}
		} else {
			reader.Seek(pos, io.SeekStart)
		}
	}

	var records []interface{}

	for reader.Len() > 0 {
		// Save position before reading header
		pos, _ := reader.Seek(0, io.SeekCurrent)

		// Read common file record header
		var commonHeader binaryFileHeaderCommon
		if err := binary.Read(reader, binary.BigEndian, &commonHeader); err != nil {
			break // Not enough bytes for file record header
		}

		// Validate record size
		if commonHeader.RecSize < 8 || commonHeader.RecSize > 16384 {
			break // Invalid record size
		}

		// Check if we have enough data for the complete record
		if reader.Len()+8 < int(commonHeader.RecSize) {
			break // Not enough data for complete record
		}

		// Reset reader to start of record for type-specific parsing
		reader.Seek(pos, io.SeekStart)

		// Parse based on record type
		switch commonHeader.RecType {
		case RecTypeTime:
			var timeRec binaryFileTimeRecord
			if err := binary.Read(reader, binary.BigEndian, &timeRec); err != nil {
				return records, fmt.Errorf("failed to parse time record: %w", err)
			}
			records = append(records, FileTimeRecord{
				Header: FileHeader{
					RecType: timeRec.RecType,
					RecFlag: timeRec.RecFlag,
					RecSize: timeRec.RecSize,
					FileId:  timeRec.FileId,
					NRecs0:  int16(timeRec.NRecs0),
					NRecs1:  int16(timeRec.NRecs1),
				},
				TBeg: int32(timeRec.TBeg),
				TEnd: int32(timeRec.TEnd),
				SID:  int64(timeRec.SID),
			})

		case RecTypeClose:
			var closeHeader binaryFileCloseRecordHeader
			if err := binary.Read(reader, binary.BigEndian, &closeHeader); err != nil {
				return records, fmt.Errorf("failed to parse close record header: %w", err)
			}

			closeRec := FileCloseRecord{
				Header: FileHeader{
					RecType: closeHeader.RecType,
					RecFlag: closeHeader.RecFlag,
					RecSize: closeHeader.RecSize,
					FileId:  closeHeader.FileId,
				},
				Xfr: StatXFR{
					Read:  int64(closeHeader.Read),
					Readv: int64(closeHeader.Readv),
					Write: int64(closeHeader.Write),
				},
			}

			// Check if ops stats are present (RecFlag & 0x02 means hasOPS)
			if (closeHeader.RecFlag&0x02) != 0 && reader.Len() >= 48 {
				var ops binaryStatOPS
				if err := binary.Read(reader, binary.BigEndian, &ops); err == nil {
					closeRec.Ops = StatOPS{
						Read:  int32(ops.Read),
						Readv: int32(ops.Readv),
						Write: int32(ops.Write),
						RsMin: int16(ops.RsMin),
						RsMax: int16(ops.RsMax),
						Rsegs: int64(ops.Rsegs),
						RdMin: int32(ops.RdMin),
						RdMax: int32(ops.RdMax),
						RvMin: int32(ops.RvMin),
						RvMax: int32(ops.RvMax),
						WrMin: int32(ops.WrMin),
						WrMax: int32(ops.WrMax),
					}
				}
			}

			// Ensure we're at the end of the record
			reader.Seek(pos+int64(closeHeader.RecSize), io.SeekStart)
			records = append(records, closeRec)

		case RecTypeOpen:
			var openHeader binaryFileOpenRecordHeader
			if err := binary.Read(reader, binary.BigEndian, &openHeader); err != nil {
				return records, fmt.Errorf("failed to parse open record header: %w", err)
			}

			openRec := FileOpenRecord{
				Header: FileHeader{
					RecType: openHeader.RecType,
					RecFlag: openHeader.RecFlag,
					RecSize: openHeader.RecSize,
					FileId:  openHeader.FileId,
				},
				FileSize: int64(openHeader.FileSize),
				User:     0,
			}

			// UserId is present if RecFlag bit 0 is set (RecFlag & 1)
			if (openHeader.RecFlag & 0x01) != 0 {
				var userId uint32
				if err := binary.Read(reader, binary.BigEndian, &userId); err == nil {
					openRec.User = userId
					openRec.Header.UserId = userId // Also set in header for consistency with close records
				}
			}

			// Read remaining bytes as filename
			currentPos, _ := reader.Seek(0, io.SeekCurrent)
			recordEnd := pos + int64(openHeader.RecSize)
			lfnLen := recordEnd - currentPos
			if lfnLen > 0 {
				lfnBytes := make([]byte, lfnLen)
				if _, err := io.ReadFull(reader, lfnBytes); err == nil {
					// Trim trailing null bytes
					for len(lfnBytes) > 0 && lfnBytes[len(lfnBytes)-1] == 0 {
						lfnBytes = lfnBytes[:len(lfnBytes)-1]
					}
					openRec.Lfn = lfnBytes
				}
			}

			// Ensure we're at the end of the record
			reader.Seek(recordEnd, io.SeekStart)
			records = append(records, openRec)

		default:
			// Skip unknown record types
			reader.Seek(pos+int64(commonHeader.RecSize), io.SeekStart)
		}
	}

	return records, nil
}

// GetRequestID returns a unique identifier for the request (for correlation)
func (p *Packet) GetRequestID() string {
	if p.MapRecord != nil {
		return fmt.Sprintf("map-%d", p.MapRecord.DictId)
	}

	if len(p.FileRecords) > 0 {
		switch rec := p.FileRecords[0].(type) {
		case FileTimeRecord:
			return fmt.Sprintf("time-%d-%d", rec.Header.FileId, rec.SID)
		case FileCloseRecord:
			return fmt.Sprintf("close-%d-%d", rec.Header.FileId, rec.Header.UserId)
		case FileOpenRecord:
			return fmt.Sprintf("open-%d-%d", rec.Header.FileId, rec.Header.UserId)
		}
	}

	return fmt.Sprintf("unknown-%d", p.Header.Pseq)
}
