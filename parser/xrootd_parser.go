package parser

import (
	"encoding/binary"
	"fmt"
)

// XRootD monitoring packet types as defined in:
// https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498
const (
	PacketTypeMap    byte = '=' // Dictionary packet
	PacketTypeFOpen  byte = 'f' // File open
	PacketTypeFClose byte = 'd' // File close
	PacketTypeGStream byte = 'g' // g-stream
	PacketTypeInfo   byte = 'i' // Info
	PacketTypePurg   byte = 'p' // Purge
	PacketTypeRedir  byte = 'r' // Redirect
	PacketTypeTime   byte = 't' // Time
	PacketTypeUser   byte = 'u' // User
	PacketTypeXFR    byte = 'x' // Transfer
)

// Record types for file records
const (
	RecTypeOpen   byte = 0
	RecTypeClose  byte = 1
	RecTypeTime   byte = 2
	RecTypeXFR    byte = 3
	RecTypeDisc   byte = 4
)

// Header represents the XRootD monitoring packet header (8 bytes)
// Ref: https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498
type Header struct {
	Code        byte   // Packet type code
	Pseq        uint8  // Packet sequence number
	Plen        uint16 // Packet length
	ServerStart int32  // Unix time at server start
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
	RecSize int16  // Record size
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

// UserRecord represents a user packet (type 'u')
type UserRecord struct {
	Header   Header
	DictId   uint32
	UserInfo UserInfo
	AuthInfo AuthInfo
}

// Packet is a parsed XRootD monitoring packet
type Packet struct {
	Header      Header
	PacketType  byte
	IsXML       bool
	MapRecord   *MapRecord
	UserRecord  *UserRecord
	FileRecords []interface{} // Can contain FileTimeRecord, FileCloseRecord, FileOpenRecord
	RawData     []byte        // Original packet data
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

	header := Header{
		Code:        b[0],
		Pseq:        uint8(b[1]),
		Plen:        binary.BigEndian.Uint16(b[2:4]),
		ServerStart: int32(binary.BigEndian.Uint32(b[4:8])),
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
		mapRec, err := parseMapRecord(b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map record: %w", err)
		}
		packet.MapRecord = mapRec
	case PacketTypeUser:
		userRec, err := parseUserRecord(b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse user record: %w", err)
		}
		packet.UserRecord = userRec
	case PacketTypeFClose, PacketTypeFOpen, PacketTypeXFR, PacketTypeTime:
		fileRecs, err := parseFileRecords(b, header.Code)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file records: %w", err)
		}
		packet.FileRecords = fileRecs
	case PacketTypeGStream, PacketTypeInfo, PacketTypePurg, PacketTypeRedir:
		// These packet types are supported but not fully parsed yet
		// They will be handled in shoveling mode as pass-through
	default:
		return nil, fmt.Errorf("unknown packet type: %c (0x%02x)", header.Code, header.Code)
	}

	return packet, nil
}

// parseMapRecord parses a dictionary mapping packet
func parseMapRecord(b []byte) (*MapRecord, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("map record too short: %d bytes", len(b))
	}

	header := Header{
		Code:        b[0],
		Pseq:        uint8(b[1]),
		Plen:        binary.BigEndian.Uint16(b[2:4]),
		ServerStart: int32(binary.BigEndian.Uint32(b[4:8])),
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
func parseUserRecord(b []byte) (*UserRecord, error) {
	if len(b) < 12 {
		return nil, fmt.Errorf("user record too short: %d bytes", len(b))
	}

	header := Header{
		Code:        b[0],
		Pseq:        uint8(b[1]),
		Plen:        binary.BigEndian.Uint16(b[2:4]),
		ServerStart: int32(binary.BigEndian.Uint32(b[4:8])),
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

// parseFileRecords parses file operation records from a packet
func parseFileRecords(b []byte, packetType byte) ([]interface{}, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("file record packet too short: %d bytes", len(b))
	}

	// Skip the main packet header (8 bytes)
	offset := 8
	var records []interface{}

	for offset < len(b) {
		if offset+16 > len(b) {
			break // Not enough bytes for file header
		}

		// Parse file header (16 bytes)
		fileHeader := FileHeader{
			RecType: b[offset],
			RecFlag: b[offset+1],
			RecSize: int16(binary.BigEndian.Uint16(b[offset+2 : offset+4])),
			FileId:  binary.BigEndian.Uint32(b[offset+4 : offset+8]),
			UserId:  binary.BigEndian.Uint32(b[offset+8 : offset+12]),
			NRecs0:  int16(binary.BigEndian.Uint16(b[offset+12 : offset+14])),
			NRecs1:  int16(binary.BigEndian.Uint16(b[offset+14 : offset+16])),
		}

		// Ensure we have enough data for the complete record
		if offset+int(fileHeader.RecSize) > len(b) {
			break
		}

		// Parse based on record type
		switch fileHeader.RecType {
		case RecTypeTime:
			if offset+32 > len(b) {
				return records, fmt.Errorf("not enough data for time record")
			}
			timeRec := FileTimeRecord{
				Header: fileHeader,
				TBeg:   int32(binary.BigEndian.Uint32(b[offset+16 : offset+20])),
				TEnd:   int32(binary.BigEndian.Uint32(b[offset+20 : offset+24])),
				SID:    int64(binary.BigEndian.Uint64(b[offset+24 : offset+32])),
			}
			records = append(records, timeRec)
			offset += 32

		case RecTypeClose:
			// Close record has variable size depending on whether ops are included
			if offset+40 > len(b) {
				return records, fmt.Errorf("not enough data for close record")
			}
			closeRec := FileCloseRecord{
				Header: fileHeader,
				Xfr: StatXFR{
					Read:  int64(binary.BigEndian.Uint64(b[offset+16 : offset+24])),
					Readv: int64(binary.BigEndian.Uint64(b[offset+24 : offset+32])),
					Write: int64(binary.BigEndian.Uint64(b[offset+32 : offset+40])),
				},
			}
			
			// Check if ops stats are present (record size > 40)
			if int(fileHeader.RecSize) >= 88 && offset+88 <= len(b) {
				closeRec.Ops = StatOPS{
					Read:  int32(binary.BigEndian.Uint32(b[offset+40 : offset+44])),
					Readv: int32(binary.BigEndian.Uint32(b[offset+44 : offset+48])),
					Write: int32(binary.BigEndian.Uint32(b[offset+48 : offset+52])),
					RsMin: int16(binary.BigEndian.Uint16(b[offset+52 : offset+54])),
					RsMax: int16(binary.BigEndian.Uint16(b[offset+54 : offset+56])),
					Rsegs: int64(binary.BigEndian.Uint64(b[offset+56 : offset+64])),
					RdMin: int32(binary.BigEndian.Uint32(b[offset+64 : offset+68])),
					RdMax: int32(binary.BigEndian.Uint32(b[offset+68 : offset+72])),
					RvMin: int32(binary.BigEndian.Uint32(b[offset+72 : offset+76])),
					RvMax: int32(binary.BigEndian.Uint32(b[offset+76 : offset+80])),
					WrMin: int32(binary.BigEndian.Uint32(b[offset+80 : offset+84])),
					WrMax: int32(binary.BigEndian.Uint32(b[offset+84 : offset+88])),
				}
			}
			records = append(records, closeRec)
			offset += int(fileHeader.RecSize)

		case RecTypeOpen:
			if offset+32 > len(b) {
				return records, fmt.Errorf("not enough data for open record")
			}
			openRec := FileOpenRecord{
				Header:   fileHeader,
				FileSize: int64(binary.BigEndian.Uint64(b[offset+16 : offset+24])),
				User:     binary.BigEndian.Uint32(b[offset+24 : offset+28]),
			}
			// Logical file name follows (variable length)
			lfnEnd := offset + int(fileHeader.RecSize)
			if lfnEnd <= len(b) {
				openRec.Lfn = b[offset+28 : lfnEnd]
			}
			records = append(records, openRec)
			offset += int(fileHeader.RecSize)

		default:
			// Skip unknown record types
			offset += int(fileHeader.RecSize)
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
