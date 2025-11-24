package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

// CollectorRecord represents a correlated file access record
type CollectorRecord struct {
	Timestamp              time.Time `json:"@timestamp"`
	StartTime              int64     `json:"start_time"`
	EndTime                int64     `json:"end_time"`
	OperationTime          int64     `json:"operation_time"`
	ServerID               string    `json:"serverID"`
	ServerHostname         string    `json:"server_hostname"`
	Server                 string    `json:"server"`
	ServerIP               string    `json:"server_ip"`
	Site                   string    `json:"site"`
	User                   string    `json:"user"`
	UserDN                 string    `json:"user_dn"`
	UserDomain             string    `json:"user_domain,omitempty"`
	VO                     string    `json:"vo,omitempty"`
	Host                   string    `json:"host"`
	TokenSubject           string    `json:"token_subject,omitempty"`
	TokenUsername          string    `json:"token_username,omitempty"`
	TokenOrg               string    `json:"token_org,omitempty"`
	TokenRole              string    `json:"token_role,omitempty"`
	TokenGroups            string    `json:"token_groups,omitempty"`
	Filename               string    `json:"filename"`
	Dirname1               string    `json:"dirname1"`
	Dirname2               string    `json:"dirname2"`
	LogicalDirname         string    `json:"logical_dirname"`
	Protocol               string    `json:"protocol"`
	AppInfo                string    `json:"appinfo"`
	IPv6                   bool      `json:"ipv6"`
	Filesize               int64     `json:"filesize"`
	ReadOperations         int32     `json:"read_operations"`
	ReadSingleOperations   int32     `json:"read_single_operations"`
	ReadVectorOperations   int32     `json:"read_vector_operations"`
	WriteOperations        int32     `json:"write_operations"`
	Read                   int64     `json:"read"`
	ReadSingleBytes        int64     `json:"read_single_bytes"`
	Readv                  int64     `json:"readv"`
	Write                  int64     `json:"write"`
	ReadMin                int32     `json:"read_min"`
	ReadMax                int32     `json:"read_max"`
	ReadAverage            int64     `json:"read_average"`
	ReadSingleMin          int32     `json:"read_single_min"`
	ReadSingleMax          int32     `json:"read_single_max"`
	ReadSingleAverage      int64     `json:"read_single_average"`
	ReadVectorMin          int32     `json:"read_vector_min"`
	ReadVectorMax          int32     `json:"read_vector_max"`
	ReadVectorAverage      int64     `json:"read_vector_average"`
	WriteMin               int32     `json:"write_min"`
	WriteMax               int32     `json:"write_max"`
	WriteAverage           int64     `json:"write_average"`
	ReadVectorCountMin     int16     `json:"read_vector_count_min"`
	ReadVectorCountMax     int16     `json:"read_vector_count_max"`
	ReadVectorCountAverage float64   `json:"read_vector_count_average"`
	ReadBytesAtClose       int64     `json:"read_bytes_at_close"`
	WriteBytesAtClose      int64     `json:"write_bytes_at_close"`
	HasFileCloseMsg        int       `json:"HasFileCloseMsg"`
}

// GStreamEvent represents a gstream event with added server information
// These events don't require correlation - just add serverID and address
type GStreamEvent struct {
	Event map[string]interface{} // The original JSON event
}

// FileState tracks the state of an open file
type FileState struct {
	FileID    uint32
	UserID    uint32
	OpenTime  int64
	FileSize  int64
	Filename  string
	ServerID  string
	StreamID  int64
	CreatedAt time.Time
}

// UserState tracks user information from user packets
type UserState struct {
	UserID    uint32
	UserInfo  parser.UserInfo
	AuthInfo  parser.AuthInfo
	TokenInfo parser.TokenInfo
	AppInfo   string
	CreatedAt time.Time
}

// PathInfo represents path mapping with associated user info
type PathInfo struct {
	Path     string
	UserInfo parser.UserInfo
}

// Correlator correlates file open and close events
type Correlator struct {
	stateMap *StateMap
	userMap  *StateMap
	dictMap  *StateMap // Maps dictid to path/user info
}

// NewCorrelator creates a new correlator
func NewCorrelator(ttl time.Duration, maxEntries int) *Correlator {
	return &Correlator{
		stateMap: NewStateMap(ttl, maxEntries, ttl/10),
		userMap:  NewStateMap(ttl, maxEntries, ttl/10),
		dictMap:  NewStateMap(ttl, maxEntries, ttl/10),
	}
}

// ProcessPacket processes a packet and returns a record if correlation is complete
// Note: In practice, each packet typically contains only one file record, but we handle
// multiple records correctly by returning only the first emitted record.
func (c *Correlator) ProcessPacket(packet *parser.Packet) (*CollectorRecord, error) {
	if packet.IsXML {
		// XML packets are not correlated
		return nil, nil
	}

	// Calculate server ID: serverStart#addr#port
	serverID := c.getServerID(packet)

	// Handle dict ID packets ('d' type for path mappings, 'i' for appinfo)
	if packet.MapRecord != nil {
		c.handleDictIDRecord(packet.MapRecord, serverID, packet.PacketType)
		return nil, nil
	}

	// Handle user packets
	if packet.UserRecord != nil {
		c.handleUserRecord(packet.UserRecord, serverID)
		return nil, nil
	}

	// Process all file records and return the first complete record
	// In practice, packets usually have one file record, but we handle multiple correctly
	for _, rec := range packet.FileRecords {
		switch r := rec.(type) {
		case parser.FileOpenRecord:
			result, err := c.handleFileOpen(r, packet, serverID)
			if err != nil || result != nil {
				return result, err
			}
		case parser.FileCloseRecord:
			result, err := c.handleFileClose(r, packet, serverID)
			if err != nil || result != nil {
				return result, err
			}
		case parser.FileTimeRecord:
			result, err := c.handleTimeRecord(r, packet, serverID)
			if err != nil || result != nil {
				return result, err
			}
		case parser.FileDisconnectRecord:
			c.handleDisconnect(r, serverID)
			// Disconnect doesn't generate a record, just cleanup
		}
	}

	return nil, nil
}

// ProcessGStreamPacket processes a gstream packet and returns enriched events
// GStream events don't need correlation - just add server information
// Returns: (events []map[string]interface{}, streamType byte, error)
func (c *Correlator) ProcessGStreamPacket(packet *parser.Packet) ([]map[string]interface{}, byte, error) {
	if packet.GStreamRecord == nil {
		return nil, 0, nil
	}

	gstream := packet.GStreamRecord
	serverID := c.getServerID(packet)

	// Extract address from packet
	addr := packet.RemoteAddr
	// Remove port for IP address
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// If split fails, use the whole address
		host = addr
	}

	// Enrich each event with server information
	enrichedEvents := make([]map[string]interface{}, 0, len(gstream.Events))
	for _, event := range gstream.Events {
		// Make a copy to avoid modifying the original
		enrichedEvent := make(map[string]interface{})
		for k, v := range event {
			enrichedEvent[k] = v
		}

		// Add server information
		enrichedEvent["sid"] = serverID
		enrichedEvent["server_ip"] = host
		enrichedEvent["from"] = addr

		enrichedEvents = append(enrichedEvents, enrichedEvent)
	}

	return enrichedEvents, gstream.StreamType, nil
}

// getServerID creates a unique server identifier from server start time, address, and port
// Format: serverStart#addr#port (matching Python implementation)
func (c *Correlator) getServerID(packet *parser.Packet) string {
	return fmt.Sprintf("%d#%s", packet.Header.ServerStart, packet.RemoteAddr)
}

// handleDictIDRecord stores path/user dictionary ID mappings
// For 'd' packets: maps dictID -> PathInfo (userInfo + path)
// For 'i' packets: adds appinfo to user state
func (c *Correlator) handleDictIDRecord(rec *parser.MapRecord, serverID string, packetType byte) {
	info := rec.Info

	// Split on newline - first part is userInfo, rest is additional info
	parts := bytes.SplitN(info, []byte("\n"), 2)
	if len(parts) == 0 {
		return
	}

	// Parse userInfo from first part
	userInfoBytes := parts[0]
	userInfo, err := parseUserInfo(userInfoBytes)
	if err != nil {
		// If we can't parse userInfo, just store the raw string for paths
		key := fmt.Sprintf("%s-dict-%d", serverID, rec.DictId)
		c.dictMap.Set(key, string(rec.Info))
		return
	}

	if packetType == parser.PacketTypeDictID { // 'd' packet
		// Path mapping: store dictID -> PathInfo
		if len(parts) > 1 {
			pathInfo := &PathInfo{
				Path:     string(parts[1]),
				UserInfo: userInfo,
			}
			key := fmt.Sprintf("%s-dict-%d", serverID, rec.DictId)
			c.dictMap.Set(key, pathInfo)
		}

		// Also store dictID -> userInfo for user lookup
		userKey := fmt.Sprintf("%s-dictid-%d", serverID, rec.DictId)
		c.dictMap.Set(userKey, userInfo)

	} else if packetType == parser.PacketTypeInfo { // 'i' packet
		// App info: rest of info after userInfo
		if len(parts) > 1 {
			appInfo := string(parts[1])

			// Store dictID -> userInfo mapping
			userKey := fmt.Sprintf("%s-dictid-%d", serverID, rec.DictId)
			c.dictMap.Set(userKey, userInfo)

			// Update or create user state with appinfo
			// Create a user key based on the userInfo string representation
			userStateKey := fmt.Sprintf("%s-userinfo-%s", serverID, userInfoString(userInfo))
			val, exists := c.userMap.Get(userStateKey)
			if exists {
				if userState, ok := val.(*UserState); ok {
					userState.AppInfo = appInfo
					c.userMap.Set(userStateKey, userState)
				}
			} else {
				// Create new user state with appinfo
				userState := &UserState{
					UserID:    rec.DictId,
					UserInfo:  userInfo,
					AppInfo:   appInfo,
					CreatedAt: time.Now(),
				}
				c.userMap.Set(userStateKey, userState)
			}
		}
	}
}

// parseUserInfo parses userInfo from bytes
// Format: [protocol/]username.pid:sid@host
func parseUserInfo(data []byte) (parser.UserInfo, error) {
	// Try to parse using the same logic as in xrootd_parser.go
	// This is a simplified version - the full parser handles this in parseUserInfo
	info := string(data)
	parts := strings.SplitN(info, "@", 2)
	if len(parts) != 2 {
		return parser.UserInfo{}, fmt.Errorf("invalid userInfo format: no @ found")
	}

	host := parts[1]
	userPart := parts[0]

	// Check for protocol
	protocol := ""
	if idx := strings.Index(userPart, "/"); idx >= 0 {
		protocol = userPart[:idx]
		userPart = userPart[idx+1:]
	}

	// Parse username.pid:sid
	pidSidParts := strings.SplitN(userPart, ".", 2)
	if len(pidSidParts) != 2 {
		return parser.UserInfo{}, fmt.Errorf("invalid userInfo format: no . found")
	}

	username := pidSidParts[0]
	pidSid := pidSidParts[1]

	// Parse pid:sid
	pidSidSplit := strings.SplitN(pidSid, ":", 2)
	pid := 0
	sid := 0
	if len(pidSidSplit) == 2 {
		pid, _ = strconv.Atoi(pidSidSplit[0])
		sid, _ = strconv.Atoi(pidSidSplit[1])
	}

	return parser.UserInfo{
		Protocol: protocol,
		Username: username,
		Pid:      pid,
		Sid:      sid,
		Host:     host,
	}, nil
}

// userInfoString creates a unique string key for a UserInfo
func userInfoString(info parser.UserInfo) string {
	return fmt.Sprintf("%s/%s.%d:%d@%s", info.Protocol, info.Username, info.Pid, info.Sid, info.Host)
}

// isIPPattern checks if a string looks like an IP address pattern
// Based on Python regex: r"^[\[\:f\d\.]+" (starts with [, :, f, or digits/dots)
func isIPPattern(s string) bool {
	if len(s) == 0 {
		return false
	}
	// Check if it starts with IP-like characters
	firstChar := s[0]
	return firstChar == '[' || firstChar == ':' || firstChar == 'f' ||
		(firstChar >= '0' && firstChar <= '9') || firstChar == '.'
}

// handleFileOpen handles a file open event
func (c *Correlator) handleFileOpen(rec parser.FileOpenRecord, packet *parser.Packet, serverID string) (*CollectorRecord, error) {
	// Filename may come from Lfn field OR from dictid lookup
	filename := string(rec.Lfn)
	if filename == "" && rec.Header.FileId != 0 {
		// No filename in open record, try to get it from dict ID
		dictKey := fmt.Sprintf("%s-dict-%d", serverID, rec.Header.FileId)
		if val, exists := c.dictMap.Get(dictKey); exists {
			if path, ok := val.(string); ok {
				filename = path
			}
		}
	}

	// Determine userId - use Header.UserId (now set by parser) or fallback to User field
	userId := rec.Header.UserId
	if userId == 0 {
		userId = rec.User
	}

	state := &FileState{
		FileID:    rec.Header.FileId,
		UserID:    userId,
		OpenTime:  int64(packet.Header.ServerStart),
		FileSize:  rec.FileSize,
		Filename:  filename,
		ServerID:  serverID,
		CreatedAt: time.Now(),
	}

	// Key is only serverID + fileID (not userId)
	key := fmt.Sprintf("%s-file-%d", serverID, rec.Header.FileId)
	c.stateMap.Set(key, state)

	return nil, nil
}

// handleFileClose handles a file close event
func (c *Correlator) handleFileClose(rec parser.FileCloseRecord, packet *parser.Packet, serverID string) (*CollectorRecord, error) {
	// Key is only serverID + fileID (matches the key used in handleFileOpen)
	key := fmt.Sprintf("%s-file-%d", serverID, rec.Header.FileId)

	// Try to get the open state
	val, exists := c.stateMap.Get(key)
	if !exists {
		// No open record found, create a standalone close record
		return c.createStandaloneCloseRecord(rec, packet), nil
	}

	state, ok := val.(*FileState)
	if !ok {
		return nil, fmt.Errorf("invalid state type")
	}

	// Create correlated record
	record := c.createCorrelatedRecord(state, rec, packet)

	// Remove from state map
	c.stateMap.Delete(key)

	return record, nil
}

// handleTimeRecord handles a time record
func (c *Correlator) handleTimeRecord(rec parser.FileTimeRecord, packet *parser.Packet, serverID string) (*CollectorRecord, error) {
	// Time records can be used to update state or create timing records
	// For now, we'll store them for potential correlation
	key := fmt.Sprintf("%s-time-%d-%d", serverID, rec.Header.FileId, rec.SID)
	state := &FileState{
		FileID:    rec.Header.FileId,
		UserID:    rec.Header.UserId,
		OpenTime:  int64(rec.TBeg),
		ServerID:  serverID,
		StreamID:  rec.SID,
		CreatedAt: time.Now(),
	}
	c.stateMap.Set(key, state)
	return nil, nil
}

// handleUserRecord handles a user packet (type 'u')
// Stores user information mapped by dictID and serverID for later correlation with file operations
// Following Python logic: dictID -> userInfo mapping, and userInfo -> full user state
func (c *Correlator) handleUserRecord(rec *parser.UserRecord, serverID string) {
	userState := &UserState{
		UserID:    rec.DictId,
		UserInfo:  rec.UserInfo,
		AuthInfo:  rec.AuthInfo,
		TokenInfo: rec.TokenInfo,
		CreatedAt: time.Now(),
	}

	// Store dictID -> userInfo mapping
	dictKey := fmt.Sprintf("%s-dictid-%d", serverID, rec.DictId)
	c.dictMap.Set(dictKey, rec.UserInfo)

	// Store userInfo -> userState mapping
	userInfoKey := fmt.Sprintf("%s-userinfo-%s", serverID, userInfoString(rec.UserInfo))
	c.userMap.Set(userInfoKey, userState)
}

// handleDisconnect handles a user disconnect event
// Cleans up all references to the disconnecting user
func (c *Correlator) handleDisconnect(rec parser.FileDisconnectRecord, serverID string) {
	// Get the userInfo from dictID mapping
	dictKey := fmt.Sprintf("%s-dictid-%d", serverID, rec.UserID)
	val, exists := c.dictMap.Get(dictKey)
	if !exists {
		// User not found in dict map, nothing to clean up
		return
	}

	userInfo, ok := val.(parser.UserInfo)
	if !ok {
		// Not a UserInfo type, skip
		return
	}

	// Delete the dictID -> userInfo mapping
	c.dictMap.Delete(dictKey)

	// Delete the userInfo -> userState mapping
	userInfoKey := fmt.Sprintf("%s-userinfo-%s", serverID, userInfoString(userInfo))
	c.userMap.Delete(userInfoKey)

	// Note: We don't delete file states here because disconnect doesn't imply
	// all files are closed. File states will expire via TTL or be removed on close.
}

// getUserInfo retrieves user information for a given userID and serverID
// Follows Python logic: userID -> dictID lookup -> userInfo -> full user state
func (c *Correlator) getUserInfo(userID uint32, fileID uint32, serverID string) *UserState {
	var userInfo parser.UserInfo
	var found bool

	// Try to get userInfo from dictID mapping (for userID if non-zero)
	if userID != 0 {
		dictKey := fmt.Sprintf("%s-dictid-%d", serverID, userID)
		if val, exists := c.dictMap.Get(dictKey); exists {
			if ui, ok := val.(parser.UserInfo); ok {
				userInfo = ui
				found = true
			}
		}
	}

	// If userID is 0 or not found, try to get from fileID (path mapping)
	if !found && fileID != 0 {
		dictKey := fmt.Sprintf("%s-dict-%d", serverID, fileID)
		if val, exists := c.dictMap.Get(dictKey); exists {
			if pathInfo, ok := val.(*PathInfo); ok {
				userInfo = pathInfo.UserInfo
				found = true
			}
		}
	}

	if !found {
		return nil
	}

	// Now look up the full user state using userInfo
	userInfoKey := fmt.Sprintf("%s-userinfo-%s", serverID, userInfoString(userInfo))
	val, exists := c.userMap.Get(userInfoKey)
	if !exists {
		// UserState not found (no 'u' packet received yet), but we have userInfo from 'd' packet
		// Create a minimal UserState with just the userInfo
		return &UserState{
			UserInfo: userInfo,
			// AuthInfo will be empty - no 'u' packet received
		}
	}

	userState, ok := val.(*UserState)
	if !ok {
		return nil
	}

	return userState
}

// extractDirnames extracts dirname1, dirname2, and logical_dirname from a filepath
func extractDirnames(filename string) (dirname1, dirname2, logicalDirname string) {
	if filename == "" || filename == "unknown" || filename == "/" {
		return "unknown directory", "unknown directory", "unknown directory"
	}

	// Clean the path to normalize it
	cleanPath := path.Clean(filename)

	// Split the path into components
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/"), "/")

	// dirname1 is the first component
	if len(parts) > 0 && parts[0] != "" {
		dirname1 = "/" + parts[0]
	} else {
		dirname1 = "unknown directory"
	}

	// dirname2 is the first 2 components joined with /
	if len(parts) > 1 && parts[0] != "" {
		dirname2 = "/" + path.Join(parts[0], parts[1])
	} else {
		dirname2 = dirname1
	}

	// Determine logical_dirname based on path patterns
	// Ref: https://github.com/opensciencegrid/xrootd-monitoring-collector/blob/master/Collectors/DetailedCollector.py#L174
	switch {
	case strings.HasPrefix(cleanPath, "/user"):
		logicalDirname = dirname2
	case strings.HasPrefix(cleanPath, "/osgconnect/public") || strings.HasPrefix(cleanPath, "/osgconnect/protected") || strings.HasPrefix(cleanPath, "/ospool/PROTECTED"):
		if len(parts) >= 3 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/ospool"):
		if len(parts) >= 4 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2], parts[3])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/path-facility"):
		if len(parts) >= 3 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/hcc"):
		if len(parts) >= 5 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2], parts[3], parts[4])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/pnfs/fnal.gov/usr"):
		if len(parts) >= 4 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2], parts[3])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/gwdata"):
		logicalDirname = dirname2
	case strings.HasPrefix(cleanPath, "/chtc/"):
		logicalDirname = "/chtc"
	case strings.HasPrefix(cleanPath, "/icecube/"):
		logicalDirname = "/icecube"
	case strings.HasPrefix(cleanPath, "/igwn"):
		if len(parts) >= 3 {
			logicalDirname = "/" + path.Join(parts[0], parts[1], parts[2])
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(cleanPath, "/store") || strings.HasPrefix(cleanPath, "/user/dteam"):
		logicalDirname = dirname2
	default:
		logicalDirname = "unknown directory"
	}

	return dirname1, dirname2, logicalDirname
}

// createCorrelatedRecord creates a collector record from correlated state
func (c *Correlator) createCorrelatedRecord(state *FileState, rec parser.FileCloseRecord, packet *parser.Packet) *CollectorRecord {
	now := time.Now()

	// Calculate averages
	var readAvg, readSingleAvg, readVectorAvg, writeAvg int64
	if rec.Ops.Read > 0 {
		readAvg = rec.Xfr.Read / int64(rec.Ops.Read)
		readSingleAvg = rec.Xfr.Read / int64(rec.Ops.Read)
	}
	if rec.Ops.Readv > 0 {
		readVectorAvg = rec.Xfr.Readv / int64(rec.Ops.Readv)
	}
	if rec.Ops.Write > 0 {
		writeAvg = rec.Xfr.Write / int64(rec.Ops.Write)
	}

	var readvCountAvg float64
	if rec.Ops.Readv > 0 {
		readvCountAvg = float64(rec.Ops.Rsegs) / float64(rec.Ops.Readv)
	}

	// Get user information if available (using userID, fileID and serverID)
	userInfo := c.getUserInfo(state.UserID, state.FileID, state.ServerID)

	// Set defaults
	user := fmt.Sprintf("%x", state.UserID)
	userDN := ""
	userDomain := ""
	vo := ""
	host := "unknown"
	protocol := "unknown"
	appInfo := ""
	ipv6 := false
	tokenSubject := ""
	tokenUsername := ""
	tokenOrg := ""
	tokenRole := ""
	tokenGroups := ""

	if userInfo != nil {
		// Use username from userInfo
		user = userInfo.UserInfo.Username
		host = userInfo.UserInfo.Host
		protocol = userInfo.UserInfo.Protocol

		// Extract user_domain from hostname if it's not an IP address
		// Python regex: r"^[\[\:f\d\.]+" checks if hostname starts with [, :, f, or digits/dots (IP patterns)
		if host != "" && !isIPPattern(host) {
			parts := strings.Split(host, ".")
			if len(parts) >= 2 {
				userDomain = strings.Join(parts[len(parts)-2:], ".")
			}
		}

		// Use DN from authInfo (split on :: and take first part)
		if userInfo.AuthInfo.DN != "" {
			parts := strings.Split(userInfo.AuthInfo.DN, "::")
			userDN = parts[0]
		}

		// Extract VO from authInfo.Org field
		if userInfo.AuthInfo.Org != "" {
			vo = userInfo.AuthInfo.Org
		}

		// Use appInfo if available
		if userInfo.AppInfo != "" {
			appInfo = userInfo.AppInfo
		}

		// Check if IPv6
		if userInfo.AuthInfo.InetVersion == "6" {
			ipv6 = true
		}

		// Extract token information if available
		if userInfo.TokenInfo.Subject != "" {
			tokenSubject = userInfo.TokenInfo.Subject
		}
		if userInfo.TokenInfo.Username != "" {
			tokenUsername = userInfo.TokenInfo.Username
		}
		if userInfo.TokenInfo.Org != "" {
			tokenOrg = userInfo.TokenInfo.Org
		}
		if userInfo.TokenInfo.Role != "" {
			tokenRole = userInfo.TokenInfo.Role
		}
		if userInfo.TokenInfo.Groups != "" {
			tokenGroups = userInfo.TokenInfo.Groups
		}
	}

	// Extract directory names from filename
	dirname1, dirname2, logicalDirname := extractDirnames(state.Filename)

	// Parse RemoteAddr to extract server IP and hostname
	serverIP := "unknown"
	serverHostname := "unknown"
	if packet.RemoteAddr != "" {
		// RemoteAddr is in format "host:port" or "[ipv6]:port"
		host, _, err := net.SplitHostPort(packet.RemoteAddr)
		if err == nil {
			serverIP = host
			serverHostname = host // Could do reverse DNS lookup here if needed
		} else {
			// If SplitHostPort fails, use the whole RemoteAddr
			serverIP = packet.RemoteAddr
			serverHostname = packet.RemoteAddr
		}
	}

	return &CollectorRecord{
		Timestamp:              now,
		StartTime:              state.OpenTime,
		EndTime:                now.Unix(),
		OperationTime:          now.Unix() - state.OpenTime,
		ServerID:               fmt.Sprintf("%d#%s", packet.Header.ServerStart, packet.RemoteAddr),
		ServerHostname:         serverHostname,
		Server:                 serverIP,
		ServerIP:               serverIP,
		Site:                   "UNKNOWN",
		User:                   user,
		UserDN:                 userDN,
		UserDomain:             userDomain,
		VO:                     vo,
		Host:                   host,
		TokenSubject:           tokenSubject,
		TokenUsername:          tokenUsername,
		TokenOrg:               tokenOrg,
		TokenRole:              tokenRole,
		TokenGroups:            tokenGroups,
		Filename:               state.Filename,
		Dirname1:               dirname1,
		Dirname2:               dirname2,
		LogicalDirname:         logicalDirname,
		Protocol:               protocol,
		AppInfo:                appInfo,
		IPv6:                   ipv6,
		Filesize:               state.FileSize,
		ReadOperations:         rec.Ops.Read,
		ReadSingleOperations:   rec.Ops.Read,
		ReadVectorOperations:   rec.Ops.Readv,
		WriteOperations:        rec.Ops.Write,
		Read:                   rec.Xfr.Read,
		ReadSingleBytes:        rec.Xfr.Read,
		Readv:                  rec.Xfr.Readv,
		Write:                  rec.Xfr.Write,
		ReadMin:                rec.Ops.RdMin,
		ReadMax:                rec.Ops.RdMax,
		ReadAverage:            readAvg,
		ReadSingleMin:          rec.Ops.RdMin,
		ReadSingleMax:          rec.Ops.RdMax,
		ReadSingleAverage:      readSingleAvg,
		ReadVectorMin:          rec.Ops.RvMin,
		ReadVectorMax:          rec.Ops.RvMax,
		ReadVectorAverage:      readVectorAvg,
		WriteMin:               rec.Ops.WrMin,
		WriteMax:               rec.Ops.WrMax,
		WriteAverage:           writeAvg,
		ReadVectorCountMin:     rec.Ops.RsMin,
		ReadVectorCountMax:     rec.Ops.RsMax,
		ReadVectorCountAverage: readvCountAvg,
		ReadBytesAtClose:       rec.Xfr.Read,
		WriteBytesAtClose:      rec.Xfr.Write,
		HasFileCloseMsg:        1,
	}
}

// createStandaloneCloseRecord creates a record from just a close event
func (c *Correlator) createStandaloneCloseRecord(rec parser.FileCloseRecord, packet *parser.Packet) *CollectorRecord {
	// Use the same serverID format as getServerID()
	serverID := c.getServerID(packet)

	state := &FileState{
		FileID:   rec.Header.FileId,
		UserID:   rec.Header.UserId,
		OpenTime: int64(packet.Header.ServerStart),
		Filename: "unknown",
		ServerID: serverID,
	}
	return c.createCorrelatedRecord(state, rec, packet)
}

// ToJSON converts a collector record to JSON
func (r *CollectorRecord) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

// Stop stops the correlator
func (c *Correlator) Stop() {
	if c.stateMap != nil {
		c.stateMap.Stop()
	}
	if c.userMap != nil {
		c.userMap.Stop()
	}
}

// GetStateSize returns the current number of tracked states
func (c *Correlator) GetStateSize() int {
	return c.stateMap.Size()
}

// GetUserMapSize returns the current number of tracked users
func (c *Correlator) GetUserMapSize() int {
	return c.userMap.Size()
}
