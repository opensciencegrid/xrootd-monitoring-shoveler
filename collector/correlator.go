package collector

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

// CollectorRecord represents a correlated file access record
type CollectorRecord struct {
	Timestamp               time.Time `json:"@timestamp"`
	StartTime               int64     `json:"start_time"`
	EndTime                 int64     `json:"end_time"`
	OperationTime           int64     `json:"operation_time"`
	ServerID                string    `json:"serverID"`
	ServerHostname          string    `json:"server_hostname"`
	Server                  string    `json:"server"`
	ServerIP                string    `json:"server_ip"`
	Site                    string    `json:"site"`
	User                    string    `json:"user"`
	UserDN                  string    `json:"user_dn"`
	Host                    string    `json:"host"`
	Filename                string    `json:"filename"`
	Dirname1                string    `json:"dirname1"`
	Dirname2                string    `json:"dirname2"`
	LogicalDirname          string    `json:"logical_dirname"`
	Protocol                string    `json:"protocol"`
	AppInfo                 string    `json:"appinfo"`
	IPv6                    bool      `json:"ipv6"`
	Filesize                int64     `json:"filesize"`
	ReadOperations          int32     `json:"read_operations"`
	ReadSingleOperations    int32     `json:"read_single_operations"`
	ReadVectorOperations    int32     `json:"read_vector_operations"`
	WriteOperations         int32     `json:"write_operations"`
	Read                    int64     `json:"read"`
	ReadSingleBytes         int64     `json:"read_single_bytes"`
	Readv                   int64     `json:"readv"`
	Write                   int64     `json:"write"`
	ReadMin                 int32     `json:"read_min"`
	ReadMax                 int32     `json:"read_max"`
	ReadAverage             int64     `json:"read_average"`
	ReadSingleMin           int32     `json:"read_single_min"`
	ReadSingleMax           int32     `json:"read_single_max"`
	ReadSingleAverage       int64     `json:"read_single_average"`
	ReadVectorMin           int32     `json:"read_vector_min"`
	ReadVectorMax           int32     `json:"read_vector_max"`
	ReadVectorAverage       int64     `json:"read_vector_average"`
	WriteMin                int32     `json:"write_min"`
	WriteMax                int32     `json:"write_max"`
	WriteAverage            int64     `json:"write_average"`
	ReadVectorCountMin      int16     `json:"read_vector_count_min"`
	ReadVectorCountMax      int16     `json:"read_vector_count_max"`
	ReadVectorCountAverage  float64   `json:"read_vector_count_average"`
	ReadBytesAtClose        int64     `json:"read_bytes_at_close"`
	WriteBytesAtClose       int64     `json:"write_bytes_at_close"`
	HasFileCloseMsg         int       `json:"HasFileCloseMsg"`
}

// FileState tracks the state of an open file
type FileState struct {
	FileID     uint32
	UserID     uint32
	OpenTime   int64
	FileSize   int64
	Filename   string
	ServerID   string
	StreamID   int64
	CreatedAt  time.Time
}

// UserState tracks user information from user packets
type UserState struct {
	UserID       uint32
	UserInfo     parser.UserInfo
	AuthInfo     parser.AuthInfo
	AppInfo      string
	CreatedAt    time.Time
}

// Correlator correlates file open and close events
type Correlator struct {
	stateMap *StateMap
	userMap  *StateMap
}

// NewCorrelator creates a new correlator
func NewCorrelator(ttl time.Duration, maxEntries int) *Correlator {
	return &Correlator{
		stateMap: NewStateMap(ttl, maxEntries, ttl/10),
		userMap:  NewStateMap(ttl, maxEntries, ttl/10),
	}
}

// ProcessPacket processes a packet and returns a record if correlation is complete
func (c *Correlator) ProcessPacket(packet *parser.Packet) (*CollectorRecord, error) {
	if packet.IsXML {
		// XML packets are not correlated
		return nil, nil
	}

	// Handle user packets
	if packet.UserRecord != nil {
		c.handleUserRecord(packet.UserRecord)
		return nil, nil
	}

	for _, rec := range packet.FileRecords {
		switch r := rec.(type) {
		case parser.FileOpenRecord:
			return c.handleFileOpen(r, packet)
		case parser.FileCloseRecord:
			return c.handleFileClose(r, packet)
		case parser.FileTimeRecord:
			return c.handleTimeRecord(r, packet)
		}
	}

	return nil, nil
}

// handleFileOpen handles a file open event
func (c *Correlator) handleFileOpen(rec parser.FileOpenRecord, packet *parser.Packet) (*CollectorRecord, error) {
	state := &FileState{
		FileID:    rec.Header.FileId,
		UserID:    rec.User,
		OpenTime:  int64(packet.Header.ServerStart),
		FileSize:  rec.FileSize,
		Filename:  string(rec.Lfn),
		CreatedAt: time.Now(),
	}

	key := fmt.Sprintf("file-%d-%d", rec.Header.FileId, rec.User)
	c.stateMap.Set(key, state)

	return nil, nil
}

// handleFileClose handles a file close event
func (c *Correlator) handleFileClose(rec parser.FileCloseRecord, packet *parser.Packet) (*CollectorRecord, error) {
	key := fmt.Sprintf("file-%d-%d", rec.Header.FileId, rec.Header.UserId)

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
func (c *Correlator) handleTimeRecord(rec parser.FileTimeRecord, packet *parser.Packet) (*CollectorRecord, error) {
	// Time records can be used to update state or create timing records
	// For now, we'll store them for potential correlation
	key := fmt.Sprintf("time-%d-%d", rec.Header.FileId, rec.SID)
	state := &FileState{
		FileID:    rec.Header.FileId,
		UserID:    rec.Header.UserId,
		OpenTime:  int64(rec.TBeg),
		StreamID:  rec.SID,
		CreatedAt: time.Now(),
	}
	c.stateMap.Set(key, state)
	return nil, nil
}

// handleUserRecord handles a user packet (type 'u')
// Stores user information mapped by dictID and SID for later correlation with file operations
func (c *Correlator) handleUserRecord(rec *parser.UserRecord) {
	userState := &UserState{
		UserID:    rec.DictId,
		UserInfo:  rec.UserInfo,
		AuthInfo:  rec.AuthInfo,
		CreatedAt: time.Now(),
	}
	
	// Store by dictID and SID (which matches the UserID and ServerStart in file operations)
	key := fmt.Sprintf("user-%d-%d", rec.DictId, rec.Header.ServerStart)
	c.userMap.Set(key, userState)
}

// getUserInfo retrieves user information for a given userID and SID
func (c *Correlator) getUserInfo(userID uint32, sid int32) *UserState {
	key := fmt.Sprintf("user-%d-%d", userID, sid)
	val, exists := c.userMap.Get(key)
	if !exists {
		return nil
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

	// Split the path by /
	parts := strings.Split(filename, "/")
	
	// dirname1 is the first component (skip empty string from leading /)
	if len(parts) > 1 && parts[1] != "" {
		dirname1 = "/" + parts[1]
	} else {
		dirname1 = "unknown directory"
	}
	
	// dirname2 is the first 2 components joined with /
	if len(parts) > 2 && parts[1] != "" {
		dirname2 = "/" + parts[1] + "/" + parts[2]
	} else {
		dirname2 = dirname1
	}
	
	// Determine logical_dirname based on path patterns
	// Ref: https://github.com/opensciencegrid/xrootd-monitoring-collector/blob/master/Collectors/DetailedCollector.py#L174
	switch {
	case strings.HasPrefix(filename, "/user"):
		logicalDirname = dirname2
	case strings.HasPrefix(filename, "/osgconnect/public") || strings.HasPrefix(filename, "/osgconnect/protected") || strings.HasPrefix(filename, "/ospool/PROTECTED"):
		if len(parts) >= 4 {
			logicalDirname = "/" + strings.Join(parts[1:4], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/ospool"):
		if len(parts) >= 5 {
			logicalDirname = "/" + strings.Join(parts[1:5], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/path-facility"):
		if len(parts) >= 4 {
			logicalDirname = "/" + strings.Join(parts[1:4], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/hcc"):
		if len(parts) >= 6 {
			logicalDirname = "/" + strings.Join(parts[1:6], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/pnfs/fnal.gov/usr"):
		if len(parts) >= 5 {
			logicalDirname = "/" + strings.Join(parts[1:5], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/gwdata"):
		logicalDirname = dirname2
	case strings.HasPrefix(filename, "/chtc/"):
		logicalDirname = "/chtc"
	case strings.HasPrefix(filename, "/icecube/"):
		logicalDirname = "/icecube"
	case strings.HasPrefix(filename, "/igwn"):
		if len(parts) >= 4 {
			logicalDirname = "/" + strings.Join(parts[1:4], "/")
		} else {
			logicalDirname = dirname2
		}
	case strings.HasPrefix(filename, "/store") || strings.HasPrefix(filename, "/user/dteam"):
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

	// Get user information if available (using userID and SID)
	userInfo := c.getUserInfo(state.UserID, packet.Header.ServerStart)
	
	// Set defaults
	user := fmt.Sprintf("%x", state.UserID)
	userDN := ""
	host := "unknown"
	protocol := "unknown"
	appInfo := ""
	ipv6 := false
	
	if userInfo != nil {
		// Use username from userInfo
		user = userInfo.UserInfo.Username
		host = userInfo.UserInfo.Host
		protocol = userInfo.UserInfo.Protocol
		
		// Use DN from authInfo
		if userInfo.AuthInfo.DN != "" {
			userDN = userInfo.AuthInfo.DN
		}
		
		// Use appInfo if available
		if userInfo.AppInfo != "" {
			appInfo = userInfo.AppInfo
		}
		
		// Check if IPv6
		if userInfo.AuthInfo.InetVersion == "6" {
			ipv6 = true
		}
	}

	// Extract directory names from filename
	dirname1, dirname2, logicalDirname := extractDirnames(state.Filename)

	return &CollectorRecord{
		Timestamp:               now,
		StartTime:               state.OpenTime,
		EndTime:                 now.Unix(),
		OperationTime:           now.Unix() - state.OpenTime,
		ServerID:                fmt.Sprintf("%d#%s", packet.Header.ServerStart, "unknown"),
		ServerHostname:          "localhost",
		Server:                  "127.0.0.1",
		ServerIP:                "127.0.0.1",
		Site:                    "UNKNOWN",
		User:                    user,
		UserDN:                  userDN,
		Host:                    host,
		Filename:                state.Filename,
		Dirname1:                dirname1,
		Dirname2:                dirname2,
		LogicalDirname:          logicalDirname,
		Protocol:                protocol,
		AppInfo:                 appInfo,
		IPv6:                    ipv6,
		Filesize:                state.FileSize,
		ReadOperations:          rec.Ops.Read,
		ReadSingleOperations:    rec.Ops.Read,
		ReadVectorOperations:    rec.Ops.Readv,
		WriteOperations:         rec.Ops.Write,
		Read:                    rec.Xfr.Read,
		ReadSingleBytes:         rec.Xfr.Read,
		Readv:                   rec.Xfr.Readv,
		Write:                   rec.Xfr.Write,
		ReadMin:                 rec.Ops.RdMin,
		ReadMax:                 rec.Ops.RdMax,
		ReadAverage:             readAvg,
		ReadSingleMin:           rec.Ops.RdMin,
		ReadSingleMax:           rec.Ops.RdMax,
		ReadSingleAverage:       readSingleAvg,
		ReadVectorMin:           rec.Ops.RvMin,
		ReadVectorMax:           rec.Ops.RvMax,
		ReadVectorAverage:       readVectorAvg,
		WriteMin:                rec.Ops.WrMin,
		WriteMax:                rec.Ops.WrMax,
		WriteAverage:            writeAvg,
		ReadVectorCountMin:      rec.Ops.RsMin,
		ReadVectorCountMax:      rec.Ops.RsMax,
		ReadVectorCountAverage:  readvCountAvg,
		ReadBytesAtClose:        rec.Xfr.Read,
		WriteBytesAtClose:       rec.Xfr.Write,
		HasFileCloseMsg:         1,
	}
}

// createStandaloneCloseRecord creates a record from just a close event
func (c *Correlator) createStandaloneCloseRecord(rec parser.FileCloseRecord, packet *parser.Packet) *CollectorRecord {
	state := &FileState{
		FileID:   rec.Header.FileId,
		UserID:   rec.Header.UserId,
		OpenTime: int64(packet.Header.ServerStart),
		Filename: "unknown",
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
