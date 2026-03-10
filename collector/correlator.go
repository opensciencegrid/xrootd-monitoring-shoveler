package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
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
	Experiment             string    `json:"experiment,omitempty"`
	Activity               string    `json:"activity,omitempty"`
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

	// Internal fields for DNS enrichment (not serialized to JSON)
	needsDNSEnrichment bool   `json:"-"` // True if record needs async DNS enrichment for user domain
	enrichmentIP       string `json:"-"` // User IP address that needs enrichment
	needsServerDNS     bool   `json:"-"` // True if server hostname needs async DNS enrichment
	serverEnrichmentIP string `json:"-"` // Server IP address that needs enrichment
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
	UserID         uint32
	UserInfo       parser.UserInfo
	AuthInfo       parser.AuthInfo
	TokenInfo      parser.TokenInfo
	AppInfo        string
	ExperimentCode string
	ActivityCode   string
	CreatedAt      time.Time
}

// PathInfo represents path mapping with associated user info
type PathInfo struct {
	Path     string
	UserInfo parser.UserInfo
}

// dnsEnrichmentRequest represents a request to enrich a record with DNS lookup
type dnsEnrichmentRequest struct {
	ip         string
	resultChan chan dnsEnrichmentResult
}

// dnsEnrichmentResult contains the result of a DNS lookup
type dnsEnrichmentResult struct {
	ip       string
	hostname string
	err      error
}

// DNSResolver interface allows mocking DNS lookups in tests
type DNSResolver interface {
	LookupAddr(ctx context.Context, addr string) ([]string, error)
}

// defaultDNSResolver wraps net.DefaultResolver
type defaultDNSResolver struct{}

func (r *defaultDNSResolver) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	return net.DefaultResolver.LookupAddr(ctx, addr)
}

// Correlator correlates file open and close events
type Correlator struct {
	stateMap  *StateMap
	userMap   *StateMap
	dictMap   *StateMap // Maps dictid to path/user info
	serverMap *StateMap // Maps serverID to server identification info
	logger    *logrus.Logger

	// DNS enrichment fields
	enableDNSEnrichment bool
	dnsCache            *StateMap // Maps IP -> hostname with TTL
	dnsRequestChan      chan dnsEnrichmentRequest
	dnsWorkerCount      int
	dnsTimeout          time.Duration
	dnsResolver         DNSResolver
	ctx                 context.Context
	cancel              context.CancelFunc
}

// CorrelatorConfig holds configuration for the correlator including DNS enrichment
type CorrelatorConfig struct {
	TTL                 time.Duration
	MaxEntries          int
	EnableDNSEnrichment bool
	DNSCacheTTL         time.Duration
	DNSWorkers          int
	DNSTimeout          time.Duration
	Logger              *logrus.Logger
}

// NewCorrelator creates a new correlator
func NewCorrelator(ttl time.Duration, maxEntries int, logger *logrus.Logger) *Correlator {
	config := CorrelatorConfig{
		TTL:                 ttl,
		MaxEntries:          maxEntries,
		EnableDNSEnrichment: false,
		Logger:              logger,
	}
	return NewCorrelatorWithConfig(config)
}

// NewCorrelatorWithConfig creates a new correlator with full configuration
func NewCorrelatorWithConfig(config CorrelatorConfig) *Correlator {
	if config.Logger == nil {
		config.Logger = logrus.New()
	}

	// Set DNS enrichment defaults
	if config.DNSCacheTTL == 0 {
		config.DNSCacheTTL = 1 * time.Hour // Default 1 hour cache
	}
	if config.DNSWorkers == 0 {
		config.DNSWorkers = 5 // Default 5 concurrent DNS lookups
	}
	if config.DNSTimeout == 0 {
		config.DNSTimeout = 2 * time.Second // Default 2 second timeout
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Correlator{
		stateMap:            NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		userMap:             NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		dictMap:             NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		serverMap:           NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		logger:              config.Logger,
		enableDNSEnrichment: config.EnableDNSEnrichment,
		dnsWorkerCount:      config.DNSWorkers,
		dnsTimeout:          config.DNSTimeout,
		dnsResolver:         &defaultDNSResolver{},
		ctx:                 ctx,
		cancel:              cancel,
	}

	if config.EnableDNSEnrichment {
		c.dnsCache = NewStateMap(config.DNSCacheTTL, config.MaxEntries, config.DNSCacheTTL/10)
		// Buffer is 2x the worker count to allow limited queuing of DNS requests without blocking producers.
		c.dnsRequestChan = make(chan dnsEnrichmentRequest, config.DNSWorkers*2)
		c.startDNSWorkers()
	}

	return c
}

// startDNSWorkers starts the DNS worker pool
func (c *Correlator) startDNSWorkers() {
	for i := 0; i < c.dnsWorkerCount; i++ {
		go c.dnsWorker()
	}
	c.logger.Infof("Started %d DNS enrichment workers with %s timeout", c.dnsWorkerCount, c.dnsTimeout)
}

// dnsWorker processes DNS enrichment requests
func (c *Correlator) dnsWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case req := <-c.dnsRequestChan:
			c.processDNSRequest(req)
		}
	}
}

// processDNSRequest performs the actual DNS lookup with timeout
func (c *Correlator) processDNSRequest(req dnsEnrichmentRequest) {
	ctx, cancel := context.WithTimeout(c.ctx, c.dnsTimeout)
	defer cancel()

	hostname := c.performDNSLookup(ctx, req.ip)

	result := dnsEnrichmentResult{
		ip:       req.ip,
		hostname: hostname,
	}

	// Cache the result (even if empty/failed)
	if hostname != "" {
		c.dnsCache.Set(req.ip, hostname)
	}

	// Send result back (non-blocking with context check)
	select {
	case req.resultChan <- result:
	case <-c.ctx.Done():
	}
}

// performDNSLookup does the actual reverse DNS lookup
func (c *Correlator) performDNSLookup(ctx context.Context, ipStr string) string {
	// Validate IP
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}

	// Perform reverse DNS lookup with context
	names, err := c.dnsResolver.LookupAddr(ctx, ipStr)
	if err != nil || len(names) == 0 {
		c.logger.Debugf("DNS lookup failed for %s: %v", ipStr, err)
		return ""
	}

	// Return first hostname, normalized
	hostname := strings.TrimSuffix(names[0], ".")
	c.logger.Debugf("DNS lookup success: %s -> %s", ipStr, hostname)
	return hostname
}

// enrichWithDNSSync performs synchronous DNS enrichment for an IP address
// Only returns hostname if it's already in cache (fast path)
// Returns (hostname, needsAsync) where needsAsync=true means caller should use enrichWithDNSBlocking
func (c *Correlator) enrichWithDNSSync(ipStr string) (string, bool) {
	if !c.enableDNSEnrichment || ipStr == "" {
		return "", false
	}

	// Fast path: check cache first (no goroutine needed)
	if val, exists := c.dnsCache.Get(ipStr); exists {
		if hostname, ok := val.(string); ok {
			c.logger.Debugf("DNS cache hit: %s -> %s", ipStr, hostname)
			return hostname, false
		}
	}

	// Cache miss: caller should use async enrichment
	c.logger.Debugf("DNS cache miss: %s, needs async lookup", ipStr)
	return "", true
}

// enrichWithDNSBlocking sends a DNS lookup request to the worker pool and blocks until the result is ready
func (c *Correlator) enrichWithDNSBlocking(ipStr string) string {
	if !c.enableDNSEnrichment || ipStr == "" {
		return ""
	}

	resultChan := make(chan dnsEnrichmentResult, 1)
	req := dnsEnrichmentRequest{
		ip:         ipStr,
		resultChan: resultChan,
	}

	// Send request to worker pool (with timeout)
	select {
	case c.dnsRequestChan <- req:
		// Request queued successfully
	case <-time.After(c.dnsTimeout):
		c.logger.Warnf("DNS enrichment queue timeout for %s", ipStr)
		return ""
	case <-c.ctx.Done():
		return ""
	}

	// Wait for result. Use 2x the DNS timeout to account for both
	// queuing delay and the actual DNS lookup performed by the worker.
	select {
	case result := <-resultChan:
		return result.hostname
	case <-time.After(c.dnsTimeout * 2):
		c.logger.Warnf("DNS enrichment result timeout for %s", ipStr)
		return ""
	case <-c.ctx.Done():
		return ""
	}
}

// EnrichRecordAsync enriches a record with DNS and calls the callback
// This is a helper for callers who want to handle async enrichment
// Spawns a goroutine, returns immediately (non-blocking)
func (c *Correlator) EnrichRecordAsync(record *CollectorRecord, callback func(*CollectorRecord)) {
	if !record.needsDNSEnrichment && !record.needsServerDNS {
		callback(record)
		return
	}

	go func() {
		if record.needsDNSEnrichment {
			hostname := c.enrichWithDNSBlocking(record.enrichmentIP)
			if hostname != "" {
				parts := strings.Split(hostname, ".")
				if len(parts) >= 2 {
					record.UserDomain = strings.Join(parts[len(parts)-2:], ".")
				}
			}
		}

		if record.needsServerDNS {
			hostname := c.enrichWithDNSBlocking(record.serverEnrichmentIP)
			if hostname != "" {
				record.ServerHostname = hostname
			}
		}

		callback(record)
	}()
}

// NeedsDNSEnrichment returns true if the record needs async DNS enrichment
func (r *CollectorRecord) NeedsDNSEnrichment() bool {
	return r.needsDNSEnrichment || r.needsServerDNS
}

// ProcessPacket processes a packet and returns records for all correlated file operations
// Returns a slice of records since a packet can contain multiple file close events that each emit a record
func (c *Correlator) ProcessPacket(packet *parser.Packet) ([]*CollectorRecord, error) {
	if packet.IsXML {
		// XML packets are not correlated
		return nil, nil
	}

	// Calculate server ID: serverStart#addr#port
	serverID := c.getServerID(packet)

	// Handle server info packets ('=' type)
	if packet.ServerInfo != nil {
		c.handleServerInfo(packet.ServerInfo, serverID)
		return nil, nil
	}

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

	// Process all file records and collect any complete records
	var records []*CollectorRecord
	for _, rec := range packet.FileRecords {
		switch r := rec.(type) {
		case parser.FileOpenRecord:
			result, err := c.handleFileOpen(r, packet, serverID)
			if err != nil {
				return records, err
			}
			if result != nil {
				records = append(records, result)
			}
		case parser.FileCloseRecord:
			result, err := c.handleFileClose(r, packet, serverID)
			if err != nil {
				return records, err
			}
			if result != nil {
				records = append(records, result)
			}
		case parser.FileTimeRecord:
			result, err := c.handleTimeRecord(r, packet, serverID)
			if err != nil {
				return records, err
			}
			if result != nil {
				records = append(records, result)
			}
		case parser.FileDisconnectRecord:
			c.handleDisconnect(r, serverID)
			// Disconnect doesn't generate a record, just cleanup
		}
	}

	if len(records) > 0 {
		return records, nil
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
	return BuildServerID(packet.Header.ServerStart, packet.RemoteAddr)
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
		key := BuildDictKey(serverID, rec.DictId)
		c.dictMap.Set(key, string(rec.Info))
		return
	}

	switch packetType {
	case parser.PacketTypeDictID: // 'd' packet
		// Path mapping: store dictID -> PathInfo
		if len(parts) > 1 {
			pathInfo := &PathInfo{
				Path:     string(parts[1]),
				UserInfo: userInfo,
			}
			key := BuildDictKey(serverID, rec.DictId)
			c.dictMap.Set(key, pathInfo)
		}

		// Also store dictID -> userInfo for user lookup
		userKey := BuildDictIDKey(serverID, rec.DictId)
		c.dictMap.Set(userKey, userInfo)

	case parser.PacketTypeInfo: // 'i' packet
		// App info: rest of info after userInfo
		if len(parts) > 1 {
			appInfo := string(parts[1])

			// Store dictID -> userInfo mapping
			userKey := BuildDictIDKey(serverID, rec.DictId)
			c.dictMap.Set(userKey, userInfo)

			// Update or create user state with appinfo
			// Create a user key based on the userInfo string representation
			userStateKey := BuildUserInfoKey(serverID, userInfo)
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
	case parser.PacketTypeEAInfo: // 'U' packet
		// Experiment/Activity info: parse eainfo from second part
		// Format: userid\neainfo where eainfo is &Uc=udid&Ec=expc&Ac=actc
		if len(parts) > 1 {
			eaInfo := string(parts[1])

			// Parse the eainfo fields
			udid, experimentCode, activityCode := parseEAInfo(eaInfo)

			if udid == 0 {
				c.logger.Debugf("Failed to parse udid from eainfo: %s", eaInfo)
				return
			}

			// Look up the existing user by udid
			existingDictKey := BuildDictIDKey(serverID, udid)
			val, exists := c.dictMap.Get(existingDictKey)
			if !exists {
				// User doesn't exist yet, create mapping from udid to this userInfo
				c.dictMap.Set(existingDictKey, userInfo)
				c.logger.Debugf("Created new dictID mapping %d -> userInfo for eainfo", udid)
			} else {
				// Get existing userInfo from udid mapping
				existingUserInfo, ok := val.(parser.UserInfo)
				if !ok {
					c.logger.Debugf("EAInfo found dictID but not a UserInfo type")
					return
				}
				userInfo = existingUserInfo
			}

			// Update or create user state with experiment/activity codes
			userStateKey := BuildUserInfoKey(serverID, userInfo)
			userStateVal, userExists := c.userMap.Get(userStateKey)
			if userExists {
				if existingUserState, ok := userStateVal.(*UserState); ok {
					existingUserState.ExperimentCode = experimentCode
					existingUserState.ActivityCode = activityCode
					c.userMap.Set(userStateKey, existingUserState)
					c.logger.Debugf("Updated user %s (udid=%d) with experiment=%s, activity=%s",
						userInfo.Username, udid, experimentCode, activityCode)
				}
			} else {
				// Create new user state with experiment/activity codes
				userState := &UserState{
					UserID:         udid,
					UserInfo:       userInfo,
					ExperimentCode: experimentCode,
					ActivityCode:   activityCode,
					CreatedAt:      time.Now(),
				}
				c.userMap.Set(userStateKey, userState)
				c.logger.Debugf("Created new user state for %s (udid=%d) with experiment=%s, activity=%s",
					userInfo.Username, udid, experimentCode, activityCode)
			}
		}
	}
}

// parseEAInfo parses experiment and activity info from eainfo string
// Format: &Uc=udid&Ec=expc&Ac=actc
// Returns: (udid, experimentCode, activityCode)
func parseEAInfo(eaInfo string) (uint32, string, string) {
	var udid uint32
	var experimentCode, activityCode string

	// Split by & and parse each key=value pair
	parts := strings.Split(eaInfo, "&")
	for _, part := range parts {
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := kv[0]
		value := kv[1]

		switch key {
		case "Uc":
			// Parse udid as uint32
			if val, err := strconv.ParseUint(value, 10, 32); err == nil {
				udid = uint32(val)
			}
		case "Ec":
			experimentCode = value
		case "Ac":
			activityCode = value
		}
	}

	return udid, experimentCode, activityCode
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

// extractIPFromHost extracts the IP address from a host string
// Host format can be: "[::ipv6:addr]" or "ipv4.addr" or "hostname"
func extractIPFromHost(host string) string {
	if host == "" {
		return ""
	}
	// Remove brackets for IPv6
	host = strings.Trim(host, "[]")
	// Remove leading :: if present
	host = strings.TrimPrefix(host, "::")
	return host
}

// handleFileOpen handles a file open event
func (c *Correlator) handleFileOpen(rec parser.FileOpenRecord, packet *parser.Packet, serverID string) (*CollectorRecord, error) {
	// Filename may come from Lfn field OR from dictid lookup
	filename := string(rec.Lfn)
	if filename == "" && rec.Header.FileId != 0 {
		// No filename in open record, try to get it from dict ID
		dictKey := BuildDictKey(serverID, rec.Header.FileId)
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
	key := BuildFileKey(serverID, rec.Header.FileId)
	c.stateMap.Set(key, state)

	return nil, nil
}

// handleFileClose handles a file close event
func (c *Correlator) handleFileClose(rec parser.FileCloseRecord, packet *parser.Packet, serverID string) (*CollectorRecord, error) {
	// Key is only serverID + fileID (matches the key used in handleFileOpen)
	key := BuildFileKey(serverID, rec.Header.FileId)

	c.logger.Debugf("Correlating file close: serverID=%s, fileID=%d, userID=%d", serverID, rec.Header.FileId, rec.Header.UserId)

	// Try to get the open state
	val, exists := c.stateMap.Get(key)
	if !exists {
		c.logger.Debugf("No open record found for file close: serverID=%s, fileID=%d - creating standalone record", serverID, rec.Header.FileId)
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
	key := BuildTimeKey(serverID, rec.Header.FileId, rec.SID)
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

// handleServerInfo stores server identification information
// Server info packets ('=' type) contain: &site=sname&port=pnum&inst=iname&pgm=prog&ver=vname
// The StateMap automatically resets TTL on each Set, so server entries persist as long as packets arrive
func (c *Correlator) handleServerInfo(info *parser.ServerInfo, serverID string) {
	// Store or update the server info - StateMap.Set resets the TTL
	c.serverMap.Set(serverID, info)
	c.logger.Debugf("Stored server info for %s: site=%s, program=%s, version=%s, instance=%s, port=%s",
		serverID, info.Site, info.Program, info.Version, info.Instance, info.Port)
}

// handleUserRecord handles a user packet (type 'u' or 'T')
// For 'u' packets: Stores user information mapped by dictID and serverID for later correlation with file operations
// For 'T' packets (token info): Augments an existing user record with token information
// Following Python logic: dictID -> userInfo mapping, and userInfo -> full user state
func (c *Correlator) handleUserRecord(rec *parser.UserRecord, serverID string) {
	// Check if this is a token record (has TokenInfo.UserDictID set)
	if rec.TokenInfo.UserDictID != 0 {
		c.logger.Debugf("Received token record for UserDictID=%d on server=%s", rec.TokenInfo.UserDictID, serverID)

		// Look up the existing user by the UserDictID from the token
		existingDictKey := BuildDictIDKey(serverID, rec.TokenInfo.UserDictID)
		val, exists := c.dictMap.Get(existingDictKey)
		if !exists {
			c.logger.Debugf("Token record references non-existent user dictID=%d", rec.TokenInfo.UserDictID)
			return
		}

		existingUserInfo, ok := val.(parser.UserInfo)
		if !ok {
			c.logger.Debugf("Token record found dictID but not a UserInfo type")
			return
		}

		// Find and augment the existing user state
		existingUserInfoKey := BuildUserInfoKey(serverID, existingUserInfo)
		userStateVal, userExists := c.userMap.Get(existingUserInfoKey)
		if !userExists {
			c.logger.Debugf("Token record found UserInfo but no UserState for user=%s", existingUserInfo.Username)
			return
		}

		existingUserState, ok := userStateVal.(*UserState)
		if !ok {
			c.logger.Debugf("Token record found user state but wrong type")
			return
		}

		// Augment the existing user state with token information
		existingUserState.TokenInfo = rec.TokenInfo
		c.userMap.Set(existingUserInfoKey, existingUserState)

		c.logger.Debugf("Augmented user %s (dictID=%d) with token info: subject=%s, org=%s",
			existingUserInfo.Username, rec.TokenInfo.UserDictID, rec.TokenInfo.Subject, rec.TokenInfo.Org)
		return
	}

	// Regular user record (not a token record)
	userState := &UserState{
		UserID:    rec.DictId,
		UserInfo:  rec.UserInfo,
		AuthInfo:  rec.AuthInfo,
		TokenInfo: rec.TokenInfo,
		CreatedAt: time.Now(),
	}

	// Store dictID -> userInfo mapping
	dictKey := BuildDictIDKey(serverID, rec.DictId)
	c.dictMap.Set(dictKey, rec.UserInfo)

	// Store userInfo -> userState mapping
	userInfoKey := BuildUserInfoKey(serverID, rec.UserInfo)
	c.userMap.Set(userInfoKey, userState)
}

// handleDisconnect handles a user disconnect event
// Cleans up all references to the disconnecting user
func (c *Correlator) handleDisconnect(rec parser.FileDisconnectRecord, serverID string) {
	// Get the userInfo from dictID mapping
	dictKey := BuildDictIDKey(serverID, rec.UserID)
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
	userInfoKey := BuildUserInfoKey(serverID, userInfo)
	c.userMap.Delete(userInfoKey)

	// Note: We don't delete file states here because disconnect doesn't imply
	// all files are closed. File states will expire via TTL or be removed on close.
}

// getUserInfo retrieves user information for a given userID and serverID
// Follows Python logic: userID -> dictID lookup -> userInfo -> full user state
func (c *Correlator) getUserInfo(userID uint32, fileID uint32, serverID string) *UserState {
	var userInfo parser.UserInfo
	var found bool

	c.logger.Debugf("Looking up user info: userID=%d, fileID=%d, serverID=%s", userID, fileID, serverID)

	// Try to get userInfo from dictID mapping (for userID if non-zero)
	if userID != 0 {
		dictKey := BuildDictIDKey(serverID, userID)
		if val, exists := c.dictMap.Get(dictKey); exists {
			if ui, ok := val.(parser.UserInfo); ok {
				userInfo = ui
				found = true
				c.logger.Debugf("Found user info from dictID %d: username=%s, host=%s", userID, ui.Username, ui.Host)
			}
		} else {
			c.logger.Debugf("User ID %d not found in dictID mapping (key: %s)", userID, dictKey)
		}
	}

	// If userID is 0 or not found, try to get from fileID (path mapping)
	if !found && fileID != 0 {
		dictKey := BuildDictKey(serverID, fileID)
		if val, exists := c.dictMap.Get(dictKey); exists {
			if pathInfo, ok := val.(*PathInfo); ok {
				userInfo = pathInfo.UserInfo
				found = true
				c.logger.Debugf("Found user info from path mapping for fileID %d: username=%s, path=%s", fileID, pathInfo.UserInfo.Username, pathInfo.Path)
			} else {
				c.logger.Debugf("FileID %d found in dict but not a PathInfo type", fileID)
			}
		} else {
			c.logger.Debugf("Path information not found for fileID %d (dictKey: %s)", fileID, dictKey)
		}
	}

	if !found {
		c.logger.Debugf("No user information found for userID=%d, fileID=%d", userID, fileID)
		return nil
	}

	// Now look up the full user state using userInfo
	userInfoKey := BuildUserInfoKey(serverID, userInfo)
	val, exists := c.userMap.Get(userInfoKey)
	if !exists {
		c.logger.Debugf("Full user state not found (no 'u' packet), using basic userInfo from 'd' packet: username=%s", userInfo.Username)
		// UserState not found (no 'u' packet received yet), but we have userInfo from 'd' packet
		// Create a minimal UserState with just the userInfo
		return &UserState{
			UserInfo: userInfo,
			// AuthInfo will be empty - no 'u' packet received
		}
	}

	userState, ok := val.(*UserState)
	if !ok {
		c.logger.Debugf("User state value exists but wrong type for key: %s", userInfoKey)
		return nil
	}

	c.logger.Debugf("Found full user state: username=%s, DN=%s, VO=%s", userState.UserInfo.Username, userState.AuthInfo.DN, userState.AuthInfo.Org)
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
	user := BuildUserHex(state.UserID)
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

	// DNS enrichment tracking
	var needsDNSEnrichment bool
	var enrichmentIP string

	if userInfo != nil {
		// Use username from userInfo
		user = userInfo.UserInfo.Username
		host = userInfo.UserInfo.Host
		protocol = userInfo.UserInfo.Protocol

		// Extract user_domain from hostname
		if host != "" {
			if isIPPattern(host) {
				// Host is an IP address - try DNS enrichment (cache only, non-blocking)
				ipStr := extractIPFromHost(host)

				// Try synchronous cache lookup first (fast path)
				hostname, needsAsync := c.enrichWithDNSSync(ipStr)

				if hostname != "" {
					// Successfully resolved - extract domain from hostname
					parts := strings.Split(hostname, ".")
					if len(parts) >= 2 {
						userDomain = strings.Join(parts[len(parts)-2:], ".")
					}
				} else if needsAsync {
					// Mark record as needing async DNS enrichment
					needsDNSEnrichment = true
					enrichmentIP = ipStr
				}
			} else {
				// Host is already a hostname - extract domain directly
				parts := strings.Split(host, ".")
				if len(parts) >= 2 {
					userDomain = strings.Join(parts[len(parts)-2:], ".")
				}
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

	// Extract experiment and activity codes
	experiment := ""
	activity := ""
	if userInfo != nil {
		experiment = userInfo.ExperimentCode
		activity = userInfo.ActivityCode
	}

	// Extract directory names from filename
	dirname1, dirname2, logicalDirname := extractDirnames(state.Filename)

	// Parse RemoteAddr to extract server IP and hostname
	serverIP := "unknown"
	serverHostname := "unknown"
	var needsServerDNS bool
	var serverEnrichmentIP string
	if packet.RemoteAddr != "" {
		// RemoteAddr is in format "host:port" or "[ipv6]:port"
		host, _, err := net.SplitHostPort(packet.RemoteAddr)
		if err == nil {
			serverIP = host
			serverHostname = host
		} else {
			// If SplitHostPort fails, use the whole RemoteAddr
			serverIP = packet.RemoteAddr
			serverHostname = packet.RemoteAddr
		}

		// Try DNS enrichment for server hostname
		if isIPPattern(serverIP) {
			ipStr := extractIPFromHost(serverIP)
			hostname, needsAsync := c.enrichWithDNSSync(ipStr)
			if hostname != "" {
				serverHostname = hostname
			} else if needsAsync {
				needsServerDNS = true
				serverEnrichmentIP = ipStr
			}
		}
	}

	// Get site information from server info map
	site := "UNKNOWN"
	if val, exists := c.serverMap.Get(state.ServerID); exists {
		if serverInfo, ok := val.(*parser.ServerInfo); ok && serverInfo != nil {
			if serverInfo.Site != "" {
				site = serverInfo.Site
			}
		}
	}

	return &CollectorRecord{
		Timestamp:              now,
		StartTime:              state.OpenTime,
		EndTime:                now.Unix(),
		OperationTime:          now.Unix() - state.OpenTime,
		ServerID:               BuildServerID(packet.Header.ServerStart, packet.RemoteAddr),
		ServerHostname:         serverHostname,
		Server:                 serverIP,
		ServerIP:               serverIP,
		Site:                   site,
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
		Experiment:             experiment,
		Activity:               activity,
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
		needsDNSEnrichment:     needsDNSEnrichment,
		enrichmentIP:           enrichmentIP,
		needsServerDNS:         needsServerDNS,
		serverEnrichmentIP:     serverEnrichmentIP,
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
	// Stop DNS workers first
	if c.cancel != nil {
		c.cancel()
	}

	if c.stateMap != nil {
		c.stateMap.Stop()
	}
	if c.userMap != nil {
		c.userMap.Stop()
	}
	if c.serverMap != nil {
		c.serverMap.Stop()
	}
	if c.dnsCache != nil {
		c.dnsCache.Stop()
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
