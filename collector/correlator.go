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
	"sync"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
)

// CollectorRecord represents a correlated file access record
type CollectorRecord struct {
	Timestamp      time.Time `json:"@timestamp"`
	StartTime      int64     `json:"start_time"`
	EndTime        int64     `json:"end_time"`
	OperationTime  int64     `json:"operation_time"`
	ServerID       string    `json:"serverID"`
	ServerHostname string    `json:"server_hostname"`
	Server         string    `json:"server"`
	ServerIP       string    `json:"server_ip"`
	Site           string    `json:"site"`
	User           string    `json:"user"`
	UserDN         string    `json:"user_dn"`
	UserDomain     string    `json:"user_domain,omitempty"`
	VO             string    `json:"vo,omitempty"`
	Host           string    `json:"host"`
	TokenSubject   string    `json:"token_subject,omitempty"`
	TokenUsername  string    `json:"token_username,omitempty"`
	TokenOrg       string    `json:"token_org,omitempty"`
	TokenRole      string    `json:"token_role,omitempty"`
	TokenGroups    string    `json:"token_groups,omitempty"`
	// Experiment and Activity are the human-readable SciTags names resolved from
	// the numeric ids below via the SciTags registry (empty when the ids are
	// absent or unknown to the registry). ExperimentID/ActivityID are the raw
	// numeric flow-label ids carried on the 'U' (MAPUEAC) stream. Activity ids
	// are namespaced per experiment, so ActivityID is only meaningful alongside
	// ExperimentID. ScitagsVO is the VO name implied by the SciTags experiment
	// (the experiment name as published by the registry). It is kept in its own
	// field so it never overwrites VO, which might come from the auth/token streams.
	Experiment             string  `json:"experiment,omitempty"`
	Activity               string  `json:"activity,omitempty"`
	ExperimentID           int     `json:"experiment_id,omitempty"`
	ActivityID             int     `json:"activity_id,omitempty"`
	ScitagsVO              string  `json:"scitags_vo,omitempty"`
	Filename               string  `json:"filename"`
	Dirname1               string  `json:"dirname1"`
	Dirname2               string  `json:"dirname2"`
	LogicalDirname         string  `json:"logical_dirname"`
	Protocol               string  `json:"protocol"`
	AppInfo                string  `json:"appinfo"`
	IPv6                   bool    `json:"ipv6"`
	Filesize               int64   `json:"filesize"`
	ReadOperations         int32   `json:"read_operations"`
	ReadSingleOperations   int32   `json:"read_single_operations"`
	ReadVectorOperations   int32   `json:"read_vector_operations"`
	WriteOperations        int32   `json:"write_operations"`
	Read                   int64   `json:"read"`
	ReadSingleBytes        int64   `json:"read_single_bytes"`
	Readv                  int64   `json:"readv"`
	Write                  int64   `json:"write"`
	ReadMin                int32   `json:"read_min"`
	ReadMax                int32   `json:"read_max"`
	ReadAverage            int64   `json:"read_average"`
	ReadSingleMin          int32   `json:"read_single_min"`
	ReadSingleMax          int32   `json:"read_single_max"`
	ReadSingleAverage      int64   `json:"read_single_average"`
	ReadVectorMin          int32   `json:"read_vector_min"`
	ReadVectorMax          int32   `json:"read_vector_max"`
	ReadVectorAverage      int64   `json:"read_vector_average"`
	WriteMin               int32   `json:"write_min"`
	WriteMax               int32   `json:"write_max"`
	WriteAverage           int64   `json:"write_average"`
	ReadVectorCountMin     int16   `json:"read_vector_count_min"`
	ReadVectorCountMax     int16   `json:"read_vector_count_max"`
	ReadVectorCountAverage float64 `json:"read_vector_count_average"`
	ReadBytesAtClose       int64   `json:"read_bytes_at_close"`
	WriteBytesAtClose      int64   `json:"write_bytes_at_close"`
	HasFileCloseMsg        int     `json:"HasFileCloseMsg"`

	// Internal fields for DNS enrichment (not serialized to JSON)
	needsDNSEnrichment bool   `json:"-"` // True if record needs async DNS enrichment for user domain
	enrichmentIP       string `json:"-"` // User IP address that needs enrichment
	needsServerDNS     bool   `json:"-"` // True if server hostname needs async DNS enrichment
	serverEnrichmentIP string `json:"-"` // Server IP address that needs enrichment
	clientHostname     string `json:"-"` // Full resolved client hostname for site matching (UserDomain keeps only the 2-label domain, which is too coarse for longest-suffix CRIC matching)

	// Src/dst site resolution results. These are carriers for the WLCG converter
	// and are deliberately NOT serialized: the site fields are emitted on the WLCG
	// record only, so the plain collector record keeps its existing shape.
	//
	// srcSite/dstSite are the WLCG RCSite names of the transfer's source and
	// destination endpoints, resolved from the configured local site, the CRIC SE
	// endpoints, the CRIC domains map and the CRIC IP ranges (in
	// site.resolution_order) and ordered by data-flow direction (read: src=server,
	// dst=client; write: inverted).
	// srcSiteStatus/dstSiteStatus record the per-endpoint resolution outcome
	// (resolved_config/resolved_hostname/resolved/resolved_ip, or
	// ambiguous/unknown_domain/no_host)
	// so the UNKNOWN rate stays measurable. A site is empty unless its status is
	// one of the resolved ones or "ambiguous", which names the first of the
	// several sites the endpoint matched and must be read as a guess.
	srcSite       string `json:"-"`
	dstSite       string `json:"-"`
	srcSiteStatus string `json:"-"`
	dstSiteStatus string `json:"-"`
}

// clientHost returns the best available fully-qualified client host name for
// site resolution: the DNS-resolved hostname when we have it, otherwise the raw
// Host (which is only usable when it is already a name rather than an IP).
func (r *CollectorRecord) clientHost() string {
	if r.clientHostname != "" {
		return r.clientHostname
	}
	return r.Host
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
	UserID       uint32
	UserInfo     parser.UserInfo
	AuthInfo     parser.AuthInfo
	TokenInfo    parser.TokenInfo
	AppInfo      string
	ExperimentID int // SciTags experiment id from the 'U' stream (&Ec=), 0 if unset
	ActivityID   int // SciTags activity id from the 'U' stream (&Ac=), 0 if unset
	CreatedAt    time.Time
}

// PathInfo represents path mapping with associated user info
type PathInfo struct {
	Path     string
	UserInfo parser.UserInfo
}

// Correlator correlates file open and close events
type Correlator struct {
	stateMap  *StateMap
	userMap   *StateMap
	dictMap   *StateMap // Maps dictid to path/user info
	serverMap *StateMap // Maps serverID to server identification info
	logger    *logrus.Logger

	// DNS enrichment fields
	enableDNSEnrichment   bool
	dnsCache              *StateMap // Maps IP -> hostname with TTL
	dnsTimeout            time.Duration
	dnsResolver           DNSResolver
	enrichmentWorkerCount int
	enrichmentQueueSize   int
	enrichmentQueue       *enrichmentWorkQueue
	enrichers             []RecordEnricher
	enrichmentWG          sync.WaitGroup
	enrichmentDropCount   int64 // atomic; counts records dropped due to full queue
	wlcgMetadata          WLCGMetadata
	scitags               *ScitagsRegistry // resolves 'U'-stream experiment/activity ids to names
	ctx                   context.Context
	cancel                context.CancelFunc

	// WLCG routing configuration
	wlcgVOs          []string
	wlcgPathPrefixes []string

	// Record drop filter
	dropPathPrefixes []string
	dropVOs          []string
}

// CorrelatorConfig holds configuration for the correlator including DNS enrichment
type CorrelatorConfig struct {
	TTL                 time.Duration
	MaxEntries          int
	EnableDNSEnrichment bool
	DNSCacheTTL         time.Duration
	DNSTimeout          time.Duration
	EnrichmentWorkers   int               // Number of enrichment worker goroutines (default: 5)
	EnrichmentQueueSize int               // Maximum number of pending enrichment requests (default: 1000000)
	WLCGMetadata        WLCGMetadata      // producer/type values used in WLCG records
	SiteRegistry        *SiteRegistry     // src/dst RCSite resolver; nil disables src_site/dst_site resolution
	SiteOverrides       *SiteOverrides    // operator pins consulted before every CRIC lookup; nil disables overriding
	SiteHostRegistry    *HostSiteRegistry // CRIC SE endpoint resolver backing the "hostname" method; nil drops that method
	SiteIPRegistry      *IPSiteRegistry   // CRIC netroutes resolver backing the "ip" method; nil drops that method
	Scitags             *ScitagsRegistry  // SciTags id->name resolver; defaults to the embedded snapshot when nil
	Logger              *logrus.Logger

	// Site resolution tuning. SiteLocalSite is the RCSite this collector runs at,
	// which resolves the reporting server without any lookup; empty disables the
	// "config" method. SiteResolutionOrder is the order the per-endpoint methods
	// are tried in (nil/empty uses DefaultSiteResolutionOrder).
	SiteLocalSite           string
	SiteLocalSiteLANClients bool // also apply SiteLocalSite to clients on private/loopback addresses
	SiteResolutionOrder     []string

	// WLCG routing: records matching any VO (case-insensitive) or path prefix are
	// converted and routed to the WLCG exchange.
	// If WLCGVOs/WLCGPathPrefixes are nil, defaults apply (["cms"], ["/store", "/user/dteam"]).
	// If they are non-nil but empty, WLCG routing is effectively disabled.
	WLCGVOs          []string
	WLCGPathPrefixes []string

	// Drop filter: records matching any VO (case-insensitive) or path prefix are
	// silently dropped before any publish. Defaults to empty (drop nothing).
	DropPathPrefixes []string
	DropVOs          []string
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

	// Always have a SciTags resolver; fall back to the embedded snapshot so
	// id->name resolution works even when the caller supplies none.
	if config.Scitags == nil {
		config.Scitags = NewScitagsRegistry(config.Logger)
	}

	// Set DNS enrichment defaults (treat non-positive values as unset)
	if config.DNSCacheTTL <= 0 {
		config.DNSCacheTTL = 1 * time.Hour // Default 1 hour cache
	}
	if config.DNSTimeout <= 0 {
		config.DNSTimeout = 2 * time.Second // Default 2 second timeout
	}
	if config.EnrichmentWorkers <= 0 {
		config.EnrichmentWorkers = 5 // Default 5 enrichment workers
	}
	if config.EnrichmentQueueSize <= 0 {
		config.EnrichmentQueueSize = defaultEnrichmentQueueMaxSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	wlcgVOs := config.WLCGVOs
	if wlcgVOs == nil {
		wlcgVOs = append([]string(nil), defaultWLCGVOs...)
	}
	wlcgPathPrefixes := config.WLCGPathPrefixes
	if wlcgPathPrefixes == nil {
		wlcgPathPrefixes = append([]string(nil), defaultWLCGPathPrefixes...)
	}

	c := &Correlator{
		stateMap:              NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		userMap:               NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		dictMap:               NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		serverMap:             NewStateMap(config.TTL, config.MaxEntries, config.TTL/10),
		logger:                config.Logger,
		enableDNSEnrichment:   config.EnableDNSEnrichment,
		dnsTimeout:            config.DNSTimeout,
		dnsResolver:           &defaultDNSResolver{},
		enrichmentWorkerCount: config.EnrichmentWorkers,
		enrichmentQueueSize:   config.EnrichmentQueueSize,
		wlcgMetadata:          config.WLCGMetadata,
		scitags:               config.Scitags,
		ctx:                   ctx,
		cancel:                cancel,
		wlcgVOs:               wlcgVOs,
		wlcgPathPrefixes:      wlcgPathPrefixes,
		dropPathPrefixes:      config.DropPathPrefixes,
		dropVOs:               config.DropVOs,
	}

	if config.EnableDNSEnrichment {
		c.dnsCache = NewStateMap(config.DNSCacheTTL, config.MaxEntries, config.DNSCacheTTL/10)
		c.registerEnricher(&dnsRecordEnricher{correlator: c})
	}

	if config.SiteRegistry != nil {
		// Registered after the DNS enricher so the resolved server/client host
		// names are already populated when src/dst site resolution runs. The
		// hostname and IP registries are optional and only used when the
		// order reaches them.
		order := NormalizeSiteResolutionOrder(config.SiteResolutionOrder, config.Logger)
		config.Logger.Infof("site: resolution order %s (local site %q)",
			strings.Join(order, " -> "), config.SiteLocalSite)
		c.registerEnricher(&siteRecordEnricher{
			domains:             config.SiteRegistry,
			overrides:           config.SiteOverrides,
			ambig:               newAmbiguityReporter(config.Logger),
			hosts:               config.SiteHostRegistry,
			ips:                 config.SiteIPRegistry,
			wlcgOnly:            c.matchesWLCG,
			localSite:           config.SiteLocalSite,
			localSiteLANClients: config.SiteLocalSiteLANClients,
			order:               order,
		})
	}

	c.startEnrichmentWorkers()

	return c
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

	// Every f-stream packet begins with a FileTOD (isTime) record whose
	// tBeg/tEnd bound the monitoring window during which the file events in
	// this packet occurred. These are the actual event timestamps; the header
	// ServerStart is only the server's boot time and must not be used for
	// per-operation timing. Extract the window once and apply it to every file
	// record in the packet.
	windowBeg, windowEnd := extractFileWindow(packet.FileRecords)

	// Process all file records and collect any complete records
	var records []*CollectorRecord
	for _, rec := range packet.FileRecords {
		switch r := rec.(type) {
		case parser.FileOpenRecord:
			result, err := c.handleFileOpen(r, packet, serverID, windowBeg)
			if err != nil {
				return records, err
			}
			if result != nil {
				records = append(records, result)
			}
		case parser.FileCloseRecord:
			result, err := c.handleFileClose(r, packet, serverID, windowBeg, windowEnd)
			if err != nil {
				return records, err
			}
			if result != nil {
				records = append(records, result)
			}
		case parser.FileTimeRecord:
			// The window was already extracted above; the FileTOD record itself
			// does not correlate to a file operation.
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
	host := extractHostFromRemoteAddr(addr)

	// Resolve server hostname via DNS lookup (mirrors Python _determineHostname).
	// lookupDNSHostname checks the cache first and, on a miss, performs a bounded
	// reverse DNS lookup and caches the result.  Falls back to the raw IP when DNS
	// is disabled or the lookup fails.
	serverHostname := host
	if isIPPattern(host) {
		ipStr := extractIPFromHost(host)
		if resolved := c.lookupDNSHostname(c.ctx, ipStr); resolved != "" {
			serverHostname = resolved
		}
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
		enrichedEvent["server_hostname"] = serverHostname
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
			udid, experimentID, activityID := parseEAInfo(eaInfo)

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
					existingUserState.ExperimentID = experimentID
					existingUserState.ActivityID = activityID
					c.userMap.Set(userStateKey, existingUserState)
					c.logger.Debugf("Updated user %s (udid=%d) with experiment_id=%d, activity_id=%d",
						userInfo.Username, udid, experimentID, activityID)
				}
			} else {
				// Create new user state with experiment/activity ids
				userState := &UserState{
					UserID:       udid,
					UserInfo:     userInfo,
					ExperimentID: experimentID,
					ActivityID:   activityID,
					CreatedAt:    time.Now(),
				}
				c.userMap.Set(userStateKey, userState)
				c.logger.Debugf("Created new user state for %s (udid=%d) with experiment_id=%d, activity_id=%d",
					userInfo.Username, udid, experimentID, activityID)
			}
		}
	}
}

// parseEAInfo parses the experiment/activity mapping carried on the 'U'
// (MAPUEAC) stream.
// Format: &Uc=udid&Ec=expid&Ac=actid
// Ec and Ac are the numeric SciTags experiment id and activity id (they are ids,
// not names). Non-numeric or missing values yield 0, matching the xrootd
// collector's atoi-based decode (PR #2855). The 0 sentinel is treated as "unset"
// downstream so it never resolves to a name.
// Returns: (udid, experimentID, activityID)
func parseEAInfo(eaInfo string) (uint32, int, int) {
	var udid uint32
	var experimentID, activityID int

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
			// atoi-style: ignore errors, leaving 0 for empty/non-numeric values
			experimentID, _ = strconv.Atoi(value)
		case "Ac":
			activityID, _ = strconv.Atoi(value)
		}
	}

	return udid, experimentID, activityID
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
	// Remove brackets for IPv6 (e.g., "[::ffff:192.168.1.1]" -> "::ffff:192.168.1.1")
	host = strings.Trim(host, "[]")
	// Remove zone suffix for scoped IPv6 literals (e.g., "fe80::1%en0").
	if zoneIdx := strings.LastIndex(host, "%"); zoneIdx >= 0 {
		host = host[:zoneIdx]
	}

	// For IPv4-in-IPv6 representations – both IPv4-mapped (::ffff:a.b.c.d)
	// and IPv4-compatible (::a.b.c.d) – extract the IPv4 portion so that
	// reverse-DNS lookups use in-addr.arpa queries rather than ip6.arpa.
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	// To4 handles the ::ffff:a.b.c.d (IPv4-mapped) case.
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	// Handle IPv4-compatible IPv6 written in dotted notation (::a.b.c.d).
	// net.ParseIP converts dotted notation to pure hex bytes, losing the
	// dot-notation hint, so we detect this case from the original string.
	// The distinguishing feature is a "." in the host (dotted-decimal IPv4).
	if strings.Contains(host, ".") {
		if idx := strings.LastIndex(host, ":"); idx >= 0 {
			if v4 := net.ParseIP(host[idx+1:]); v4 != nil {
				if v4addr := v4.To4(); v4addr != nil {
					return v4addr.String()
				}
			}
		}
	}
	return host
}

// extractHostFromRemoteAddr extracts a host from remote address strings.
// Handles the normal forms "host:port" and "[ipv6]:port", plus unambiguous
// legacy unbracketed "ipv6:port" values observed in some message payloads.
func extractHostFromRemoteAddr(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}

	trimmed := strings.Trim(remoteAddr, "[]")
	if zoneIdx := strings.LastIndex(trimmed, "%"); zoneIdx >= 0 {
		trimmed = trimmed[:zoneIdx]
	}

	// If this is already a bare IP literal (with optional brackets/zone),
	// keep it unchanged and do not attempt legacy host:port splitting.
	if net.ParseIP(trimmed) != nil {
		return remoteAddr
	}

	// Try legacy unbracketed IPv6-with-port. We only split on the final colon
	// when the suffix is a valid port and the prefix parses as an IP literal.
	if strings.Count(remoteAddr, ":") > 1 {
		idx := strings.LastIndex(remoteAddr, ":")
		if idx > 0 && idx < len(remoteAddr)-1 {
			hostCandidate := remoteAddr[:idx]
			portCandidate := remoteAddr[idx+1:]
			if port, err := strconv.Atoi(portCandidate); err == nil && port >= 0 && port <= 65535 {
				testHost := strings.Trim(hostCandidate, "[]")
				if zoneIdx := strings.LastIndex(testHost, "%"); zoneIdx >= 0 {
					testHost = testHost[:zoneIdx]
				}
				if net.ParseIP(testHost) != nil {
					return hostCandidate
				}
			}
		}
	}

	return remoteAddr
}

// normalizeVO collapses duplicate whitespace-separated VO tokens while preserving
// first-seen token order. This protects downstream output from repeated values
// such as "cms cms cms" emitted by some upstream auth records.
func normalizeVO(raw string) string {
	tokens := strings.Fields(raw)
	if len(tokens) == 0 {
		return ""
	}

	seen := make(map[string]struct{}, len(tokens))
	unique := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		k := strings.ToLower(token)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		unique = append(unique, token)
	}

	return strings.Join(unique, " ")
}

// handleFileOpen handles a file open event.
// windowBeg is the tBeg of the packet's FileTOD record — the begin of the
// monitoring window during which the open occurred — and is stored as the
// operation start time for use when the matching close arrives.
func (c *Correlator) handleFileOpen(rec parser.FileOpenRecord, packet *parser.Packet, serverID string, windowBeg int64) (*CollectorRecord, error) {
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

	openTime := windowBeg
	if openTime <= 0 {
		openTime = time.Now().Unix()
	}

	state := &FileState{
		FileID:    rec.Header.FileId,
		UserID:    userId,
		OpenTime:  openTime,
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

// handleFileClose handles a file close event.
// windowBeg/windowEnd are the tBeg/tEnd of the packet's FileTOD record. The
// close occurred within this window, so windowEnd is used as the operation end
// time; windowBeg is the start-time fallback when no matching open was seen.
func (c *Correlator) handleFileClose(rec parser.FileCloseRecord, packet *parser.Packet, serverID string, windowBeg, windowEnd int64) (*CollectorRecord, error) {
	// Key is only serverID + fileID (matches the key used in handleFileOpen)
	key := BuildFileKey(serverID, rec.Header.FileId)

	c.logger.Debugf("Correlating file close: serverID=%s, fileID=%d, userID=%d", serverID, rec.Header.FileId, rec.Header.UserId)

	// Try to get the open state
	val, exists := c.stateMap.Get(key)
	if !exists {
		c.logger.Debugf("No open record found for file close: serverID=%s, fileID=%d - creating standalone record", serverID, rec.Header.FileId)
		// No open record found, create a standalone close record
		return c.createStandaloneCloseRecord(rec, packet, windowBeg, windowEnd), nil
	}

	state, ok := val.(*FileState)
	if !ok {
		return nil, fmt.Errorf("invalid state type")
	}

	// Create correlated record
	record := c.createCorrelatedRecord(state, rec, packet, windowEnd)

	// Remove from state map
	c.stateMap.Delete(key)

	return record, nil
}

// extractFileWindow returns the tBeg/tEnd of the FileTOD (isTime) record that
// leads an f-stream packet. These Unix timestamps bound the monitoring window
// during which the packet's file events occurred and are the basis for the
// operation start/end times. Returns (0, 0) when no time record is present.
func extractFileWindow(fileRecords []interface{}) (windowBeg, windowEnd int64) {
	for _, rec := range fileRecords {
		if tr, ok := rec.(parser.FileTimeRecord); ok {
			return int64(tr.TBeg), int64(tr.TEnd)
		}
	}
	return 0, 0
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

// createCorrelatedRecord creates a collector record from correlated state.
// windowEnd is the tEnd of the close packet's FileTOD record — the end of the
// monitoring window in which the file closed — and is used as the operation
// end time. It falls back to the current time if the packet carried no window.
func (c *Correlator) createCorrelatedRecord(state *FileState, rec parser.FileCloseRecord, packet *parser.Packet, windowEnd int64) *CollectorRecord {
	now := time.Now()

	// Operation start/end come from the XRootD FileTOD window, not the server
	// boot time. Fall back to wall-clock time only when a packet lacks a window.
	startTime := state.OpenTime
	if startTime <= 0 {
		startTime = now.Unix()
	}
	endTime := windowEnd
	if endTime <= 0 {
		endTime = now.Unix()
	}

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
	var clientHostname string

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
					userDomain = extractDomainFromHostname(hostname)
					clientHostname = hostname
				} else if needsAsync {
					// Mark record as needing async DNS enrichment
					needsDNSEnrichment = true
					enrichmentIP = ipStr
				}
			} else {
				// Host is already a hostname - extract domain directly
				userDomain = extractDomainFromHostname(host)
				clientHostname = host
			}
		}

		// Use DN from authInfo (split on :: and take first part)
		if userInfo.AuthInfo.DN != "" {
			parts := strings.Split(userInfo.AuthInfo.DN, "::")
			userDN = parts[0]
		}

		// Extract VO from authInfo.Org field
		if userInfo.AuthInfo.Org != "" {
			vo = normalizeVO(userInfo.AuthInfo.Org)
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

	// Resolve SciTags experiment/activity. The 'U' stream carries numeric ids
	// (namespaced per experiment); resolve them to names via the registry while
	// keeping the raw ids on the record. The resolved experiment name is also
	// exposed as the SciTags-derived VO in its own field; the authoritative VO
	// field is intentionally left untouched, since it comes from the auth/token
	// streams and downstream consumers already depend on it.
	experimentID := 0
	activityID := 0
	experiment := ""
	activity := ""
	scitagsVO := ""
	if userInfo != nil {
		experimentID = userInfo.ExperimentID
		activityID = userInfo.ActivityID
		if experimentID != 0 {
			experiment = c.scitags.ExperimentName(experimentID)
			if experiment == "" {
				scitagsUnmappedIDsTotal.WithLabelValues("experiment").Inc()
			} else {
				// The SciTags experiment name is the VO the flow belongs to.
				scitagsVO = experiment
			}
			if activityID != 0 {
				activity = c.scitags.ActivityName(experimentID, activityID)
				if activity == "" {
					scitagsUnmappedIDsTotal.WithLabelValues("activity").Inc()
				}
			}
		}
	}

	// Extract directory names from filename
	dirname1, dirname2, logicalDirname := extractDirnames(state.Filename)

	// Parse RemoteAddr to extract server IP and hostname
	serverIP := "unknown"
	serverHostname := "unknown"
	var needsServerDNS bool
	var serverEnrichmentIP string
	if packet.RemoteAddr != "" {
		serverIP = extractHostFromRemoteAddr(packet.RemoteAddr)
		serverHostname = serverIP

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
		StartTime:              startTime,
		EndTime:                endTime,
		OperationTime:          endTime - startTime,
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
		ExperimentID:           experimentID,
		ActivityID:             activityID,
		ScitagsVO:              scitagsVO,
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
		clientHostname:         clientHostname,
	}
}

// createStandaloneCloseRecord creates a record from just a close event.
// With no matching open, the best available start time is the begin of the
// close packet's monitoring window (windowBeg); windowEnd is the end time.
func (c *Correlator) createStandaloneCloseRecord(rec parser.FileCloseRecord, packet *parser.Packet, windowBeg, windowEnd int64) *CollectorRecord {
	// Use the same serverID format as getServerID()
	serverID := c.getServerID(packet)

	state := &FileState{
		FileID:   rec.Header.FileId,
		UserID:   rec.Header.UserId,
		OpenTime: windowBeg,
		Filename: "unknown",
		ServerID: serverID,
	}
	return c.createCorrelatedRecord(state, rec, packet, windowEnd)
}

// ToJSON converts a collector record to JSON
func (r *CollectorRecord) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

// Stop stops the correlator
func (c *Correlator) Stop() {
	// Close queue first so workers drain in-flight requests before exiting.
	if c.enrichmentQueue != nil {
		c.enrichmentQueue.Close()
	}
	if c.cancel != nil {
		// Cancel the context before waiting so workers can unblock on c.ctx.Done().
		c.cancel()
	}
	c.enrichmentWG.Wait()
	c.enrichmentQueue = nil

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
