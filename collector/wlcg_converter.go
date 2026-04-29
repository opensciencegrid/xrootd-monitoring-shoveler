package collector

import (
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// WLCGRecord represents a WLCG-formatted file access record
// Format documented at: https://twiki.cern.ch/twiki/bin/view/Main/GenericFileMonitoring
type WLCGRecord struct {
	SiteName               string                 `json:"site_name"`
	Fallback               bool                   `json:"fallback"`
	UserDN                 string                 `json:"user_dn"`
	User                   string                 `json:"user,omitempty"`
	ClientHost             string                 `json:"client_host"`
	ClientDomain           string                 `json:"client_domain"`
	ServerHost             string                 `json:"server_host"`
	ServerDomain           string                 `json:"server_domain"`
	ServerIP               string                 `json:"server_ip"`
	UniqueID               string                 `json:"unique_id"`
	FileLFN                string                 `json:"file_lfn"`
	FileSize               int64                  `json:"file_size"`
	ReadBytes              int64                  `json:"read_bytes"`
	ReadSingleBytes        int64                  `json:"read_single_bytes"`
	ReadVectorBytes        int64                  `json:"read_vector_bytes"`
	IPv6                   bool                   `json:"ipv6"`
	StartTime              int64                  `json:"start_time"`
	EndTime                int64                  `json:"end_time"`
	OperationTime          int64                  `json:"operation_time"`
	Operation              string                 `json:"operation"`
	ServerSite             string                 `json:"server_site"`
	UserProtocol           string                 `json:"user_protocol,omitempty"`
	VO                     string                 `json:"vo,omitempty"`
	WriteBytes             int64                  `json:"write_bytes"`
	ReadAverage            int64                  `json:"read_average,omitempty"`
	ReadBytesAtClose       int64                  `json:"read_bytes_at_close,omitempty"`
	ReadMax                int32                  `json:"read_max,omitempty"`
	ReadMin                int32                  `json:"read_min,omitempty"`
	ReadOperations         int32                  `json:"read_operations,omitempty"`
	ReadSigma              int32                  `json:"read_sigma,omitempty"`
	ReadSingleAverage      int64                  `json:"read_single_average,omitempty"`
	ReadSingleMax          int32                  `json:"read_single_max,omitempty"`
	ReadSingleMin          int32                  `json:"read_single_min,omitempty"`
	ReadSingleOperations   int32                  `json:"read_single_operations,omitempty"`
	ReadSingleSigma        int32                  `json:"read_single_sigma,omitempty"`
	ReadVectorAverage      int64                  `json:"read_vector_average,omitempty"`
	ReadVectorCountAverage float64                `json:"read_vector_count_average,omitempty"`
	ReadVectorCountMax     int16                  `json:"read_vector_count_max,omitempty"`
	ReadVectorCountMin     int16                  `json:"read_vector_count_min,omitempty"`
	ReadVectorCountSigma   int16                  `json:"read_vector_count_sigma,omitempty"`
	ReadVectorMax          int32                  `json:"read_vector_max,omitempty"`
	ReadVectorMin          int32                  `json:"read_vector_min,omitempty"`
	ReadVectorOperations   int32                  `json:"read_vector_operations,omitempty"`
	ReadVectorSigma        int32                  `json:"read_vector_sigma,omitempty"`
	WriteAverage           int64                  `json:"write_average,omitempty"`
	WriteBytesAtClose      int64                  `json:"write_bytes_at_close,omitempty"`
	WriteMax               int32                  `json:"write_max,omitempty"`
	WriteMin               int32                  `json:"write_min,omitempty"`
	WriteOperations        int32                  `json:"write_operations,omitempty"`
	WriteSigma             int32                  `json:"write_sigma,omitempty"`
	Experiment             string                 `json:"experiment,omitempty"`
	Activity               string                 `json:"activity,omitempty"`
	CRABId                 string                 `json:"CRAB_Id,omitempty"`
	CRABRetry              string                 `json:"CRAB_Retry,omitempty"`
	CRABWorkflow           string                 `json:"CRAB_Workflow,omitempty"`
	Metadata               map[string]interface{} `json:"metadata"`
}

// IsWLCGPacket determines if a record should be converted to WLCG format
// Based on reference implementation:
// - Path starts with /store or /user/dteam
// - VO is "cms"
func IsWLCGPacket(record *CollectorRecord) bool {
	// Check if VO is cms
	if strings.EqualFold(record.VO, "cms") {
		return true
	}

	// Check if path starts with /store or /user/dteam
	filename := strings.TrimSpace(record.Filename)
	if strings.HasPrefix(filename, "/store") || strings.HasPrefix(filename, "/user/dteam") {
		return true
	}

	return false
}

// ConvertToWLCG converts a CollectorRecord to WLCG format
// Based on references/wlcg_converter.py
func ConvertToWLCG(record *CollectorRecord) (*WLCGRecord, error) {
	// Generate unique ID
	uniqueID := uuid.New().String()

	// Extract server domain from server hostname
	serverDomain := ""
	if record.ServerHostname != "" {
		parts := strings.Split(record.ServerHostname, ".")
		if len(parts) >= 2 {
			serverDomain = strings.Join(parts[len(parts)-2:], ".")
		}
	}

	// Determine operation type
	operation := "unknown"
	if record.Read > 0 || record.Readv > 0 {
		operation = "read"
	} else if record.Write > 0 {
		operation = "write"
	}

	// Extract user from DN (everything after CN=)
	user := ""
	if record.UserDN != "" {
		parts := strings.Split(record.UserDN, "CN=")
		if len(parts) > 1 {
			user = parts[len(parts)-1]
		}
	}

	wlcg := &WLCGRecord{
		SiteName:               record.Site,
		Fallback:               true,
		UserDN:                 record.UserDN,
		User:                   user,
		ClientHost:             record.Host,
		ClientDomain:           record.UserDomain,
		ServerHost:             record.ServerHostname,
		ServerDomain:           serverDomain,
		ServerIP:               record.ServerIP,
		UniqueID:               uniqueID,
		FileLFN:                record.Filename,
		FileSize:               record.Filesize,
		ReadBytes:              record.Read + record.Readv,
		ReadSingleBytes:        record.Read,
		ReadVectorBytes:        record.Readv,
		IPv6:                   record.IPv6,
		StartTime:              record.StartTime,
		EndTime:                record.EndTime,
		OperationTime:          record.OperationTime,
		Operation:              operation,
		ServerSite:             record.Site,
		UserProtocol:           record.Protocol,
		VO:                     record.VO,
		WriteBytes:             record.Write,
		ReadAverage:            record.ReadAverage,
		ReadBytesAtClose:       record.ReadBytesAtClose,
		ReadMax:                record.ReadMax,
		ReadMin:                record.ReadMin,
		ReadOperations:         record.ReadOperations,
		ReadSingleAverage:      record.ReadSingleAverage,
		ReadSingleMax:          record.ReadSingleMax,
		ReadSingleMin:          record.ReadSingleMin,
		ReadSingleOperations:   record.ReadSingleOperations,
		ReadVectorAverage:      record.ReadVectorAverage,
		ReadVectorCountAverage: record.ReadVectorCountAverage,
		ReadVectorCountMax:     record.ReadVectorCountMax,
		ReadVectorCountMin:     record.ReadVectorCountMin,
		ReadVectorMax:          record.ReadVectorMax,
		ReadVectorMin:          record.ReadVectorMin,
		ReadVectorOperations:   record.ReadVectorOperations,
		WriteAverage:           record.WriteAverage,
		WriteBytesAtClose:      record.WriteBytesAtClose,
		WriteMax:               record.WriteMax,
		WriteMin:               record.WriteMin,
		WriteOperations:        record.WriteOperations,
		Experiment:             record.Experiment,
		Activity:               record.Activity,
	}

	// Parse appinfo for CRAB information if present
	// Format: 162_https://glidein.cern.ch/162/190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2_0
	// Results in: CRAB_Id=162, CRAB_Workflow=190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2, CRAB_Retry=0
	if record.AppInfo != "" {
		parts := strings.Split(record.AppInfo, "_")
		if len(parts) == 3 {
			wlcg.CRABId = parts[0]
			wlcg.CRABRetry = parts[2]

			// Extract workflow from URL (everything after last /)
			urlParts := strings.Split(parts[1], "/")
			if len(urlParts) > 0 {
				wlcg.CRABWorkflow = urlParts[len(urlParts)-1]
			}
		}
	}

	// Add metadata
	hostname, _ := os.Hostname()
	wlcg.Metadata = map[string]interface{}{
		"producer":    "cms",
		"type":        "aaa-ng",
		"timestamp":   time.Now().UnixNano() / int64(time.Millisecond),
		"type_prefix": "raw",
		"host":        hostname,
		"_id":         uniqueID,
	}

	return wlcg, nil
}

// ToJSON converts a WLCG record to JSON
func (w *WLCGRecord) ToJSON() ([]byte, error) {
	return json.Marshal(w)
}

// TPCPathCheckWLCG checks if a TPC source/destination URL should be converted to WLCG format
// Based on references/wlcg_converter.py::tpcPathCheckWLCG
func TPCPathCheckWLCG(urlStr string) bool {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	path := strings.TrimLeft(parsedURL.Path, "/")
	return strings.HasPrefix(path, "store") || strings.HasPrefix(path, "user/dteam")
}

// CachePathCheckWLCG checks if a cache file path should be converted to WLCG format
// Based on DetailedCollector.py::process_gstream
func CachePathCheckWLCG(path string) bool {
	cleanPath := strings.TrimSpace(path)
	return strings.HasPrefix(cleanPath, "/store") || strings.HasPrefix(cleanPath, "/user/dteam")
}

func renameField(m map[string]interface{}, from, to string) {
	if v, ok := m[from]; ok {
		m[to] = v
		delete(m, from)
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	}
	return 0
}

// TransformCacheEvent renames raw XRootD cache gstream fields to human-readable names
// and computes derived byte-count fields. Does not mutate the input map.
func TransformCacheEvent(event map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(event)+2)
	for k, v := range event {
		out[k] = v
	}

	renameField(out, "lfn", "file_path")
	renameField(out, "blk_size", "block_size")
	renameField(out, "n_blks", "numbers_blocks")
	renameField(out, "n_blks_done", "numbers_blocks_done")
	renameField(out, "access_cnt", "access_count")
	renameField(out, "attach_t", "attach_time")
	renameField(out, "detach_t", "detach_time")
	renameField(out, "b_hit", "bytes_hit_cache")
	renameField(out, "b_miss", "bytes_miss_cache")
	renameField(out, "b_bypass", "bytes_bypass_cache")
	renameField(out, "b_todisk", "bytes_to_disk")
	renameField(out, "b_prefetch", "bytes_by_prefetch")
	renameField(out, "n_cks_errs", "numbers_checksum_errors")

	todisk := toFloat64(out["bytes_to_disk"])
	bypass := toFloat64(out["bytes_bypass_cache"])
	prefetch := toFloat64(out["bytes_by_prefetch"])
	out["bytes_read_by_remote"] = todisk + bypass
	out["bytes_explicit_remote_read"] = todisk + bypass - prefetch

	return out
}

// TransformTPCEvent renames raw XRootD TPC gstream fields to human-readable names.
// Does not mutate the input map.
func TransformTPCEvent(event map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(event))
	for k, v := range event {
		out[k] = v
	}

	renameField(out, "TPC", "tpc_protocol")
	renameField(out, "Client", "client")
	renameField(out, "Src", "source")
	renameField(out, "Dst", "destination")
	renameField(out, "Size", "size")

	if xeqRaw, ok := out["Xeq"]; ok {
		if xeqMap, ok := xeqRaw.(map[string]interface{}); ok {
			xeq := make(map[string]interface{}, len(xeqMap))
			for k, v := range xeqMap {
				xeq[k] = v
			}
			renameField(xeq, "Beg", "begin_transfer")
			renameField(xeq, "End", "end_transfer")
			renameField(xeq, "IPv", "ip_version")
			renameField(xeq, "RC", "return_code")
			renameField(xeq, "Strm", "used_streams")
			renameField(xeq, "Type", "flow_direction")
			out["xeq"] = xeq
		}
		delete(out, "Xeq")
	}

	return out
}

// GStreamMetadata contains metadata added to gstream events in WLCG format
type GStreamMetadata struct {
	Producer   string `json:"producer"`
	Type       string `json:"type"`
	Timestamp  int64  `json:"timestamp"`
	TypePrefix string `json:"type_prefix"`
	Host       string `json:"host"`
	ID         string `json:"_id"`
}

// ConvertGStreamToWLCG adds WLCG metadata to a gstream event map
// Based on references/wlcg_converter.py::ConvertGstream
func ConvertGStreamToWLCG(event map[string]interface{}, isTPC bool) (map[string]interface{}, error) {
	eventCopy := make(map[string]interface{})
	for k, v := range event {
		eventCopy[k] = v
	}

	hostname, _ := os.Hostname()
	eventType := "metric"
	if isTPC {
		eventType = "tpc"
	}

	eventCopy["metadata"] = GStreamMetadata{
		Producer:   "cms-xrootd-cache",
		Type:       eventType,
		Timestamp:  time.Now().UnixNano() / int64(time.Millisecond),
		TypePrefix: "raw",
		Host:       hostname,
		ID:         uuid.New().String(),
	}

	return eventCopy, nil
}
