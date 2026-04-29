package collector

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestIsWLCGPacket(t *testing.T) {
	tests := []struct {
		name     string
		record   *CollectorRecord
		expected bool
	}{
		{
			name: "CMS VO should be WLCG",
			record: &CollectorRecord{
				VO:       "cms",
				Filename: "/some/other/path",
			},
			expected: true,
		},
		{
			name: "CMS VO case insensitive",
			record: &CollectorRecord{
				VO:       "CMS",
				Filename: "/some/other/path",
			},
			expected: true,
		},
		{
			name: "/store path should be WLCG",
			record: &CollectorRecord{
				VO:       "atlas",
				Filename: "/store/data/file.root",
			},
			expected: true,
		},
		{
			name: "/user/dteam path should be WLCG",
			record: &CollectorRecord{
				VO:       "other",
				Filename: "/user/dteam/test.dat",
			},
			expected: true,
		},
		{
			name: "Non-WLCG path and VO",
			record: &CollectorRecord{
				VO:       "osg",
				Filename: "/ospool/protected/data.txt",
			},
			expected: false,
		},
		{
			name: "Empty filename and VO",
			record: &CollectorRecord{
				VO:       "",
				Filename: "",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsWLCGPacket(tt.record)
			if result != tt.expected {
				t.Errorf("IsWLCGPacket() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestConvertToWLCG(t *testing.T) {
	now := time.Now()
	record := &CollectorRecord{
		Timestamp:              now,
		StartTime:              now.Unix() - 100,
		EndTime:                now.Unix(),
		OperationTime:          100,
		ServerID:               "12345#host.example.com:1094",
		ServerHostname:         "xrootd.cern.ch",
		Server:                 "192.168.1.1",
		ServerIP:               "192.168.1.1",
		Site:                   "T2_US_Nebraska",
		User:                   "testuser",
		UserDN:                 "/DC=org/DC=example/OU=People/CN=Test User",
		UserDomain:             "example.com",
		VO:                     "cms",
		Host:                   "client.example.com",
		Filename:               "/store/data/Run2018D/file.root",
		Protocol:               "root",
		AppInfo:                "162_https://glidein.cern.ch/162/190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2_0",
		IPv6:                   false,
		Filesize:               1234567890,
		ReadOperations:         100,
		ReadSingleOperations:   50,
		ReadVectorOperations:   50,
		WriteOperations:        10,
		Read:                   1000000,
		ReadSingleBytes:        500000,
		Readv:                  500000,
		Write:                  100000,
		ReadMin:                100,
		ReadMax:                10000,
		ReadAverage:            5000,
		ReadSingleMin:          100,
		ReadSingleMax:          8000,
		ReadSingleAverage:      4000,
		ReadVectorMin:          200,
		ReadVectorMax:          12000,
		ReadVectorAverage:      6000,
		WriteMin:               50,
		WriteMax:               5000,
		WriteAverage:           2500,
		ReadVectorCountMin:     5,
		ReadVectorCountMax:     50,
		ReadVectorCountAverage: 25.5,
		ReadBytesAtClose:       1000000,
		WriteBytesAtClose:      100000,
		HasFileCloseMsg:        1,
	}

	wlcg, err := ConvertToWLCG(record)
	if err != nil {
		t.Fatalf("ConvertToWLCG() error = %v", err)
	}

	// Verify basic fields
	if wlcg.SiteName != "T2_US_Nebraska" {
		t.Errorf("SiteName = %v, expected T2_US_Nebraska", wlcg.SiteName)
	}

	if !wlcg.Fallback {
		t.Error("Fallback should be true")
	}

	if wlcg.UserDN != "/DC=org/DC=example/OU=People/CN=Test User" {
		t.Errorf("UserDN = %v, expected /DC=org/DC=example/OU=People/CN=Test User", wlcg.UserDN)
	}

	if wlcg.User != "Test User" {
		t.Errorf("User = %v, expected 'Test User' (extracted from DN)", wlcg.User)
	}

	if wlcg.ClientHost != "client.example.com" {
		t.Errorf("ClientHost = %v, expected client.example.com", wlcg.ClientHost)
	}

	if wlcg.ServerHost != "xrootd.cern.ch" {
		t.Errorf("ServerHost = %v, expected xrootd.cern.ch", wlcg.ServerHost)
	}

	if wlcg.ServerDomain != "cern.ch" {
		t.Errorf("ServerDomain = %v, expected cern.ch", wlcg.ServerDomain)
	}

	if wlcg.FileLFN != "/store/data/Run2018D/file.root" {
		t.Errorf("FileLFN = %v, expected /store/data/Run2018D/file.root", wlcg.FileLFN)
	}

	if wlcg.FileSize != 1234567890 {
		t.Errorf("FileSize = %v, expected 1234567890", wlcg.FileSize)
	}

	if wlcg.ReadBytes != 1500000 { // Read + Readv
		t.Errorf("ReadBytes = %v, expected 1500000", wlcg.ReadBytes)
	}

	if wlcg.ReadSingleBytes != 1000000 {
		t.Errorf("ReadSingleBytes = %v, expected 1000000", wlcg.ReadSingleBytes)
	}

	if wlcg.ReadVectorBytes != 500000 {
		t.Errorf("ReadVectorBytes = %v, expected 500000", wlcg.ReadVectorBytes)
	}

	if wlcg.WriteBytes != 100000 {
		t.Errorf("WriteBytes = %v, expected 100000", wlcg.WriteBytes)
	}

	if wlcg.Operation != "read" {
		t.Errorf("Operation = %v, expected 'read'", wlcg.Operation)
	}

	if wlcg.VO != "cms" {
		t.Errorf("VO = %v, expected cms", wlcg.VO)
	}

	if wlcg.UserProtocol != "root" {
		t.Errorf("UserProtocol = %v, expected root", wlcg.UserProtocol)
	}

	// Verify UUID format
	if len(wlcg.UniqueID) == 0 {
		t.Error("UniqueID should not be empty")
	}
	if !strings.Contains(wlcg.UniqueID, "-") {
		t.Error("UniqueID should be in UUID format with dashes")
	}

	// Verify CRAB info extraction
	if wlcg.CRABId != "162" {
		t.Errorf("CRABId = %v, expected 162", wlcg.CRABId)
	}

	if wlcg.CRABRetry != "0" {
		t.Errorf("CRABRetry = %v, expected 0", wlcg.CRABRetry)
	}

	if wlcg.CRABWorkflow != "190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2" {
		t.Errorf("CRABWorkflow = %v, unexpected value", wlcg.CRABWorkflow)
	}

	// Verify metadata
	if wlcg.Metadata == nil {
		t.Fatal("Metadata should not be nil")
	}

	if wlcg.Metadata["producer"] != "cms" {
		t.Errorf("Metadata producer = %v, expected cms", wlcg.Metadata["producer"])
	}

	if wlcg.Metadata["type"] != "aaa-ng" {
		t.Errorf("Metadata type = %v, expected aaa-ng", wlcg.Metadata["type"])
	}

	if wlcg.Metadata["_id"] != wlcg.UniqueID {
		t.Error("Metadata _id should match UniqueID")
	}

	// Verify JSON marshaling works
	jsonBytes, err := wlcg.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error = %v", err)
	}

	// Verify it's valid JSON
	var decoded map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
}

func TestConvertToWLCG_WriteOperation(t *testing.T) {
	now := time.Now()
	record := &CollectorRecord{
		Timestamp:     now,
		StartTime:     now.Unix() - 50,
		EndTime:       now.Unix(),
		OperationTime: 50,
		Site:          "T2_US_Nebraska",
		Filename:      "/store/data/output.root",
		VO:            "cms",
		Read:          0,
		Readv:         0,
		Write:         5000000,
	}

	wlcg, err := ConvertToWLCG(record)
	if err != nil {
		t.Fatalf("ConvertToWLCG() error = %v", err)
	}

	if wlcg.Operation != "write" {
		t.Errorf("Operation = %v, expected 'write'", wlcg.Operation)
	}

	if wlcg.WriteBytes != 5000000 {
		t.Errorf("WriteBytes = %v, expected 5000000", wlcg.WriteBytes)
	}
}

func TestConvertToWLCG_UnknownOperation(t *testing.T) {
	now := time.Now()
	record := &CollectorRecord{
		Timestamp:     now,
		StartTime:     now.Unix() - 10,
		EndTime:       now.Unix(),
		OperationTime: 10,
		Site:          "T2_US_Nebraska",
		Filename:      "/store/data/file.root",
		VO:            "cms",
		Read:          0,
		Readv:         0,
		Write:         0,
	}

	wlcg, err := ConvertToWLCG(record)
	if err != nil {
		t.Fatalf("ConvertToWLCG() error = %v", err)
	}

	if wlcg.Operation != "unknown" {
		t.Errorf("Operation = %v, expected 'unknown'", wlcg.Operation)
	}
}

func TestGenerateUUID(t *testing.T) {
	// Test that ConvertToWLCG generates valid UUIDs
	now := time.Now()
	record := &CollectorRecord{
		Timestamp: now,
		StartTime: now.Unix(),
		EndTime:   now.Unix(),
		Site:      "TEST",
		Filename:  "/store/test.root",
		VO:        "cms",
	}

	wlcg1, _ := ConvertToWLCG(record)
	wlcg2, _ := ConvertToWLCG(record)

	// Check basic format (8-4-4-4-12 hex characters)
	if len(wlcg1.UniqueID) != 36 {
		t.Errorf("UUID length = %d, expected 36", len(wlcg1.UniqueID))
	}

	// Check that two generated UUIDs are different
	if wlcg1.UniqueID == wlcg2.UniqueID {
		t.Error("Two generated UUIDs should be different")
	}

	// Check for dashes in correct positions
	if wlcg1.UniqueID[8] != '-' || wlcg1.UniqueID[13] != '-' || wlcg1.UniqueID[18] != '-' || wlcg1.UniqueID[23] != '-' {
		t.Errorf("UUID format incorrect: %s", wlcg1.UniqueID)
	}
}

func TestCachePathCheckWLCG(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "/store path should be WLCG",
			path:     "/store/data/file.root",
			expected: true,
		},
		{
			name:     "/user/dteam path should be WLCG",
			path:     "/user/dteam/test.dat",
			expected: true,
		},
		{
			name:     "Non-WLCG path",
			path:     "/ospool/data/file.txt",
			expected: false,
		},
		{
			name:     "Empty path",
			path:     "",
			expected: false,
		},
		{
			name:     "Path with whitespace",
			path:     "  /store/data/file.root  ",
			expected: true,
		},
		{
			name:     "partial store path",
			path:     "/some/store/data/file.root",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CachePathCheckWLCG(tt.path)
			if result != tt.expected {
				t.Errorf("CachePathCheckWLCG(%q) = %v, expected %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestTPCPathCheckWLCG(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{
			name:     "store path in URL",
			url:      "root://xrootd.cern.ch//store/data/file.root",
			expected: true,
		},
		{
			name:     "user/dteam path in URL",
			url:      "root://xrootd.cern.ch//user/dteam/test.dat",
			expected: true,
		},
		{
			name:     "Non-WLCG path",
			url:      "root://xrootd.cern.ch//ospool/data/file.txt",
			expected: false,
		},
		{
			name:     "Invalid URL",
			url:      "://invalid",
			expected: false,
		},
		{
			name:     "Empty URL",
			url:      "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TPCPathCheckWLCG(tt.url)
			if result != tt.expected {
				t.Errorf("TPCPathCheckWLCG(%q) = %v, expected %v", tt.url, result, tt.expected)
			}
		})
	}
}

func TestConvertGStreamToWLCG(t *testing.T) {
	event := map[string]interface{}{
		"file_path":        "/store/data/file.root",
		"bytes_hit_cache":  int64(1000000),
		"bytes_miss_cache": int64(500000),
		"server_hostname":  "xrootd.cern.ch",
	}

	wlcgEvent, err := ConvertGStreamToWLCG(event, false)
	if err != nil {
		t.Fatalf("ConvertGStreamToWLCG() error = %v", err)
	}

	if wlcgEvent["file_path"] != "/store/data/file.root" {
		t.Errorf("file_path = %v, expected /store/data/file.root", wlcgEvent["file_path"])
	}

	if wlcgEvent["metadata"] == nil {
		t.Fatal("metadata should not be nil")
	}

	metadata, ok := wlcgEvent["metadata"].(GStreamMetadata)
	if !ok {
		t.Fatal("metadata should be GStreamMetadata type")
	}

	if metadata.Producer != "cms-xrootd-cache" {
		t.Errorf("Producer = %v, expected cms-xrootd-cache", metadata.Producer)
	}

	if metadata.Type != "metric" {
		t.Errorf("Type = %v, expected metric", metadata.Type)
	}
}

func TestTransformCacheEvent(t *testing.T) {
	raw := map[string]interface{}{
		"event":      "file_close",
		"lfn":        "/store/user/matevz/file.root",
		"size":       float64(2446541517),
		"blk_size":   float64(131072),
		"n_blks":     float64(18666),
		"n_blks_done": float64(6784),
		"access_cnt": float64(4),
		"attach_t":   float64(1688057096),
		"detach_t":   float64(1688057104),
		"b_hit":      float64(865075200),
		"b_miss":     float64(24051712),
		"b_bypass":   float64(0),
		"n_cks_errs": float64(0),
		"b_todisk":   float64(0),
		"b_prefetch": float64(0),
	}

	result := TransformCacheEvent(raw)

	// Verify renamed fields are present under new names
	newNames := []string{
		"file_path", "block_size", "numbers_blocks", "numbers_blocks_done",
		"access_count", "attach_time", "detach_time", "bytes_hit_cache",
		"bytes_miss_cache", "bytes_bypass_cache", "bytes_to_disk",
		"bytes_by_prefetch", "numbers_checksum_errors",
	}
	for _, name := range newNames {
		if _, ok := result[name]; !ok {
			t.Errorf("expected field %q to be present in result", name)
		}
	}

	// Verify old names are absent
	oldNames := []string{
		"lfn", "blk_size", "n_blks", "n_blks_done", "access_cnt",
		"attach_t", "detach_t", "b_hit", "b_miss", "b_bypass",
		"b_todisk", "b_prefetch", "n_cks_errs",
	}
	for _, name := range oldNames {
		if _, ok := result[name]; ok {
			t.Errorf("old field %q should not be present in result", name)
		}
	}

	// Verify specific renamed values
	if result["file_path"] != "/store/user/matevz/file.root" {
		t.Errorf("file_path = %v, expected /store/user/matevz/file.root", result["file_path"])
	}

	// Verify derived fields (both b_todisk and b_bypass are 0)
	if result["bytes_read_by_remote"] != float64(0) {
		t.Errorf("bytes_read_by_remote = %v, expected 0", result["bytes_read_by_remote"])
	}
	if result["bytes_explicit_remote_read"] != float64(0) {
		t.Errorf("bytes_explicit_remote_read = %v, expected 0", result["bytes_explicit_remote_read"])
	}

	// Verify unrelated fields pass through unchanged
	if result["size"] != float64(2446541517) {
		t.Errorf("size = %v, expected 2446541517", result["size"])
	}
	if result["event"] != "file_close" {
		t.Errorf("event = %v, expected file_close", result["event"])
	}

	// Verify original is not mutated
	if _, ok := raw["lfn"]; !ok {
		t.Error("original map should not be mutated: lfn key missing")
	}
	if _, ok := raw["file_path"]; ok {
		t.Error("original map should not be mutated: file_path key should not be added")
	}
}

func TestTransformCacheEvent_DerivedFields(t *testing.T) {
	raw := map[string]interface{}{
		"b_todisk":   float64(1000),
		"b_bypass":   float64(500),
		"b_prefetch": float64(200),
	}

	result := TransformCacheEvent(raw)

	if result["bytes_read_by_remote"] != float64(1500) {
		t.Errorf("bytes_read_by_remote = %v, expected 1500", result["bytes_read_by_remote"])
	}
	if result["bytes_explicit_remote_read"] != float64(1300) {
		t.Errorf("bytes_explicit_remote_read = %v, expected 1300", result["bytes_explicit_remote_read"])
	}
}

func TestTransformTPCEvent(t *testing.T) {
	raw := map[string]interface{}{
		"TPC":    "xroot",
		"Client": "abh.47358:24@cent7a.slac.stanford.edu",
		"Xeq": map[string]interface{}{
			"Beg":  "2022-04-01T04:22:15.765838Z",
			"End":  "2022-04-01T04:22:15.891503Z",
			"RC":   float64(0),
			"Strm": float64(1),
			"Type": "pull",
			"IPv":  float64(6),
		},
		"Src":  "xroot://cent7b.slac.stanford.edu:1094//store/data/file.root",
		"Dst":  "xroot://griddev08.slac.stanford.edu:1094//store/data/file.root",
		"Size": float64(6293536),
	}

	out := TransformTPCEvent(raw)

	if out["tpc_protocol"] != "xroot" {
		t.Errorf("tpc_protocol = %v, expected xroot", out["tpc_protocol"])
	}
	if out["client"] != "abh.47358:24@cent7a.slac.stanford.edu" {
		t.Errorf("client = %v, expected abh.47358:24@cent7a.slac.stanford.edu", out["client"])
	}
	if out["source"] != "xroot://cent7b.slac.stanford.edu:1094//store/data/file.root" {
		t.Errorf("source = %v, expected xroot://cent7b.slac.stanford.edu:1094//store/data/file.root", out["source"])
	}
	if out["destination"] != "xroot://griddev08.slac.stanford.edu:1094//store/data/file.root" {
		t.Errorf("destination = %v, expected xroot://griddev08.slac.stanford.edu:1094//store/data/file.root", out["destination"])
	}
	if out["size"] != float64(6293536) {
		t.Errorf("size = %v, expected 6293536", out["size"])
	}

	for _, old := range []string{"TPC", "Client", "Src", "Dst", "Size", "Xeq"} {
		if _, ok := out[old]; ok {
			t.Errorf("old top-level key %q should not be present in result", old)
		}
	}

	xeq, ok := out["xeq"].(map[string]interface{})
	if !ok {
		t.Fatalf("xeq should be map[string]interface{}, got %T", out["xeq"])
	}
	if xeq["begin_transfer"] != "2022-04-01T04:22:15.765838Z" {
		t.Errorf("begin_transfer = %v, expected 2022-04-01T04:22:15.765838Z", xeq["begin_transfer"])
	}
	if xeq["end_transfer"] != "2022-04-01T04:22:15.891503Z" {
		t.Errorf("end_transfer = %v, expected 2022-04-01T04:22:15.891503Z", xeq["end_transfer"])
	}
	if xeq["return_code"] != float64(0) {
		t.Errorf("return_code = %v, expected 0", xeq["return_code"])
	}
	if xeq["used_streams"] != float64(1) {
		t.Errorf("used_streams = %v, expected 1", xeq["used_streams"])
	}
	if xeq["flow_direction"] != "pull" {
		t.Errorf("flow_direction = %v, expected pull", xeq["flow_direction"])
	}
	if xeq["ip_version"] != float64(6) {
		t.Errorf("ip_version = %v, expected 6", xeq["ip_version"])
	}

	for _, old := range []string{"Beg", "End", "RC", "Strm", "Type", "IPv"} {
		if _, ok := xeq[old]; ok {
			t.Errorf("old xeq key %q should not be present in result", old)
		}
	}

	if raw["TPC"] != "xroot" {
		t.Error("original raw map should not be mutated: TPC key changed")
	}
}

func TestConvertGStreamToWLCG_TPC(t *testing.T) {
	event := map[string]interface{}{
		"source":      "root://src.cern.ch//store/data/file1.root",
		"destination": "root://dst.cern.ch//store/data/file2.root",
		"size":        int64(1000000),
	}

	wlcgEvent, err := ConvertGStreamToWLCG(event, true)
	if err != nil {
		t.Fatalf("ConvertGStreamToWLCG() error = %v", err)
	}

	metadata, ok := wlcgEvent["metadata"].(GStreamMetadata)
	if !ok {
		t.Fatal("metadata should be GStreamMetadata type")
	}

	if metadata.Type != "tpc" {
		t.Errorf("Type = %v, expected tpc", metadata.Type)
	}
}
