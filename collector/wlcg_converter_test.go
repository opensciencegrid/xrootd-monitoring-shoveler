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
