package collector

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/input"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
)

// TestMessagesFile reads the tests/messages file, parses all packets,
// and reports statistics on packet types and collector output records.
func TestMessagesFile(t *testing.T) {
	// Path to the messages file
	messagesPath := "../tests/messages"

	// Check if file exists
	if _, err := os.Stat(messagesPath); os.IsNotExist(err) {
		t.Skipf("Skipping test: %s does not exist", messagesPath)
	}

	// Create a FileReader to read the packets
	fr := input.NewFileReader(messagesPath, true) // base64 encoded
	if err := fr.Start(); err != nil {
		t.Fatalf("Failed to start file reader: %v", err)
	}
	defer fr.Stop()

	// Create a correlator with 5 minute TTL
	correlator := NewCorrelator(5*time.Minute, 10000)
	defer correlator.Stop()

	// Statistics
	var stats struct {
		TotalPackets     int
		ParsedOK         int
		ParseErrors      int
		MapRecords       int
		UserRecords      int
		FileOpenRecords  int
		FileCloseRecords int
		FileTimeRecords  int
		XMLPackets       int
		OtherPackets     int
		EmittedRecords   int
		ErrorsByType     map[string]int
	}
	stats.ErrorsByType = make(map[string]int)

	// Collect emitted records
	var emittedRecords []*CollectorRecord

	// Process all packets
	t.Log("Processing packets from", messagesPath)
	packetNum := 0
	for pkt := range fr.Packets() {
		stats.TotalPackets++
		packetNum++

		// Parse packet (catch panics since parser may have bugs)
		var packet *parser.Packet
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("parser panic: %v", r)
					if packetNum <= 5 {
						t.Logf("Packet %d caused panic: %v", packetNum, r)
					}
				}
			}()
			packet, err = parser.ParsePacket(pkt)
		}()

		if err != nil {
			stats.ParseErrors++
			errType := fmt.Sprintf("%T", err)
			if strings.Contains(err.Error(), "length") {
				errType = "length_error"
			} else if strings.Contains(err.Error(), "invalid") {
				errType = "invalid_error"
			} else if strings.Contains(err.Error(), "panic") {
				errType = "parser_panic"
			}
			stats.ErrorsByType[errType]++
			continue
		}
		stats.ParsedOK++

		// Count packet types based on records
		if packet.IsXML {
			stats.XMLPackets++
		}

		if packet.MapRecord != nil {
			stats.MapRecords++
		}

		if packet.UserRecord != nil {
			stats.UserRecords++
		}

		for _, rec := range packet.FileRecords {
			switch rec.(type) {
			case parser.FileOpenRecord:
				stats.FileOpenRecords++
			case parser.FileCloseRecord:
				stats.FileCloseRecords++
			case parser.FileTimeRecord:
				stats.FileTimeRecords++
			}
		}

		if len(packet.FileRecords) == 0 && packet.MapRecord == nil &&
			packet.UserRecord == nil && !packet.IsXML {
			stats.OtherPackets++
		}

		// Process through correlator
		record, err := correlator.ProcessPacket(packet)
		if err != nil {
			t.Logf("Warning: Correlator error: %v", err)
			continue
		}

		// If we got a complete record, collect it
		if record != nil {
			stats.EmittedRecords++
			emittedRecords = append(emittedRecords, record)
			if stats.EmittedRecords <= 5 {
				t.Logf("Emitted record #%d: Filename=%s Read=%d Write=%d HasClose=%d",
					stats.EmittedRecords, record.Filename, record.Read, record.Write, record.HasFileCloseMsg)
			}
		}
	}

	// Report stats
	t.Log("\n=== Packet Processing Statistics ===")
	t.Logf("Total packets read:        %d", stats.TotalPackets)
	t.Logf("Successfully parsed:       %d", stats.ParsedOK)
	t.Logf("Parse errors:              %d", stats.ParseErrors)
	t.Logf("Parsing success rate:      %.2f%%", 100.0*float64(stats.ParsedOK)/float64(stats.TotalPackets))

	if len(stats.ErrorsByType) > 0 {
		t.Log("\nParse errors by type:")
		for errType, count := range stats.ErrorsByType {
			t.Logf("  %-20s: %d", errType, count)
		}
	}

	t.Log("\n=== Packet Types ===")
	t.Logf("Map records:               %d", stats.MapRecords)
	t.Logf("User records:              %d", stats.UserRecords)
	t.Logf("File open records:         %d", stats.FileOpenRecords)
	t.Logf("File close records:        %d", stats.FileCloseRecords)
	t.Logf("File time records:         %d", stats.FileTimeRecords)
	t.Logf("XML packets:               %d", stats.XMLPackets)
	t.Logf("Other packets:             %d", stats.OtherPackets)

	t.Log("\n=== Collector Output ===")
	t.Logf("Records emitted:           %d", stats.EmittedRecords)

	// Show sample of emitted records
	if len(emittedRecords) > 0 {
		t.Log("\n=== Sample Emitted Records (first 5) ===")
		for i, record := range emittedRecords {
			if i >= 5 {
				break
			}
			recordJSON, err := record.ToJSON()
			if err != nil {
				t.Logf("Error marshaling record %d: %v", i, err)
				continue
			}

			// Pretty print the JSON
			var prettyJSON map[string]interface{}
			if err := json.Unmarshal(recordJSON, &prettyJSON); err == nil {
				t.Logf("\nRecord %d:", i+1)
				t.Logf("  Filename:        %v", prettyJSON["filename"])
				t.Logf("  Start time:      %v", prettyJSON["start_time"])
				t.Logf("  End time:        %v", prettyJSON["end_time"])
				t.Logf("  Operation time:  %v ms", prettyJSON["operation_time"])
				t.Logf("  Read bytes:      %v", prettyJSON["read"])
				t.Logf("  Write bytes:     %v", prettyJSON["write"])
				t.Logf("  Read ops:        %v", prettyJSON["read_operations"])
				t.Logf("  Has close msg:   %v", prettyJSON["HasFileCloseMsg"])
			}
		}
	}

	// Calculate some statistics
	if stats.EmittedRecords > 0 {
		t.Log("\n=== Collector Record Analysis ===")

		var totalReadBytes, totalWriteBytes int64
		var totalReadOps, totalWriteOps int32
		var withCloseMsg int
		var totalLatency int64
		var recordsWithLatency int

		for _, record := range emittedRecords {
			totalReadBytes += record.Read
			totalWriteBytes += record.Write
			totalReadOps += record.ReadOperations
			totalWriteOps += record.WriteOperations

			if record.HasFileCloseMsg == 1 {
				withCloseMsg++
			}

			if record.StartTime > 0 && record.EndTime > 0 {
				latency := record.EndTime - record.StartTime
				if latency > 0 {
					totalLatency += latency
					recordsWithLatency++
				}
			}
		}

		t.Logf("Total read bytes:          %d", totalReadBytes)
		t.Logf("Total write bytes:         %d", totalWriteBytes)
		t.Logf("Total read operations:     %d", totalReadOps)
		t.Logf("Total write operations:    %d", totalWriteOps)
		t.Logf("Records with close msg:    %d (%.1f%%)", withCloseMsg,
			float64(withCloseMsg)/float64(stats.EmittedRecords)*100)

		if recordsWithLatency > 0 {
			avgLatency := float64(totalLatency) / float64(recordsWithLatency)
			t.Logf("Average operation latency: %.2f ms", avgLatency)
		}
	}

	// Basic assertions to ensure the test is meaningful
	if stats.TotalPackets == 0 {
		t.Error("Expected to read at least some packets")
	}

	if stats.ParsedOK == 0 {
		t.Error("Expected at least some packets to parse successfully")
	}

	// Show parsing success rate
	successRate := float64(stats.ParsedOK) / float64(stats.TotalPackets) * 100
	t.Logf("\nParsing success rate: %.2f%%", successRate)
}

// TestMessagesFileFirstPacket decodes and inspects the first packet in detail
func TestMessagesFileFirstPacket(t *testing.T) {
	messagesPath := "../tests/messages"

	// Check if file exists
	if _, err := os.Stat(messagesPath); os.IsNotExist(err) {
		t.Skipf("Skipping test: %s does not exist", messagesPath)
	}

	// Read first line manually
	f, err := os.Open(messagesPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	// Read first line
	var line struct {
		Remote  string `json:"remote"`
		Version string `json:"version"`
		Data    string `json:"data"`
	}

	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&line); err != nil {
		t.Fatalf("Failed to decode first line: %v", err)
	}

	t.Logf("First packet metadata:")
	t.Logf("  Remote:  %s", line.Remote)
	t.Logf("  Version: %s", line.Version)
	t.Logf("  Data:    %s... (%d chars)", line.Data[:min(50, len(line.Data))], len(line.Data))

	// Decode base64
	data, err := base64.StdEncoding.DecodeString(line.Data)
	if err != nil {
		t.Fatalf("Failed to decode base64: %v", err)
	}

	t.Logf("\nDecoded packet: %d bytes", len(data))
	t.Logf("First 20 bytes (hex): % x", data[:min(20, len(data))])

	// Parse the packet
	packet, err := parser.ParsePacket(data)
	if err != nil {
		t.Fatalf("Failed to parse packet: %v", err)
	}

	t.Logf("\nParsed packet structure:")
	t.Logf("  Packet type:      %c", packet.PacketType)
	t.Logf("  Is XML:           %v", packet.IsXML)
	t.Logf("  Has Map record:   %v", packet.MapRecord != nil)
	t.Logf("  Has User record:  %v", packet.UserRecord != nil)
	t.Logf("  File records:     %d", len(packet.FileRecords))

	// Show map record
	if packet.MapRecord != nil {
		t.Log("\nMap record details:")
		t.Logf("  DictId: %d", packet.MapRecord.DictId)
		t.Logf("  Info:   %s", string(packet.MapRecord.Info))
	}

	// Show user record
	if packet.UserRecord != nil {
		t.Log("\nUser record details:")
		t.Logf("  DictId:   %d", packet.UserRecord.DictId)
		t.Logf("  Username: %s", packet.UserRecord.UserInfo.Username)
		t.Logf("  Host:     %s", packet.UserRecord.UserInfo.Host)
		t.Logf("  Protocol: %s", packet.UserRecord.AuthInfo.AuthProtocol)
	}

	// Show file records
	for i, rec := range packet.FileRecords {
		t.Logf("\nFile record %d:", i)
		switch fr := rec.(type) {
		case parser.FileOpenRecord:
			t.Logf("  Type:      File Open")
			t.Logf("  FileID:    %d", fr.Header.FileId)
			t.Logf("  UserID:    %d", fr.Header.UserId)
			t.Logf("  FileSize:  %d", fr.FileSize)
			if len(fr.Lfn) > 0 {
				t.Logf("  Filename:  %s", string(fr.Lfn))
			}
		case parser.FileCloseRecord:
			t.Logf("  Type:      File Close")
			t.Logf("  FileID:    %d", fr.Header.FileId)
			t.Logf("  UserID:    %d", fr.Header.UserId)
			t.Logf("  Read:      %d bytes", fr.Xfr.Read)
			t.Logf("  Readv:     %d bytes", fr.Xfr.Readv)
			t.Logf("  Write:     %d bytes", fr.Xfr.Write)
			t.Logf("  Read ops:  %d", fr.Ops.Read)
			t.Logf("  Readv ops: %d", fr.Ops.Readv)
			t.Logf("  Write ops: %d", fr.Ops.Write)
		case parser.FileTimeRecord:
			t.Logf("  Type:      File Time")
			t.Logf("  FileID:    %d", fr.Header.FileId)
			t.Logf("  TBeg:      %d", fr.TBeg)
			t.Logf("  TEnd:      %d", fr.TEnd)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
