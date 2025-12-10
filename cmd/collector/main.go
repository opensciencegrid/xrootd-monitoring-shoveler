package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/collector"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/connectors"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/input"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
)

var (
	version string
	commit  string
	date    string
	builtBy string
)

func main() {
	// Parse command-line flags
	configPath := flag.String("c", "", "path to configuration file")
	flag.StringVar(configPath, "config", "", "path to configuration file (alias for -c)")
	flag.Parse()

	shoveler.ShovelerVersion = version
	shoveler.ShovelerCommit = commit
	shoveler.ShovelerDate = date
	shoveler.ShovelerBuiltBy = builtBy

	logger := logrus.New()
	textFormatter := logrus.TextFormatter{}
	textFormatter.DisableLevelTruncation = true
	textFormatter.FullTimestamp = true
	logger.SetFormatter(&textFormatter)
	logrus.SetFormatter(&textFormatter)

	// Load the configuration
	config := shoveler.Config{}
	config.ReadConfigWithPathAndPrefix(*configPath, "COLLECTOR")

	if config.Debug {
		logger.SetLevel(logrus.DebugLevel)
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.WarnLevel)
		logrus.SetLevel(logrus.WarnLevel)
	}

	// Set the logger after the level is configured
	shoveler.SetLogger(logger)

	// Log the version information
	logrus.Infoln("Starting xrootd-monitoring-collector", version, "commit:", commit, "built on:", date, "built by:", builtBy)
	logrus.Infoln("Mode: collector (forced)")
	logrus.Debugln("Queue directory:", config.QueueDir)

	// Initialize output connectors
	var outputConnectors []connectors.OutputConnector

	// Initialize message queue if needed
	var cq *shoveler.ConfirmationQueue
	if config.Output.Type == "" || config.Output.Type == "mq" || config.Output.Type == "both" {
		cq = shoveler.NewConfirmationQueue(&config)
		if config.MQ == "amqp" {
			// Only start AMQP if URL is configured
			if config.AmqpURL != nil && config.AmqpURL.String() != "" {
				// Start the AMQP go func
				go shoveler.StartAMQP(&config, cq)
			} else {
				logger.Warnln("Output type is 'mq' with AMQP but no amqp.url configured - skipping AMQP output")
			}
		} else if config.MQ == "stomp" {
			// Start the STOMP go func
			go shoveler.StartStomp(&config, cq)
		}
		queueConnector := connectors.NewQueueConnector(cq)
		outputConnectors = append(outputConnectors, queueConnector)
	}

	// Initialize file writer if needed
	if config.Output.Type == "file" || config.Output.Type == "both" {
		if config.Output.Path == "" {
			logger.Fatalln("Output type is 'file' or 'both' but no output.path configured")
		}
		fileConnector, err := connectors.NewFileConnector(config.Output.Path, logger)
		if err != nil {
			logger.Fatalln("Failed to create file connector:", err)
		}
		outputConnectors = append(outputConnectors, fileConnector)
	}

	// Create multi-output connector
	output := connectors.NewMultiOutputConnector(outputConnectors, logger)
	defer func() {
		if err := output.Close(); err != nil {
			logger.Errorln("Failed to close output connectors:", err)
		}
	}()

	// Start the metrics
	if config.Metrics {
		shoveler.StartMetrics(config.MetricsPort)
	}

	// Start pprof profiling if enabled
	if config.Profile {
		shoveler.StartProfile(config.ProfilePort)
	}

	// Always run in collector mode
	runCollectorMode(&config, output, logger)
}

// emitRecord handles outputting a record to the configured destinations
func emitRecord(recordJSON []byte, output connectors.OutputConnector, logger *logrus.Logger) {
	if err := output.Write(recordJSON); err != nil {
		logger.Errorln("Failed to write record:", err)
	}
}

// emitWLCGRecord handles outputting a WLCG-formatted record to the WLCG exchange
func emitWLCGRecord(recordJSON []byte, config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	if err := output.WriteToExchange(recordJSON, config.AmqpExchangeWLCG); err != nil {
		logger.Errorln("Failed to write WLCG record:", err)
	}
}

// emitGStreamEvent handles outputting a gstream event to the appropriate exchange
func emitGStreamEvent(eventJSON []byte, streamType byte, config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	// Determine exchange based on stream type
	var exchange string
	switch streamType {
	case 'C': // Cache events
		exchange = config.AmqpExchangeCache
	case 'T': // TCP events
		exchange = config.AmqpExchangeTCP
	case 'P': // TPC events
		exchange = config.AmqpExchangeTPC
	default:
		logger.Warnf("Unknown gstream type: %c (0x%02x), using default exchange", streamType, streamType)
		exchange = config.AmqpExchange
	}

	if err := output.WriteToExchange(eventJSON, exchange); err != nil {
		logger.Errorln("Failed to write gstream event:", err)
	}
}

// runCollectorMode runs the collector mode with full packet parsing and correlation
func runCollectorMode(config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	// Support UDP, file, and RabbitMQ inputs
	switch config.Input.Type {
	case "file":
		runCollectorModeFile(config, output, logger)
	case "rabbitmq", "amqp":
		runCollectorModeRabbitMQ(config, output, logger)
	default:
		// Default to UDP
		runCollectorModeUDP(config, output, logger)
	}
}

// handleParsedPacket processes a parsed packet (gstream or regular correlation)
func handleParsedPacket(packet *parser.Packet, correlator *collector.Correlator, config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	// Debug: Print packet details
	if logger.Level == logrus.DebugLevel && packet != nil {
		serverID := fmt.Sprintf("%d#%s", packet.Header.ServerStart, packet.RemoteAddr)
		logger.Debugf("Parsed packet from %s (ServerID: %s) - Type: %c, IsXML: %v, MapRecord: %v, UserRecord: %v, FileRecords: %d",
			packet.RemoteAddr, serverID, packet.PacketType, packet.IsXML, packet.MapRecord != nil, packet.UserRecord != nil, len(packet.FileRecords))
		if packet.MapRecord != nil {
			logger.Debugf("  MapRecord - DictId: %d, Info: %s", packet.MapRecord.DictId, packet.MapRecord.Info)
		}
		if packet.UserRecord != nil {
			logger.Debugf("  UserRecord - DictId: %d, Username: %s, Protocol: %s, Host: %s",
				packet.UserRecord.DictId, packet.UserRecord.UserInfo.Username,
				packet.UserRecord.UserInfo.Protocol, packet.UserRecord.UserInfo.Host)
			if packet.UserRecord.AuthInfo.DN != "" || packet.UserRecord.AuthInfo.Org != "" {
				logger.Debugf("    AuthInfo - DN: %s, Org: %s, Role: %s, Groups: %s",
					packet.UserRecord.AuthInfo.DN, packet.UserRecord.AuthInfo.Org,
					packet.UserRecord.AuthInfo.Role, packet.UserRecord.AuthInfo.Groups)
			}
			if packet.UserRecord.TokenInfo.Subject != "" || packet.UserRecord.TokenInfo.UserDictID != 0 {
				logger.Debugf("    TokenInfo - UserDictID: %d, Subject: %s, Username: %s, Org: %s, Role: %s, Groups: %s",
					packet.UserRecord.TokenInfo.UserDictID, packet.UserRecord.TokenInfo.Subject,
					packet.UserRecord.TokenInfo.Username, packet.UserRecord.TokenInfo.Org,
					packet.UserRecord.TokenInfo.Role, packet.UserRecord.TokenInfo.Groups)
			}
		}
		for i, rec := range packet.FileRecords {
			switch r := rec.(type) {
			case parser.FileOpenRecord:
				logger.Debugf("  FileRecord[%d] - Open: FileId=%d, User=%d, Lfn=%s", i, r.Header.FileId, r.User, string(r.Lfn))
			case parser.FileCloseRecord:
				logger.Debugf("  FileRecord[%d] - Close: FileId=%d, Read=%d, Write=%d", i, r.Header.FileId, r.Xfr.Read, r.Xfr.Write)
			case parser.FileTimeRecord:
				logger.Debugf("  FileRecord[%d] - Time: TBeg=%d, TEnd=%d", i, r.TBeg, r.TEnd)
			}
		}
	}

	// Check for gstream packets first - they bypass correlation
	if packet != nil && packet.GStreamRecord != nil {
		events, streamType, err := correlator.ProcessGStreamPacket(packet)
		if err != nil {
			logger.Errorln("Failed to process gstream packet:", err)
			return
		}

		// Emit each gstream event to the appropriate exchange
		for _, event := range events {
			eventJSON, err := json.Marshal(event)
			if err != nil {
				logger.Errorln("Failed to marshal gstream event:", err)
				continue
			}

			logger.Debugln("Emitting gstream event:", string(eventJSON))
			emitGStreamEvent(eventJSON, streamType, config, output, logger)
		}
		return
	}

	// Process packet through correlator
	records, err := correlator.ProcessPacket(packet)
	if err != nil {
		logger.Errorln("Failed to process packet:", err)
		return
	}

	// If we got complete records, emit them
	for _, record := range records {
		shoveler.RecordsEmitted.Inc()

		// Calculate latency if we have timing info
		if record.StartTime > 0 && record.EndTime > 0 {
			latency := record.EndTime - record.StartTime
			shoveler.RequestLatencyMs.Observe(float64(latency))
		}

		// Check if this should be converted to WLCG format
		if collector.IsWLCGPacket(record) {
			logger.Debugln("Converting record to WLCG format")
			wlcgRecord, err := collector.ConvertToWLCG(record)
			if err != nil {
				logger.Errorln("Failed to convert to WLCG format:", err)
				continue
			}

			wlcgJSON, err := wlcgRecord.ToJSON()
			if err != nil {
				logger.Errorln("Failed to marshal WLCG record:", err)
				continue
			}

			logger.Debugln("Emitting WLCG record:", string(wlcgJSON))
			emitWLCGRecord(wlcgJSON, config, output, logger)
		} else {
			// Convert to JSON and enqueue (normal path)
			recordJSON, err := record.ToJSON()
			if err != nil {
				logger.Errorln("Failed to marshal record:", err)
				continue
			}

			logger.Debugln("Emitting collector record:", string(recordJSON))
			emitRecord(recordJSON, output, logger)
		}
	}
}

// processPackets is the common packet processing loop for all input types
func processPackets(source input.PacketSource, correlator *collector.Correlator, config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	for pktWithAddr := range source.PacketsWithAddr() {
		shoveler.PacketsReceived.Inc()

		// Parse packet
		startParse := time.Now()
		packet, err := parser.ParsePacket(pktWithAddr.Data)
		parseTime := time.Since(startParse).Milliseconds()
		shoveler.ParseTimeMs.Observe(float64(parseTime))

		if err != nil {
			shoveler.ParseErrors.WithLabelValues(fmt.Sprintf("%v", err)).Inc()
			logger.Debugln("Failed to parse packet:", err)
			continue
		}
		shoveler.PacketsParsedOK.Inc()

		// Set remote address for server ID calculation
		if packet != nil {
			packet.RemoteAddr = pktWithAddr.RemoteAddr
		}

		// Handle the parsed packet
		handleParsedPacket(packet, correlator, config, output, logger)
	}
}

// runCollectorModeFile processes packets from a file in collector mode
func runCollectorModeFile(config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	// Create correlator
	ttl := time.Duration(config.State.EntryTTL) * time.Second
	correlator := collector.NewCorrelator(ttl, config.State.MaxEntries, config.State.DisableReverseDNS, logger)
	defer correlator.Stop()

	// Update state size metric periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			shoveler.StateSize.Set(float64(correlator.GetStateSize()))
		}
	}()

	// Create file reader
	fr := input.NewFileReaderWithFollow(config.Input.Path, config.Input.Base64Encoded, config.Input.Follow)
	if err := fr.Start(); err != nil {
		logger.Fatalln("Failed to start file reader:", err)
	}
	defer func() {
		if err := fr.Stop(); err != nil {
			logger.Errorln("Failed to stop file reader:", err)
		}
	}()

	logger.Infoln("Collector mode: Reading packets from file:", config.Input.Path, "Follow:", config.Input.Follow)

	// Process packets using common logic
	processPackets(fr, correlator, config, output, logger)
}

// runCollectorModeUDP processes packets from UDP in collector mode
func runCollectorModeUDP(config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) {
	// Create correlator
	ttl := time.Duration(config.State.EntryTTL) * time.Second
	correlator := collector.NewCorrelator(ttl, config.State.MaxEntries, config.State.DisableReverseDNS, logger)
	defer correlator.Stop()

	// Update state size metric periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			shoveler.StateSize.Set(float64(correlator.GetStateSize()))
		}
	}()

	// Create UDP listener
	udpListener := input.NewUDPListener(config.ListenIp, config.ListenPort, 1024*1024)
	if err := udpListener.Start(); err != nil {
		logger.Fatalln("Failed to start UDP listener:", err)
	}
	defer func() {
		if err := udpListener.Stop(); err != nil {
			logger.Errorln("Failed to stop UDP listener:", err)
		}
	}()

	logger.Infoln("Collector mode: Listening for UDP messages at:", net.JoinHostPort(config.ListenIp, fmt.Sprintf("%d", config.ListenPort)))

	// Process packets using common logic
	processPackets(udpListener, correlator, config, output, logger)
}

// runCollectorModeRabbitMQ processes packets from RabbitMQ in collector mode
func runCollectorModeRabbitMQ(config *shoveler.Config, output connectors.OutputConnector, logger *logrus.Logger) error {
	// Create correlator
	ttl := time.Duration(config.State.EntryTTL) * time.Second
	correlator := collector.NewCorrelator(ttl, config.State.MaxEntries, config.State.DisableReverseDNS, logger)
	defer correlator.Stop()

	// Update state size metric periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			shoveler.StateSize.Set(float64(correlator.GetStateSize()))
		}
	}()

	// Create RabbitMQ reader
	// Use config values for connection details
	brokerURL := config.Input.BrokerURL
	if brokerURL == "" && config.AmqpURL != nil {
		brokerURL = config.AmqpURL.String()
	}

	queueName := config.Input.Topic
	if queueName == "" {
		queueName = "xrootd.monitoring"
	}

	exchange := ""    // Can be added to config if needed
	routingKey := "#" // Can be added to config if needed
	tokenPath := config.AmqpToken

	reader := input.NewRabbitMQReader(brokerURL, queueName, exchange, routingKey, tokenPath, logger)
	if err := reader.Start(); err != nil {
		logger.Fatalln("Failed to start RabbitMQ reader:", err)
	}
	defer func() {
		if err := reader.Stop(); err != nil {
			logger.Errorln("Failed to stop RabbitMQ reader:", err)
		}
	}()

	logger.Infoln("Collector mode: Reading JSON messages from RabbitMQ queue:", queueName)

	// Process packets using common logic
	processPackets(reader, correlator, config, output, logger)
	logger.Infoln("RabbitMQ reader stopped")
	return nil
}
