package main

import (
	"fmt"
	"net"
	"time"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/collector"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
)

var (
	version string
	commit  string
	date    string
	builtBy string
)
var DEBUG bool = false

func main() {

	shoveler.ShovelerVersion = version
	shoveler.ShovelerCommit = commit
	shoveler.ShovelerDate = date
	shoveler.ShovelerBuiltBy = builtBy

	logger := logrus.New()
	textFormatter := logrus.TextFormatter{}
	textFormatter.DisableLevelTruncation = true
	textFormatter.FullTimestamp = true
	logrus.SetFormatter(&textFormatter)

	shoveler.SetLogger(logger)

	// Load the configuration
	config := shoveler.Config{}
	config.ReadConfig()

	if DEBUG || config.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.WarnLevel)
	}

	// Log the version information
	logrus.Infoln("Starting xrootd-monitoring-shoveler", version, "commit:", commit, "built on:", date, "built by:", builtBy)
	logrus.Infoln("Mode:", config.Mode)

	// Start the message queue
	cq := shoveler.NewConfirmationQueue(&config)

	if config.MQ == "amqp" {
		// Start the AMQP go func
		go shoveler.StartAMQP(&config, cq)
	} else if config.MQ == "stomp" {
		// Start the STOMP go func
		go shoveler.StartStomp(&config, cq)
	}

	// Start the metrics
	if config.Metrics {
		shoveler.StartMetrics(config.MetricsPort)
	}

	// Run based on mode
	if config.Mode == "collector" {
		runCollectorMode(&config, cq, logger)
	} else {
		// Default to shoveling mode for backward compatibility
		runShovelingMode(&config, cq, logger)
	}
}

// runShovelingMode runs the traditional shoveling mode (minimal processing)
func runShovelingMode(config *shoveler.Config, cq *shoveler.ConfirmationQueue, logger *logrus.Logger) {
	// Process incoming UDP packets
	addr := net.UDPAddr{
		Port: config.ListenPort,
		IP:   net.ParseIP(config.ListenIp),
	}
	conn, err := net.ListenUDP("udp", &addr)
	logger.Debugln("Listening for UDP messages at:", addr.String())

	if err != nil {
		panic(err)
	}

	// Set the read buffer size to 1 MB
	err = conn.SetReadBuffer(1024 * 1024)

	if err != nil {
		logger.Warningln("Failed to set read buffer size to 1 MB:", err)
	}

	defer func(conn *net.UDPConn) {
		err := conn.Close()
		if err != nil {
			logger.Errorln("Error closing UDP connection:", err)
		}
	}(conn)

	// Create the UDP forwarding destinations
	var udpDestinations []net.Conn
	if len(config.DestUdp) > 0 {
		for _, dest := range config.DestUdp {
			udpConn, err := net.Dial("udp", dest)
			if err != nil {
				logger.Warningln("Unable to parse destination:", dest, "Will not forward UDP packets to this destination:", err)
			}
			udpDestinations = append(udpDestinations, udpConn)
			logger.Infoln("Adding udp forward destination:", dest)
		}
	}

	var buf [65536]byte
	for {
		rlen, remote, err := conn.ReadFromUDP(buf[:])
		// Do stuff with the read bytes
		if err != nil {
			// output errors
			logger.Errorln("Failed to read from UDP connection:", err)
			// If we failed to read from the UDP connection, I'm not
			// sure what to do, maybe just continue as if nothing happened?
			continue
		}
		shoveler.PacketsReceived.Inc()

		if config.Verify && !shoveler.VerifyPacket(buf[:rlen]) {
			shoveler.ValidationsFailed.Inc()
			continue
		}

		msg := shoveler.PackageUdp(buf[:rlen], remote, config)

		// Send the message to the queue
		logger.Debugln("Sending msg:", string(msg))
		cq.Enqueue(msg)

		// Send to the UDP destinations
		if len(udpDestinations) > 0 {
			for _, udpConn := range udpDestinations {
				_, err := udpConn.Write(msg)
				if err != nil {
					logger.Errorln("Failed to send message to UDP destination "+udpConn.RemoteAddr().String()+":", err)
				}
			}
		}
	}
}

// runCollectorMode runs the collector mode with full packet parsing and correlation
func runCollectorMode(config *shoveler.Config, cq *shoveler.ConfirmationQueue, logger *logrus.Logger) {
	// Create correlator
	ttl := time.Duration(config.State.EntryTTL) * time.Second
	correlator := collector.NewCorrelator(ttl, config.State.MaxEntries)
	defer correlator.Stop()

	// Update state size metric periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			shoveler.StateSize.Set(float64(correlator.GetStateSize()))
		}
	}()

	// Process incoming UDP packets
	addr := net.UDPAddr{
		Port: config.ListenPort,
		IP:   net.ParseIP(config.ListenIp),
	}
	conn, err := net.ListenUDP("udp", &addr)
	logger.Infoln("Collector mode: Listening for UDP messages at:", addr.String())

	if err != nil {
		panic(err)
	}

	// Set the read buffer size to 1 MB
	err = conn.SetReadBuffer(1024 * 1024)
	if err != nil {
		logger.Warningln("Failed to set read buffer size to 1 MB:", err)
	}

	defer func(conn *net.UDPConn) {
		err := conn.Close()
		if err != nil {
			logger.Errorln("Error closing UDP connection:", err)
		}
	}(conn)

	var buf [65536]byte
	for {
		rlen, _, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			logger.Errorln("Failed to read from UDP connection:", err)
			continue
		}
		shoveler.PacketsReceived.Inc()

		// Parse packet
		startParse := time.Now()
		packet, err := parser.ParsePacket(buf[:rlen])
		parseTime := time.Since(startParse).Milliseconds()
		shoveler.ParseTimeMs.Observe(float64(parseTime))

		if err != nil {
			shoveler.ParseErrors.WithLabelValues(fmt.Sprintf("%v", err)).Inc()
			logger.Debugln("Failed to parse packet:", err)
			continue
		}
		shoveler.PacketsParsedOK.Inc()

		// Process packet through correlator
		record, err := correlator.ProcessPacket(packet)
		if err != nil {
			logger.Errorln("Failed to process packet:", err)
			continue
		}

		// If we got a complete record, emit it
		if record != nil {
			shoveler.RecordsEmitted.Inc()
			
			// Calculate latency if we have timing info
			if record.StartTime > 0 && record.EndTime > 0 {
				latency := record.EndTime - record.StartTime
				shoveler.RequestLatencyMs.Observe(float64(latency))
			}

			// Convert to JSON and enqueue
			recordJSON, err := record.ToJSON()
			if err != nil {
				logger.Errorln("Failed to marshal record:", err)
				continue
			}

			logger.Debugln("Emitting collector record:", string(recordJSON))
			cq.Enqueue(recordJSON)
		}
	}
}
