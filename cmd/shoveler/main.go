package main

import (
	"flag"
	"net"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	"github.com/opensciencegrid/xrootd-monitoring-shoveler/input"
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
	logrus.SetFormatter(&textFormatter)

	shoveler.SetLogger(logger)

	// Load the configuration
	config := shoveler.Config{}
	config.ReadConfigWithPath(*configPath)

	if DEBUG || config.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.WarnLevel)
	}

	// Log the version information
	logrus.Infoln("Starting xrootd-monitoring-shoveler", version, "commit:", commit, "built on:", date, "built by:", builtBy)

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

	// Shoveler always runs in shoveling mode (minimal processing)
	runShovelingMode(&config, cq, logger)
}

// runShovelingMode runs the traditional shoveling mode (minimal processing)
func runShovelingMode(config *shoveler.Config, cq *shoveler.ConfirmationQueue, logger *logrus.Logger) {
	// Support both UDP and file inputs
	if config.Input.Type == "file" {
		runShovelingModeFile(config, cq, logger)
	} else {
		// Default to UDP
		runShovelingModeUDP(config, cq, logger)
	}
}

// runShovelingModeFile processes packets from a file in shoveling mode
func runShovelingModeFile(config *shoveler.Config, cq *shoveler.ConfirmationQueue, logger *logrus.Logger) {
	fr := input.NewFileReaderWithFollow(config.Input.Path, config.Input.Base64Encoded, config.Input.Follow)
	if err := fr.Start(); err != nil {
		logger.Fatalln("Failed to start file reader:", err)
	}
	defer func() {
		if err := fr.Stop(); err != nil {
			logger.Errorln("Failed to stop file reader:", err)
		}
	}()

	logger.Infoln("Shoveling mode: Reading packets from file:", config.Input.Path, "Follow:", config.Input.Follow)

	for pkt := range fr.PacketsWithAddr() {
		shoveler.PacketsReceived.Inc()

		if config.Verify && !shoveler.VerifyPacket(pkt.Data) {
			shoveler.ValidationsFailed.Inc()
			continue
		}

		var remoteAddr *net.UDPAddr
		if pkt.RemoteAddr != "" {
			var err error
			remoteAddr, err = net.ResolveUDPAddr("udp", pkt.RemoteAddr)
			if err != nil {
				logger.Warningln("Failed to parse remote addr:", pkt.RemoteAddr, err)
			}
		}
		msg := shoveler.PackageUdp(pkt.Data, remoteAddr, config)

		logger.Debugln("Sending msg:", string(msg))
		cq.Enqueue(msg)
	}
}

// runShovelingModeUDP processes packets from UDP in shoveling mode
func runShovelingModeUDP(config *shoveler.Config, cq *shoveler.ConfirmationQueue, logger *logrus.Logger) {
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
