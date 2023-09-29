package main

import (
	"net"

	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
	log "github.com/sirupsen/logrus"
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

	// Load the configuration
	config := shoveler.Config{}
	config.ReadConfig()
	if DEBUG || config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	// Configure the mapper
	shoveler.ConfigureMap()

	textFormatter := log.TextFormatter{}
	textFormatter.DisableLevelTruncation = true
	textFormatter.FullTimestamp = true
	log.SetFormatter(&textFormatter)

	// Log the version information
	log.Infoln("Starting xrootd-monitoring-shoveler", version, "commit:", commit, "built on:", date, "built by:", builtBy)

	// Start the message queue
	cq := shoveler.NewConfirmationQueue()

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

	// Process incoming UDP packets
	addr := net.UDPAddr{
		Port: config.ListenPort,
		IP:   net.ParseIP(config.ListenIp),
	}
	conn, err := net.ListenUDP("udp", &addr)
	log.Debugln("Listening for UDP messages at:", addr.String())

	if err != nil {
		panic(err)
	}

	// Set the read buffer size to 1 MB
	err = conn.SetReadBuffer(1024 * 1024)

	if err != nil {
		log.Warningln("Failed to set read buffer size to 1 MB:", err)
	}

	defer func(conn *net.UDPConn) {
		err := conn.Close()
		if err != nil {
			log.Errorln("Error closing UDP connection:", err)
		}
	}(conn)

	// Create the UDP forwarding destinations
	var udpDestinations []net.Conn
	if len(config.DestUdp) > 0 {
		for _, dest := range config.DestUdp {
			udpConn, err := net.Dial("udp", dest)
			if err != nil {
				log.Warningln("Unable to parse destination:", dest, "Will not forward UDP packets to this destination:", err)
			}
			udpDestinations = append(udpDestinations, udpConn)
			log.Infoln("Adding udp forward destination:", dest)
		}
	}

	var buf [65536]byte
	for {
		rlen, remote, err := conn.ReadFromUDP(buf[:])
		// Do stuff with the read bytes
		if err != nil {
			// output errors
			log.Errorln("Failed to read from UDP connection:", err)
			// If we failed to read from the UDP connection, I'm not
			// sure what to do, maybe just continue as if nothing happened?
			continue
		}
		shoveler.PacketsReceived.Inc()

		if config.Verify && !shoveler.VerifyPacket(buf[:rlen]) {
			shoveler.ValidationsFailed.Inc()
			continue
		}

		// Send to the metrics processor, if enabled
		if config.Metrics {
			shoveler.ProcessMetricsPacket(buf[:rlen])
		}

		msg := shoveler.PackageUdp(buf[:rlen], remote)

		// Send the message to the queue
		log.Debugln("Sending msg:", string(msg))
		cq.Enqueue(msg)

		// Send to the UDP destinations
		if len(udpDestinations) > 0 {
			for _, udpConn := range udpDestinations {
				_, err := udpConn.Write(msg)
				if err != nil {
					log.Errorln("Failed to send message to UDP destination "+udpConn.RemoteAddr().String()+":", err)
				}
			}
		}

	}
}
