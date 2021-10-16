package main

import (
	"net"

	queue "github.com/opensciencegrid/xrootd-monitoring-shoveler/queue"
	log "github.com/sirupsen/logrus"
)

var (
	version string
	commit string
	date string
	builtBy string
)
var DEBUG bool = false

func main() {
	// Load the configuration
	config := Config{}
	config.ReadConfig()
	if DEBUG || config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}
	textFormatter := log.TextFormatter{}
	textFormatter.DisableLevelTruncation = true
	textFormatter.FullTimestamp = true
	log.SetFormatter(&textFormatter)

	// Log the version information
	log.Infoln("Starting xrootd-monitoring-shoveler", version, "commit:", commit, "built on:", date)


	// Start the message queue
	q := queue.New()

	// Start the AMQP go func
	go StartAMQP(&config, q)

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

	defer conn.Close()

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

		if config.Verify && !verifyPacket(buf[:rlen]) {
			continue
		}

		msg := packageUdp(buf[:rlen], remote)

		// Send the message to the queue
		log.Debugln("Sending msg:", string(msg))
		q.Insert(msg)

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
