package main

import (
	"net"

	queue "github.com/opensciencegrid/xrootd-monitoring-shoveler/queue"
	log "github.com/sirupsen/logrus"
)

var VERSION string

func main() {
	// Load the configuration
	config := Config{}
	config.ReadConfig()
	log.SetLevel(log.WarnLevel)
	textFormatter := log.TextFormatter{}
	textFormatter.DisableLevelTruncation = true
	textFormatter.FullTimestamp = true
	log.SetFormatter(&textFormatter)

	// Start the message queue
	q := queue.New()

	// Start the AMQP go func
	go StartAMQP(&config, q)

	// Process incoming UDP packets
	addr := net.UDPAddr{
		Port: config.UDPPort,
		IP:   net.ParseIP(config.UDPIp),
	}
	conn, err := net.ListenUDP("udp", &addr)

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
				log.Warningln("Unable to parse destination:", dest, "Will not forward UDP packets to this destination")
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
		msg := packageUdp(buf[:rlen], remote)

		// Send the message to the queue
		log.Debugln("Sending msg:", string(msg))
		q.Insert(msg)

		// Send to the UDP destinations
		if len(udpDestinations) > 0 {
			for _, udpConn := range udpDestinations {
				udpConn.Write(msg)
			}
		}

	}
}
