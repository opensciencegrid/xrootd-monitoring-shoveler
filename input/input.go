package input

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/streadway/amqp"
)

// PacketSource is a common interface for packet sources
type PacketSource interface {
	Start() error
	Stop() error
	Packets() <-chan []byte
}

// PacketWithAddr holds a packet and its source address
type PacketWithAddr struct {
	Data       []byte
	RemoteAddr string
}

// UDPListener listens for UDP packets
type UDPListener struct {
	host            string
	port            int
	bufferSize      int
	conn            *net.UDPConn
	packets         chan []byte
	packetsWithAddr chan PacketWithAddr
	stopChan        chan struct{}
}

// NewUDPListener creates a new UDP listener
func NewUDPListener(host string, port int, bufferSize int) *UDPListener {
	return &UDPListener{
		host:            host,
		port:            port,
		bufferSize:      bufferSize,
		packets:         make(chan []byte, 100),
		packetsWithAddr: make(chan PacketWithAddr, 100),
		stopChan:        make(chan struct{}),
	}
}

// PacketsWithAddr returns the channel of received packets with their source addresses
func (u *UDPListener) PacketsWithAddr() <-chan PacketWithAddr {
	return u.packetsWithAddr
}

// Start starts listening for UDP packets
func (u *UDPListener) Start() error {
	addr := net.UDPAddr{
		Port: u.port,
		IP:   net.ParseIP(u.host),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP: %w", err)
	}

	u.conn = conn

	// Set read buffer size
	if u.bufferSize > 0 {
		if err := conn.SetReadBuffer(u.bufferSize); err != nil {
			return fmt.Errorf("failed to set read buffer: %w", err)
		}
	}

	// Start reading in a goroutine
	go u.readLoop()

	return nil
}

// Stop stops the UDP listener
func (u *UDPListener) Stop() error {
	close(u.stopChan)
	if u.conn != nil {
		return u.conn.Close()
	}
	return nil
}

// Packets returns the channel of received packets
func (u *UDPListener) Packets() <-chan []byte {
	return u.packets
}

// readLoop reads UDP packets and sends them to the channel
func (u *UDPListener) readLoop() {
	defer close(u.packets)
	defer close(u.packetsWithAddr)

	buf := make([]byte, 65536)
	for {
		select {
		case <-u.stopChan:
			return
		default:
			n, remoteAddr, err := u.conn.ReadFromUDP(buf)
			if err != nil {
				// Check if we're stopping
				select {
				case <-u.stopChan:
					return
				default:
					// Log error but continue
					continue
				}
			}

			// Make a copy of the data
			data := make([]byte, n)
			copy(data, buf[:n])

			// Get remote address as string
			var addrStr string
			if remoteAddr != nil {
				addrStr = remoteAddr.String()
			} else {
				addrStr = "unknown:0"
			}

			// Send to both channels (non-blocking)
			select {
			case u.packets <- data:
			case <-u.stopChan:
				return
			default:
				// Channel full, drop packet
			}

			select {
			case u.packetsWithAddr <- PacketWithAddr{Data: data, RemoteAddr: addrStr}:
			case <-u.stopChan:
				return
			default:
				// Channel full, drop packet
			}
		}
	}
}

// MessageBusConsumer consumes messages from a message bus
type MessageBusConsumer interface {
	Start() error
	Stop() error
	Packets() <-chan []byte
}

// RabbitMQConsumer consumes messages from RabbitMQ/AMQP
type RabbitMQConsumer struct {
	brokerURL     string
	exchange      string
	queue         string
	base64Encoded bool
	conn          *amqp.Connection
	channel       *amqp.Channel
	packets       chan []byte
	stopChan      chan struct{}
}

// NewRabbitMQConsumer creates a new RabbitMQ consumer
func NewRabbitMQConsumer(brokerURL, exchange, queue string, base64Encoded bool) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		brokerURL:     brokerURL,
		exchange:      exchange,
		queue:         queue,
		base64Encoded: base64Encoded,
		packets:       make(chan []byte, 100),
		stopChan:      make(chan struct{}),
	}
}

// Start starts consuming from RabbitMQ
func (r *RabbitMQConsumer) Start() error {
	conn, err := amqp.Dial(r.brokerURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	r.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	r.channel = ch

	// Declare queue if needed
	if r.queue != "" {
		_, err = ch.QueueDeclare(
			r.queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare queue: %w", err)
		}
	}

	// Start consuming
	msgs, err := ch.Consume(
		r.queue, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	// Start processing in a goroutine
	go r.processMessages(msgs)

	return nil
}

// Stop stops the RabbitMQ consumer
func (r *RabbitMQConsumer) Stop() error {
	close(r.stopChan)

	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}

	return nil
}

// Packets returns the channel of received packets
func (r *RabbitMQConsumer) Packets() <-chan []byte {
	return r.packets
}

// processMessages processes messages from RabbitMQ
func (r *RabbitMQConsumer) processMessages(msgs <-chan amqp.Delivery) {
	defer close(r.packets)

	for {
		select {
		case <-r.stopChan:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			var data []byte
			var err error

			if r.base64Encoded {
				// Decode base64
				data, err = base64.StdEncoding.DecodeString(string(msg.Body))
				if err != nil {
					// Log error but continue
					continue
				}
			} else {
				data = msg.Body
			}

			// Send to channel (non-blocking)
			select {
			case r.packets <- data:
			case <-r.stopChan:
				return
			default:
				// Channel full, drop packet
			}
		}
	}
}

// FileReader reads newline-delimited JSON objects from a file. Each line must be a
// JSON object with at least the fields: remote, version, data. The data field is
// base64 encoded binary packet data by default (can be disabled per constructor).
type FileReader struct {
	path          string
	base64Encoded bool
	follow        bool // If true, wait for new lines to appear (tail -f behavior)
	file          *os.File
	packets       chan []byte
	stopChan      chan struct{}
}

// jsonLine represents the expected JSON structure per-line in the input file.
type jsonLine struct {
	Remote  string `json:"remote"`
	Version string `json:"version"`
	Data    string `json:"data"`
}

// NewFileReader creates a new FileReader. If base64Encoded is true, the
// "data" field will be base64 decoded before being emitted on the Packets()
// channel. Follow is set to false (stops at EOF).
func NewFileReader(path string, base64Encoded bool) *FileReader {
	return &FileReader{
		path:          path,
		base64Encoded: base64Encoded,
		follow:        false,
		packets:       make(chan []byte, 100),
		stopChan:      make(chan struct{}),
	}
}

// NewFileReaderWithFollow creates a new FileReader with follow mode enabled.
// If follow is true, the reader will wait for new lines to appear (tail -f behavior).
func NewFileReaderWithFollow(path string, base64Encoded bool, follow bool) *FileReader {
	return &FileReader{
		path:          path,
		base64Encoded: base64Encoded,
		follow:        follow,
		packets:       make(chan []byte, 100),
		stopChan:      make(chan struct{}),
	}
}

// Start opens the file and begins streaming parsed packets to the Packets()
// channel. It returns an error if the file cannot be opened.
func (f *FileReader) Start() error {
	file, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", f.path, err)
	}
	f.file = file

	go f.readLoop()

	return nil
}

// Stop closes the file and signals the read loop to stop.
func (f *FileReader) Stop() error {
	close(f.stopChan)
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

// Packets returns the channel that emits decoded packet bytes.
func (f *FileReader) Packets() <-chan []byte {
	return f.packets
}

// readLoop reads the file line by line, parses JSON, decodes the data field,
// and emits the binary packet bytes onto the packets channel. If follow mode is
// enabled, it will wait for new lines to appear at the end of the file instead
// of stopping at EOF.
func (f *FileReader) readLoop() {
	defer close(f.packets)

	// Use a buffered scanner to iterate lines
	reader := bufio.NewReader(f.file)
	scanner := bufio.NewScanner(reader)
	// Allow larger lines (up to 10MB)
	buf := make([]byte, 0, 1024*64)
	scanner.Buffer(buf, 10*1024*1024)

	for {
		select {
		case <-f.stopChan:
			return
		default:
			// fallthrough
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil && err != io.EOF {
				// scanning error, stop
				return
			}
			// Reached EOF
			if !f.follow {
				// Not in follow mode, exit
				return
			}
			// In follow mode, wait a bit and check for new content
			select {
			case <-f.stopChan:
				return
			case <-time.After(100 * time.Millisecond):
				// Retry scanning
				continue
			}
		}

		line := scanner.Bytes()
		var jl jsonLine
		if err := json.Unmarshal(line, &jl); err != nil {
			// skip malformed lines
			continue
		}

		var data []byte
		if f.base64Encoded {
			d, err := base64.StdEncoding.DecodeString(jl.Data)
			if err != nil {
				// skip lines with bad base64
				continue
			}
			data = d
		} else {
			data = []byte(jl.Data)
		}

		// Emit to channel (non-blocking)
		select {
		case f.packets <- data:
		case <-f.stopChan:
			return
		default:
			// Channel full, drop packet
		}
	}
}
