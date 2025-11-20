package input

import (
	"encoding/base64"
	"fmt"
	"net"

	"github.com/streadway/amqp"
)

// PacketSource is a common interface for packet sources
type PacketSource interface {
	Start() error
	Stop() error
	Packets() <-chan []byte
}

// UDPListener listens for UDP packets
type UDPListener struct {
	host       string
	port       int
	bufferSize int
	conn       *net.UDPConn
	packets    chan []byte
	stopChan   chan struct{}
}

// NewUDPListener creates a new UDP listener
func NewUDPListener(host string, port int, bufferSize int) *UDPListener {
	return &UDPListener{
		host:       host,
		port:       port,
		bufferSize: bufferSize,
		packets:    make(chan []byte, 100),
		stopChan:   make(chan struct{}),
	}
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

	buf := make([]byte, 65536)
	for {
		select {
		case <-u.stopChan:
			return
		default:
			n, _, err := u.conn.ReadFromUDP(buf)
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

			// Send to channel (non-blocking)
			select {
			case u.packets <- data:
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
