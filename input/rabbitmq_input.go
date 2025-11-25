package input

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// RabbitMQMessage represents the JSON structure from the message bus
type RabbitMQMessage struct {
	Remote  string `json:"remote"`
	Version string `json:"version"`
	Data    string `json:"data"` // Base64-encoded packet data
}

// RabbitMQReader reads JSON-encoded XRootD monitoring packets from RabbitMQ
type RabbitMQReader struct {
	brokerURL          string
	queueName          string
	exchange           string
	routingKey         string
	token              string
	tokenPath          string
	conn               *amqp.Connection
	channel            *amqp.Channel
	packetsWithAddr    chan PacketWithAddr
	stop               chan struct{}
	reconnectDelay     time.Duration
	logger             *logrus.Logger
	unackedCount       int
	lastAckTime        time.Time
	lastAckDeliveryTag uint64
}

// NewRabbitMQReader creates a new RabbitMQ reader
func NewRabbitMQReader(brokerURL, queueName, exchange, routingKey, tokenPath string, logger *logrus.Logger) *RabbitMQReader {
	if logger == nil {
		logger = logrus.New()
	}

	return &RabbitMQReader{
		brokerURL:       brokerURL,
		queueName:       queueName,
		exchange:        exchange,
		routingKey:      routingKey,
		tokenPath:       tokenPath,
		packetsWithAddr: make(chan PacketWithAddr, 100),
		stop:            make(chan struct{}),
		reconnectDelay:  5 * time.Second,
		logger:          logger,
	}
}

// Start begins reading from RabbitMQ
func (r *RabbitMQReader) Start() error {
	// Check if broker URL already has credentials
	brokerURL, err := url.Parse(r.brokerURL)
	if err != nil {
		return fmt.Errorf("invalid broker URL: %w", err)
	}

	// Only read token if URL doesn't have credentials and token path is provided
	if brokerURL.User == nil && r.tokenPath != "" {
		token, err := r.readToken()
		if err != nil {
			return fmt.Errorf("failed to read token: %w", err)
		}
		r.token = token
	}

	// Start the connection goroutine
	go r.connectionLoop()

	return nil
}

// Stop stops the RabbitMQ reader
func (r *RabbitMQReader) Stop() error {
	close(r.stop)
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			r.logger.Debugln("Error closing RabbitMQ channel:", err)
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			r.logger.Debugln("Error closing RabbitMQ connection:", err)
		}
	}
	close(r.packetsWithAddr)
	return nil
}

// PacketsWithAddr returns the channel for receiving parsed packets with remote addresses
func (r *RabbitMQReader) PacketsWithAddr() <-chan PacketWithAddr {
	return r.packetsWithAddr
}

// readToken reads the authentication token from file
func (r *RabbitMQReader) readToken() (string, error) {
	token, err := os.ReadFile(r.tokenPath)
	if err != nil {
		return "", err
	}
	return string(token), nil
}

// connect establishes connection to RabbitMQ
func (r *RabbitMQReader) connect() error {
	// Parse broker URL
	brokerURL, err := url.Parse(r.brokerURL)
	if err != nil {
		return fmt.Errorf("invalid broker URL: %w", err)
	}

	// Add credentials if token is available and URL doesn't have credentials
	if r.token != "" && brokerURL.User == nil {
		brokerURL.User = url.UserPassword("shoveler", r.token)
	}

	// Connect to RabbitMQ
	r.logger.Infoln("Connecting to RabbitMQ:", brokerURL.Redacted())
	conn, err := amqp.Dial(brokerURL.String())
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	r.conn = conn

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			r.logger.Debugln("Error closing connection:", closeErr)
		}
		return fmt.Errorf("failed to open channel: %w", err)
	}
	r.channel = ch

	// Set QoS to prefetch messages
	err = ch.Qos(
		1000,  // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		if closeErr := ch.Close(); closeErr != nil {
			r.logger.Debugln("Error closing channel:", closeErr)
		}
		if closeErr := conn.Close(); closeErr != nil {
			r.logger.Debugln("Error closing connection:", closeErr)
		}
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Check if queue exists (passive declaration - doesn't create, just verifies)
	if r.queueName != "" {
		_, err = ch.QueueDeclarePassive(
			r.queueName, // name
			true,        // durable
			false,       // delete when unused
			false,       // exclusive
			false,       // no-wait
			nil,         // arguments
		)
		if err != nil {
			// Queue doesn't exist, log a warning but continue
			// (it might be created by another process or exchange binding)
			r.logger.Warnf("Queue %s does not exist or is not accessible: %v", r.queueName, err)
			r.logger.Warn("Attempting to consume anyway - queue may be auto-created by binding")
		}

		// Bind queue to exchange if both are specified
		if r.exchange != "" {
			err = ch.QueueBind(
				r.queueName,  // queue name
				r.routingKey, // routing key
				r.exchange,   // exchange
				false,
				nil,
			)
			if err != nil {
				if closeErr := ch.Close(); closeErr != nil {
					r.logger.Debugln("Error closing channel:", closeErr)
				}
				if closeErr := conn.Close(); closeErr != nil {
					r.logger.Debugln("Error closing connection:", closeErr)
				}
				return fmt.Errorf("failed to bind queue: %w", err)
			}
		}
	}

	r.logger.Infoln("Successfully connected to RabbitMQ")
	return nil
}

// connectionLoop manages the connection and handles reconnection
func (r *RabbitMQReader) connectionLoop() {
	for {
		select {
		case <-r.stop:
			return
		default:
		}

		// Attempt to connect
		err := r.connect()
		if err != nil {
			r.logger.Errorln("Connection failed:", err)
			r.logger.Infof("Retrying in %v...", r.reconnectDelay)
			time.Sleep(r.reconnectDelay)
			continue
		}

		// Start consuming messages
		err = r.consume()
		if err != nil {
			r.logger.Errorln("Consume error:", err)
		}

		// Clean up connection
		if r.channel != nil {
			if closeErr := r.channel.Close(); closeErr != nil {
				r.logger.Debugln("Error closing channel during cleanup:", closeErr)
			}
		}
		if r.conn != nil {
			if closeErr := r.conn.Close(); closeErr != nil {
				r.logger.Debugln("Error closing connection during cleanup:", closeErr)
			}
		}

		// Wait before reconnecting with jitter
		jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
		sleepTime := r.reconnectDelay + jitter
		r.logger.Infof("Reconnecting in %v...", sleepTime)
		time.Sleep(sleepTime)
	}
}

// consume starts consuming messages from the queue
func (r *RabbitMQReader) consume() error {
	// Start consuming
	msgs, err := r.channel.Consume(
		r.queueName, // queue
		"",          // consumer tag
		false,       // auto-ack (we'll manually ack)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	r.logger.Infoln("Started consuming messages from queue:", r.queueName)

	// Reset ack tracking
	r.unackedCount = 0
	r.lastAckTime = time.Now()
	r.lastAckDeliveryTag = 0

	// Create ticker for periodic acks (every second)
	ackTicker := time.NewTicker(1 * time.Second)
	defer ackTicker.Stop()

	// Process messages
	for {
		select {
		case <-r.stop:
			// Ack any remaining messages before stopping
			if r.unackedCount > 0 && r.lastAckDeliveryTag > 0 {
				r.logger.Debugf("Acknowledging final %d messages before stopping", r.unackedCount)
				if ackErr := r.channel.Ack(r.lastAckDeliveryTag, true); ackErr != nil {
					r.logger.Debugln("Failed to ack final messages:", ackErr)
				}
			}
			return nil

		case <-ackTicker.C:
			// Periodic ack: acknowledge if we have unacked messages
			if r.unackedCount > 0 && r.lastAckDeliveryTag > 0 {
				r.logger.Debugf("Periodic ack: acknowledging %d messages", r.unackedCount)
				if ackErr := r.channel.Ack(r.lastAckDeliveryTag, true); ackErr != nil {
					r.logger.Debugln("Failed to ack messages:", ackErr)
				} else {
					r.unackedCount = 0
					r.lastAckTime = time.Now()
				}
			}

		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("message channel closed")
			}

			// Process the message
			err := r.processMessage(msg)
			if err != nil {
				r.logger.Debugln("Failed to process message:", err)

				// Ack all previous messages before nacking this one
				if r.unackedCount > 0 && r.lastAckDeliveryTag > 0 {
					r.logger.Debugf("Acknowledging %d previous messages before nack", r.unackedCount)
					if ackErr := r.channel.Ack(r.lastAckDeliveryTag, true); ackErr != nil {
						r.logger.Debugln("Failed to ack messages before nack:", ackErr)
					}
					r.unackedCount = 0
				}

				// Reject the current message (don't requeue)
				if nackErr := msg.Nack(false, false); nackErr != nil {
					r.logger.Debugln("Failed to Nack message:", nackErr)
				}
				r.lastAckDeliveryTag = msg.DeliveryTag
			} else {
				// Message processed successfully
				r.unackedCount++
				r.lastAckDeliveryTag = msg.DeliveryTag

				// Batch acknowledge every 100 messages
				if r.unackedCount >= 100 {
					r.logger.Debugf("Batch ack: acknowledging %d messages", r.unackedCount)
					if ackErr := r.channel.Ack(r.lastAckDeliveryTag, true); ackErr != nil {
						r.logger.Debugln("Failed to ack messages:", ackErr)
					} else {
						r.unackedCount = 0
						r.lastAckTime = time.Now()
					}
				}
			}
		}
	}
}

// processMessage decodes and forwards a single message
func (r *RabbitMQReader) processMessage(msg amqp.Delivery) error {
	// Parse JSON message
	var rmqMsg RabbitMQMessage
	err := json.Unmarshal(msg.Body, &rmqMsg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Decode base64 data
	packetData, err := base64.StdEncoding.DecodeString(rmqMsg.Data)
	if err != nil {
		return fmt.Errorf("failed to decode base64 data: %w", err)
	}

	// Send packet data with remote address through channel
	select {
	case r.packetsWithAddr <- PacketWithAddr{Data: packetData, RemoteAddr: rmqMsg.Remote}:
	case <-r.stop:
		return nil
	}

	return nil
}
