package input

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestNewRabbitMQReader(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Suppress logs during tests

	reader := NewRabbitMQReader(
		"amqp://localhost:5672",
		"test-queue",
		"",
		"",
		"",
		logger,
	)

	if reader == nil {
		t.Fatal("NewRabbitMQReader returned nil")
	}

	if reader.brokerURL != "amqp://localhost:5672" {
		t.Errorf("Expected broker URL 'amqp://localhost:5672', got '%s'", reader.brokerURL)
	}

	if reader.queueName != "test-queue" {
		t.Errorf("Expected queue name 'test-queue', got '%s'", reader.queueName)
	}

	if reader.reconnectDelay != 5*time.Second {
		t.Errorf("Expected reconnect delay 5s, got %v", reader.reconnectDelay)
	}
}

func TestRabbitMQMessage(t *testing.T) {
	// Test that the RabbitMQMessage struct can be created
	msg := RabbitMQMessage{
		Remote:  "127.0.0.1:9930",
		Version: "0.1.3",
		Data:    "dGVzdCBkYXRh", // "test data" in base64
	}

	if msg.Remote != "127.0.0.1:9930" {
		t.Errorf("Expected remote '127.0.0.1:9930', got '%s'", msg.Remote)
	}

	if msg.Version != "0.1.3" {
		t.Errorf("Expected version '0.1.3', got '%s'", msg.Version)
	}

	if msg.Data != "dGVzdCBkYXRh" {
		t.Errorf("Expected data 'dGVzdCBkYXRh', got '%s'", msg.Data)
	}
}
