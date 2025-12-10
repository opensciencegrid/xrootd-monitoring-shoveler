package connectors

import "context"

// Input interface for receiving messages from any source (e.g., RabbitMQ, file)
type Input interface {
	Next(ctx context.Context) ([]byte, error)
	Close() error
}

// Output interface for sending messages to any destination (e.g., RabbitMQ, file)
type Output interface {
	Send(data []byte) error
	Close() error
}
