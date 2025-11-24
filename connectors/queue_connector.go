package connectors

import (
	shoveler "github.com/opensciencegrid/xrootd-monitoring-shoveler"
)

// QueueConnector writes output to a confirmation queue (RabbitMQ/STOMP)
type QueueConnector struct {
	queue *shoveler.ConfirmationQueue
}

// NewQueueConnector creates a new queue output connector
func NewQueueConnector(queue *shoveler.ConfirmationQueue) *QueueConnector {
	return &QueueConnector{
		queue: queue,
	}
}

// Write writes data to the queue with default exchange
func (qc *QueueConnector) Write(data []byte) error {
	qc.queue.Enqueue(data)
	return nil
}

// WriteToExchange writes data to the queue with a specific exchange
func (qc *QueueConnector) WriteToExchange(data []byte, exchange string) error {
	qc.queue.EnqueueToExchange(data, exchange)
	return nil
}

// Close closes the queue
func (qc *QueueConnector) Close() error {
	return qc.queue.Close()
}

// Sync is a no-op for queue connector
func (qc *QueueConnector) Sync() error {
	return nil
}
