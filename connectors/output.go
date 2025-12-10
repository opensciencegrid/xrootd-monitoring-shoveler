package connectors

import (
	"github.com/sirupsen/logrus"
)

// OutputConnector is the interface for all output connectors
type OutputConnector interface {
	// Write writes data to the output destination
	Write(data []byte) error

	// WriteToExchange writes data to a specific exchange (for MQ connectors)
	// For non-MQ connectors, exchange parameter is ignored
	WriteToExchange(data []byte, exchange string) error

	// Close closes the output connector
	Close() error

	// Sync flushes any buffered data (optional, can be no-op)
	Sync() error
}

// MultiOutputConnector combines multiple output connectors
type MultiOutputConnector struct {
	connectors []OutputConnector
	logger     *logrus.Logger
}

// NewMultiOutputConnector creates a new multi-output connector
func NewMultiOutputConnector(connectors []OutputConnector, logger *logrus.Logger) *MultiOutputConnector {
	if logger == nil {
		logger = logrus.New()
	}
	return &MultiOutputConnector{
		connectors: connectors,
		logger:     logger,
	}
}

// Write writes data to all configured connectors
func (m *MultiOutputConnector) Write(data []byte) error {
	return m.WriteToExchange(data, "")
}

// WriteToExchange writes data to all configured connectors with optional exchange
func (m *MultiOutputConnector) WriteToExchange(data []byte, exchange string) error {
	var lastErr error
	for _, connector := range m.connectors {
		if err := connector.WriteToExchange(data, exchange); err != nil {
			m.logger.Errorln("Failed to write to output connector:", err)
			lastErr = err
		}
	}
	return lastErr
}

// Close closes all configured connectors
func (m *MultiOutputConnector) Close() error {
	var lastErr error
	for _, connector := range m.connectors {
		if err := connector.Close(); err != nil {
			m.logger.Errorln("Failed to close output connector:", err)
			lastErr = err
		}
	}
	return lastErr
}

// Sync syncs all configured connectors
func (m *MultiOutputConnector) Sync() error {
	var lastErr error
	for _, connector := range m.connectors {
		if err := connector.Sync(); err != nil {
			m.logger.Errorln("Failed to sync output connector:", err)
			lastErr = err
		}
	}
	return lastErr
}
