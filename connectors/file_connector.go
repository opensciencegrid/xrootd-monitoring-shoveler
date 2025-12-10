package connectors

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// FileConnector writes output to a file
type FileConnector struct {
	file   *os.File
	path   string
	mu     sync.Mutex
	logger *logrus.Logger
}

// NewFileConnector creates a new file output connector
func NewFileConnector(path string, logger *logrus.Logger) (*FileConnector, error) {
	if logger == nil {
		logger = logrus.New()
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	logger.Infoln("File connector initialized, writing to:", path)

	return &FileConnector{
		file:   file,
		path:   path,
		logger: logger,
	}, nil
}

// Write writes data to the file
func (fc *FileConnector) Write(data []byte) error {
	return fc.WriteToExchange(data, "")
}

// WriteToExchange writes data to the file (exchange parameter is ignored)
func (fc *FileConnector) WriteToExchange(data []byte, exchange string) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Write the data followed by a newline
	_, err := fc.file.Write(data)
	if err != nil {
		return err
	}

	_, err = fc.file.Write([]byte("\n"))
	return err
}

// Close closes the file
func (fc *FileConnector) Close() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.file != nil {
		return fc.file.Close()
	}
	return nil
}

// Sync flushes the file to disk
func (fc *FileConnector) Sync() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.file != nil {
		return fc.file.Sync()
	}
	return nil
}
