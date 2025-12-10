package shoveler

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// FileWriter writes correlated records to a file for debugging
type FileWriter struct {
	file   *os.File
	path   string
	mu     sync.Mutex
	logger *logrus.Logger
}

// NewFileWriter creates a new file writer
func NewFileWriter(path string, logger *logrus.Logger) (*FileWriter, error) {
	if logger == nil {
		logger = logrus.New()
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	logger.Infoln("File writer initialized, writing to:", path)

	return &FileWriter{
		file:   file,
		path:   path,
		logger: logger,
	}, nil
}

// Write writes a record to the file
func (fw *FileWriter) Write(data []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Write the data followed by a newline
	_, err := fw.file.Write(data)
	if err != nil {
		return err
	}

	_, err = fw.file.Write([]byte("\n"))
	return err
}

// Close closes the file
func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.file != nil {
		return fw.file.Close()
	}
	return nil
}

// Sync flushes the file to disk
func (fw *FileWriter) Sync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.file != nil {
		return fw.file.Sync()
	}
	return nil
}
