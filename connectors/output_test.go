package connectors

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileConnector(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test-connector-*.jsonl")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	require.NoError(t, tmpFile.Close())
	t.Cleanup(func() { _ = os.Remove(tmpPath) })

	// Create file connector
	fc, err := NewFileConnector(tmpPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fc.Close() })

	// Write some data
	testData := []byte(`{"test": "data1"}`)
	err = fc.Write(testData)
	assert.NoError(t, err)

	// Write with exchange (should be ignored for file connector)
	testData2 := []byte(`{"test": "data2"}`)
	err = fc.WriteToExchange(testData2, "some-exchange")
	assert.NoError(t, err)

	// Sync
	err = fc.Sync()
	assert.NoError(t, err)

	// Close and read back
	require.NoError(t, fc.Close())

	content, err := os.ReadFile(tmpPath)
	require.NoError(t, err)

	// Should have two lines with newlines
	expected := "{\"test\": \"data1\"}\n{\"test\": \"data2\"}\n"
	assert.Equal(t, expected, string(content))
}

func TestMultiOutputConnector(t *testing.T) {
	// Create two temporary files
	tmpFile1, err := os.CreateTemp("", "test-multi-1-*.jsonl")
	require.NoError(t, err)
	tmpPath1 := tmpFile1.Name()
	require.NoError(t, tmpFile1.Close())
	t.Cleanup(func() { _ = os.Remove(tmpPath1) })

	tmpFile2, err := os.CreateTemp("", "test-multi-2-*.jsonl")
	require.NoError(t, err)
	tmpPath2 := tmpFile2.Name()
	require.NoError(t, tmpFile2.Close())
	t.Cleanup(func() { _ = os.Remove(tmpPath2) })

	// Create two file connectors
	fc1, err := NewFileConnector(tmpPath1, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fc1.Close() })

	fc2, err := NewFileConnector(tmpPath2, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fc2.Close() })

	// Create multi-output connector
	multi := NewMultiOutputConnector([]OutputConnector{fc1, fc2}, nil)

	// Write some data
	testData := []byte(`{"test": "multi"}`)
	err = multi.Write(testData)
	assert.NoError(t, err)

	// Close all
	err = multi.Close()
	assert.NoError(t, err)

	// Both files should have the same content
	content1, err := os.ReadFile(tmpPath1)
	require.NoError(t, err)
	content2, err := os.ReadFile(tmpPath2)
	require.NoError(t, err)

	expected := "{\"test\": \"multi\"}\n"
	assert.Equal(t, expected, string(content1))
	assert.Equal(t, expected, string(content2))
}
