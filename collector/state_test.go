package collector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateMap_SetAndGet(t *testing.T) {
	sm := NewStateMap(1*time.Second, 0, 100*time.Millisecond)
	defer sm.Stop()

	// Set a value
	ok := sm.Set("key1", "value1")
	assert.True(t, ok)

	// Get the value
	val, exists := sm.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", val)

	// Get non-existent key
	_, exists = sm.Get("key2")
	assert.False(t, exists)
}

func TestStateMap_MaxEntries(t *testing.T) {
	sm := NewStateMap(1*time.Second, 2, 100*time.Millisecond)
	defer sm.Stop()

	// Add two entries (at capacity)
	ok := sm.Set("key1", "value1")
	assert.True(t, ok)
	ok = sm.Set("key2", "value2")
	assert.True(t, ok)

	// Try to add a third entry (should fail)
	ok = sm.Set("key3", "value3")
	assert.False(t, ok)

	// Update existing entry (should succeed)
	ok = sm.Set("key1", "updated")
	assert.True(t, ok)

	val, exists := sm.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "updated", val)
}

func TestStateMap_TTL(t *testing.T) {
	sm := NewStateMap(200*time.Millisecond, 0, 50*time.Millisecond)
	defer sm.Stop()

	// Set a value
	sm.Set("key1", "value1")

	// Value should exist immediately
	val, exists := sm.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", val)

	// Wait for TTL to expire
	time.Sleep(250 * time.Millisecond)

	// Value should not exist after TTL
	_, exists = sm.Get("key1")
	assert.False(t, exists)
}

func TestStateMap_Janitor(t *testing.T) {
	sm := NewStateMap(100*time.Millisecond, 0, 50*time.Millisecond)
	defer sm.Stop()

	// Add multiple entries
	sm.Set("key1", "value1")
	sm.Set("key2", "value2")
	sm.Set("key3", "value3")

	// All entries should exist
	assert.Equal(t, 3, sm.Size())

	// Wait for janitor to clean up expired entries
	time.Sleep(200 * time.Millisecond)

	// All entries should be cleaned up
	assert.Equal(t, 0, sm.Size())
}

func TestStateMap_Delete(t *testing.T) {
	sm := NewStateMap(1*time.Second, 0, 100*time.Millisecond)
	defer sm.Stop()

	sm.Set("key1", "value1")
	assert.Equal(t, 1, sm.Size())

	sm.Delete("key1")
	assert.Equal(t, 0, sm.Size())

	_, exists := sm.Get("key1")
	assert.False(t, exists)
}

func TestStateMap_GetAll(t *testing.T) {
	sm := NewStateMap(1*time.Second, 0, 100*time.Millisecond)
	defer sm.Stop()

	sm.Set("key1", "value1")
	sm.Set("key2", "value2")
	sm.Set("key3", "value3")

	all := sm.GetAll()
	assert.Len(t, all, 3)
	assert.Equal(t, "value1", all["key1"])
	assert.Equal(t, "value2", all["key2"])
	assert.Equal(t, "value3", all["key3"])
}

func TestStateMap_ConcurrentAccess(t *testing.T) {
	sm := NewStateMap(1*time.Second, 0, 100*time.Millisecond)
	defer sm.Stop()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				sm.Set("key", id)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Value should exist
	_, exists := sm.Get("key")
	assert.True(t, exists)
}

func TestStateMap_Stop(t *testing.T) {
	sm := NewStateMap(1*time.Second, 0, 50*time.Millisecond)

	sm.Set("key1", "value1")
	require.Equal(t, 1, sm.Size())

	// Stop should complete without hanging
	sm.Stop()

	// Map should still be accessible after stop (janitor is stopped)
	val, exists := sm.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", val)
}
