package collector

import (
	"sync"
	"time"
)

// StateEntry represents a tracked state entry with TTL
type StateEntry struct {
	Key       string
	Value     interface{}
	ExpiresAt time.Time
}

// StateMap is a concurrent-safe map with per-entry TTL
type StateMap struct {
	mu         sync.RWMutex
	entries    map[string]*StateEntry
	ttl        time.Duration
	maxEntries int
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

// NewStateMap creates a new state map with TTL and janitor
func NewStateMap(ttl time.Duration, maxEntries int, cleanupInterval time.Duration) *StateMap {
	sm := &StateMap{
		entries:    make(map[string]*StateEntry),
		ttl:        ttl,
		maxEntries: maxEntries,
		stopChan:   make(chan struct{}),
	}

	// Start the janitor goroutine
	sm.wg.Add(1)
	go sm.janitor(cleanupInterval)

	return sm
}

// Set adds or updates an entry in the state map
func (sm *StateMap) Set(key string, value interface{}) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check max entries limit
	if sm.maxEntries > 0 && len(sm.entries) >= sm.maxEntries {
		if _, exists := sm.entries[key]; !exists {
			// Key doesn't exist and we're at capacity
			return false
		}
	}

	sm.entries[key] = &StateEntry{
		Key:       key,
		Value:     value,
		ExpiresAt: time.Now().Add(sm.ttl),
	}

	return true
}

// Get retrieves an entry from the state map
func (sm *StateMap) Get(key string) (interface{}, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entry, exists := sm.entries[key]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry.Value, true
}

// Delete removes an entry from the state map
func (sm *StateMap) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.entries, key)
}

// Size returns the current number of entries
func (sm *StateMap) Size() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.entries)
}

// janitor periodically removes expired entries
func (sm *StateMap) janitor(interval time.Duration) {
	defer sm.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.cleanup()
		case <-sm.stopChan:
			return
		}
	}
}

// cleanup removes expired entries
func (sm *StateMap) cleanup() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	evicted := 0

	for key, entry := range sm.entries {
		if now.After(entry.ExpiresAt) {
			delete(sm.entries, key)
			evicted++
		}
	}

	return evicted
}

// Stop stops the janitor and cleans up resources
func (sm *StateMap) Stop() {
	close(sm.stopChan)
	sm.wg.Wait()
}

// GetAll returns all non-expired entries
func (sm *StateMap) GetAll() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	now := time.Now()
	result := make(map[string]interface{})

	for key, entry := range sm.entries {
		if !now.After(entry.ExpiresAt) {
			result[key] = entry.Value
		}
	}

	return result
}
