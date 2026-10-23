package collector

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

// StateEntry represents a tracked state entry.
//
// An entry can expire in two ways, and either, both or neither can apply:
//   - ExpiresAt is a fixed deadline (zero: none). Set from the map's TTL when
//     the entry is written, or by ExpireIn when the thing it describes ends.
//   - IdleTTL expires the entry once nothing has written or touched it for
//     that long (zero: never). LastSeen is when that last happened.
type StateEntry struct {
	Key       string
	Value     interface{}
	ExpiresAt time.Time
	IdleTTL   time.Duration
	LastSeen  time.Time

	// ending marks an ExpiresAt set by ExpireIn: the entry describes something
	// that has ended (a disconnected session) and is only kept for a grace
	// period. A later Set of the same key within that period keeps the
	// deadline, so an update arriving after the end cannot revive the entry.
	ending bool

	// node is the entry's place in the LRU index, nil when the map has none
	node *list.Element
}

// StateMap is a concurrent-safe map with per-entry expiry
type StateMap struct {
	mu         sync.RWMutex
	entries    map[string]*StateEntry
	ttl        time.Duration // fixed lifetime given to entries by Set (0: none)
	idleTTL    time.Duration // idle lifetime given to entries by Set (0: none)
	maxEntries int
	stopChan   chan struct{}
	wg         sync.WaitGroup

	// keepAlive, when set, exempts an entry from idle expiry while it returns
	// true for the entry's value. It never overrides a fixed deadline.
	keepAlive func(value interface{}) bool

	// onExpire, when set, is called for every entry removed because it
	// expired, after the map's lock is released: by the janitor, or by a
	// write that replaces an expired entry the janitor has not swept yet.
	// ended reports an entry whose deadline was set by ExpireIn.
	onExpire func(key string, value interface{}, ended bool)

	// lru, when set, bounds this map together with the others attached to the
	// same index; see lruIndex. The map's own maxEntries is then not used: the
	// index's entry cap applies to all of them, and evicts instead of refusing.
	lru *lruIndex

	// onEvict, when set, is called for every entry the LRU index evicts, after
	// the map's lock is released.
	onEvict func(key string, value interface{})
}

// NewStateMap creates a new state map with TTL and janitor. A ttl of 0 gives
// entries written by Set no fixed lifetime.
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

// SetIdleTTL sets the idle lifetime that Set gives entries (0: none).
func (sm *StateMap) SetIdleTTL(d time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.idleTTL = d
}

// SetKeepAlive installs the idle-expiry exemption; see StateMap.keepAlive.
func (sm *StateMap) SetKeepAlive(fn func(value interface{}) bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.keepAlive = fn
}

// attachLRU puts the map under an LRU index. Call before the map is used.
func (sm *StateMap) attachLRU(idx *lruIndex) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lru = idx
}

// SetOnEvict installs the eviction callback; see StateMap.onEvict.
func (sm *StateMap) SetOnEvict(fn func(key string, value interface{})) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onEvict = fn
}

// SetOnExpire installs the expiry callback; see StateMap.onExpire.
func (sm *StateMap) SetOnExpire(fn func(key string, value interface{}, ended bool)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onExpire = fn
}

// Set adds or updates an entry in the state map with the map's TTL and idle
// TTL. Updating an entry that ExpireIn has given a grace deadline keeps that
// deadline.
func (sm *StateMap) Set(key string, value interface{}) bool {
	return sm.set(key, value, sm.ttl, sm.idleTTL, false)
}

// SetWithTTL adds or updates an entry with its own fixed lifetime and idle
// lifetime instead of the map's (0: none). It replaces any grace deadline.
func (sm *StateMap) SetWithTTL(key string, value interface{}, ttl, idleTTL time.Duration) bool {
	return sm.set(key, value, ttl, idleTTL, true)
}

func (sm *StateMap) set(key string, value interface{}, ttl, idleTTL time.Duration, explicit bool) bool {
	ok, stale := sm.store(key, value, ttl, idleTTL, explicit)
	if stale != nil {
		if onExpire := sm.expiryCallback(); onExpire != nil {
			onExpire(stale.Key, stale.Value, stale.ending)
		}
	}
	if ok && sm.lru != nil {
		sm.lru.enforce()
	}
	return ok
}

// expiryCallback returns the onExpire callback.
func (sm *StateMap) expiryCallback() func(key string, value interface{}, ended bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.onExpire
}

// store writes the entry under the map's lock. stale is the entry it
// replaced if that one had expired and the janitor had not swept it yet: it
// is gone, and the caller reports it as the janitor would have.
func (sm *StateMap) store(key string, value interface{}, ttl, idleTTL time.Duration, explicit bool) (ok bool, stale *StateEntry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	prev, exists := sm.entries[key]

	// Check max entries limit
	if sm.lru == nil && sm.maxEntries > 0 && len(sm.entries) >= sm.maxEntries && !exists {
		// Key doesn't exist and we're at capacity
		return false, nil
	}

	now := time.Now()
	if exists && sm.expired(prev, now) {
		stale = prev
	}
	entry := &StateEntry{
		Key:      key,
		Value:    value,
		IdleTTL:  idleTTL,
		LastSeen: now,
	}
	if ttl > 0 {
		entry.ExpiresAt = now.Add(ttl)
	}
	// Keep a grace deadline only while it runs: an ended entry whose grace
	// has passed, and which the janitor has not swept yet, is gone, and a write
	// after that starts a new entry rather than one that is already expired.
	if !explicit && exists && prev.ending && stale == nil {
		entry.ExpiresAt = prev.ExpiresAt
		entry.ending = true
	}
	if sm.lru != nil {
		bytes := stateSize(key, value)
		if exists && prev.node != nil && sm.lru.update(prev.node, bytes) {
			entry.node = prev.node
		} else {
			entry.node = sm.lru.insert(sm, key, bytes)
		}
	}
	sm.entries[key] = entry

	return true, stale
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
	if sm.expired(entry, time.Now()) {
		return nil, false
	}

	return entry.Value, true
}

// Touch marks an entry as in use now, restarting its idle lifetime and making
// it the most recently used. It does not move a fixed deadline. Reports
// whether the entry exists.
func (sm *StateMap) Touch(key string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, exists := sm.entries[key]
	if !exists {
		return false
	}
	entry.LastSeen = time.Now()
	if entry.node != nil {
		sm.lru.touch(entry.node)
	}
	return true
}

// Demote makes an entry the next one the LRU index evicts. Reports whether
// the entry exists.
func (sm *StateMap) Demote(key string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, exists := sm.entries[key]
	if !exists {
		return false
	}
	if entry.node != nil {
		sm.lru.demote(entry.node)
	}
	return true
}

// evict removes key on behalf of the LRU index, unless the entry has been
// replaced since node was chosen. Reports whether it removed anything.
func (sm *StateMap) evict(key string, node *list.Element) bool {
	sm.mu.Lock()
	entry, exists := sm.entries[key]
	if !exists || entry.node != node {
		sm.mu.Unlock()
		return false
	}
	delete(sm.entries, key)
	onEvict := sm.onEvict
	sm.mu.Unlock()

	if onEvict != nil {
		onEvict(key, entry.Value)
	}
	return true
}

// SetIdle gives an existing entry an idle lifetime and touches it. Reports
// whether the entry exists.
func (sm *StateMap) SetIdle(key string, idleTTL time.Duration) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, exists := sm.entries[key]
	if !exists {
		return false
	}
	entry.IdleTTL = idleTTL
	entry.LastSeen = time.Now()
	if entry.node != nil {
		sm.lru.touch(entry.node)
	}
	return true
}

// ExpireIn schedules an existing entry to expire d from now, because what it
// describes has ended. The deadline survives later Sets of the key (see
// StateEntry.ending). An entry already scheduled keeps its earlier deadline,
// so a repeated end cannot extend it. Reports whether the entry exists.
func (sm *StateMap) ExpireIn(key string, d time.Duration) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	entry, exists := sm.entries[key]
	if !exists {
		return false
	}
	if !entry.ending {
		entry.ExpiresAt = time.Now().Add(d)
		entry.ending = true
	}
	return true
}

// Delete removes an entry from the state map, reporting whether it was there
func (sm *StateMap) Delete(key string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	entry, exists := sm.entries[key]
	if exists {
		sm.unlink(entry)
		delete(sm.entries, key)
	}
	return exists
}

// unlink takes an entry being removed out of the LRU index. The caller holds
// sm.mu.
func (sm *StateMap) unlink(entry *StateEntry) {
	if entry.node != nil {
		sm.lru.remove(entry.node)
	}
}

// DeletePrefix removes every entry whose key starts with prefix and returns
// how many it removed. It walks the whole map, so it is for rare events.
func (sm *StateMap) DeletePrefix(prefix string) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	n := 0
	for key, entry := range sm.entries {
		if strings.HasPrefix(key, prefix) {
			sm.unlink(entry)
			delete(sm.entries, key)
			n++
		}
	}
	return n
}

// Size returns the current number of entries
func (sm *StateMap) Size() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.entries)
}

// expired reports whether entry has expired at now. The caller holds sm.mu.
func (sm *StateMap) expired(entry *StateEntry, now time.Time) bool {
	if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
		return true
	}
	if entry.IdleTTL > 0 && now.Sub(entry.LastSeen) > entry.IdleTTL {
		return sm.keepAlive == nil || !sm.keepAlive(entry.Value)
	}
	return false
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

// cleanup removes expired entries, then reports each to onExpire
func (sm *StateMap) cleanup() int {
	sm.mu.Lock()

	now := time.Now()
	var gone []*StateEntry

	for key, entry := range sm.entries {
		if sm.expired(entry, now) {
			sm.unlink(entry)
			delete(sm.entries, key)
			gone = append(gone, entry)
		}
	}
	onExpire := sm.onExpire
	sm.mu.Unlock()

	if onExpire != nil {
		for _, entry := range gone {
			onExpire(entry.Key, entry.Value, entry.ending)
		}
	}

	return len(gone)
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
		if !sm.expired(entry, now) {
			result[key] = entry.Value
		}
	}

	return result
}
