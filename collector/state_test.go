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

func TestStateMap_NoTTLNeverAges(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()

	sm.Set("k", "v")
	time.Sleep(50 * time.Millisecond)
	_, ok := sm.Get("k")
	assert.True(t, ok)
}

func TestStateMap_IdleTTL(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()
	sm.SetIdleTTL(100 * time.Millisecond)

	sm.Set("k", "v")
	for i := 0; i < 4; i++ {
		time.Sleep(50 * time.Millisecond)
		require.True(t, sm.Touch("k"))
	}
	_, ok := sm.Get("k")
	assert.True(t, ok, "kept alive by Touch")

	time.Sleep(150 * time.Millisecond)
	_, ok = sm.Get("k")
	assert.False(t, ok, "idle past its idle TTL")
}

func TestStateMap_KeepAliveExemptsFromIdleOnly(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()
	sm.SetIdleTTL(50 * time.Millisecond)
	sm.SetKeepAlive(func(v interface{}) bool { return v == "busy" })

	sm.Set("busy", "busy")
	sm.Set("idle", "idle")
	time.Sleep(100 * time.Millisecond)
	_, ok := sm.Get("busy")
	assert.True(t, ok)
	_, ok = sm.Get("idle")
	assert.False(t, ok)

	// A fixed deadline still applies
	require.True(t, sm.ExpireIn("busy", 20*time.Millisecond))
	time.Sleep(50 * time.Millisecond)
	_, ok = sm.Get("busy")
	assert.False(t, ok)
}

func TestStateMap_ExpireInSurvivesSet(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()

	sm.Set("k", "v1")
	require.True(t, sm.ExpireIn("k", 50*time.Millisecond))
	require.True(t, sm.ExpireIn("k", time.Hour), "a repeated end does not extend")
	sm.Set("k", "v2") // an update after the end does not revive it

	val, ok := sm.Get("k")
	require.True(t, ok)
	assert.Equal(t, "v2", val)

	time.Sleep(80 * time.Millisecond)
	_, ok = sm.Get("k")
	assert.False(t, ok)

	assert.False(t, sm.ExpireIn("missing", time.Second))
}

func TestStateMap_SetWithTTLReplacesGrace(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()

	sm.Set("k", "old")
	sm.ExpireIn("k", 30*time.Millisecond)
	sm.SetWithTTL("k", "new", 0, 0)

	time.Sleep(60 * time.Millisecond)
	val, ok := sm.Get("k")
	assert.True(t, ok)
	assert.Equal(t, "new", val)
}

// An ended entry whose grace has passed is gone even before the janitor
// sweeps it: a Set of the key then starts a new entry, not an expired one.
func TestStateMap_SetAfterGraceBeforeSweep(t *testing.T) {
	sm := NewStateMap(0, 0, time.Hour) // the janitor does not run here
	defer sm.Stop()

	sm.Set("k", "old")
	sm.ExpireIn("k", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	sm.Set("k", "new")

	val, ok := sm.Get("k")
	require.True(t, ok)
	assert.Equal(t, "new", val)
}

// A write that replaces an expired entry the janitor has not swept yet
// reports it to onExpire, as the janitor would have: otherwise its expiry
// callback would never run.
func TestStateMap_SetReportsUnsweptExpiry(t *testing.T) {
	sm := NewStateMap(0, 0, time.Hour) // the janitor does not run here
	defer sm.Stop()
	var got []interface{}
	sm.SetIdleTTL(10 * time.Millisecond)
	sm.SetOnExpire(func(_ string, v interface{}, _ bool) { got = append(got, v) })

	sm.Set("k", "old")
	sm.Set("k", "live") // replaces a live entry: nothing to report
	time.Sleep(30 * time.Millisecond)
	sm.Set("k", "new")

	assert.Equal(t, []interface{}{"live"}, got)
	val, ok := sm.Get("k")
	require.True(t, ok)
	assert.Equal(t, "new", val)
}

func TestStateMap_OnExpire(t *testing.T) {
	sm := NewStateMap(0, 0, 10*time.Millisecond)
	defer sm.Stop()

	type call struct {
		key   string
		ended bool
	}
	got := make(chan call, 4)
	sm.SetIdleTTL(30 * time.Millisecond)
	sm.SetOnExpire(func(key string, _ interface{}, ended bool) { got <- call{key, ended} })

	sm.Set("idle", 1)
	sm.SetWithTTL("ended", 2, 0, 0)
	sm.ExpireIn("ended", 10*time.Millisecond)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case c := <-got:
			seen[c.key] = c.ended
		case <-time.After(time.Second):
			t.Fatal("onExpire not called")
		}
	}
	assert.Equal(t, map[string]bool{"idle": false, "ended": true}, seen)
}

func TestStateMap_DeletePrefix(t *testing.T) {
	sm := NewStateMap(0, 0, time.Second)
	defer sm.Stop()

	sm.Set("1000#a:1094-file-1", 1)
	sm.Set("1000#a:1094-dict-2", 2)
	sm.Set("1000#a:10945-file-1", 3) // another server, same leading characters
	assert.Equal(t, 2, sm.DeletePrefix("1000#a:1094-"))
	assert.Equal(t, 1, sm.Size())
	assert.True(t, sm.Delete("1000#a:10945-file-1"))
	assert.False(t, sm.Delete("1000#a:10945-file-1"))
}
