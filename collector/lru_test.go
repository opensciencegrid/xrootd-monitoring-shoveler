package collector

import (
	"fmt"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lruMaps(t *testing.T, maxEntries int, n int) (*lruIndex, []*StateMap) {
	t.Helper()
	idx := newLRUIndex(maxEntries)
	maps := make([]*StateMap, n)
	for i := range maps {
		maps[i] = NewStateMap(0, 0, time.Hour)
		maps[i].attachLRU(idx)
		t.Cleanup(maps[i].Stop)
	}
	return idx, maps
}

func present(sm *StateMap, key string) bool {
	_, ok := sm.Get(key)
	return ok
}

// checkAccounting asserts the index charges exactly what the maps hold.
func checkAccounting(t *testing.T, idx *lruIndex, maps ...*StateMap) {
	t.Helper()
	want, entries := 0, 0
	for _, sm := range maps {
		sm.mu.RLock()
		for k, e := range sm.entries {
			want += stateSize(k, e.Value)
			entries++
			require.NotNil(t, e.node, "entry %s is in the index", k)
		}
		sm.mu.RUnlock()
	}
	bytes, n, _ := idx.stats()
	assert.Equal(t, want, bytes, "charged bytes")
	assert.Equal(t, entries, n, "indexed entries")
}

// The entry cap evicts the least recently used entry, across all the maps,
// instead of refusing the new one as the old per-map cap did.
func TestLRU_EntryCapEvictsLeastRecentlyUsed(t *testing.T) {
	idx, m := lruMaps(t, 3, 2)

	m[0].Set("a", "1")
	m[1].Set("b", "2")
	m[0].Set("c", "3")
	m[0].Touch("a") // a is now the most recently used

	assert.True(t, m[1].Set("d", "4"), "a full index never refuses")
	assert.False(t, present(m[1], "b"), "b was the least recently used")
	assert.True(t, present(m[0], "a"))
	assert.True(t, present(m[0], "c"))
	assert.True(t, present(m[1], "d"))
	checkAccounting(t, idx, m...)
}

// A demoted entry is the next to go, however recently it was used.
func TestLRU_DemoteGoesFirst(t *testing.T) {
	idx, m := lruMaps(t, 2, 1)

	m[0].Set("old", "1")
	m[0].Set("ended", "2")
	m[0].Demote("ended")
	m[0].Set("new", "3")

	assert.False(t, present(m[0], "ended"))
	assert.True(t, present(m[0], "old"))
	checkAccounting(t, idx, m...)
}

// Over the byte budget, eviction goes down to 15/16 of it, so a full index
// does not evict on every insert.
func TestLRU_ByteBudgetLowWater(t *testing.T) {
	idx, m := lruMaps(t, 0, 1)
	one := stateSize("k00", "v")
	idx.maxBytes = one * 32

	for i := 0; i < 33; i++ {
		m[0].Set(fmt.Sprintf("k%02d", i), "v")
	}
	bytes, n, _ := idx.stats()
	assert.LessOrEqual(t, bytes, idx.maxBytes-idx.maxBytes/16)
	assert.Equal(t, 30, n)
	assert.False(t, present(m[0], "k00"))
	assert.True(t, present(m[0], "k32"))
	checkAccounting(t, idx, m...)
}

// A key evicted and written again gets a fresh place in the index, and every
// way out of a map (delete, prefix delete, expiry) uncharges the entry.
func TestLRU_AccountingStaysExact(t *testing.T) {
	idx, m := lruMaps(t, 4, 2)
	m[0].SetIdleTTL(20 * time.Millisecond)

	for i := 0; i < 10; i++ {
		m[i%2].Set(fmt.Sprintf("srv-%d", i), fmt.Sprintf("value-%d", i))
	}
	m[0].Set("srv-0", "again")
	checkAccounting(t, idx, m...)

	m[1].Delete("srv-9")
	m[1].DeletePrefix("srv-")
	checkAccounting(t, idx, m...)

	time.Sleep(40 * time.Millisecond)
	m[0].cleanup()
	checkAccounting(t, idx, m...)
	_, n, _ := idx.stats()
	assert.Equal(t, 0, n)
}

// Eviction from the correlator keeps the rest of the state consistent: an
// evicted session takes its dictid entry, an evicted open leaves its session.
func TestLRU_CorrelatorEvictionCallbacks(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{TTL: time.Minute})
	defer c.Stop()
	before := testutil.ToFloat64(stateEvictedTotal)

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	serverID := c.getServerID(snapLogin())
	us := c.getUserInfo(snapLoginDic, 0, serverID)
	require.NotNil(t, us)
	require.True(t, us.hasOpenFiles())

	// Squeeze the budget to nothing: everything goes
	c.lru.mu.Lock()
	c.lru.maxBytes = 1
	c.lru.mu.Unlock()
	c.lru.enforce()

	assert.Equal(t, 0, c.GetStateSize())
	assert.Equal(t, 0, c.GetUserMapSize())
	_, ok := c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.False(t, ok, "the evicted session's dictid entry went with it")
	assert.False(t, us.hasOpenFiles(), "the evicted open left its session")
	assert.Greater(t, testutil.ToFloat64(stateEvictedTotal), before)
}

// A live transfer kept warm by its XFR records outlives an open whose close
// was lost, even though it was opened first.
func TestLRU_LiveTransferOutlivesStaleOpen(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{TTL: time.Minute, MaxEntries: 3})
	defer c.Stop()

	snapRun(t, c, snapOpen(true))                            // the transfer that goes on
	snapRun(t, c, withFileID(snapOpen(true), snapFileDic+1)) // its close will be lost
	snapRun(t, c, lcXfr())                                   // the first file is transferring
	snapRun(t, c, snapDict())                                // two more entries: one has to go

	serverID := c.getServerID(snapOpen(true))
	_, live := c.stateMap.Get(BuildFileKey(serverID, snapFileDic))
	_, lost := c.stateMap.Get(BuildFileKey(serverID, snapFileDic+1))
	assert.True(t, live, "the live transfer is kept")
	assert.False(t, lost, "the cold open is evicted")
}

// With state.max_memory set the LRU bounds the memory, so state.file_ttl
// defaults to 2 hours and applies only on servers that send XFR records,
// where silence means the close was lost. Elsewhere the LRU removes lost
// opens.
func TestLRU_FileTTLWithMaxMemory(t *testing.T) {
	const memCap = 1 << 40 // never reached here
	t.Run("defaults to 2 hours", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{MaxMemory: memCap})
		defer c.Stop()
		assert.Equal(t, DefaultFileTTLBounded, c.fileTTL)
	})

	t.Run("no XFR from the server: the open is kept", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{FileTTL: lcTTL, MaxMemory: memCap})
		defer c.Stop()
		snapRun(t, c, snapLogin())
		snapRun(t, c, snapOpen(true))
		time.Sleep(lcTTL * 2)
		assert.Equal(t, 1, c.GetStateSize())
	})

	t.Run("XFR from the server: a silent open goes", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{FileTTL: lcTTL, MaxMemory: memCap})
		defer c.Stop()
		snapRun(t, c, snapLogin())
		snapRun(t, c, lcXfr()) // the server reports in-flight transfers
		snapRun(t, c, snapOpen(true))
		time.Sleep(lcTTL * 2)
		assert.Equal(t, 0, c.GetStateSize())
	})
}

// A session with files open is not evicted, nor is its dictid entry: its
// files hold it, and its next open has to find its client. Before, both went
// as soon as they were the coldest, and that open came out "unknown".
func TestLRU_SessionWithOpenFilesIsPinned(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{TTL: time.Minute, MaxEntries: 4})
	defer c.Stop()
	before := testutil.ToFloat64(stateEvictPinnedTotal)

	snapRun(t, c, snapLogin())     // the session's dictid and userMap entries
	snapRun(t, c, snapOpen(true))  // one file open on it
	c.dictMap.Set("filler-1", "x") // at the cap
	snapRun(t, c, lcXfr())         // the file is transferring: the session is now the coldest
	c.dictMap.Set("filler-2", "x") // one has to go

	serverID := c.getServerID(snapLogin())
	assert.True(t, present(c.dictMap, BuildDictIDKey(serverID, snapLoginDic)), "the session's dictid entry is kept")
	assert.Equal(t, 1, c.GetUserMapSize(), "the session is kept")
	assert.True(t, present(c.stateMap, BuildFileKey(serverID, snapFileDic)), "the open file is kept")
	assert.False(t, present(c.dictMap, "filler-1"), "the coldest entry not pinned goes instead")
	assert.Equal(t, 2.0, testutil.ToFloat64(stateEvictPinnedTotal)-before)

	// The session's next open still finds its client
	const second = snapFileDic + 1
	snapRun(t, c, withFileID(snapOpen(true), second))
	cl := snapClose()
	r := cl.FileRecords[1].(parser.FileCloseRecord)
	r.Header.FileId = second
	cl.FileRecords[1] = r
	recs := snapRun(t, c, cl)
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)

	// With no file open, the session can be evicted again
	snapRun(t, c, snapClose())
	c.lru.mu.Lock()
	c.lru.maxBytes = 1
	c.lru.mu.Unlock()
	c.lru.enforce()
	assert.Equal(t, 0, c.GetUserMapSize())
}

// Only the login's dictid entry is pinned. A 'd' record keeps the user info
// under a dictid of its own too, and that entry is not kept just because its
// session has a file open.
func TestLRU_PathDictIDNotPinned(t *testing.T) {
	c := NewCorrelatorWithConfig(CorrelatorConfig{TTL: time.Minute, MaxEntries: 5})
	defer c.Stop()

	snapRun(t, c, snapLogin())     // the session's dictid and userMap entries
	snapRun(t, c, snapDict())      // the path, and the user info under the 'd' dictid
	snapRun(t, c, snapOpen(true))  // a file open on the session: at the cap
	snapRun(t, c, lcXfr())         // the file is transferring
	c.dictMap.Set("filler-1", "x") // two have to go
	c.dictMap.Set("filler-2", "x")

	serverID := c.getServerID(snapLogin())
	assert.True(t, present(c.dictMap, BuildDictIDKey(serverID, snapLoginDic)), "the login's dictid entry is kept")
	assert.False(t, present(c.dictMap, BuildDictIDKey(serverID, snapFileDic)), "the 'd' record's is not pinned")
	assert.True(t, present(c.dictMap, "filler-1"))
}

// When every entry left is pinned, eviction stops instead of cycling through
// them for ever, and the index stays over its limit.
func TestLRU_AllPinnedStops(t *testing.T) {
	idx, m := lruMaps(t, 2, 1)
	m[0].SetPinned(func(key string, _ interface{}) bool { return key != "free" })
	m[0].Set("a", "x")
	m[0].Set("b", "x")
	m[0].Set("free", "x")
	m[0].Set("c", "x")

	assert.True(t, present(m[0], "a"))
	assert.True(t, present(m[0], "b"))
	assert.True(t, present(m[0], "c"))
	assert.False(t, present(m[0], "free"), "the one entry not pinned goes")
	_, n, _ := idx.stats()
	assert.Equal(t, 3, n, "over the cap of 2, with everything left pinned")
	checkAccounting(t, idx, m...)
}

func withFileID(p *parser.Packet, id uint32) *parser.Packet {
	o := p.FileRecords[1].(parser.FileOpenRecord)
	o.Header.FileId = id
	p.FileRecords[1] = o
	return p
}

// The control loop: over the cap it lowers the budget and evicts; under the
// band it raises the budget, never past the ceiling; inside the band it holds.
func TestMemoryControl_Loop(t *testing.T) {
	idx, m := lruMaps(t, 0, 1)
	for i := 0; i < 1000; i++ {
		m[0].Set(fmt.Sprintf("key-%04d", i), "some value held by the entry")
	}
	held, _, _ := idx.stats()

	const capBytes = 1 << 30
	mc := newMemoryControl(idx, capBytes, logrus.New())
	mc.release = func() {}
	_, _, start := idx.stats()
	assert.Equal(t, capBytes>>3, start, "starts at an eighth of the cap")

	rss := capBytes - capBytes>>4 // inside the band [cap-cap/8, cap]
	mc.rss = func() int { return rss }
	mc.tick(time.Now())
	_, _, b := idx.stats()
	assert.Equal(t, start, b, "inside the band: hold")

	rss = capBytes / 2 // plenty of room: climb, from what is held
	mc.tick(time.Now())
	_, _, b = idx.stats()
	assert.Equal(t, max(start, held+(mc.band-rss)/memChargedWorst), b)

	for i := 0; i < 200; i++ {
		mc.tick(time.Now())
	}
	_, _, b = idx.stats()
	assert.LessOrEqual(t, b, mc.ceil, "never past the ceiling")

	rss = capBytes + capBytes/4 // over the cap: shrink from what is held
	mc.tick(time.Now())
	_, _, b = idx.stats()
	assert.Equal(t, mc.floor, b, "a large overage takes the budget to the floor")
	bytes, _, _ := idx.stats()
	assert.LessOrEqual(t, bytes, b)
}

// Over the cap with the state already at its floor and nothing left to evict,
// the loop says the memory is elsewhere instead of evicting forever.
func TestMemoryControl_Floored(t *testing.T) {
	idx := newLRUIndex(0)
	mc := newMemoryControl(idx, 1<<30, logrus.New())
	mc.release = func() {}
	mc.rss = func() int { return 2 << 30 }
	before := testutil.ToFloat64(stateMemoryFlooredTotal)

	mc.tick(time.Now())
	assert.Equal(t, before+1, testutil.ToFloat64(stateMemoryFlooredTotal))
}

// Off by default: no budget, nothing evicted for size.
func TestMemoryControl_Off(t *testing.T) {
	idx := newLRUIndex(0)
	mc := newMemoryControl(idx, 0, logrus.New())
	mc.rss = func() int { t.Fatal("rss read with the loop off"); return 0 }
	mc.tick(time.Now())
	_, _, b := idx.stats()
	assert.Equal(t, 0, b)
}

func TestProcessMemoryReadable(t *testing.T) {
	assert.Greater(t, processMemory(), 0)
}
