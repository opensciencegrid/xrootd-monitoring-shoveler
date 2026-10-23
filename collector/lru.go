package collector

import (
	"container/list"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/sirupsen/logrus"
)

// Bounding the correlation state's memory
// (XrdMonDecode: lruPut/Touch/Demote/EnforceBudget/MemoryTick).
//
// Lifetimes (state.user_ttl and the rest) decide when an entry is no longer
// needed. Memory is a separate question, answered here: every entry in the
// correlation maps is on one least-recently-used list, charged an estimate of
// its size, and when the total passes a budget the coldest entries go first.
// An entry is warmed whenever a packet uses it, so what goes is what nothing
// has asked for in the longest time: an open whose close was lost, a session
// that ended, a 'd' record whose open never came. A live transfer is not
// evicted ahead of those, as it would be by a count cap that drops whatever
// arrives once full, which is what state.max_entries used to do.
//
// The budget itself is steered from the process's measured memory
// (state.max_memory) by a control loop, because the per-entry charge is only
// an estimate: the loop lowers the budget when the process is over its cap
// and raises it slowly when there is room.

// lruEntryOverhead is the fixed charge per entry on top of its key and the
// strings it holds: the StateEntry, its map slot, its list element and node.
// Deliberately an estimate; the control loop corrects for what it misses.
const lruEntryOverhead = 160

// lruNode is one entry's place in the index. The node is owned by the index
// and only read or written under lruIndex.mu.
type lruNode struct {
	sm    *StateMap
	key   string
	bytes int
	dead  bool // evicted: the owning map must not reuse this node
}

// lruIndex is the least-recently-used order over the entries of every
// StateMap attached to it, with their charged size.
//
// Lock order: a StateMap's mu, then lruIndex.mu. Eviction never holds
// lruIndex.mu while taking a map's lock: it unlinks the victim, releases the
// index, and only then removes the entry from its map.
type lruIndex struct {
	mu         sync.Mutex
	order      *list.List // front: least recently used
	bytes      int        // charged bytes of every node in order
	maxBytes   int        // byte budget (0: unbounded)
	maxEntries int        // hard cap on entries (0: none)
}

func newLRUIndex(maxEntries int) *lruIndex {
	return &lruIndex{order: list.New(), maxEntries: maxEntries}
}

// insert adds a new most-recently-used node. The caller holds sm.mu.
func (x *lruIndex) insert(sm *StateMap, key string, bytes int) *list.Element {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.bytes += bytes
	return x.order.PushBack(&lruNode{sm: sm, key: key, bytes: bytes})
}

// update re-charges an existing node and makes it most recently used. It
// returns false for a node already evicted, which the caller then replaces.
// The caller holds sm.mu.
func (x *lruIndex) update(e *list.Element, bytes int) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := e.Value.(*lruNode)
	if n.dead {
		return false
	}
	x.bytes += bytes - n.bytes
	n.bytes = bytes
	x.order.MoveToBack(e)
	return true
}

// touch makes a node most recently used.
func (x *lruIndex) touch(e *list.Element) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !e.Value.(*lruNode).dead {
		x.order.MoveToBack(e)
	}
}

// demote makes a node the next to be evicted. Used when a session ends: it is
// dead weight from then on and should not have to age past every live entry
// before eviction can reach it.
func (x *lruIndex) demote(e *list.Element) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !e.Value.(*lruNode).dead {
		x.order.MoveToFront(e)
	}
}

// remove unlinks a node whose entry is being deleted. The caller holds sm.mu.
func (x *lruIndex) remove(e *list.Element) {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := e.Value.(*lruNode)
	if n.dead {
		return
	}
	n.dead = true
	x.bytes -= n.bytes
	x.order.Remove(e)
}

// over reports whether the index exceeds its budget or its entry cap.
// The caller holds x.mu.
func (x *lruIndex) over(limit int) bool {
	return (limit > 0 && x.bytes > limit) ||
		(x.maxEntries > 0 && x.order.Len() > x.maxEntries)
}

// enforce evicts least-recently-used entries until the index is within its
// entry cap and, once over the byte budget, down to a low-water mark of 15/16
// of it, so that a full index does not evict on every insert. Returns how
// many entries it evicted. Must be called with no StateMap lock held.
func (x *lruIndex) enforce() int {
	x.mu.Lock()
	limit := x.maxBytes
	if !x.over(limit) {
		x.mu.Unlock()
		return 0
	}
	low := limit - limit/16
	evicted := 0
	for x.order.Len() > 0 && x.over(low) {
		e := x.order.Front()
		n := e.Value.(*lruNode)
		n.dead = true
		x.bytes -= n.bytes
		x.order.Remove(e)

		x.mu.Unlock()
		if n.sm.evict(n.key, e) {
			evicted++
		}
		x.mu.Lock()
	}
	x.mu.Unlock()
	if evicted > 0 {
		stateEvictedTotal.Add(float64(evicted))
	}
	return evicted
}

// stats returns the charged bytes, the entry count and the byte budget.
func (x *lruIndex) stats() (bytes, entries, maxBytes int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.bytes, x.order.Len(), x.maxBytes
}

// memoryControl steers the index's byte budget so the process stays under a
// memory cap. It is a direct port of xrdmoncollect's MemoryTick: additive
// increase, proportional decrease, fast down and slow up, holding the process
// in a band just under the cap.
type memoryControl struct {
	idx    *lruIndex
	logger *logrus.Logger

	maxRSS int // the cap (state.max_memory); 0 disables the loop
	ceil   int // highest budget the loop will grant
	band   int // top of the target band: maxRSS - maxRSS/8
	floor  int // lowest budget: below this the collector stops doing its job

	rss     func() int // process memory in bytes; 0 means unknown
	release func()     // hand freed memory back to the OS

	lastFloorWarn time.Time
}

const (
	memTickInterval   = 15 * time.Second // control period
	memStateFloor     = 8 << 20          // absolute floor on the state budget
	memFloorWarnEvery = 5 * time.Minute  // quiet time between floor warnings
	memBandShift      = 3                // band top is maxRSS - maxRSS>>3
	memChargedWorst   = 8                // worst real bytes per charged byte
)

func newMemoryControl(idx *lruIndex, maxRSS int, logger *logrus.Logger) *memoryControl {
	m := &memoryControl{
		idx:     idx,
		logger:  logger,
		maxRSS:  maxRSS,
		rss:     processMemory,
		release: debug.FreeOSMemory,
	}
	if maxRSS <= 0 {
		m.maxRSS = 0
		return m
	}
	m.ceil = maxRSS
	m.band = maxRSS - maxRSS>>memBandShift
	m.floor = min(max(memStateFloor, maxRSS/128), maxRSS>>memBandShift)

	// Start conservatively, at an eighth of the cap: at most a minute of
	// under-use, where starting at the ceiling would let the state run to
	// the cap before the first sample looked.
	idx.mu.Lock()
	idx.maxBytes = max(m.floor, maxRSS>>memBandShift)
	idx.mu.Unlock()
	return m
}

// tick runs one step of the loop.
func (m *memoryControl) tick(now time.Time) {
	if m.maxRSS == 0 {
		return
	}
	rss := m.rss()
	if rss <= 0 {
		return // unreadable: hold station
	}

	x := m.idx
	switch {
	case rss > m.maxRSS:
		// Every charged byte frees at least about four real ones, so shrink
		// by a quarter of the overage. Shrink from what the state holds, not
		// from a budget it is already below.
		over := rss - m.maxRSS
		step := max(over/4, m.floor/16)
		x.mu.Lock()
		held := min(x.maxBytes, x.bytes)
		budget := m.floor
		if held > step {
			budget = held - step
		}
		x.maxBytes = max(budget, m.floor)
		atFloor := x.maxBytes <= m.floor
		x.mu.Unlock()

		evicted := x.enforce()
		m.release()

		if atFloor && evicted == 0 {
			stateMemoryFlooredTotal.Inc()
			if now.Sub(m.lastFloorWarn) >= memFloorWarnEvery {
				m.lastFloorWarn = now
				m.logger.Warnf("state: process memory %dM exceeds state.max_memory %dM with the "+
					"correlation state already at its %dM floor; the memory is not in the "+
					"correlation state (check the enrichment and output queues)",
					rss>>20, m.maxRSS>>20, m.floor>>20)
			}
		}

	case rss < m.band:
		// Grant a fraction of the real headroom, converted at the worst
		// plausible charged-to-real ratio so one step cannot overshoot the
		// band. Climb from what is held, so the budget never runs far ahead
		// of demand that has actually been carried.
		head := m.band - rss
		step := max(head/memChargedWorst, m.floor/16)
		x.mu.Lock()
		held := min(x.maxBytes, x.bytes)
		x.maxBytes = min(m.ceil, max(x.maxBytes, held+step))
		x.mu.Unlock()
	}
	// Otherwise the process is inside the band: hold station.
}

// processMemory is the memory the Go runtime holds from the OS, less what it
// has already returned: the Go equivalent of the resident set size that
// xrdmoncollect samples from /proc.
func processMemory() int {
	s := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 || s[1].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return int(s[0].Value.Uint64() - s[1].Value.Uint64())
}

// stateSize estimates the memory one entry holds, for the LRU charge.
func stateSize(key string, value interface{}) int {
	n := lruEntryOverhead + len(key)
	switch v := value.(type) {
	case *FileState:
		n += len(v.Filename) + len(v.ServerID)
	case *UserState:
		a, t := v.AuthInfo, v.TokenInfo
		n += userInfoSize(v.UserInfo) + len(v.AppInfo) + len(v.serverID) +
			len(a.AuthProtocol) + len(a.DN) + len(a.Hostname) + len(a.Org) + len(a.Role) +
			len(a.Groups) + len(a.Info) + len(a.ExecName) + len(a.MonInfo) +
			len(t.Subject) + len(t.Username) + len(t.Org) + len(t.Role) + len(t.Groups)
	case *PathInfo:
		n += len(v.Path) + userInfoSize(v.UserInfo)
	case parser.UserInfo:
		n += userInfoSize(v)
	case string:
		n += len(v)
	}
	return n
}

func userInfoSize(u parser.UserInfo) int {
	return len(u.Protocol) + len(u.Username) + len(u.Host)
}
