package collector

// Each kind of correlation state ends with the thing it describes, as in
// XRootD's own xrdmoncollect: a session with its disconnect (plus a grace
// period), an open file with its close, a server with its incarnation. Only
// the 'd'/'i' dictionary records keep the age TTL (state.entry_ttl).

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lcTTL = 200 * time.Millisecond // stands in for state.entry_ttl

func lcCorrelator(cfg CorrelatorConfig) *Correlator {
	if cfg.TTL == 0 {
		cfg.TTL = lcTTL // the janitors sweep every TTL/10
	}
	return NewCorrelatorWithConfig(cfg)
}

func lcDisc() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeFStat, ServerStart: 1000},
		PacketType: parser.PacketTypeFStat,
		RemoteAddr: snapAddr,
		FileRecords: []interface{}{
			parser.FileDisconnectRecord{Header: parser.FileHeader{RecType: parser.RecTypeDisc}, UserID: snapLoginDic},
		},
	}
}

func lcXfr() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeFStat, ServerStart: 1000},
		PacketType: parser.PacketTypeFStat,
		RemoteAddr: snapAddr,
		FileRecords: []interface{}{
			parser.FileXFRRecord{Header: parser.FileHeader{RecType: parser.RecTypeXFR, FileId: snapFileDic}},
		},
	}
}

// lcAppInfo is an 'i' record of snapLogin's session. XRootD gives it a
// dictid of its own, not the login's.
func lcAppInfo() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeInfo, ServerStart: 1000},
		PacketType: parser.PacketTypeInfo,
		RemoteAddr: snapAddr,
		MapRecord: &parser.MapRecord{
			DictId: 60,
			Info:   []byte("xroot/alice.4021:11@" + snapHost + "\nmyapp"),
		},
	}
}

// lcEAInfo is a 'U' record of snapLogin's session, naming login udid.
func lcEAInfo(udid uint32) *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeEAInfo, ServerStart: 1000},
		PacketType: parser.PacketTypeEAInfo,
		RemoteAddr: snapAddr,
		MapRecord: &parser.MapRecord{
			DictId: 61,
			Info:   []byte("xroot/alice.4021:11@" + snapHost + "\n&Uc=" + strconv.Itoa(int(udid)) + "&Ec=2&Ac=3"),
		},
	}
}

// A transfer longer than state.entry_ttl keeps its open entry, its path and
// its client: before, all three had aged out and the close was emitted as a
// standalone record with filename and host "unknown".
func TestLifecycle_TransferLongerThanEntryTTL(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{})
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapDict())
	snapRun(t, c, snapOpen(false))

	time.Sleep(lcTTL * 2) // the transfer outlives the old TTL twice over

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, snapPath, recs[0].Filename)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
	assert.Equal(t, int64(1791215515), recs[0].StartTime, "matched to its open, not standalone")
}

// A session older than state.entry_ttl opens a file with no 'd' record (the
// path inline on the open). The login is still there: sessions no longer age.
func TestLifecycle_SessionOlderThanEntryTTL(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{})
	defer c.Stop()

	snapRun(t, c, snapLogin())
	time.Sleep(lcTTL * 2)

	snapRun(t, c, snapOpen(true))
	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
}

// The 'd' path records still age out on state.entry_ttl: they are only needed
// until the open that uses them.
func TestLifecycle_DictPathKeepsEntryTTL(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{})
	defer c.Stop()

	snapRun(t, c, snapDict())
	key := BuildDictKey(c.getServerID(snapDict()), snapFileDic)
	_, ok := c.dictMap.Get(key)
	require.True(t, ok)

	time.Sleep(lcTTL + lcTTL/2)
	_, ok = c.dictMap.Get(key)
	assert.False(t, ok)
}

// A close that arrives after its session's disconnect, inside the grace
// period, still gets the client from the session. The grace then runs out.
func TestLifecycle_DisconnectGrace(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserTTL: lcTTL})
	defer c.Stop()
	before := testutil.ToFloat64(expiredUsersTotal.WithLabelValues("disconnected"))

	snapRun(t, c, snapLogin())
	snapRun(t, c, lcDisc()) // no files open: nothing to sweep
	snapRun(t, c, snapOpen(true))
	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host, "a straggler inside the grace period")

	serverID := c.getServerID(snapLogin())
	time.Sleep(lcTTL + lcTTL/2)
	_, ok := c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.False(t, ok, "the session is gone after the grace period")
	assert.Equal(t, 0, c.GetUserMapSize())
	assert.Equal(t, before+1, testutil.ToFloat64(expiredUsersTotal.WithLabelValues("disconnected")))
}

// A file still open when its session's disconnect grace runs out is an open
// whose close was lost: it is dropped with the session, not left to linger.
func TestLifecycle_DisconnectDropsLeakedOpens(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserTTL: lcTTL})
	defer c.Stop()
	before := testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect"))

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	require.Equal(t, 1, c.GetStateSize())

	snapRun(t, c, lcDisc())
	assert.Equal(t, 1, c.GetStateSize(), "kept during the grace, for a late close")
	time.Sleep(lcTTL + lcTTL/2)
	assert.Equal(t, 0, c.GetStateSize())
	assert.Equal(t, before+1, testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect")))
}

// A close that arrives after the disconnect, in a packet reordered behind it,
// still matches its open and its client.
func TestLifecycle_CloseAfterDisconnect(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserTTL: lcTTL})
	defer c.Stop()
	before := testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect"))

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	snapRun(t, c, lcDisc())
	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapPath, recs[0].Filename)
	assert.Equal(t, snapHost, recs[0].Host)

	time.Sleep(lcTTL + lcTTL/2)
	assert.Equal(t, before, testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect")))
}

// A file closed before the disconnect is not counted as leaked.
func TestLifecycle_DisconnectAfterCloseDropsNothing(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserTTL: lcTTL})
	defer c.Stop()
	before := testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect"))

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	snapRun(t, c, snapClose())
	snapRun(t, c, lcDisc())
	time.Sleep(lcTTL + lcTTL/2)
	assert.Equal(t, before, testutil.ToFloat64(staleOpensTotal.WithLabelValues("disconnect")))
}

// The same login sent twice is one session: open files registered with it
// survive, so the disconnect still finds them.
func TestLifecycle_RepeatedLoginKeepsOpenFiles(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserTTL: lcTTL})
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	snapRun(t, c, snapLogin()) // retransmitted
	snapRun(t, c, lcDisc())
	time.Sleep(lcTTL + lcTTL/2)
	assert.Equal(t, 0, c.GetStateSize(), "the leaked open was still found when the session ended")
}

// A session with no disconnect and no activity idles out after UserIdleTTL,
// taking its dictid entry with it; one with a file open does not.
func TestLifecycle_UserIdleTTL(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserIdleTTL: lcTTL})
	defer c.Stop()
	serverID := c.getServerID(snapLogin())

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))

	time.Sleep(lcTTL * 2)
	_, ok := c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.True(t, ok, "a session with an open file is not idle")

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)

	time.Sleep(lcTTL * 2)
	assert.Equal(t, 0, c.GetUserMapSize(), "idle with nothing open: expired")
	_, ok = c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.False(t, ok, "its dictid entry went with it")
}

// state.file_ttl drops an open file whose close and disconnect were both
// lost. On a server that sends XFR records it is silence; elsewhere it is the
// open's age. Its session, with no file open any more, can then idle out.
func TestLifecycle_FileTTL(t *testing.T) {
	t.Run("defaults to 7 days, negative turns it off", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{})
		defer c.Stop()
		assert.Equal(t, DefaultFileTTL, c.fileTTL)
		off := lcCorrelator(CorrelatorConfig{FileTTL: -1})
		defer off.Stop()
		assert.Zero(t, off.fileTTL)
	})

	t.Run("no XFR from the server: the open goes at its age", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{FileTTL: lcTTL, UserIdleTTL: lcTTL})
		defer c.Stop()
		before := testutil.ToFloat64(staleOpensTotal.WithLabelValues("file_ttl"))
		snapRun(t, c, snapLogin())
		snapRun(t, c, snapOpen(true))
		time.Sleep(lcTTL * 2)
		assert.Equal(t, 0, c.GetStateSize(), "older than file_ttl: its close was lost")
		assert.Equal(t, before+1, testutil.ToFloat64(staleOpensTotal.WithLabelValues("file_ttl")))
		time.Sleep(lcTTL * 2)
		assert.Equal(t, 0, c.GetUserMapSize(), "the session, with no file open, idles out")
	})

	t.Run("XFR keeps a live file, silence expires it", func(t *testing.T) {
		c := lcCorrelator(CorrelatorConfig{FileTTL: lcTTL})
		defer c.Stop()
		before := testutil.ToFloat64(staleOpensTotal.WithLabelValues("file_ttl"))

		snapRun(t, c, snapLogin())
		snapRun(t, c, lcXfr()) // the server reports in-flight transfers
		snapRun(t, c, snapOpen(true))
		for i := 0; i < 4; i++ {
			time.Sleep(lcTTL / 2)
			snapRun(t, c, lcXfr())
		}
		assert.Equal(t, 1, c.GetStateSize(), "refreshed by its XFR records")

		time.Sleep(lcTTL * 2)
		assert.Equal(t, 0, c.GetStateSize(), "silent past file_ttl: its close was lost")
		assert.Equal(t, before+1, testutil.ToFloat64(staleOpensTotal.WithLabelValues("file_ttl")))
	})
}

// A server incarnation silent for ServerTTL is dropped with everything it
// owned, including sessions whose disconnect it never sent.
func TestLifecycle_ServerTTLReapsIncarnation(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{ServerTTL: lcTTL})
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapDict())
	snapRun(t, c, snapOpen(true))
	require.Equal(t, 1, c.GetStateSize())
	require.Equal(t, 1, c.GetUserMapSize())

	time.Sleep(lcTTL * 2)
	assert.Equal(t, 0, c.GetStateSize())
	assert.Equal(t, 0, c.GetUserMapSize())
	assert.Equal(t, 0, c.dictMap.Size())
}

// A 'U' record that arrives before the login: the login completes the state
// it created, so its SciTags are kept.
func TestLifecycle_EAInfoBeforeLogin(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{})
	defer c.Stop()

	snapRun(t, c, lcEAInfo(snapLoginDic))
	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
	assert.Equal(t, 2, recs[0].ExperimentID)
	assert.Equal(t, 3, recs[0].ActivityID)
}

// 'i' then 'U' and no login (lost): the 'U' record gives the state the
// login's dictid, so the dictid entry it created goes with the session.
func TestLifecycle_AppInfoThenEAInfoWithoutLogin(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{UserIdleTTL: lcTTL})
	defer c.Stop()

	snapRun(t, c, lcAppInfo())
	snapRun(t, c, lcEAInfo(snapLoginDic))
	serverID := c.getServerID(snapLogin())
	v, ok := c.userMap.Get(BuildUserInfoKey(serverID, snapLogin().UserRecord.UserInfo))
	require.True(t, ok)
	assert.Equal(t, snapLoginDic, v.(*UserState).UserID)

	time.Sleep(lcTTL * 2) // idles out
	assert.Equal(t, 0, c.GetUserMapSize())
	_, ok = c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.False(t, ok, "its dictid entry went with it")
}

// A session idles out, and a record of the key arrives before the sweep. The
// expired session is cleaned up as the sweep would have: its dictid entry,
// which has no lifetime of its own, does not stay behind.
func TestLifecycle_RecordAfterIdleExpiryBeforeSweep(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{TTL: time.Hour, UserIdleTTL: 10 * time.Millisecond}) // sweeps every 6 min
	defer c.Stop()

	snapRun(t, c, snapLogin())
	time.Sleep(30 * time.Millisecond) // idle past UserIdleTTL, not swept
	snapRun(t, c, lcAppInfo())

	serverID := c.getServerID(snapLogin())
	_, ok := c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.False(t, ok, "the expired session's dictid entry is gone")
	v, ok := c.userMap.Get(BuildUserInfoKey(serverID, snapLogin().UserRecord.UserInfo))
	require.True(t, ok)
	assert.Equal(t, "myapp", v.(*UserState).AppInfo)
}

// The same login sent again after its session idled out, before the sweep:
// the new state is that login's, and keeps its dictid entry.
func TestLifecycle_LoginAgainAfterIdleExpiryKeepsDictID(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{TTL: time.Hour, UserIdleTTL: 10 * time.Millisecond}) // sweeps every 6 min
	defer c.Stop()

	snapRun(t, c, snapLogin())
	time.Sleep(30 * time.Millisecond)
	snapRun(t, c, snapLogin())

	serverID := c.getServerID(snapLogin())
	_, ok := c.dictMap.Get(BuildDictIDKey(serverID, snapLoginDic))
	assert.True(t, ok)
	us := c.getUserInfo(snapLoginDic, 0, serverID)
	require.NotNil(t, us)
	assert.Equal(t, "/CN=alice", us.AuthInfo.DN)
}

// The janitors and the eviction read a session's dictid from other goroutines
// while 'U' records and logins update it. Run under -race.
func TestLifecycle_DictIDReadWhileUpdated(t *testing.T) {
	c := lcCorrelator(CorrelatorConfig{})
	defer c.Stop()

	snapRun(t, c, snapLogin())
	serverID := c.getServerID(snapLogin())
	v, ok := c.userMap.Get(BuildUserInfoKey(serverID, snapLogin().UserRecord.UserInfo))
	require.True(t, ok)
	us := v.(*UserState)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = us.dictID()
			}
		}
	}()
	for i := uint32(0); i < 100; i++ {
		snapRun(t, c, lcEAInfo(100+i)) // each names another dictid: a write
	}
	close(done)
	wg.Wait()
	assert.Equal(t, uint32(199), us.dictID())
}
