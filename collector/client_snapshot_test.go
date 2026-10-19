package collector

// The client is resolved at file open and carried on FileState to the close.
// Before this, a transfer that outlived its dictionary entries came out with
// host "unknown" and traffic_scope UNKNOWN, although the client was known at
// open. These tests run the real correlator with a short TTL standing in for
// state.entry_ttl.

import (
	"testing"
	"time"

	"github.com/opensciencegrid/xrootd-monitoring-shoveler/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	snapTTL      = 400 * time.Millisecond
	snapLoginDic = uint32(7)  // login dictid ('u' record)
	snapFileDic  = uint32(99) // file dictid ('d' record and the f-stream FileId)
	snapHost     = "b9p29p8xxx.cern.ch"
	snapPath     = "/15/10248/2b8bd266"
	snapAddr     = "188.184.1.1:1094"
)

func snapLogin() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeUser, ServerStart: 1000},
		PacketType: parser.PacketTypeUser,
		RemoteAddr: snapAddr,
		UserRecord: &parser.UserRecord{
			DictId: snapLoginDic,
			UserInfo: parser.UserInfo{
				Protocol: "xroot", Username: "alice", Pid: 4021, Sid: 11, Host: snapHost,
			},
			AuthInfo: parser.AuthInfo{DN: "/CN=alice", Org: "alice"},
		},
	}
}

// 'd' record: Info is "userInfo\npath" and its dictid is the f-stream FileId.
func snapDict() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeDictID, ServerStart: 1000},
		PacketType: parser.PacketTypeDictID,
		RemoteAddr: snapAddr,
		MapRecord: &parser.MapRecord{
			DictId: snapFileDic,
			Info:   []byte("xroot/alice.4021:11@" + snapHost + "\n" + snapPath),
		},
	}
}

func snapOpen(inlineLfn bool) *parser.Packet {
	var lfn []byte
	if inlineLfn {
		lfn = []byte(snapPath)
	}
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeFStat, ServerStart: 1000},
		PacketType: parser.PacketTypeFStat,
		RemoteAddr: snapAddr,
		FileRecords: []interface{}{
			parser.FileTimeRecord{Header: parser.FileHeader{RecType: parser.RecTypeTime}, TBeg: 1791215515, TEnd: 1791215515},
			parser.FileOpenRecord{
				Header:   parser.FileHeader{RecType: parser.RecTypeOpen, FileId: snapFileDic, UserId: snapLoginDic},
				FileSize: 1260075252,
				User:     snapLoginDic,
				Lfn:      lfn,
			},
		},
	}
}

func snapClose() *parser.Packet {
	return &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeFStat, ServerStart: 1000},
		PacketType: parser.PacketTypeFStat,
		RemoteAddr: snapAddr,
		FileRecords: []interface{}{
			parser.FileTimeRecord{Header: parser.FileHeader{RecType: parser.RecTypeTime}, TBeg: 1791215515, TEnd: 1791215815},
			parser.FileCloseRecord{
				Header: parser.FileHeader{RecType: parser.RecTypeClose, FileId: snapFileDic},
				Xfr:    parser.StatXFR{Read: 971833808},
			},
		},
	}
}

func snapRun(t *testing.T, c *Correlator, p *parser.Packet) []*CollectorRecord {
	t.Helper()
	recs, err := c.ProcessPacket(p)
	require.NoError(t, err)
	return recs
}

// The session's login entry expires between the open and the close while the
// open entry, written later, is still there. The close used to find no client
// and report host "unknown"; the client captured at open is used instead.
func TestClientSnapshot_LoginExpiresDuringTransfer(t *testing.T) {
	c := NewCorrelator(snapTTL, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapDict())
	time.Sleep(snapTTL * 5 / 8) // the session is already old when it opens
	snapRun(t, c, snapOpen(true))

	time.Sleep(snapTTL * 5 / 8) // login and 'd' expired, the open has not

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, "alice", recs[0].User)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
	assert.Equal(t, "alice", recs[0].VO)
	assert.True(t, recs[0].userInfoKnown)
}

// A session older than the TTL opens a file: its login entry is gone, but the
// 'd' record sent with the open still names the client. The f-stream open
// reaches the collector after the 'd' (it waits for the server's f-stream
// flush), so the 'd' entry expires first; the client captured at open
// outlives it.
func TestClientSnapshot_OldSessionResolvedFromDictAtOpen(t *testing.T) {
	c := NewCorrelator(snapTTL, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	time.Sleep(snapTTL + snapTTL/4) // the login entry expires

	snapRun(t, c, snapDict())
	time.Sleep(snapTTL * 3 / 8) // f-stream flush delay
	snapRun(t, c, snapOpen(false))

	time.Sleep(snapTTL * 3 / 4) // the 'd' entry expires, the open has not

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, "xroot", recs[0].Protocol)
	assert.Equal(t, snapPath, recs[0].Filename, "the path is taken from the 'd' record at open")
}

// An open without an inline LFN takes its path from the 'd' record, which is
// stored as a *PathInfo.
func TestClientSnapshot_FilenameFromDictPathInfo(t *testing.T) {
	c := NewCorrelator(snapTTL, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapDict())
	snapRun(t, c, snapOpen(false))

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapPath, recs[0].Filename)
	assert.Equal(t, "/15", recs[0].Dirname1)
}

// Only the 'd' record was there at open, so the open-time client is partial.
// The login that turns up before the close is used instead: it carries the
// authentication the 'd' record does not.
func TestClientSnapshot_PartialAtOpenUpgradedAtClose(t *testing.T) {
	c := NewCorrelator(5*time.Second, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapDict())
	snapRun(t, c, snapOpen(false))
	snapRun(t, c, snapLogin())

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapHost, recs[0].Host)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
}

// An 'i' (app info) record made a userMap entry for the session before its
// login, so the open captured a client with no authentication. The login
// writes a new UserState for the same session; the close takes that one.
func TestClientSnapshot_LoginAfterAppInfoIsUsed(t *testing.T) {
	c := NewCorrelator(5*time.Second, 0, nil)
	defer c.Stop()

	snapRun(t, c, &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeInfo, ServerStart: 1000},
		PacketType: parser.PacketTypeInfo,
		RemoteAddr: snapAddr,
		MapRecord: &parser.MapRecord{
			DictId: snapLoginDic,
			Info:   []byte("xroot/alice.4021:11@" + snapHost + "\nmyapp"),
		},
	})
	snapRun(t, c, snapOpen(true))
	snapRun(t, c, snapLogin())

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
	assert.Equal(t, "alice", recs[0].VO)
}

// A new session takes over the user.pid:sid@host key (a reused socket) while
// a file of the old one is open. The close keeps the client captured at open
// rather than the new session's.
func TestClientSnapshot_KeyTakenByNewSessionKeepsOpenClient(t *testing.T) {
	c := NewCorrelator(5*time.Second, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapOpen(true))
	relogin := snapLogin()
	relogin.UserRecord.DictId = snapLoginDic + 1
	relogin.UserRecord.AuthInfo = parser.AuthInfo{DN: "/CN=bob", Org: "atlas"}
	snapRun(t, c, relogin)

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, "/CN=alice", recs[0].UserDN)
	assert.Equal(t, "alice", recs[0].VO)
}

// A 'd' record whose user part does not parse is kept as the raw
// "userInfo\npath" string. The open takes the path after the newline.
func TestClientSnapshot_FilenameFromUnparsedDict(t *testing.T) {
	c := NewCorrelator(5*time.Second, 0, nil)
	defer c.Stop()

	d := snapDict()
	d.MapRecord.Info = []byte("no-user-part\n" + snapPath)
	snapRun(t, c, d)
	snapRun(t, c, snapOpen(false))

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, snapPath, recs[0].Filename)
}

// Token information that arrives during the transfer is on the record: the
// open-time client is the live userMap object, not a copy.
func TestClientSnapshot_TokenAfterOpenIsKept(t *testing.T) {
	c := NewCorrelator(5*time.Second, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	snapRun(t, c, snapDict())
	snapRun(t, c, snapOpen(true))
	snapRun(t, c, &parser.Packet{
		Header:     parser.Header{Code: parser.PacketTypeToken, ServerStart: 1000},
		PacketType: parser.PacketTypeToken,
		RemoteAddr: snapAddr,
		UserRecord: &parser.UserRecord{
			DictId:    500,
			TokenInfo: parser.TokenInfo{UserDictID: snapLoginDic, Subject: "sub-1", Org: "alice"},
		},
	})

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, "sub-1", recs[0].TokenSubject)
}

// The limit: no 'd' record for the file and a login already expired at open.
// Nothing names the client at open either, so the record stays "unknown".
func TestClientSnapshot_NothingToCaptureAtOpen(t *testing.T) {
	c := NewCorrelator(snapTTL, 0, nil)
	defer c.Stop()

	snapRun(t, c, snapLogin())
	time.Sleep(snapTTL + 100*time.Millisecond)

	snapRun(t, c, snapOpen(true)) // inline LFN, no 'd'

	recs := snapRun(t, c, snapClose())
	require.Len(t, recs, 1)
	assert.Equal(t, "unknown", recs[0].Host)
	assert.Equal(t, snapPath, recs[0].Filename)
}
