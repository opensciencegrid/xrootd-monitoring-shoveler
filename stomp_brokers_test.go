package shoveler

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	stomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/server"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTopic = "/topic/shoveler-test"

func TestBrokerAddrs(t *testing.T) {
	ip := func(s string) net.IPAddr { return net.IPAddr{IP: net.ParseIP(s)} }

	v4, v6 := brokerAddrs([]net.IPAddr{
		ip("188.184.111.185"), ip("2001:1458:d00:2c::100:43"),
		ip("188.184.105.33"), ip("2001:1458:d00:2d::100:43b"),
		ip("188.184.105.33"), // listed twice
	}, "61113")
	assert.Equal(t, []string{"188.184.105.33:61113", "188.184.111.185:61113"}, v4)
	assert.Equal(t, []string{"[2001:1458:d00:2c::100:43]:61113", "[2001:1458:d00:2d::100:43b]:61113"}, v6)

	v4, v6 = brokerAddrs(nil, "61113")
	assert.Empty(t, v4)
	assert.Empty(t, v6)
}

// testBroker is an in-process STOMP broker with one subscriber on testTopic,
// which records every message the broker delivers.
type testBroker struct {
	addr     string
	listener net.Listener
	mu       sync.Mutex
	got      []string
}

func startTestBroker(t *testing.T) *testBroker {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &testBroker{addr: l.Addr().String(), listener: l}
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = l.Close() })

	conn, err := stomp.Dial("tcp", b.addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Disconnect() })
	sub, err := conn.Subscribe(testTopic, stomp.AckAuto)
	require.NoError(t, err)
	go func() {
		for m := range sub.C {
			if m.Err != nil {
				return
			}
			b.mu.Lock()
			b.got = append(b.got, string(m.Body))
			b.mu.Unlock()
		}
	}()
	return b
}

func (b *testBroker) received() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.got...)
}

// startDroppingBroker accepts STOMP connections and closes each one when the
// first message arrives, without a receipt: a broker that fails mid-send. A
// DISCONNECT gets its receipt, so closing a sender that sent nothing is quick.
func startDroppingBroker(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				if _, err := r.ReadString(0); err != nil { // CONNECT
					return
				}
				if _, err := c.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00")); err != nil {
					return
				}
				frame, err := r.ReadString(0)
				if err != nil {
					return
				}
				if strings.HasPrefix(frame, "DISCONNECT") {
					for _, line := range strings.Split(frame, "\n") {
						if id, ok := strings.CutPrefix(line, "receipt:"); ok {
							_, _ = c.Write([]byte("RECEIPT\nreceipt-id:" + id + "\n\n\x00"))
						}
					}
				}
				// first SEND: drop the connection
			}(c)
		}
	}()
	return l.Addr().String()
}

func newTestQueue(t *testing.T) *ConfirmationQueue {
	q := NewConfirmationQueue(&Config{QueueDir: path.Join(t.TempDir(), "shoveler-queue")})
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// useBrokers points resolveBrokers at the given addresses for the test
func useBrokers(t *testing.T, addrs func() ([]string, error)) {
	orig := resolveBrokers
	resolveBrokers = func(string) ([]string, []string, string, error) {
		a, err := addrs()
		return a, nil, "", err
	}
	t.Cleanup(func() { resolveBrokers = orig })
}

// newTestBrokers starts the senders for the brokers resolveBrokers returns
func newTestBrokers(t *testing.T, queue *ConfirmationQueue) *stompBrokers {
	messages := make(chan *MessageStruct)
	go readMsgStomp(messages, queue)
	alias, err := url.Parse("broker.test:61613")
	require.NoError(t, err)
	b := newStompBrokers(StompSession{stompUrl: *alias, topic: testTopic}, messages, queue)
	t.Cleanup(func() {
		for _, s := range b.senders {
			s.close()
		}
	})
	return b
}

// waitDelivered waits until the brokers together received every message, and
// returns how many each one received.
func waitDelivered(t *testing.T, brokers []*testBroker, n int) []int {
	deadline := time.Now().Add(10 * time.Second)
	for {
		seen := map[string]bool{}
		counts := make([]int, len(brokers))
		for i, b := range brokers {
			got := b.received()
			counts[i] = len(got)
			for _, m := range got {
				seen[m] = true
			}
		}
		if len(seen) == n {
			return counts
		}
		if time.Now().After(deadline) {
			t.Fatalf("brokers received %d distinct messages of %d", len(seen), n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStompBrokersSendToEveryBroker(t *testing.T) {
	brokers := []*testBroker{startTestBroker(t), startTestBroker(t), startTestBroker(t)}
	useBrokers(t, func() ([]string, error) {
		return []string{brokers[0].addr, brokers[1].addr, brokers[2].addr}, nil
	})
	queue := newTestQueue(t)
	b := newTestBrokers(t, queue)
	b.refresh()
	assert.Len(t, b.senders, 3)

	const n = 300
	for i := 0; i < n; i++ {
		queue.Enqueue([]byte(fmt.Sprintf("msg-%d", i)))
	}
	counts := waitDelivered(t, brokers, n)
	for i, c := range counts {
		assert.Positive(t, c, "broker %d received no messages", i)
	}
}

func TestStompBrokersRefresh(t *testing.T) {
	brokers := []*testBroker{startTestBroker(t), startTestBroker(t), startTestBroker(t)}
	var mu sync.Mutex
	addrs := []string{brokers[0].addr, brokers[1].addr, brokers[2].addr}
	var lookupErr error
	useBrokers(t, func() ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		return addrs, lookupErr
	})
	b := newTestBrokers(t, newTestQueue(t))
	b.refresh()
	require.Len(t, b.senders, 3)
	removed := b.senders[brokers[2].addr]

	// A broker leaves the alias: its sender stops
	mu.Lock()
	addrs = addrs[:2]
	mu.Unlock()
	b.refresh()
	assert.Len(t, b.senders, 2)
	assert.NotContains(t, b.senders, brokers[2].addr)
	select {
	case <-removed.done:
	default:
		t.Fatal("the removed broker's sender is still running")
	}

	// The alias does not resolve: the senders running are kept
	mu.Lock()
	lookupErr = errors.New("no such host")
	mu.Unlock()
	b.refresh()
	assert.Len(t, b.senders, 2)
}

func TestStompBrokersRequeueFailedSend(t *testing.T) {
	good := startTestBroker(t)
	dropping := startDroppingBroker(t)
	useBrokers(t, func() ([]string, error) { return []string{good.addr, dropping}, nil })
	queue := newTestQueue(t)
	requeued := testutil.ToFloat64(StompRequeued)

	b := newTestBrokers(t, queue)
	b.refresh()
	const n = 50
	for i := 0; i < n; i++ {
		queue.Enqueue([]byte(fmt.Sprintf("msg-%d", i)))
	}
	// Every message reaches the good broker, including those the dropping one
	// failed to send
	waitDelivered(t, []*testBroker{good}, n)
	assert.Greater(t, testutil.ToFloat64(StompRequeued), requeued)
}

func TestStompBrokersFallBackToAlias(t *testing.T) {
	useBrokers(t, func() ([]string, error) { return nil, errors.New("no such host") })
	b := newTestBrokers(t, newTestQueue(t))
	b.refresh()
	// No brokers found and none running: one sender for the alias itself
	require.Len(t, b.senders, 1)
	assert.Equal(t, "broker.test:61613", b.senders[""].session.dialAddr())
}

// A broker being restarted fails its connection and is taken out of the alias:
// the failure has the alias looked up again, which closes that broker's sender
// and keeps the others.
func TestStompBrokersFailureRefreshesAlias(t *testing.T) {
	good := startTestBroker(t)
	restarting := startDroppingBroker(t)
	var mu sync.Mutex
	addrs := []string{good.addr, restarting}
	useBrokers(t, func() ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		return addrs, nil
	})
	queue := newTestQueue(t)
	b := newTestBrokers(t, queue)
	b.refresh()
	require.Len(t, b.senders, 2)
	gone := b.senders[restarting]

	mu.Lock()
	addrs = []string{good.addr}
	mu.Unlock()
	queue.Enqueue([]byte("msg-0"))
	queue.Enqueue([]byte("msg-1"))
	select {
	case <-b.failed: // what run waits on
	case <-time.After(10 * time.Second):
		t.Fatal("the failed connection did not ask for the alias to be looked up again")
	}
	b.refresh()

	assert.Len(t, b.senders, 1)
	assert.Contains(t, b.senders, good.addr)
	select {
	case <-gone.done:
	default:
		t.Fatal("the restarting broker's sender is still running")
	}
	waitDelivered(t, []*testBroker{good}, 2)
}

// With brokers under IPv4 and IPv6, the senders use the family the alias is
// reached over. It is chosen again only while no sender is connected.
func TestStompBrokersIPFamily(t *testing.T) {
	overV4 := startTestBroker(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	overV6 := l.Addr().String()
	require.NoError(t, l.Close()) // nothing listens: this host cannot reach the brokers over IPv6
	orig := resolveBrokers
	resolveBrokers = func(string) ([]string, []string, string, error) {
		return []string{overV4.addr}, []string{overV6}, "", nil
	}
	t.Cleanup(func() { resolveBrokers = orig })
	var mu sync.Mutex
	ipv6, probeErr := true, error(nil)
	origProbe := reachedOverIPv6
	reachedOverIPv6 = func(string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return ipv6, probeErr
	}
	t.Cleanup(func() { reachedOverIPv6 = origProbe })
	setProbe := func(v6 bool, err error) {
		mu.Lock()
		defer mu.Unlock()
		ipv6, probeErr = v6, err
	}

	b := newTestBrokers(t, newTestQueue(t))
	b.refresh()
	assert.Equal(t, []string{overV6}, senderAddrs(b))

	// No sender connected and the alias cannot be reached to check: kept
	setProbe(false, errors.New("connection refused"))
	b.refresh()
	assert.Equal(t, []string{overV6}, senderAddrs(b))

	// No sender connected: the family is chosen again
	setProbe(false, nil)
	b.refresh()
	assert.Equal(t, []string{overV4.addr}, senderAddrs(b))
	require.Eventually(t, b.anyConnected, 5*time.Second, 10*time.Millisecond)

	// A sender connected: the family is kept, whatever the check says
	setProbe(true, nil)
	b.refresh()
	assert.Equal(t, []string{overV4.addr}, senderAddrs(b))
}

func senderAddrs(b *stompBrokers) []string {
	var addrs []string
	for addr := range b.senders {
		addrs = append(addrs, addr)
	}
	return addrs
}

// A broker that takes the connection but never answers the CONNECT: connecting
// gives up after connectTimeout instead of waiting forever.
func TestGetStompConnectionTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	accepted := make(chan net.Conn, 1)
	go func() {
		defer close(accepted)
		if c, err := l.Accept(); err == nil {
			accepted <- c // kept open, unanswered
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		for c := range accepted {
			_ = c.Close()
		}
	})
	orig := connectTimeout
	connectTimeout = 200 * time.Millisecond
	t.Cleanup(func() { connectTimeout = orig })

	session := &StompSession{addr: l.Addr().String()}
	start := time.Now()
	_, err = GetStompConnection(session)
	assert.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
}
