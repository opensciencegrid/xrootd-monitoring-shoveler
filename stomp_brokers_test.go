package shoveler

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
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
	addr, _ := startCountingDroppingBroker(t)
	return addr
}

// startCountingDroppingBroker is startDroppingBroker, also counting the
// connections it accepts.
func startCountingDroppingBroker(t *testing.T) (string, *atomic.Int32) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	accepted := &atomic.Int32{}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
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
	return l.Addr().String(), accepted
}

func newTestQueue(t *testing.T) *ConfirmationQueue {
	q := NewConfirmationQueue(&Config{QueueDir: path.Join(t.TempDir(), "shoveler-queue")})
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// useBrokers points resolveBrokers at the given IPv4 addresses for the test
func useBrokers(t *testing.T, addrs func() ([]string, error)) {
	useBrokerFamilies(t, func() ([]string, []string, error) {
		a, err := addrs()
		return a, nil, err
	})
}

// useBrokerFamilies points resolveBrokers at the given IPv4 and IPv6 addresses
// for the test
func useBrokerFamilies(t *testing.T, addrs func() (v4 []string, v6 []string, err error)) {
	orig := resolveBrokers
	resolveBrokers = func(string) ([]string, []string, string, error) {
		v4, v6, err := addrs()
		return v4, v6, "", err
	}
	t.Cleanup(func() { resolveBrokers = orig })
}

// newTestBrokers starts the feeder; refresh starts the senders for the brokers
// resolveBrokers returns.
func newTestBrokers(t *testing.T, queue *ConfirmationQueue) *stompBrokers {
	return newTestBrokersWith(t, queue, StompSession{topic: testTopic})
}

// newTestBrokersWith is newTestBrokers with the senders copying template.
func newTestBrokersWith(t *testing.T, queue *ConfirmationQueue, template StompSession) *stompBrokers {
	out := newStompOutput(queue)
	go out.feed()
	alias, err := url.Parse("broker.test:61613")
	require.NoError(t, err)
	template.stompUrl = *alias
	b := newStompBrokers(template, out)
	t.Cleanup(func() {
		out.halt()
		queue.Wake() // a feeder waiting on an empty queue returns
		<-out.fed
		for _, s := range b.senders {
			<-s.done
		}
		for _, s := range b.retired {
			<-s.done
		}
	})
	return b
}

// waitStopped waits for a sender to exit.
func waitStopped(t *testing.T, s *stompSender) {
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender is still running")
	}
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
	published := testutil.ToFloat64(MessagesPublished)

	const n = 300
	for i := 0; i < n; i++ {
		queue.Enqueue([]byte(fmt.Sprintf("msg-%d", i)))
	}
	counts := waitDelivered(t, brokers, n)
	for i, c := range counts {
		assert.Positive(t, c, "broker %d received no messages", i)
	}
	require.Eventually(t, func() bool { return testutil.ToFloat64(MessagesPublished) == published+n },
		5*time.Second, 10*time.Millisecond, "every confirmed message is counted as published")
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
	waitStopped(t, removed)

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
	// Either sender can take a message, so keep sending until the restarting
	// broker's sender has taken one and failed.
	sent := 0
	deadline := time.After(10 * time.Second)
waitFailure:
	for {
		queue.Enqueue([]byte(fmt.Sprintf("msg-%d", sent)))
		sent++
		select {
		case <-b.failed: // what run waits on
			break waitFailure
		case <-deadline:
			t.Fatal("the failed connection did not ask for the alias to be looked up again")
		case <-time.After(20 * time.Millisecond):
		}
	}
	b.refresh()

	assert.Len(t, b.senders, 1)
	assert.Contains(t, b.senders, good.addr)
	waitStopped(t, gone)
	waitDelivered(t, []*testBroker{good}, sent)
}

// With brokers under IPv4 and IPv6, the senders use the family the alias is
// reached over. It is checked the first time, and again once every sender has
// failed.
func TestStompBrokersIPFamily(t *testing.T) {
	overV4 := startTestBroker(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	overV6 := l.Addr().String()
	require.NoError(t, l.Close()) // nothing listens: this host cannot reach the brokers over IPv6
	useBrokerFamilies(t, func() ([]string, []string, error) {
		return []string{overV4.addr}, []string{overV6}, nil
	})
	var mu sync.Mutex
	ipv6, probeErr, probes := true, error(nil), 0
	origProbe := reachedOverIPv6
	reachedOverIPv6 = func(string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		probes++
		return ipv6, probeErr
	}
	t.Cleanup(func() { reachedOverIPv6 = origProbe })
	setProbe := func(v6 bool, err error) {
		mu.Lock()
		defer mu.Unlock()
		ipv6, probeErr = v6, err
	}
	probeCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return probes
	}

	queue := newTestQueue(t)
	b := newTestBrokers(t, queue)
	b.refresh()
	assert.Equal(t, []string{overV6}, senderAddrs(b))
	require.Eventually(t, b.allFailing, 5*time.Second, 10*time.Millisecond, "the IPv6 sender cannot connect")

	// Every sender failed and the alias cannot be reached to check: kept
	setProbe(false, errors.New("connection refused"))
	b.refresh()
	assert.Equal(t, []string{overV6}, senderAddrs(b))

	// Every sender failed: the family is checked again
	setProbe(false, nil)
	b.refresh()
	assert.Equal(t, []string{overV4.addr}, senderAddrs(b))

	// The IPv4 sender is connecting or connected: no check
	checked := probeCount()
	b.refresh()
	queue.Enqueue([]byte("msg-0"))
	waitDelivered(t, []*testBroker{overV4}, 1)
	setProbe(true, nil)
	b.refresh()
	assert.Equal(t, []string{overV4.addr}, senderAddrs(b))
	assert.Equal(t, checked, probeCount(), "no TCP check while a sender works")
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

// A failed send goes back to the front of the queue, ahead of the backlog.
func TestStompRequeueGoesFirst(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	queue.Enqueue([]byte("backlog-0"))
	queue.Enqueue([]byte("backlog-1"))

	failed := &MessageStruct{Message: []byte("failed")}
	out.take(failed)
	out.requeue(failed)
	assert.Zero(t, out.sendsInProgress())

	for _, want := range []string{"failed", "backlog-0", "backlog-1"} {
		msg, err := queue.Dequeue()
		require.NoError(t, err)
		assert.Equal(t, want, string(msg.Message))
	}
}

// startSilentBroker accepts STOMP connections, answers CONNECT, then reads
// every frame and answers none: a send waits for its receipt forever.
func startSilentBroker(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = l.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go func(c net.Conn) {
				r := bufio.NewReader(c)
				if _, err := r.ReadString(0); err != nil { // CONNECT
					return
				}
				if _, err := c.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00")); err != nil {
					return
				}
				for {
					if _, err := r.ReadString(0); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return l.Addr().String()
}

// On shutdown the message waiting for a receipt and the one the feeder holds
// go back to the front of the queue, in the order they were taken.
func TestStompSaveAndStop(t *testing.T) {
	queue := newTestQueue(t)
	b := newTestBrokers(t, queue)
	// Started after the senders, so its connections close before the senders'
	// cleanup waits for them.
	silent := startSilentBroker(t)
	useBrokers(t, func() ([]string, error) { return []string{silent}, nil })
	b.refresh()

	for i := 0; i < 3; i++ {
		queue.Enqueue([]byte(fmt.Sprintf("msg-%d", i)))
	}
	// msg-0 is waiting for its receipt, msg-1 is waiting in the feeder
	require.Eventually(t, func() bool { return b.out.sendsInProgress() == 2 && queue.Size() == 1 },
		5*time.Second, 10*time.Millisecond)

	b.out.saveAndStop(200 * time.Millisecond)
	for _, want := range []string{"msg-0", "msg-1", "msg-2"} {
		msg, err := queue.Dequeue()
		require.NoError(t, err)
		assert.Equal(t, want, string(msg.Message))
	}
}

// StopSTOMP stops the registered publisher, and a publisher starting after it
// does not register.
func TestStopSTOMP(t *testing.T) {
	t.Cleanup(func() {
		stompRunning.Lock()
		stompRunning.out, stompRunning.stopping = nil, false
		stompRunning.Unlock()
	})
	out := newStompOutput(newTestQueue(t))
	go out.feed()
	require.True(t, registerStomp(out))

	StopSTOMP(time.Second)
	select {
	case <-out.fed:
	case <-time.After(5 * time.Second):
		t.Fatal("the feeder is still running")
	}
	assert.False(t, registerStomp(newStompOutput(newTestQueue(t))))
}

// startClosingBroker accepts STOMP connections and closes each one shortly
// after CONNECTED, with no message sent: a broker lost while idle.
func startClosingBroker(t *testing.T) string {
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
				if _, err := bufio.NewReader(c).ReadString(0); err != nil { // CONNECT
					return
				}
				_, _ = c.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00"))
				time.Sleep(100 * time.Millisecond)
			}(c)
		}
	}()
	return l.Addr().String()
}

// A connection that dies while idle is noticed without a send: the sender
// counts as failing, has the alias looked up again, and stops counting the
// connection as open.
func TestStompSenderNoticesIdleLoss(t *testing.T) {
	closing := startClosingBroker(t)
	useBrokers(t, func() ([]string, error) { return []string{closing}, nil })
	b := newTestBrokers(t, newTestQueue(t))
	open := testutil.ToFloat64(StompConnections)
	b.refresh()

	select {
	case <-b.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the lost connection did not ask for the alias to be looked up again")
	}
	assert.True(t, b.allFailing())
	require.Eventually(t, func() bool { return testutil.ToFloat64(StompConnections) == open },
		time.Second, 10*time.Millisecond, "the lost connection is no longer counted, ahead of the reconnect")
}

// A message the feeder handed over and no broker confirmed yet is saved on
// shutdown, from the moment of the handoff.
func TestStompSaveAndStopKeepsHandedMessage(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	go out.feed()
	queue.Enqueue([]byte("handed"))

	select {
	case <-out.messages: // a sender took it and has not sent it yet
	case <-time.After(5 * time.Second):
		t.Fatal("the feeder did not hand the message over")
	}
	out.saveAndStop(100 * time.Millisecond)

	msg, err := queue.Dequeue()
	require.NoError(t, err)
	assert.Equal(t, "handed", string(msg.Message))
}

// A send that fails after shutdown saved its message leaves the queue alone:
// the message is in it once.
func TestStompRequeueAfterSave(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	msg := &MessageStruct{Message: []byte("late")}
	out.take(msg)
	out.saveAndStop(0)
	out.requeue(msg)

	assert.Equal(t, 1, queue.Size())
}

// A sender that cannot connect has the alias looked up again on its first
// failure, so a broken IP family is noticed before the 10-minute lookup.
func TestStompConnectFailureRefreshesAlias(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unreachable := l.Addr().String()
	require.NoError(t, l.Close())
	useBrokers(t, func() ([]string, error) { return []string{unreachable}, nil })
	b := newTestBrokers(t, newTestQueue(t))
	b.refresh()

	select {
	case <-b.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed connect did not ask for the alias to be looked up again")
	}
	assert.True(t, b.allFailing())
}

// A broker leaving the alias while its sender waits for a receipt does not hold
// up refresh: the sender is told to stop and exits by itself.
func TestStompRefreshDoesNotWaitForSenders(t *testing.T) {
	good := startTestBroker(t)
	queue := newTestQueue(t)
	b := newTestBrokers(t, queue)
	silent := startSilentBroker(t) // after the senders, so it closes first
	var mu sync.Mutex
	addrs := []string{silent}
	useBrokers(t, func() ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		return addrs, nil
	})
	b.refresh()
	queue.Enqueue([]byte("stuck"))
	require.Eventually(t, func() bool { return b.out.sendsInProgress() == 1 },
		5*time.Second, 10*time.Millisecond, "the silent broker's sender is waiting for a receipt")

	mu.Lock()
	addrs = []string{good.addr}
	mu.Unlock()
	refreshed := make(chan struct{})
	go func() {
		b.refresh()
		close(refreshed)
	}()
	select {
	case <-refreshed:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh waited for the sender stuck on a receipt")
	}
	assert.Equal(t, []string{good.addr}, senderAddrs(b))
}

// In single-connection mode, a shutdown while every send fails stops the
// retries and saves the message without waiting out the timeout.
func TestStompSingleStopsRetrying(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	go out.feed()
	dropping := startDroppingBroker(t)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		out.runSingle(&StompSession{addr: dropping, topic: testTopic})
	}()
	queue.Enqueue([]byte("retried"))
	require.Eventually(t, func() bool { return out.sendsInProgress() == 1 },
		5*time.Second, 10*time.Millisecond)

	start := time.Now()
	out.saveAndStop(5 * time.Second)
	assert.Less(t, time.Since(start), 2*time.Second, "the retries stopped with the shutdown")
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the single connection is still running")
	}

	msg, err := queue.Dequeue()
	require.NoError(t, err)
	assert.Equal(t, "retried", string(msg.Message))
}

// testCert is a self-signed certificate for 127.0.0.1 and the pool that trusts
// it, used as both the broker's and the client's certificate.
func testCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "broker.test"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// startClosingTLSBroker accepts STOMP over TLS and closes each connection
// cleanly shortly after CONNECTED, with no message sent: a broker restarted
// while the connection is idle.
func startClosingTLSBroker(t *testing.T, cert tls.Certificate) string {
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()                            // sends the TLS close_notify
				if _, err := bufio.NewReader(c).ReadString(0); err != nil { // CONNECT
					return
				}
				_, _ = c.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00"))
				time.Sleep(100 * time.Millisecond)
			}(c)
		}
	}()
	return l.Addr().String()
}

// Over TLS, a broker that closes an idle connection cleanly is noticed without
// a send, as over plain TCP.
func TestStompSenderNoticesIdleLossOverTLS(t *testing.T) {
	cert, pool := testCert(t)
	orig := stompTLSRootCAs
	stompTLSRootCAs = pool
	t.Cleanup(func() { stompTLSRootCAs = orig })
	broker := startClosingTLSBroker(t, cert)
	useBrokers(t, func() ([]string, error) { return []string{broker}, nil })
	b := newTestBrokersWith(t, newTestQueue(t), StompSession{topic: testTopic, cert: []tls.Certificate{cert}})
	open := testutil.ToFloat64(StompConnections)
	b.refresh()

	select {
	case <-b.failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the lost TLS connection did not ask for the alias to be looked up again")
	}
	assert.True(t, b.allFailing())
	require.Eventually(t, func() bool { return testutil.ToFloat64(StompConnections) == open },
		time.Second, 10*time.Millisecond)
}

// In single-connection mode, a failed send waits reconnectDelay before the
// next connection, as the senders do.
func TestStompSingleWaitsBeforeReconnecting(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	go out.feed()
	dropping, accepted := startCountingDroppingBroker(t)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		out.runSingle(&StompSession{addr: dropping, topic: testTopic})
	}()
	queue.Enqueue([]byte("refused"))

	time.Sleep(time.Second)
	assert.LessOrEqual(t, accepted.Load(), int32(2), "a failing broker is not reconnected to in a loop")
	out.saveAndStop(time.Second)
	<-stopped
}

// A message the feeder takes after a shutdown saved the rest goes straight
// back to the queue.
func TestStompTakeAfterSave(t *testing.T) {
	queue := newTestQueue(t)
	out := newStompOutput(queue)
	queue.Enqueue([]byte("late"))
	msg, err := queue.Dequeue()
	require.NoError(t, err)

	out.saveAndStop(0)
	assert.False(t, out.take(msg))
	assert.Equal(t, 1, queue.Size())
}

// A shutdown that has begun stops run before any refresh, even with a
// connection failure waiting.
func TestStompBrokersRunStopsBeforeRefresh(t *testing.T) {
	var lookups atomic.Int32
	useBrokers(t, func() ([]string, error) {
		lookups.Add(1)
		return nil, errors.New("no such host")
	})
	b := newTestBrokers(t, newTestQueue(t))
	b.connectionFailed()
	b.out.halt()

	done := make(chan struct{})
	go func() {
		b.run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run is still running after the shutdown")
	}
	assert.Zero(t, lookups.Load(), "no alias lookup after the shutdown")
}

// startRestartedBroker drops its first connection on the first SEND, as a
// broker restart does, then answers every frame that asks for a receipt.
func startRestartedBroker(t *testing.T) (string, *atomic.Int32) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	accepted := &atomic.Int32{}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			first := accepted.Add(1) == 1
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				if _, err := r.ReadString(0); err != nil { // CONNECT
					return
				}
				if _, err := c.Write([]byte("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\x00")); err != nil {
					return
				}
				for {
					frame, err := r.ReadString(0)
					if err != nil || first {
						return
					}
					for _, line := range strings.Split(frame, "\n") {
						if id, ok := strings.CutPrefix(line, "receipt:"); ok {
							_, _ = c.Write([]byte("RECEIPT\nreceipt-id:" + id + "\n\n\x00"))
						}
					}
				}
			}(c)
		}
	}()
	return l.Addr().String(), accepted
}

// In single-connection mode, a send that fails once, as on a broker restart,
// reconnects and resends at once.
func TestStompSingleReconnectsAtOnce(t *testing.T) {
	broker, accepted := startRestartedBroker(t)
	session := &StompSession{addr: broker, topic: testTopic}
	require.True(t, session.reconnect(nil))
	t.Cleanup(session.disconnect)

	start := time.Now()
	assert.True(t, session.publish([]byte("after-restart"), make(chan struct{})))
	assert.Less(t, time.Since(start), 2*time.Second, "no reconnectDelay after a single failure")
	assert.Equal(t, int32(2), accepted.Load())
}

// A sender stopped before it starts opens no connection.
func TestStompStoppedSenderDoesNotConnect(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	var dials atomic.Int32
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			dials.Add(1)
			_ = c.Close()
		}
	}()

	s := &stompSender{
		session: &StompSession{addr: l.Addr().String(), topic: testTopic},
		failed:  func() {},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	s.halt()
	s.run(newStompOutput(newTestQueue(t)))
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, dials.Load())
}
