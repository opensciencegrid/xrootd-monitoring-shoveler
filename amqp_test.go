package shoveler

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unreachableAMQPConfig returns a shoveler-mode config whose broker refuses
// connections, so workers hold their message and keep retrying.
func unreachableAMQPConfig(t *testing.T) *Config {
	t.Helper()
	u, err := url.Parse("amqp://shoveler:tok1@127.0.0.1:1/vhost")
	require.NoError(t, err)
	return &Config{
		Mode:               "shoveler",
		AmqpURL:            u,
		AmqpExchange:       "test",
		AmqpPublishWorkers: 1,
		QueueDir:           t.TempDir(),
	}
}

func newTestPool(t *testing.T, config *Config) (*WorkerPool, *ConfirmationQueue) {
	t.Helper()
	cq := NewConfirmationQueue(config)
	pool := newWorkerPool(config, cq)
	pool.Start()
	t.Cleanup(func() {
		pool.cancel()
		pool.mu.Lock()
		pool.stopWorkers()
		pool.mu.Unlock()
	})
	return pool, cq
}

func rotate(pool *WorkerPool, password string) {
	u := copyURL(pool.url)
	u.User = url.UserPassword("shoveler", password)
	pool.Restart(u)
}

// A token rotation must replace only the workers: the pool, its feeder and
// the shared channel keep running, and no message is lost.
func TestRestartKeepsPoolRunningAndMessages(t *testing.T) {
	config := unreachableAMQPConfig(t)
	pool, cq := newTestPool(t, config)

	const n = 20
	for i := 0; i < n; i++ {
		cq.Enqueue([]byte("msg"))
	}
	// The worker holds one message; the feeder moves the rest to the channel.
	require.Eventually(t, func() bool {
		return cq.Size() == 0 && len(pool.messagesQueue) == n-1
	}, 5*time.Second, 10*time.Millisecond)

	for i := 0; i < 3; i++ {
		rotate(pool, "tok"+string(rune('2'+i)))
		require.NoError(t, pool.ctx.Err(), "a rotation must not end the pool")
		// The new worker takes the handed-back message first.
		require.Eventually(t, func() bool {
			return len(pool.retry) == 0 && len(pool.messagesQueue) == n-1
		}, 5*time.Second, 10*time.Millisecond, "no message may be lost on rotation %d", i+1)
	}

	// The feeder is still running after the rotations.
	cq.Enqueue([]byte("after"))
	require.Eventually(t, func() bool {
		return cq.Size() == 0 && len(pool.messagesQueue) == n
	}, 5*time.Second, 10*time.Millisecond, "the feeder must survive rotations")
	assert.Equal(t, "tok4", passwordOf(pool.workers[0].amqpURL))
}

func passwordOf(u url.URL) string {
	p, _ := u.User.Password()
	return p
}

// Closing a session that never connected must stop its reconnect loop,
// otherwise every rotation during an outage leaks a redialing goroutine.
func TestSessionCloseStopsReconnectWhenNotConnected(t *testing.T) {
	u, err := url.Parse("amqp://shoveler:tok@127.0.0.1:1/vhost")
	require.NoError(t, err)
	session := New(*u)
	assert.ErrorIs(t, session.Close(), errAlreadyClosed)
	_ = session.Close() // a second Close must not panic

	select {
	case <-session.stopped:
	case <-time.After(2 * reconnectDelay):
		t.Fatal("handleReconnect kept running after Close")
	}
}

// A late ack for an earlier message (e.g. of a resend) must not confirm the
// message being published now.
func TestAwaitConfirmIgnoresOtherMessagesConfirms(t *testing.T) {
	ctx := context.Background()
	done := make(chan bool)

	confirms := make(chan amqp.Confirmation, 4)
	confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true} // previous message's resend
	confirms <- amqp.Confirmation{DeliveryTag: 2, Ack: false}
	assert.ErrorIs(t, awaitConfirm(ctx, done, confirms, map[uint64]bool{2: true}, time.Second), errNotConfirmed)

	confirms <- amqp.Confirmation{DeliveryTag: 3, Ack: true} // stale
	confirms <- amqp.Confirmation{DeliveryTag: 5, Ack: true}
	assert.NoError(t, awaitConfirm(ctx, done, confirms, map[uint64]bool{4: true, 5: true}, time.Second),
		"an ack of either of this message's publishes confirms it")

	confirms <- amqp.Confirmation{DeliveryTag: 6, Ack: true} // stale only
	assert.ErrorIs(t, awaitConfirm(ctx, done, confirms, map[uint64]bool{7: true}, 50*time.Millisecond), errConfirmTimeout)

	close(confirms)
	assert.ErrorIs(t, awaitConfirm(ctx, done, confirms, map[uint64]bool{8: true}, time.Second), errNotConfirmed,
		"a closed channel means the message wasn't confirmed")
}

// Cancelling the worker (token rotation) must interrupt the wait for a
// confirm, so the message is handed back and the rotation proceeds.
func TestAwaitConfirmStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	err := awaitConfirm(ctx, make(chan bool), make(chan amqp.Confirmation), map[uint64]bool{1: true}, time.Minute)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}

// The token goes into the pool's URL only; config.AmqpURL stays without
// credentials for other readers (e.g. the collector's input).
func TestLoadCredentialsLeavesConfigURLAlone(t *testing.T) {
	u, err := url.Parse("amqps://broker.example.org/vhost")
	require.NoError(t, err)
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("tok\n"), 0600))
	config := &Config{AmqpURL: u, AmqpToken: tokenFile, AmqpPublishWorkers: 1, QueueDir: t.TempDir()}

	pool := newWorkerPool(config, nil)
	require.NoError(t, pool.loadCredentials())
	assert.Nil(t, config.AmqpURL.User, "config.AmqpURL must not get the token")
	assert.Equal(t, "tok", passwordOf(*pool.url))
}
