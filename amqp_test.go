package shoveler

import (
	"net/url"
	"testing"
	"time"

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
	u := copyURL(pool.config.AmqpURL)
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
