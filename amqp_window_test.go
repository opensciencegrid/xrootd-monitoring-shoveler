package shoveler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeChannel is a confirm-mode channel: publishes are numbered 1, 2, 3, ...
// and the test acks or nacks them by tag.
type fakeChannel struct {
	mu        sync.Mutex
	attempts  int // publish calls, including ones on a closed channel
	published []string
	tags      uint64
	confirmCh chan amqp.Confirmation
	closed    bool
	autoNack  bool // nack every publish, like a broker refusing it
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{confirmCh: make(chan amqp.Confirmation, 1024)}
}

func (f *fakeChannel) publish(exchange string, data []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.closed {
		return 0, errors.New("channel closed")
	}
	f.tags++
	f.published = append(f.published, string(data))
	if f.autoNack {
		f.confirmCh <- amqp.Confirmation{DeliveryTag: f.tags, Ack: false}
	}
	return f.tags, nil
}

func (f *fakeChannel) confirms() <-chan amqp.Confirmation { return f.confirmCh }

// close simulates the broker (or the worker) closing the channel.
func (f *fakeChannel) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.confirmCh)
	}
}

func (f *fakeChannel) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeChannel) bodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.published...)
}

func (f *fakeChannel) confirm(tag uint64, ack bool) {
	f.confirmCh <- amqp.Confirmation{DeliveryTag: tag, Ack: ack}
}

// fakeSource hands the worker the current channel, or nil while reconnecting.
type fakeSource struct {
	mu sync.Mutex
	ch *fakeChannel
}

func (s *fakeSource) current() confirmChannel {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		return nil
	}
	return s.ch
}

func (s *fakeSource) set(ch *fakeChannel) {
	s.mu.Lock()
	s.ch = ch
	s.mu.Unlock()
}

func (s *fakeSource) Close() error { return nil }

type windowHarness struct {
	worker *PublishWorker
	source *fakeSource
	queue  chan *MessageStruct
	retry  chan *MessageStruct
	cancel context.CancelFunc
}

func newWindowHarness(t *testing.T, window int) *windowHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &windowHarness{
		source: &fakeSource{},
		queue:  make(chan *MessageStruct, 1000),
		retry:  make(chan *MessageStruct, window+1),
		cancel: cancel,
	}
	h.worker = &PublishWorker{
		config:        &Config{AmqpExchange: "test"},
		session:       h.source,
		window:        window,
		messagesQueue: h.queue,
		retry:         h.retry,
		ctx:           ctx,
	}
	t.Cleanup(func() {
		cancel()
		h.worker.Stop()
	})
	return h
}

func (h *windowHarness) enqueue(bodies ...string) {
	for _, b := range bodies {
		h.queue <- &MessageStruct{Message: []byte(b)}
	}
}

func seq(prefix string, from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("%s%d", prefix, i))
	}
	return out
}

func eventuallyPublished(t *testing.T, ch *fakeChannel, want []string) {
	t.Helper()
	require.Eventually(t, func() bool { return assert.ObjectsAreEqual(want, ch.bodies()) },
		2*time.Second, time.Millisecond, "published %v, want %v", ch.bodies(), want)
}

// The worker publishes up to window messages without waiting for confirms,
// then waits; each ack frees a slot.
func TestWindowFillsBeforeBlocking(t *testing.T) {
	h := newWindowHarness(t, 4)
	ch := newFakeChannel()
	h.source.set(ch)
	before := metricValue(MessagesPublished)
	h.worker.Start()

	h.enqueue(seq("m", 1, 10)...)
	eventuallyPublished(t, ch, seq("m", 1, 4))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, seq("m", 1, 4), ch.bodies(), "a full window must block")
	assert.Equal(t, before, metricValue(MessagesPublished), "nothing is published until acked")

	ch.confirm(1, true)
	ch.confirm(2, true)
	eventuallyPublished(t, ch, seq("m", 1, 6))
	require.Eventually(t, func() bool { return metricValue(MessagesPublished) == before+2 },
		time.Second, time.Millisecond)
}

// A nack makes the worker drop the channel and republish everything still
// unconfirmed, in order, on the next channel before anything new.
func TestNackRepublishesUnconfirmedInOrder(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch1 := newFakeChannel()
	h.source.set(ch1)
	before := metricValue(MessagesPublished)
	h.worker.Start()

	h.enqueue(seq("m", 1, 5)...)
	eventuallyPublished(t, ch1, seq("m", 1, 5))
	ch1.confirm(1, true)
	ch1.confirm(2, true)
	ch1.confirm(3, false) // nack
	require.Eventually(t, ch1.isClosed, time.Second, time.Millisecond, "a nack must drop the channel")

	h.enqueue("m6")
	ch2 := newFakeChannel()
	h.source.set(ch2)
	eventuallyPublished(t, ch2, seq("m", 3, 6))
	assert.Equal(t, before+2, metricValue(MessagesPublished), "only the two acks count")
}

// When the channel closes (e.g. the broker refused a message), unconfirmed
// messages are republished in order on the new channel.
func TestChannelCloseRepublishesUnconfirmedInOrder(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch1 := newFakeChannel()
	h.source.set(ch1)
	h.worker.Start()

	h.enqueue(seq("m", 1, 4)...)
	eventuallyPublished(t, ch1, seq("m", 1, 4))
	ch1.confirm(1, true)
	time.Sleep(20 * time.Millisecond)
	h.source.set(nil) // reconnecting
	ch1.close()
	h.enqueue("m5")
	time.Sleep(20 * time.Millisecond)

	ch2 := newFakeChannel()
	h.source.set(ch2)
	eventuallyPublished(t, ch2, seq("m", 2, 5))
}

// On cancellation (token rotation or shutdown) every unconfirmed message is
// handed back in order, for the next worker generation to send first.
func TestCancelHandsBackUnconfirmedInOrder(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch := newFakeChannel()
	h.source.set(ch)
	before := metricValue(MessagesPublished)
	h.worker.Start()

	h.enqueue(seq("m", 1, 5)...)
	eventuallyPublished(t, ch, seq("m", 1, 5))
	ch.confirm(1, true)
	require.Eventually(t, func() bool { return metricValue(MessagesPublished) == before+1 },
		time.Second, time.Millisecond)

	h.cancel()
	h.worker.Stop()
	var handedBack []string
	for len(h.retry) > 0 {
		handedBack = append(handedBack, string((<-h.retry).Message))
	}
	assert.Equal(t, seq("m", 2, 5), handedBack)
	assert.Equal(t, before+1, metricValue(MessagesPublished))
}

// Handed-back messages are published before new ones.
func TestHandedBackMessagesGoFirst(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch := newFakeChannel()
	h.source.set(ch)
	h.retry <- &MessageStruct{Message: []byte("r1")}
	h.retry <- &MessageStruct{Message: []byte("r2")}
	h.enqueue("q1", "q2")
	h.worker.Start()
	eventuallyPublished(t, ch, []string{"r1", "r2", "q1", "q2"})
}

// A confirm for a tag the worker isn't waiting on (e.g. a duplicate) never
// counts a message as published.
func TestUnknownConfirmsAreIgnored(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch := newFakeChannel()
	h.source.set(ch)
	before := metricValue(MessagesPublished)
	h.worker.Start()

	h.enqueue("m1", "m2")
	eventuallyPublished(t, ch, []string{"m1", "m2"})
	ch.confirm(99, true)
	ch.confirm(1, true)
	ch.confirm(1, true) // duplicate
	require.Eventually(t, func() bool { return metricValue(MessagesPublished) == before+1 },
		time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, before+1, metricValue(MessagesPublished))
}

// A channel that stops confirming is dropped (not resent on), so the
// unconfirmed messages go out again on a fresh channel.
func TestStuckChannelIsReplaced(t *testing.T) {
	old := confirmTimeout
	confirmTimeout = 50 * time.Millisecond
	t.Cleanup(func() { confirmTimeout = old })

	h := newWindowHarness(t, 8)
	ch1 := newFakeChannel()
	h.source.set(ch1)
	h.worker.Start()
	h.enqueue("m1", "m2")
	eventuallyPublished(t, ch1, []string{"m1", "m2"})
	require.Eventually(t, ch1.isClosed, time.Second, time.Millisecond, "a silent channel must be dropped")
	assert.Equal(t, []string{"m1", "m2"}, ch1.bodies(), "no resend on the stuck channel")

	ch2 := newFakeChannel()
	h.source.set(ch2)
	eventuallyPublished(t, ch2, []string{"m1", "m2"})
}

func (f *fakeChannel) publishAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// Right after the worker drops a channel the session may still offer it
// until it notices the close; the worker must wait for a different channel
// rather than spin publishing on the dead one.
func TestDroppedChannelIsNotRetried(t *testing.T) {
	h := newWindowHarness(t, 8)
	ch1 := newFakeChannel()
	h.source.set(ch1)
	h.worker.Start()
	h.enqueue("m1", "m2")
	eventuallyPublished(t, ch1, []string{"m1", "m2"})

	ch1.confirm(1, false) // nack: the worker drops ch1, but the source still offers it
	require.Eventually(t, ch1.isClosed, time.Second, time.Millisecond)
	attempts := ch1.publishAttempts()
	time.Sleep(10 * channelPoll)
	assert.Equal(t, attempts, ch1.publishAttempts(), "no publishing on the dropped channel")

	ch2 := newFakeChannel()
	h.source.set(ch2)
	eventuallyPublished(t, ch2, []string{"m1", "m2"})
}

// refusingSource hands out a fresh channel each time the worker needs one;
// every channel nacks whatever is published on it, like a broker refusing a
// message for a missing exchange.
type refusingSource struct {
	mu       sync.Mutex
	ch       *fakeChannel
	channels int
}

func (s *refusingSource) current() confirmChannel {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil || s.ch.isClosed() {
		s.ch = newFakeChannel()
		s.ch.autoNack = true
		s.channels++
	}
	return s.ch
}

func (s *refusingSource) Close() error { return nil }

func (s *refusingSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.channels
}

// A broker that keeps refusing must not be hammered with channels: the
// worker backs off between attempts.
func TestRefusalsBackOff(t *testing.T) {
	oldMin, oldMax := channelRetryMin, channelRetryMax
	channelRetryMin, channelRetryMax = 50*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { channelRetryMin, channelRetryMax = oldMin, oldMax })

	h := newWindowHarness(t, 8)
	src := &refusingSource{}
	h.worker.session = src
	h.worker.Start()
	h.enqueue("m1")

	time.Sleep(time.Second)
	// Without backoff this is thousands; with 50, 100, 200, 200, ... ms
	// waits it is at most about 7.
	assert.LessOrEqual(t, src.count(), 8)
	assert.GreaterOrEqual(t, src.count(), 3, "it keeps retrying")
}

// On shutdown the pool saves every message it took from the queue but the
// broker hasn't confirmed (in the workers' windows and the shared channel)
// back to the front of the queue, in order, so a restart sends them first.
func TestPoolSaveAndStopKeepsInFlightMessages(t *testing.T) {
	brokerURL, err := url.Parse("amqp://user:pw@broker.example.org/vhost")
	require.NoError(t, err)
	config := &Config{Mode: "shoveler", AmqpURL: brokerURL, AmqpExchange: "test", AmqpPublishWorkers: 1,
		AmqpConfirmWindow: 4, QueueDir: filepath.Join(t.TempDir(), "queue")}
	cq := NewConfirmationQueue(config)
	src := &fakeSource{}
	ch := newFakeChannel() // never confirms
	src.set(ch)
	pool := newWorkerPool(config, cq)
	pool.newSource = func(url.URL, int) channelSource { return src }
	pool.Start()

	for _, b := range seq("m", 1, 10) {
		cq.Enqueue([]byte(b))
	}
	// The window of 4 is published; the rest wait in the shared channel.
	eventuallyPublished(t, ch, seq("m", 1, 4))
	require.Eventually(t, func() bool { return cq.Size() == 0 }, time.Second, time.Millisecond)

	pool.saveAndStop(5 * time.Second)
	require.NoError(t, cq.Close())

	cq = NewConfirmationQueue(config)
	assert.Equal(t, seq("m", 1, 10), drainBodies(t, cq))
	require.NoError(t, cq.Close())
}
