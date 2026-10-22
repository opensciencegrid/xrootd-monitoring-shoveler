package shoveler

import (
	"context"
	"errors"
	"fmt"
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
