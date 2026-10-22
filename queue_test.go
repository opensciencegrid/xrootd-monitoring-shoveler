package shoveler

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueueInsert tests the good validation
func TestQueueInsert(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	queue.Enqueue([]byte("test1"))
	queue.Enqueue([]byte("test2"))
	msg, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg.Message)

	msg, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test2"), msg.Message)

}

// TestQueueEmptyDequeue Make sure the queue stalls on a third dequeue
func TestQueueEmptyDequeue(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	queue.Enqueue([]byte("test1"))
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	msg, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg.Message)
	doneChan := make(chan bool)
	go func() {
		_, err := queue.Dequeue()
		assert.NoError(t, err)
		doneChan <- true
	}()
	select {
	case <-doneChan:
		assert.Fail(t, "Dequeue Returned before expected")
	case <-time.After(100 * time.Millisecond):
	}

	queue.Enqueue([]byte("test1"))
	select {
	case <-doneChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Dequeue did not return as expected")
	}

}

// TestQueueLotsEntries adds many, many entries to the queue, and makes sure they are de-queued correctly
func TestQueueLotsEntries(t *testing.T) {

	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	config := Config{QueueDir: queuePath}
	queue := NewConfirmationQueue(&config)
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		queue.Enqueue([]byte(msgString))
	}

	//assert.Equal(t, 100000, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		msg, err := queue.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, msgString, string(msg.Message))
	}
	assert.Equal(t, 0, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		queue.Enqueue([]byte(msgString))
	}

	assert.Equal(t, 100000, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		msg, err := queue.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, msgString, string(msg.Message))
	}

}

// The queue drops its oldest messages beyond queue_max_messages, so a long
// outage or approval wait can't fill the disk.
func TestQueueMaxMessagesDropsOldest(t *testing.T) {
	config := &Config{QueueDir: t.TempDir(), QueueMaxMessages: 150}
	cq := NewConfirmationQueue(config)
	before := metricValue(QueueDropped)
	for i := 0; i < 200; i++ {
		cq.Enqueue([]byte(fmt.Sprint(i)))
	}
	assert.Equal(t, 150, cq.Size())
	assert.Equal(t, uint64(50), metricValue(QueueDropped)-before)
	msg, err := cq.Dequeue()
	require.NoError(t, err)
	assert.Equal(t, "50", string(msg.Message), "the oldest messages are the ones dropped")
}

func drainBodies(t *testing.T, cq *ConfirmationQueue) []string {
	t.Helper()
	var out []string
	for cq.Size() > 0 {
		m, err := cq.Dequeue()
		require.NoError(t, err)
		out = append(out, string(m.Message))
	}
	return out
}

func msgs(bodies ...string) []*MessageStruct {
	var out []*MessageStruct
	for _, b := range bodies {
		out = append(out, &MessageStruct{Message: []byte(b)})
	}
	return out
}

// Messages saved as in-flight are older than anything still queued, so they
// come out first, also after a restart.
func TestSaveInFlightComesFirstAcrossRestart(t *testing.T) {
	config := &Config{QueueDir: filepath.Join(t.TempDir(), "queue")}
	cq := NewConfirmationQueue(config)
	cq.Enqueue([]byte("q1"))
	cq.Enqueue([]byte("q2"))
	cq.SaveInFlight(msgs("f1", "f2"))
	assert.Equal(t, 4, cq.Size())
	require.NoError(t, cq.Close())

	cq = NewConfirmationQueue(config)
	assert.Equal(t, []string{"f1", "f2", "q1", "q2"}, drainBodies(t, cq))
	require.NoError(t, cq.Close())
}

// Saving again while earlier in-flight messages are still queued puts the
// newly saved ones (taken from the front earlier) ahead of them.
func TestSaveInFlightAheadOfEarlierSaved(t *testing.T) {
	config := &Config{QueueDir: filepath.Join(t.TempDir(), "queue")}
	cq := NewConfirmationQueue(config)
	cq.SaveInFlight(msgs("a", "b", "c", "d"))
	first, err := cq.Dequeue()
	require.NoError(t, err)
	second, err := cq.Dequeue()
	require.NoError(t, err)
	cq.SaveInFlight([]*MessageStruct{first, second})
	assert.Equal(t, []string{"a", "b", "c", "d"}, drainBodies(t, cq))
	require.NoError(t, cq.Close())
}

// The first MaxInMemory messages live in memory; Close writes them to disk
// so a clean shutdown keeps them.
func TestCloseKeepsInMemoryMessages(t *testing.T) {
	config := &Config{QueueDir: filepath.Join(t.TempDir(), "queue")}
	cq := NewConfirmationQueue(config)
	cq.Enqueue([]byte("m1"))
	cq.Enqueue([]byte("m2"))
	require.NoError(t, cq.Close())

	cq = NewConfirmationQueue(config)
	assert.Equal(t, []string{"m1", "m2"}, drainBodies(t, cq))
	require.NoError(t, cq.Close())
}

// Wake makes a Dequeue blocked on an empty queue return, for shutdown.
func TestWakeInterruptsDequeue(t *testing.T) {
	cq := NewConfirmationQueue(&Config{QueueDir: filepath.Join(t.TempDir(), "queue")})
	done := make(chan error)
	go func() {
		_, err := cq.Dequeue()
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cq.Wake()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, ErrInterrupted)
	case <-time.After(time.Second):
		t.Fatal("Dequeue still blocked after Wake")
	}
	require.NoError(t, cq.Close())
}
