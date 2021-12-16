package main

import (
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"path"
	"strconv"
	"testing"
	"time"
)

// TestQueueInsert tests the good validation
func TestQueueInsert(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	viper.Set("queue_directory", queuePath)
	queue := NewConfirmationQueue()
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
	assert.Equal(t, []byte("test1"), msg)

	msg, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test2"), msg)

}

// TestQueueEmptyDequeue Make sure the queue stalls on a third dequeue
func TestQueueEmptyDequeue(t *testing.T) {
	queuePath := path.Join(t.TempDir(), "shoveler-queue")
	viper.Set("queue_directory", queuePath)
	queue := NewConfirmationQueue()
	queue.Enqueue([]byte("test1"))
	defer func(queue *ConfirmationQueue) {
		err := queue.Close()
		if err != nil {
			assert.NoError(t, err)
		}
	}(queue)
	msg, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg)
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
	viper.Set("queue_directory", queuePath)
	queue := NewConfirmationQueue()
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

	assert.Equal(t, 100000, queue.Size())
	for i := 1; i <= 100000; i++ {
		msgString := "test." + strconv.Itoa(i)
		msg, err := queue.Dequeue()
		assert.NoError(t, err)
		assert.Equal(t, msgString, string(msg))
	}
	assert.Equal(t, 0, queue.Size())

}
