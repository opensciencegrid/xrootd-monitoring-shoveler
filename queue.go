package main

import (
	"errors"
	"github.com/joncrlsn/dque"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"path"
	"sync"
	"time"
)

type MessageStruct struct {
	Message []byte
}

type ConfirmationQueue struct {
	msgQueue  *dque.DQue
	mutex     sync.Mutex
	emptyCond *sync.Cond
	inMemory  [][]byte
}

var (
	ErrEmpty    = errors.New("queue is empty")
	MaxInMemory = 100
)

// NewConfirmationQueue returns an initialized list.
func NewConfirmationQueue() *ConfirmationQueue { return new(ConfirmationQueue).Init() }

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
	return &MessageStruct{}
}

func (cq *ConfirmationQueue) Init() *ConfirmationQueue {
	// Set the attributes
	viper.SetDefault("queue_directory", "/tmp/shoveler-queue")
	queueDir := viper.GetString("queue_directory")

	qName := path.Base(queueDir)
	qDir := path.Dir(queueDir)
	segmentSize := 10000
	var err error
	cq.msgQueue, err = dque.NewOrOpen(qName, qDir, segmentSize, ItemBuilder)
	if err != nil {
		log.Panicln("Failed to create queue:", err)
	}
	err = cq.msgQueue.TurboOn()
	if err != nil {
		log.Errorln("Failed to turn on dque Turbo mode, the queue will be safer but much slower:", err)
	}

	cq.emptyCond = sync.NewCond(&cq.mutex)

	// Start the metrics goroutine
	go cq.queueMetrics()
	return cq

}

func (cq *ConfirmationQueue) Size() int {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	return len(cq.inMemory) + cq.msgQueue.SizeUnsafe()
}

// queueMetrics updates the queue size prometheus metric
// Should be run within a go routine
func (cq *ConfirmationQueue) queueMetrics() {
	// Setup the timer, every 5 seconds update the queue
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// Do a select on the timer
	for {
		<-ticker.C
		// Update the prometheus
		queueSizeInt := cq.Size()
		queueSize.Set(float64(queueSizeInt))
		log.Debugln("Queue Size:", queueSizeInt)

	}

}

// Enqueue the message
func (cq *ConfirmationQueue) Enqueue(msg []byte) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	// Check size of in memory queue
	if len(cq.inMemory) < MaxInMemory {
		// Add to in memory queue
		cq.inMemory = append(cq.inMemory, msg)
	} else {
		// Add to on disk queue
		err := cq.msgQueue.Enqueue(&MessageStruct{Message: msg})
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
	}
	cq.emptyCond.Broadcast()
}

// dequeueLocked dequeues a message, assuming the queue has already been locked
func (cq *ConfirmationQueue) dequeueLocked() ([]byte, error) {
	// Check if we have a message available in the queue
	if len(cq.inMemory) == 0 {
		return nil, ErrEmpty
	}
	// We know we have something to dequeue
	toReturn := cq.inMemory[0]
	cq.inMemory = cq.inMemory[1:]

	// See if we have anything on the on-disk
	for len(cq.inMemory) < MaxInMemory {
		// Dequeue something from the on disk
		msgStruct, err := cq.msgQueue.Dequeue()
		if err == dque.ErrEmpty {
			// Queue is empty
			break
		}
		// Add the new message to the back of the in memory queue
		cq.inMemory = append(cq.inMemory, msgStruct.(*MessageStruct).Message)
	}
	return toReturn, nil
}

// Dequeue Blocking function to receive a message
func (cq *ConfirmationQueue) Dequeue() ([]byte, error) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	for {
		msg, err := cq.dequeueLocked()
		if err == ErrEmpty {
			cq.emptyCond.Wait()
			// Wait() atomically unlocks mutexEmptyCond and suspends execution of the calling goroutine.
			// Receiving the signal does not guarantee an item is available, let's loop and check again.
			continue
		} else if err != nil {
			return nil, err
		}
		return msg, nil
	}
}
