package shoveler

import (
	"container/list"

	"github.com/joncrlsn/dque"

	"errors"
	"path"
	"sync"
	"time"
)

type MessageStruct struct {
	Message  []byte
	Exchange string // Optional: specific exchange for this message (empty = use default)
}

type ConfirmationQueue struct {
	diskQueue *dque.DQue
	// inflight holds messages saved at shutdown that had been taken from
	// the queue but not confirmed by the broker. They are older than
	// everything in diskQueue and memQueue, so they are dequeued first.
	inflight *dque.DQue
	// interrupted makes a Dequeue on an empty queue return ErrInterrupted
	// instead of waiting (set by Wake, for shutdown).
	interrupted bool
	mutex       sync.Mutex
	emptyCond   *sync.Cond
	memQueue    *list.List
	usingDisk   bool
	// maxMessages caps the queue; beyond it the oldest messages are dropped
	// (0 = no limit).
	maxMessages int
	// dropping is true while the queue is at its cap, to warn once per episode.
	dropping bool
}

var (
	ErrEmpty       = errors.New("queue is empty")
	ErrInterrupted = errors.New("queue wait interrupted for shutdown")
	MaxInMemory    = 100
	LowWaterMark   = 50
)

// NewConfirmationQueue returns an initialized list.
func NewConfirmationQueue(config *Config) *ConfirmationQueue {
	return new(ConfirmationQueue).Init(config)
}

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
	return &MessageStruct{}
}

// Init initializes the queue
func (cq *ConfirmationQueue) Init(config *Config) *ConfirmationQueue {
	qName := path.Base(config.QueueDir)
	qDir := path.Dir(config.QueueDir)
	segmentSize := 10000
	var err error
	cq.diskQueue, err = dque.NewOrOpen(qName, qDir, segmentSize, ItemBuilder)
	if err != nil {
		log.Panicln("Failed to create queue:", err)
	}
	err = cq.diskQueue.TurboOn()
	if err != nil {
		log.Errorln("Failed to turn on dque Turbo mode, the queue will be safer but much slower:", err)
	}

	// Check if we have any messages in the queue
	if cq.diskQueue.Size() > 0 {
		cq.usingDisk = true
	}

	// Messages saved at the last shutdown before the broker confirmed them.
	cq.inflight, err = dque.NewOrOpen(qName+"-inflight", qDir, segmentSize, ItemBuilder)
	if err != nil {
		log.Panicln("Failed to create the in-flight queue:", err)
	}
	if n := cq.inflight.Size(); n > 0 {
		log.Infoln(n, "messages saved at the last shutdown will be sent first")
	}

	cq.maxMessages = config.QueueMaxMessages
	cq.emptyCond = sync.NewCond(&cq.mutex)

	// Start the metrics goroutine
	cq.memQueue = list.New()
	go cq.queueMetrics()
	return cq

}

func (cq *ConfirmationQueue) Size() int {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	return cq.sizeLocked()
}

// sizeLocked is Size for callers that hold the mutex.
func (cq *ConfirmationQueue) sizeLocked() int {
	size := cq.inflight.SizeUnsafe()
	if cq.usingDisk {
		return size + cq.diskQueue.SizeUnsafe()
	}
	return size + cq.memQueue.Len()
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
		QueueSize.Set(float64(queueSizeInt))
		log.Debugln("Queue Size:", queueSizeInt)

	}

}

// Enqueue the message
func (cq *ConfirmationQueue) Enqueue(msg []byte) {
	cq.EnqueueToExchange(msg, "")
}

// EnqueueToExchange enqueues a message with an optional specific exchange
// If exchange is empty, the default exchange from config will be used
func (cq *ConfirmationQueue) EnqueueToExchange(msg []byte, exchange string) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	msgStruct := &MessageStruct{Message: msg, Exchange: exchange}

	// Still using in-memory
	if !cq.usingDisk && (cq.memQueue.Len()+1) < MaxInMemory {
		cq.memQueue.PushBack(msgStruct)
	} else if !cq.usingDisk && (cq.memQueue.Len()+1) >= MaxInMemory {
		// Not using disk queue, but the next message would go over MaxInMemory
		// Transfer everything to the on-disk queue
		for cq.memQueue.Len() > 0 {
			toEnqueue := cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct)
			err := cq.diskQueue.Enqueue(toEnqueue)
			if err != nil {
				log.Errorln("Failed to enqueue message:", err)
			}
		}
		// Enqueue the current
		err := cq.diskQueue.Enqueue(msgStruct)
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
		cq.usingDisk = true

	} else {
		// Last option is we are using disk
		err := cq.diskQueue.Enqueue(msgStruct)
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
	}
	cq.enforceMaxLocked()
	cq.emptyCond.Broadcast()
}

// enforceMaxLocked drops the oldest messages beyond maxMessages.
func (cq *ConfirmationQueue) enforceMaxLocked() {
	if cq.maxMessages <= 0 {
		return
	}
	over := cq.sizeLocked() - cq.maxMessages
	if over <= 0 {
		cq.dropping = false
		return
	}
	if !cq.dropping {
		log.Warningln("Queue is full at", cq.maxMessages,
			"messages (queue_max_messages); dropping the oldest until it drains")
		cq.dropping = true
	}
	for ; over > 0; over-- {
		if _, err := cq.dequeueLocked(); err != nil {
			return
		}
		QueueDropped.Inc()
	}
}

// dequeueLocked dequeues a message, assuming the queue has already been locked
func (cq *ConfirmationQueue) dequeueLocked() (*MessageStruct, error) {
	// Messages saved in flight at the last shutdown are the oldest.
	if cq.inflight.SizeUnsafe() > 0 {
		item, err := cq.inflight.Dequeue()
		if err != nil {
			return nil, err
		}
		return item.(*MessageStruct), nil
	}
	// Check if we have a message available in the queue
	if !cq.usingDisk && cq.memQueue.Len() == 0 {
		return nil, ErrEmpty
	} else if cq.usingDisk && cq.diskQueue.Size() == 0 {
		return nil, ErrEmpty
	}

	if !cq.usingDisk {
		return cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct), nil
	} else if cq.usingDisk && (cq.diskQueue.Size()-1) >= LowWaterMark {
		// If we are using disk, and the on disk size is larger than the low water mark
		msgStruct, err := cq.diskQueue.Dequeue()
		if err != nil {
			log.Errorln("Failed to dequeue: ", err)
		}
		return msgStruct.(*MessageStruct), err
	} else {
		// Using disk, but the next enqueue makes it < LowWaterMark, transfer everything from on disk to in-memory
		for cq.diskQueue.Size() > 0 {
			msgStruct, err := cq.diskQueue.Dequeue()
			if err != nil {
				log.Errorln("Failed to dequeue: ", err)
			}
			cq.memQueue.PushBack(msgStruct.(*MessageStruct))
		}
		cq.usingDisk = false
		return cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct), nil
	}

}

// Dequeue Blocking function to receive a message
func (cq *ConfirmationQueue) Dequeue() (*MessageStruct, error) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	for {
		msg, err := cq.dequeueLocked()
		if err == ErrEmpty {
			if cq.interrupted {
				return nil, ErrInterrupted
			}
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

// SaveInFlight puts messages that were taken from the queue but not
// confirmed by the broker back at the front, in order, on disk, so they are
// sent first (also after a restart). Messages saved earlier and still
// waiting stay behind them: they were taken from the front later.
func (cq *ConfirmationQueue) SaveInFlight(msgs []*MessageStruct) {
	if len(msgs) == 0 {
		return
	}
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	var earlier []*MessageStruct
	for cq.inflight.SizeUnsafe() > 0 {
		item, err := cq.inflight.Dequeue()
		if err != nil {
			log.Errorln("Failed to read the in-flight queue:", err)
			break
		}
		earlier = append(earlier, item.(*MessageStruct))
	}
	for _, m := range append(msgs, earlier...) {
		if err := cq.inflight.Enqueue(m); err != nil {
			log.Errorln("Failed to save an in-flight message:", err)
		}
	}
	cq.emptyCond.Broadcast()
}

// Wake makes Dequeue return ErrInterrupted instead of waiting on an empty
// queue, so a shutdown isn't stuck behind it.
func (cq *ConfirmationQueue) Wake() {
	cq.mutex.Lock()
	cq.interrupted = true
	cq.mutex.Unlock()
	cq.emptyCond.Broadcast()
}

// Close writes any messages held in memory to disk and closes the on-disk
// queues.
func (cq *ConfirmationQueue) Close() error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	// Below MaxInMemory messages live only in memQueue (diskQueue is then
	// empty), so writing them out in order keeps them across a restart.
	for !cq.usingDisk && cq.memQueue.Len() > 0 {
		m := cq.memQueue.Remove(cq.memQueue.Front()).(*MessageStruct)
		if err := cq.diskQueue.Enqueue(m); err != nil {
			log.Errorln("Failed to save a queued message:", err)
		}
	}
	errInflight := cq.inflight.Close()
	if err := cq.diskQueue.Close(); err != nil {
		return err
	}
	return errInflight
}
