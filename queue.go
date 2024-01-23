package shoveler

import (
	"container/list"

	"github.com/joncrlsn/dque"

	"errors"
	"path"
	"sync"
	"time"

	"github.com/spf13/viper"
)

type MessageStruct struct {
	Message []byte
}

type ConfirmationQueue struct {
	diskQueue *dque.DQue
	mutex     sync.Mutex
	emptyCond *sync.Cond
	memQueue  *list.List
	usingDisk bool
}

var (
	ErrEmpty     = errors.New("queue is empty")
	MaxInMemory  = 100
	LowWaterMark = 50
)

// NewConfirmationQueue returns an initialized list.
func NewConfirmationQueue() *ConfirmationQueue { return new(ConfirmationQueue).Init() }

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
	return &MessageStruct{}
}

// Init initializes the queue
func (cq *ConfirmationQueue) Init() *ConfirmationQueue {
	// Set the attributes
	viper.SetDefault("queue_directory", "/var/spool/xrootd-monitoring-shoveler/queue")
	queueDir := viper.GetString("queue_directory")

	qName := path.Base(queueDir)
	qDir := path.Dir(queueDir)
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

	cq.emptyCond = sync.NewCond(&cq.mutex)

	// Start the metrics goroutine
	cq.memQueue = list.New()
	go cq.queueMetrics()
	return cq

}

func (cq *ConfirmationQueue) Size() int {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	if cq.usingDisk {
		return cq.diskQueue.SizeUnsafe()
	} else {
		return cq.memQueue.Len()
	}
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
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	// Check size of in memory queue

	// Still using in-memory
	if !cq.usingDisk && (cq.memQueue.Len()+1) < MaxInMemory {
		cq.memQueue.PushBack(msg)
	} else if !cq.usingDisk && (cq.memQueue.Len()+1) >= MaxInMemory {
		// Not using disk queue, but the next message would go over MaxInMemory
		// Transfer everything to the on-disk queue
		for cq.memQueue.Len() > 0 {
			toEnqueue := cq.memQueue.Remove(cq.memQueue.Front()).([]byte)
			err := cq.diskQueue.Enqueue(&MessageStruct{Message: toEnqueue})
			if err != nil {
				log.Errorln("Failed to enqueue message:", err)
			}
		}
		// Enqueue the current
		err := cq.diskQueue.Enqueue(&MessageStruct{Message: msg})
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
		cq.usingDisk = true

	} else {
		// Last option is we are using disk
		err := cq.diskQueue.Enqueue(&MessageStruct{Message: msg})
		if err != nil {
			log.Errorln("Failed to enqueue message:", err)
		}
	}
	cq.emptyCond.Broadcast()
}

// dequeueLocked dequeues a message, assuming the queue has already been locked
func (cq *ConfirmationQueue) dequeueLocked() ([]byte, error) {
	// Check if we have a message available in the queue
	if !cq.usingDisk && cq.memQueue.Len() == 0 {
		return nil, ErrEmpty
	} else if cq.usingDisk && cq.diskQueue.Size() == 0 {
		return nil, ErrEmpty
	}

	if !cq.usingDisk {
		return cq.memQueue.Remove(cq.memQueue.Front()).([]byte), nil
	} else if cq.usingDisk && (cq.diskQueue.Size()-1) >= LowWaterMark {
		// If we are using disk, and the on disk size is larger than the low water mark
		msgStruct, err := cq.diskQueue.Dequeue()
		if err != nil {
			log.Errorln("Failed to dequeue: ", err)
		}
		return msgStruct.(*MessageStruct).Message, err
	} else {
		// Using disk, but the next enqueue makes it < LowWaterMark, transfer everything from on disk to in-memory
		for cq.diskQueue.Size() > 0 {
			msgStruct, err := cq.diskQueue.Dequeue()
			if err != nil {
				log.Errorln("Failed to dequeue: ", err)
			}
			cq.memQueue.PushBack(msgStruct.(*MessageStruct).Message)
		}
		cq.usingDisk = false
		return cq.memQueue.Remove(cq.memQueue.Front()).([]byte), nil
	}

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

// Close will close the on-disk files
func (cq *ConfirmationQueue) Close() error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	return cq.diskQueue.Close()
}
