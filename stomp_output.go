package shoveler

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// stompOutput is the running STOMP publisher: the feeder that takes messages
// from the queue and the connections that send them. It keeps track of every
// message taken from the queue and not yet confirmed by a broker, so a
// shutdown can put them back at the front of the queue.
type stompOutput struct {
	queue    *ConfirmationQueue
	messages chan *MessageStruct
	stop     chan struct{} // closed to stop the feeder and every connection
	stopOnce sync.Once
	fed      chan struct{} // closed when the feeder has returned

	mu sync.Mutex
	// unconfirmed holds every message taken from the queue and not yet
	// confirmed by a broker, by the order it was taken: from the feeder taking
	// it, through the handoff, until the receipt arrives.
	unconfirmed map[*MessageStruct]uint64
	taken       uint64
	// parked are unconfirmed messages nothing is sending any more: the one the
	// feeder still had when it stopped, or one a connection stopped retrying.
	parked map[*MessageStruct]bool
	saved  bool // saveAndStop has saved the unconfirmed messages
}

func newStompOutput(queue *ConfirmationQueue) *stompOutput {
	return &stompOutput{
		queue:    queue,
		messages: make(chan *MessageStruct),
		stop:     make(chan struct{}),
		fed:      make(chan struct{}),

		unconfirmed: map[*MessageStruct]uint64{},
		parked:      map[*MessageStruct]bool{},
	}
}

// stompRunning holds the started STOMP publisher so StopSTOMP can save the
// messages it holds on shutdown.
var stompRunning struct {
	sync.Mutex
	out      *stompOutput
	stopping bool
}

// registerStomp records a started publisher; false if shutdown has begun.
func registerStomp(out *stompOutput) bool {
	stompRunning.Lock()
	defer stompRunning.Unlock()
	if stompRunning.stopping {
		return false
	}
	stompRunning.out = out
	return true
}

// StopSTOMP stops STOMP publishing and saves the messages it holds (taken
// from the queue but not yet confirmed by a broker) back to the front of the
// queue, so they are sent first after a restart. It waits up to timeout for
// the sends in progress to finish. Close the queue after it returns.
func StopSTOMP(timeout time.Duration) {
	stompRunning.Lock()
	stompRunning.stopping = true
	out := stompRunning.out
	stompRunning.Unlock()
	if out != nil {
		out.saveAndStop(timeout)
	}
}

// feed hands messages from the queue to the connections until stopped.
func (o *stompOutput) feed() {
	defer close(o.fed)
	for {
		select {
		case <-o.stop:
			return
		default:
		}
		msg, err := o.queue.Dequeue()
		if errors.Is(err, ErrInterrupted) {
			return
		}
		if err != nil {
			log.Errorln("Failed to read from queue:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if !o.take(msg) {
			return
		}
		select {
		case o.messages <- msg:
		case <-o.stop:
			// Shutting down: saveAndStop saves it after the ones being sent.
			o.park(msg)
			return
		}
	}
}

// halt stops the feeder and the connections. It is safe to call twice.
func (o *stompOutput) halt() {
	o.stopOnce.Do(func() { close(o.stop) })
}

// take records a message taken from the queue. Once a shutdown has saved the
// unconfirmed messages, it puts the message straight back and returns false.
func (o *stompOutput) take(msg *MessageStruct) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.saved {
		o.queue.SaveInFlight([]*MessageStruct{msg})
		return false
	}
	o.taken++
	o.unconfirmed[msg] = o.taken
	return true
}

// confirm records that a broker confirmed the message.
func (o *stompOutput) confirm(msg *MessageStruct) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.unconfirmed, msg)
}

// park records an unconfirmed message nothing is sending any more, so a
// shutdown saves it without waiting for it.
func (o *stompOutput) park(msg *MessageStruct) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.parked[msg] = true
}

// requeue puts a message whose send failed back at the front of the queue,
// for any connection to send next. After a shutdown saved it, the message is
// already in the queue. The lock is held through the write, so a shutdown
// either waits for it or saves the message itself.
func (o *stompOutput) requeue(msg *MessageStruct) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.saved {
		return
	}
	o.queue.SaveInFlight([]*MessageStruct{msg})
	delete(o.unconfirmed, msg)
	StompRequeued.Inc()
}

// sendsInProgress counts the unconfirmed messages a connection is sending.
func (o *stompOutput) sendsInProgress() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.unconfirmed) - len(o.parked)
}

// saveAndStop stops publishing and waits up to timeout for the sends in
// progress. Then it saves the unconfirmed messages, parked ones included, in
// the order they were taken. They go ahead of messages requeued earlier.
// A message saved here whose receipt arrives later is sent again after the
// restart.
func (o *stompOutput) saveAndStop(timeout time.Duration) {
	o.halt()
	o.queue.Wake() // a feeder waiting on an empty queue returns
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-o.fed:
		default:
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if o.sendsInProgress() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	o.mu.Lock()
	saved := make([]*MessageStruct, 0, len(o.unconfirmed))
	for msg := range o.unconfirmed {
		saved = append(saved, msg)
	}
	order := o.unconfirmed
	sort.Slice(saved, func(i, j int) bool { return order[saved[i]] < order[saved[j]] })
	o.unconfirmed = map[*MessageStruct]uint64{}
	o.parked = map[*MessageStruct]bool{}
	o.saved = true
	o.mu.Unlock()

	if len(saved) > 0 {
		log.Infoln("Saving", len(saved), "unconfirmed STOMP messages to the queue")
		o.queue.SaveInFlight(saved)
	}
}

// runSingle sends every message over one connection to the broker DNS picks,
// and reconnects every hour so the connections stay spread over the brokers.
func (o *stompOutput) runSingle(session *StompSession) {
	defer session.disconnect()
	if !session.reconnect(o.stop) {
		return
	}
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-ticker.C:
			if !session.reconnect(o.stop) {
				return
			}
		case msg := <-o.messages:
			// STOMP sends to one destination per session, so the Exchange
			// field is unused here.
			if !session.publish(msg.Message, o.stop) {
				o.park(msg)
				return
			}
			o.confirm(msg)
		}
	}
}
