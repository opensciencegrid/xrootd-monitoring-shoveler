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

	mu       sync.Mutex
	held     *MessageStruct // taken by the feeder, not yet handed to a connection
	inflight map[*StompSession]sending
	taken    uint64 // counts messages handed out, to keep their order on a save
}

// sending is a message a connection is sending, waiting for its receipt.
type sending struct {
	msg   *MessageStruct
	order uint64
}

func newStompOutput(queue *ConfirmationQueue) *stompOutput {
	return &stompOutput{
		queue:    queue,
		messages: make(chan *MessageStruct),
		stop:     make(chan struct{}),
		fed:      make(chan struct{}),
		inflight: map[*StompSession]sending{},
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
		select {
		case o.messages <- msg:
		case <-o.stop:
			// Shutting down: saveAndStop saves it after the ones being sent.
			o.mu.Lock()
			o.held = msg
			o.mu.Unlock()
			return
		}
	}
}

// halt stops the feeder and the connections. It is safe to call twice.
func (o *stompOutput) halt() {
	o.stopOnce.Do(func() { close(o.stop) })
}

// track records the message a connection is about to send.
func (o *stompOutput) track(session *StompSession, msg *MessageStruct) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.taken++
	o.inflight[session] = sending{msg: msg, order: o.taken}
}

// untrack records that the broker confirmed the connection's message.
func (o *stompOutput) untrack(session *StompSession) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.inflight, session)
}

// requeue puts a message whose send failed back at the front of the queue,
// for any connection to send next.
func (o *stompOutput) requeue(session *StompSession, msg *MessageStruct) {
	o.untrack(session)
	o.queue.SaveInFlight([]*MessageStruct{msg})
	StompRequeued.Inc()
}

// sendsInProgress counts the messages being sent.
func (o *stompOutput) sendsInProgress() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.inflight)
}

// saveAndStop stops publishing, waits up to timeout for the sends in progress
// to be confirmed, and saves the rest, in the order they were taken, followed
// by the one the feeder held. A message saved here whose receipt arrives later
// is sent again after the restart.
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
	pending := make([]sending, 0, len(o.inflight))
	for _, s := range o.inflight {
		pending = append(pending, s)
	}
	o.inflight = map[*StompSession]sending{}
	held := o.held
	o.held = nil
	o.mu.Unlock()

	sort.Slice(pending, func(i, j int) bool { return pending[i].order < pending[j].order })
	saved := make([]*MessageStruct, 0, len(pending)+1)
	for _, s := range pending {
		saved = append(saved, s.msg)
	}
	if held != nil {
		saved = append(saved, held)
	}
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
			o.track(session, msg)
			session.publish(msg.Message)
			o.untrack(session)
		}
	}
}
