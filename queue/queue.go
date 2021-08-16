package queue

import (
	"container/list"
)

type MessageQueue struct {
	insert chan []byte
	Receive chan []byte
	l *list.List
}

func (mq *MessageQueue) Init() *MessageQueue {

	mq.insert = make(chan []byte)
	mq.Receive = make(chan []byte, 2)

	mq.l = list.New()

	// Start the event loop
	go mq.EventLoop()
	return mq
}

// New returns an initialized list.
func New() *MessageQueue { return new(MessageQueue).Init() }

func (mq *MessageQueue) EventLoop() {
	successfulSent := make(chan bool)
	pendingMsgs := 0

	for {
		select {
		case receivedMsg := <-mq.insert:
			mq.l.PushBack(receivedMsg)
			mq.trySendMsg(&pendingMsgs, successfulSent)

		case <-successfulSent:
			pendingMsgs -= 1
			if (mq.l.Len() > 0) {
				mq.trySendMsg(&pendingMsgs, successfulSent)
			}
		}

	}
}

func (mq *MessageQueue) trySendMsg(pendingMsgs *int, successfulSent chan<- bool) {
	if (*pendingMsgs < 1) {
		*pendingMsgs += 1
		e := mq.l.Front()
		mq.l.Remove(e)
		go SendMsg(mq.Receive, e.Value.([]byte), successfulSent)
	}
}

// SendMsg
// Send the message to the sendChannel.  After, send a boolean message
// to the success channel.
// This function is designed to run inside it's own go routine.
func SendMsg(sendChannel chan<- []byte, msg []byte, success chan<- bool) {
	sendChannel <- msg
	success <- true
}

func (mq *MessageQueue) Insert(msg []byte) {
	mq.insert <- msg
}
