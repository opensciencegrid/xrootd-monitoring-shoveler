package main

import (
	stomp "github.com/go-stomp/stomp/v3"
	"net/url"
)

func StartStomp(config *Config, queue *ConfirmationQueue) error {

	// TODO: Get the username, password, server, topic from the config
	stompSession := NewStompConnection()

	// Message loop, constantly be dequeing and sending the message
	// No fancy stuff needed
	for {
		msg, err := queue.Dequeue()
		if err != nil {
			return err
		}
		stompSession.publish(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

type StompSession struct {
	Username string
	Password string
	stompUrl url.URL
	Topic    string
	conn     *stomp.Conn
}

func NewStompConnection() *StompSession {
	return &StompSession{}
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() {
	// Close the current session

	// Start a new session
	conn, err := stomp.Dial("tcp", session.stompUrl.String())
}

// publish will send the message to the stomp message bus
// It will also handle any error in sending by calling handleReconnect
func (session *StompSession) publish(msg []byte) error {

	return nil
}
