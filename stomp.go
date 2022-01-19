package main

import (
	"net/url"

	stomp "github.com/go-stomp/stomp/v3"
	log "github.com/sirupsen/logrus"
)

func StartStomp(config *Config, queue *ConfirmationQueue) error {

	// TODO: Get the username, password, server, topic from the config
	stompUser := config.StompUser
	stompPassword := config.StompPassword
	stompUrl := config.StompURL
	stompTopic := config.StompTopic

	stompSession := NewStompConnection(stompUser, stompPassword,
		*stompUrl, stompTopic)

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

}

type StompSession struct {
	username string
	password string
	stompUrl url.URL
	topic    string
	conn     *stomp.Conn
}

func NewStompConnection(username string, password string,
	stompUrl url.URL, topic string) *StompSession {
	session := StompSession{
		username: username,
		password: password,
		stompUrl: stompUrl,
		topic:    topic,
	}
	go session.handleReconnect()
	return &session
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() {
	// Close the current session
	session.conn.Disconnect()

	// Start a new session
	conn, err := stomp.Dial("tcp", session.stompUrl.String(),
		stomp.ConnOpt.Login(session.username, session.password))
	if err != nil {
		log.Errorln("Failed to connect. Retrying:", err.Error)
	}

	session.conn = conn
}

// publish will send the message to the stomp message bus
// It will also handle any error in sending by calling handleReconnect
func (session *StompSession) publish(msg []byte) error {
	err := session.conn.Send(
		session.topic,
		"text/plain",
		msg)

	return err
}
