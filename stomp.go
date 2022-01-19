package main

import (
	"net/url"

	stomp "github.com/go-stomp/stomp/v3"
	log "github.com/sirupsen/logrus"
)

func StartStomp(config *Config, queue *ConfirmationQueue) {

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
			log.Errorln("Failed to read from queue:", err)
			continue
		}
		stompSession.publish(msg)
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
	session.handleReconnect()
	return &session
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() error {
	// Close the current session
	session.conn.Disconnect()

	// Start a new session
	conn, err := stomp.Dial("tcp", session.stompUrl.String(),
		stomp.ConnOpt.Login(session.username, session.password))
	if err != nil {
		return err
	}

	session.conn = conn
	return nil
}

// publish will send the message to the stomp message bus
// It will also handle any error in sending by calling handleReconnect
func (session *StompSession) publish(msg []byte) {
sendMessageLoop:
	for {
		err := session.conn.Send(
			session.topic,
			"text/plain",
			msg)

		if err != nil {
		reconnectLoop:
			for {
				reconnectError := session.handleReconnect()
				if reconnectError == nil {
					break reconnectLoop
				} else {
					log.Errorln("Failed to reconnect:", reconnectError.Error())
				}
			}
		} else {
			break sendMessageLoop
		}
	}
}
