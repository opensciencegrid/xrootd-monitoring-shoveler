package main

import (
	"net/url"
	"crypto/tls"
	"strings"
	"time"

	stomp "github.com/go-stomp/stomp/v3"
	log "github.com/sirupsen/logrus"
)

func StartStomp(config *Config, queue *ConfirmationQueue) {

	// TODO: Get the username, password, server, topic from the config
	stompUser := config.StompUser
	stompPassword := config.StompPassword
	stompUrl := config.StompURL
	stompHost := config.StompHost
	stompTopic := config.StompTopic
	stompTLS := config.StompTLS

	if !strings.HasPrefix(stompTopic, "/topic/") {
		stompTopic = "/topic/" + stompTopic
	}

	stompSession := NewStompConnection(stompUser, stompPassword,
		*stompUrl, stompHost, stompTopic, stompTLS)

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
        stompHost string
	topic    string
	conn     *stomp.Conn
        stompTLS bool
}

func NewStompConnection(username string, password string,
	stompUrl url.URL, stompHost string, topic string, stompTLS bool) *StompSession {
	session := StompSession{
		username: username,
		password: password,
		stompUrl: stompUrl,
		stompHost: stompHost,
		topic:    topic,
		stompTLS: stompTLS,
	}

	session.handleReconnect()

	return &session
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() {
	// Close the current session
	if session.conn != nil {
		err := session.conn.Disconnect()
		if err != nil {
			log.Errorln("Error handling the diconnection:", err.Error())
		}
	}

reconnectLoop:
	for {

                if session.stompTLS {
			netConn, err := tls.Dial("tcp", session.stompUrl.String(), &tls.Config{})
			if err != nil {
                        	log.Errorln("Failed to reconnect, retrying:", err.Error())
				<-time.After(reconnectDelay)
				continue
			}
			stompConn, err := stomp.Connect(netConn,
					stomp.ConnOpt.Login(session.username, session.password),
					stomp.ConnOpt.Host(session.stompHost))
			if err != nil {
				log.Errorln("Failed to reconnect, retrying:", err.Error())
				<-time.After(reconnectDelay)
			} else {
				session.conn = stompConn
			}

                } else {

			// Start a new session
			conn, err := stomp.Dial("tcp", session.stompUrl.String(),
				stomp.ConnOpt.Login(session.username, session.password),
                        	stomp.ConnOpt.Host(session.stompHost))

			if err == nil {
				session.conn = conn
				break reconnectLoop
			} else {
				log.Errorln("Failed to reconnect, retrying:", err.Error())
				<-time.After(reconnectDelay)
			}
		}
	}
}

// publish will send the message to the stomp message bus
// It will also handle any error in sending by calling handleReconnect
func (session *StompSession) publish(msg []byte) {
sendMessageLoop:
	for {
		err := session.conn.Send(
			session.topic,
			"text/plain",
			msg,
			stomp.SendOpt.Receipt)

		if err != nil {
			log.Errorln("Failed to publish message:", err)
			session.handleReconnect()
		} else {
			break sendMessageLoop
		}
	}
}
