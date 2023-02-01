package main

import (
	"net/url"
	"strings"
	"time"
  "crypto/tls"

	stomp "github.com/go-stomp/stomp/v3"
	log "github.com/sirupsen/logrus"
)

func StartStomp(config *Config, queue *ConfirmationQueue) {

	// TODO: Get the username, password, server, topic from the config
	stompUser := config.StompUser
	stompPassword := config.StompPassword
	stompUrl := config.StompURL
	stompTopic := config.StompTopic
  stompCert := config.StompCert
  stompCertKey := config.StompCertKey

	if !strings.HasPrefix(stompTopic, "/topic/") {
		stompTopic = "/topic/" + stompTopic
	}

  stompSession := GetNewStompConnection(stompUser, stompPassword,
    *stompUrl, stompTopic, stompCert, stompCertKey)

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

func GetNewStompConnection(username string, password string,
  stompUrl url.URL, topic string, stompCert string, stompCertKey string) *StompSession {
  if stompCert != "" && stompCertKey != "" {
    cert, err := tls.LoadX509KeyPair(stompCert, stompCertKey)
    if err != nil {
      log.Errorln("Failed to load certificate:", err)
    }

    return NewStompConnection(username, password,
    stompUrl, topic, cert)
  } else {
    return NewStompConnection(username, password,
    stompUrl, topic)
  }
}

type StompSession struct {
	username string
	password string
	stompUrl url.URL
	topic    string
  cert     []tls.Certificate
	conn     *stomp.Conn
}

func NewStompConnection(username string, password string,
	stompUrl url.URL, topic string, cert ...tls.Certificate) *StompSession {
	session := StompSession{
		username: username,
		password: password,
		stompUrl: stompUrl,
		topic:    topic,
    cert:     cert,
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
		// Start a new session
		conn, err := GetStompConnection(session)
		if err == nil {
			session.conn = conn
			break reconnectLoop
		} else {
			log.Errorln("Failed to reconnect, retrying:", err.Error())
			<-time.After(reconnectDelay)
		}
	}
}

func GetStompConnection(session *StompSession) (*stomp.Conn, error) {
  if session.cert != nil {
    netConn, err := tls.Dial("tcp", session.stompUrl.String(), &tls.Config{Certificates: session.cert})
    if err != nil {
      log.Errorln("Failed to connect using TLS:", err.Error())
    }
    return stomp.Connect(netConn)
  } 
  cfg := stomp.ConnOpt.Login(session.username, session.password)
  return stomp.Dial("tcp", session.stompUrl.String(), cfg)
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
