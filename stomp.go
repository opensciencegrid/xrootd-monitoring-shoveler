package shoveler

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	stomp "github.com/go-stomp/stomp/v3"
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

	messagesQueue := make(chan *MessageStruct)
	go readMsgStomp(messagesQueue, queue)

	if config.StompAllBrokers {
		// One sender per broker behind the alias, all taking from the same queue
		template := StompSession{username: stompUser, password: stompPassword,
			stompUrl: *stompUrl, topic: stompTopic, cert: loadStompCert(stompCert, stompCertKey)}
		newStompBrokers(template, messagesQueue, queue).run()
		return
	}

	stompSession := GetNewStompConnection(stompUser, stompPassword,
		*stompUrl, stompTopic, stompCert, stompCertKey)

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// Message loop, constantly be dequeing and sending the message
	for {
		select {
		// Add reconnection every hour to make sure connection to brokers is kept balanced
		case <-ticker.C:
			stompSession.handleReconnect()
		case msgStruct := <-messagesQueue:
			// Note: STOMP doesn't support multiple topics/destinations per session
			// so we just use the Message and ignore the Exchange field
			stompSession.publish(msgStruct.Message)
		}
	}
}

func GetNewStompConnection(username string, password string,
	stompUrl url.URL, topic string, stompCert string, stompCertKey string) *StompSession {
	return NewStompConnection(username, password,
		stompUrl, topic, loadStompCert(stompCert, stompCertKey)...)
}

// loadStompCert loads the client certificate, or returns none when it is not configured
func loadStompCert(stompCert string, stompCertKey string) []tls.Certificate {
	if stompCert == "" || stompCertKey == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(stompCert, stompCertKey)
	if err != nil {
		log.Errorln("Failed to load certificate:", err)
	}
	return []tls.Certificate{cert}
}

type StompSession struct {
	username string
	password string
	stompUrl url.URL
	topic    string
	cert     []tls.Certificate
	conn     *stomp.Conn
	// addr is the broker to dial, host:port. Empty means stompUrl, which leaves the
	// choice of broker behind the alias to DNS.
	addr string
	// serverName is the name the broker's TLS certificate is checked against when
	// addr is an IP address: the alias in stompUrl.
	serverName string
}

// dialAddr is the address this session connects to
func (session *StompSession) dialAddr() string {
	if session.addr != "" {
		return session.addr
	}
	return session.stompUrl.String()
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

func readMsgStomp(messagesQueue chan<- *MessageStruct, queue *ConfirmationQueue) {
	for {
		msgStruct, err := queue.Dequeue()
		if err != nil {
			log.Errorln("Failed to read from queue:", err)
			continue
		}
		messagesQueue <- msgStruct
	}
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() {
	session.reconnect(nil)
}

// reconnect closes the current session and dials until a new one is open. It
// returns false, with no session open, if stop is closed first; a nil stop never is.
func (session *StompSession) reconnect(stop <-chan struct{}) bool {
	session.disconnect()

	for {
		// Start a new session
		conn, err := GetStompConnection(session)
		if err == nil {
			session.conn = conn
			StompConnections.Inc()
			return true
		}
		log.Errorln("Failed to reconnect to", session.dialAddr()+", retrying:", err.Error())
		select {
		case <-stop:
			return false
		case <-time.After(reconnectDelay):
		}
	}
}

// disconnect closes the current session, if there is one
func (session *StompSession) disconnect() {
	if session.conn == nil {
		return
	}
	err := session.conn.Disconnect()
	if err != nil {
		log.Errorln("Error handling the disconnection:", err.Error())
	}
	session.conn = nil
	StompConnections.Dec()
}

// connectTimeout bounds connecting to a broker: the TCP dial, the TLS handshake
// and the STOMP CONNECT. go-stomp sets no deadline on them, so a broker that
// takes the connection but never answers would block the reconnect forever. A
// var so tests can shorten it.
var connectTimeout = 30 * time.Second

func GetStompConnection(session *StompSession) (*stomp.Conn, error) {
	dialer := net.Dialer{Timeout: connectTimeout}
	netConn, err := dialer.Dial("tcp", session.dialAddr())
	if err != nil {
		return nil, err
	}
	_ = netConn.SetDeadline(time.Now().Add(connectTimeout))

	conn := netConn
	var opts []func(*stomp.Conn) error
	if session.cert != nil {
		serverName := session.serverName
		if serverName == "" {
			serverName, _, _ = net.SplitHostPort(session.dialAddr())
		}
		tlsConn := tls.Client(netConn, &tls.Config{Certificates: session.cert, ServerName: serverName})
		if err := tlsConn.Handshake(); err != nil {
			_ = netConn.Close()
			return nil, fmt.Errorf("failed to connect using TLS: %w", err)
		}
		conn = tlsConn
	} else {
		// As stomp.Dial does: the host header is the broker's address
		host, _, _ := net.SplitHostPort(netConn.RemoteAddr().String())
		opts = append(opts, stomp.ConnOpt.Host(host), stomp.ConnOpt.Login(session.username, session.password))
	}

	stompConn, err := stomp.Connect(conn, opts...)
	if err != nil {
		_ = conn.Close() // stomp.Connect leaves it open on failure
		return nil, err
	}
	_ = netConn.SetDeadline(time.Time{})
	return stompConn, nil
}

// publish will send the message to the stomp message bus
// It will also handle any error in sending by calling handleReconnect
func (session *StompSession) publish(msg []byte) {
sendMessageLoop:
	for {
		err := session.send(msg)

		if err != nil {
			log.Errorln("Failed to publish message:", err)
			session.handleReconnect()
		} else {
			MessagesPublished.Inc()
			break sendMessageLoop
		}
	}
}

// send sends one message and waits for the broker's receipt
func (session *StompSession) send(msg []byte) error {
	return session.conn.Send(
		session.topic,
		"text/plain",
		msg,
		stomp.SendOpt.Receipt)
}
