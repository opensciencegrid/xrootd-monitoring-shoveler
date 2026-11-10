package shoveler

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	stomp "github.com/go-stomp/stomp/v3"
)

// StartStomp sends the queued messages to the STOMP broker in stomp.url, over
// one connection to every broker behind the alias with stomp.all_brokers, or
// over one connection otherwise. It runs until StopSTOMP.
func StartStomp(config *Config, queue *ConfirmationQueue) {
	out := newStompOutput(queue)
	if !registerStomp(out) {
		return
	}
	go out.feed()

	session := stompSessionFrom(config)
	if config.StompAllBrokers {
		newStompBrokers(session, out).run()
		return
	}
	out.runSingle(&session)
}

// stompSessionFrom builds an unconnected session from the stomp settings.
func stompSessionFrom(config *Config) StompSession {
	topic := config.StompTopic
	if !strings.HasPrefix(topic, "/topic/") {
		topic = "/topic/" + topic
	}
	return newStompSession(config.StompUser, config.StompPassword, *config.StompURL,
		topic, loadStompCert(config.StompCert, config.StompCertKey)...)
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
	// lost is closed when the current connection stops reading: the broker
	// closed it, or go-stomp did after missed heartbeats.
	lost <-chan struct{}
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
	session := newStompSession(username, password, stompUrl, topic, cert...)
	session.handleReconnect()
	return &session
}

// newStompSession builds an unconnected session.
func newStompSession(username string, password string,
	stompUrl url.URL, topic string, cert ...tls.Certificate) StompSession {
	return StompSession{
		username: username,
		password: password,
		stompUrl: stompUrl,
		topic:    topic,
		cert:     cert,
	}
}

// handleReconnect reconnects to the stomp server
func (session *StompSession) handleReconnect() {
	session.reconnect(nil)
}

// reconnect closes the current session and dials until a new one is open. It
// returns false, with no session open, if stop is closed first; a nil stop never is.
func (session *StompSession) reconnect(stop <-chan struct{}) bool {
	for {
		err := session.connect()
		if err == nil {
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

// connect closes the current session and makes one attempt at a new one.
func (session *StompSession) connect() error {
	session.disconnect()
	conn, err := GetStompConnection(session)
	if err != nil {
		return err
	}
	session.conn = conn
	StompConnections.Inc()
	return nil
}

// watchedConn closes lost when a read fails. go-stomp reads all the time, for
// frames and heartbeats, so a connection that dies while idle is noticed
// without waiting for the next send.
type watchedConn struct {
	net.Conn
	lost chan struct{}
	once sync.Once
}

func (w *watchedConn) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	if err != nil {
		w.once.Do(func() { close(w.lost) })
	}
	return n, err
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
	dialed, err := dialer.Dial("tcp", session.dialAddr())
	if err != nil {
		return nil, err
	}
	netConn := &watchedConn{Conn: dialed, lost: make(chan struct{})}
	_ = netConn.SetDeadline(time.Now().Add(connectTimeout))

	var conn net.Conn = netConn
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
	session.lost = netConn.lost
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
