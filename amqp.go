package main

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// This should run in a new go co-routine.
func StartAMQP(config *Config, queue *ConfirmationQueue) {

	// Get the configuration URL
	amqpURL := config.AmqpURL
	tokenStat, err := os.Stat(config.AmqpToken)
	if err != nil {
		log.Fatalln("Failed to stat token file:", err)
	}
	tokenAge := tokenStat.ModTime()
	tokenContents, err := readToken(config.AmqpToken)
	if err != nil {
		log.Fatalln("Failed to read token, cannot recover")
	}
	// Set the username/password
	amqpURL.User = url.UserPassword("shoveler", tokenContents)
	amqpQueue := New(*amqpURL)

	// Constantly check for new messages
	messagesQueue := make(chan []byte)
	triggerReconnect := make(chan bool)
	go readMsg(messagesQueue, queue)

	go CheckTokenFile(config, tokenAge, triggerReconnect)

	// Listen to the channel for messages
	for {
		select {
		case <-triggerReconnect:
			log.Debugln("Triggering reconnect")
			amqpQueue.newConnection(*amqpURL)
		case msg := <-messagesQueue:
			// Handle a new message to put on the message queue
		TryPush:
			for {
				err = amqpQueue.Push(config.AmqpExchange, msg)
				if err != nil {
					// How to handle a failure to push?
					// The UnsafePush function already should have tried to reconnect
					log.Errorln("Failed to push message:", err)
					// Try again in 1 second
					// Sleep for random amount between 1 and 5 seconds
					// Watch for new token files
					randSleep := rand.Intn(4000) + 1000
					log.Debugln("Sleeping for", randSleep/1000, "seconds")
					select {
					case <-triggerReconnect:
						log.Debugln("Triggering reconnect from within failure")
						amqpQueue.newConnection(*amqpURL)
					case <-time.After(time.Duration(randSleep) * time.Millisecond):
						continue TryPush
					}

				}
				break TryPush
			}
		}
	}
}

// Listen to the channel for messages
func CheckTokenFile(config *Config, tokenAge time.Time, triggerReconnect chan<- bool) {
	// Create a timer to check for changes in the token file ever 10 seconds
	amqpURL := config.AmqpURL
	checkTokenFile := time.NewTicker(10 * time.Second)
	for {
		<-checkTokenFile.C
		log.Debugln("Checking the age of the token file...")
		// Recheck the age of the token file
		tokenStat, err := os.Stat(config.AmqpToken)
		if err != nil {
			log.Fatalln("Failed to stat token file", config.AmqpToken, "error:", err)
		}
		newTokenAge := tokenStat.ModTime()
		if newTokenAge.After(tokenAge) {
			tokenAge = newTokenAge
			log.Debugln("Token file was updated, recreating AMQP connection...")
			// New Token, reload the connection
			tokenContents, err := readToken(config.AmqpToken)
			if err != nil {
				log.Fatalln("Failed to read token, cannot recover")
			}

			// Set the username/password
			amqpURL.User = url.UserPassword("shoveler", tokenContents)
			triggerReconnect <- true

		}

	}
}

// Read a message from the queue
func readMsg(messagesQueue chan<- []byte, queue *ConfirmationQueue) {
	for {
		msg, err := queue.Dequeue()
		if err != nil {
			log.Errorln("Failed to read from queue:", err)
			continue
		}
		messagesQueue <- msg
	}
}

// Read the token from the token location
func readToken(tokenLocation string) (string, error) {
	// Get the token password
	// Read in the token file
	tokenContents, err := ioutil.ReadFile(tokenLocation)
	if err != nil {
		log.Errorln("Unable to read file:", tokenLocation)
		return "", err
	}
	tokenContentsStr := strings.TrimSpace(string(tokenContents))

	return tokenContentsStr, nil
}

// Copied from the amqp documentation at: https://pkg.go.dev/github.com/streadway/amqp
type Session struct {
	url             url.URL
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(url url.URL) *Session {
	session := Session{
		url:  url,
		done: make(chan bool),
	}
	go session.handleReconnect()
	return &session
}

// newConnection will close the current connection, cleaning
// up the go-routines and connections.  Then attempt to reconnect
func (session *Session) newConnection(url url.URL) {
	err := session.Close()
	if err != nil {
		log.Errorln("Failed to close session:", err)
	}
	session.url = url
	go session.handleReconnect()
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect() {
	for {
		session.isReady = false
		log.Debugln("Attempting to connect")

		conn, err := session.connect()
		rabbitmqReconnects.Inc()
		if err != nil {
			log.Warningln("Failed to connect. Retrying:", err.Error())

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect() (*amqp.Connection, error) {
	log.Debugln("Connecting to URL:", session.url.String())
	conn, err := amqp.Dial(session.url.String())

	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	log.Debugln("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReady = false

		err := session.init(conn)

		if err != nil {
			log.Warningln("Failed to initialize channel. Retrying...")

			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case err := <-session.notifyConnClose:
			log.Warningln("Connection closed. Reconnecting...", err)
			return false
		case err := <-session.notifyChanClose:
			log.Warningln("Channel closed. Re-running init...", err)
		}
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}

	session.changeChannel(ch)
	session.isReady = true
	log.Debugln("Setup!")

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (session *Session) Push(exchange string, data []byte) error {
	if !session.isReady {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := session.UnsafePush(exchange, data)
		if err != nil {
			log.Warningln("Push failed. Retrying...")
			select {
			case <-session.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		return nil
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (session *Session) UnsafePush(exchange string, data []byte) error {
	if !session.isReady {
		return errNotConnected
	}
	return session.channel.Publish(
		exchange, // Exchange
		"",       // Routing key
		false,    // Mandatory
		false,    // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !session.isReady {
		return errAlreadyClosed
	}
	close(session.done)
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	session.isReady = false
	return nil
}
