package shoveler

import (
	"context"
	"errors"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// WorkerPool manages multiple publishing workers
type WorkerPool struct {
	config        *Config
	queue         *ConfirmationQueue
	workers       []*PublishWorker
	tokenAge      time.Time
	useToken      bool
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	messagesQueue chan *MessageStruct // Shared channel for all workers
	wg            sync.WaitGroup
}

// PublishWorker handles publishing messages to AMQP
type PublishWorker struct {
	id            int
	config        *Config
	amqpURL       url.URL
	session       *Session
	messagesQueue chan *MessageStruct // Reference to shared channel
	ctx           context.Context
	wg            sync.WaitGroup
}

// This should run in a new go co-routine.
func StartAMQP(config *Config, queue *ConfirmationQueue) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &WorkerPool{
		config:        config,
		queue:         queue,
		workers:       make([]*PublishWorker, 0, config.AmqpPublishWorkers),
		ctx:           ctx,
		cancel:        cancel,
		messagesQueue: make(chan *MessageStruct, 1000), // Shared buffered channel
	}

	// Check if we need to use tokens
	if config.AmqpURL.User == nil {
		pool.useToken = true
		tokenStat, err := os.Stat(config.AmqpToken)
		if err != nil {
			log.Fatalln("Failed to stat token file:", err)
		}
		pool.tokenAge = tokenStat.ModTime()
		tokenContents, err := readToken(config.AmqpToken)
		if err != nil {
			log.Fatalln("Failed to read token, cannot recover")
		}
		// Set the username/password in a copy of the URL
		amqpURL := copyURL(config.AmqpURL)
		amqpURL.User = url.UserPassword("shoveler", tokenContents)
		config.AmqpURL = amqpURL
	} else {
		log.Debugln("Using credentials from AMQP URL, skipping token file")
	}

	// Start worker pool
	pool.Start()

	// Monitor token file for changes if using tokens
	if pool.useToken {
		go pool.CheckTokenFile()
	}

	// Keep the main routine running
	<-pool.ctx.Done()
	pool.Stop()
}

// Start initializes and starts all workers
func (p *WorkerPool) Start() {
	// In shoveler mode, only use 1 worker to preserve message ordering
	workerCount := p.config.AmqpPublishWorkers
	if p.config.Mode == "shoveler" {
		workerCount = 1
		log.Infof("Starting AMQP worker pool with 1 worker (shoveler mode - message ordering required)")
	} else {
		log.Infof("Starting AMQP worker pool with %d workers", workerCount)
	}

	for i := 0; i < workerCount; i++ {
		worker := &PublishWorker{
			id:            i,
			config:        p.config,
			amqpURL:       *copyURL(p.config.AmqpURL),
			messagesQueue: p.messagesQueue, // Share the pool's channel
			ctx:           p.ctx,
		}
		p.workers = append(p.workers, worker)
		worker.Start()
	}

	// Start feeding messages to the shared queue
	p.wg.Add(1)
	go p.feedMessages()
}

// Stop gracefully shuts down all workers
func (p *WorkerPool) Stop() {
	log.Infoln("Stopping AMQP worker pool")
	p.cancel()

	for _, worker := range p.workers {
		worker.Stop()
	}

	p.wg.Wait()
	close(p.messagesQueue)
	log.Infoln("AMQP worker pool stopped")
}

// Restart stops all workers and starts new ones with updated credentials
func (p *WorkerPool) Restart(newURL *url.URL) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Infoln("Restarting AMQP worker pool with new credentials")

	// Cancel old context to stop all workers
	p.cancel()

	// Wait for all workers to finish
	for _, worker := range p.workers {
		worker.wg.Wait()
	}

	// Update config URL
	p.config.AmqpURL = newURL

	// Create new context for new workers
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel

	// Clear old workers
	p.workers = make([]*PublishWorker, 0, p.config.AmqpPublishWorkers)

	// Start new workers with updated credentials
	// In shoveler mode, only use 1 worker to preserve message ordering
	workerCount := p.config.AmqpPublishWorkers
	if p.config.Mode == "shoveler" {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		worker := &PublishWorker{
			id:            i,
			config:        p.config,
			amqpURL:       *copyURL(p.config.AmqpURL),
			messagesQueue: p.messagesQueue, // Share the pool's channel
			ctx:           p.ctx,
		}
		p.workers = append(p.workers, worker)
		worker.Start()
	}

	log.Infoln("AMQP worker pool restarted")
}

// feedMessages reads from the queue and feeds the shared message channel
func (p *WorkerPool) feedMessages() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			msgStruct, err := p.queue.Dequeue()
			if err != nil {
				log.Errorln("Failed to read from queue:", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Send to shared channel - all workers compete for messages
			select {
			case p.messagesQueue <- msgStruct:
				// Message sent successfully
			case <-p.ctx.Done():
				return
			}
		}
	}
}

// CheckTokenFile monitors token file for changes
func (p *WorkerPool) CheckTokenFile() {
	checkTokenFile := time.NewTicker(10 * time.Second)
	defer checkTokenFile.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-checkTokenFile.C:
			log.Debugln("Checking the age of the token file...")
			tokenStat, err := os.Stat(p.config.AmqpToken)
			if err != nil {
				log.Fatalln("Failed to stat token file", p.config.AmqpToken, "error:", err)
			}

			newTokenAge := tokenStat.ModTime()
			if newTokenAge.After(p.tokenAge) {
				p.tokenAge = newTokenAge
				log.Infoln("Token file was updated, recreating AMQP connections...")

				// Read new token
				tokenContents, err := readToken(p.config.AmqpToken)
				if err != nil {
					log.Fatalln("Failed to read token, cannot recover")
				}

				// Create new URL with updated credentials
				newURL := copyURL(p.config.AmqpURL)
				newURL.User = url.UserPassword("shoveler", tokenContents)

				// Restart workers with new credentials
				p.Restart(newURL)
			}
		}
	}
}

// Start starts the worker
func (w *PublishWorker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop waits for the worker to finish
func (w *PublishWorker) Stop() {
	w.wg.Wait()
}

// run is the main worker loop
func (w *PublishWorker) run() {
	defer w.wg.Done()

	log.Debugf("Worker %d: Starting with own AMQP connection", w.id)

	// Create own AMQP session
	w.session = New(w.amqpURL)
	defer func() {
		if w.session != nil {
			w.session.Close()
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			log.Debugf("Worker %d: Stopping", w.id)
			return
		case msgStruct := <-w.messagesQueue:
			w.publishMessage(msgStruct)
		}
	}
}

// publishMessage publishes a single message with retry logic
func (w *PublishWorker) publishMessage(msgStruct *MessageStruct) {
	// Use specific exchange if provided, otherwise use default
	exchange := w.config.AmqpExchange
	if msgStruct.Exchange != "" {
		exchange = msgStruct.Exchange
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			err := w.session.Push(exchange, msgStruct.Message)
			if err != nil {
				log.Warningf("Worker %d: Failed to push message: %v", w.id, err)
				// Random backoff between 1-5 seconds
				randSleep := rand.Intn(4000) + 1000
				select {
				case <-w.ctx.Done():
					return
				case <-time.After(time.Duration(randSleep) * time.Millisecond):
					continue
				}
			}
			// Successfully published
			return
		}
	}
}

// copyURL creates a deep copy of a URL
func copyURL(original *url.URL) *url.URL {
	if original == nil {
		return nil
	}
	copy := *original
	if original.User != nil {
		userInfo := *original.User
		copy.User = &userInfo
	}
	return &copy
}

// Read the token from the token location
func readToken(tokenLocation string) (string, error) {
	// Get the token password
	// Read in the token file
	tokenContents, err := os.ReadFile(tokenLocation)
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

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect() {
	for {
		session.isReady = false
		log.Debugln("Attempting to connect")

		conn, err := session.connect()
		RabbitmqReconnects.Inc()
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
