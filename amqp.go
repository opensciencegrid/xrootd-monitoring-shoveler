package shoveler

import (
	"context"
	"errors"
	"fmt"
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
	config *Config
	queue  *ConfirmationQueue
	// url is the broker URL with the current credentials. It is kept here,
	// not written back to config.AmqpURL, so other readers of the config
	// (e.g. the collector's input) never see the output's token.
	url      *url.URL
	workers  []*PublishWorker
	tokenAge time.Time
	useToken bool
	mu       sync.RWMutex

	// ctx lives as long as the pool. The feeder and the token watcher run on
	// it; Stop cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	// workersCancel stops the current generation of workers. Restart (on a
	// token rotation) replaces only the workers, so the feeder, the shared
	// channel and StartAMQP are unaffected.
	workersCancel context.CancelFunc

	messagesQueue chan *MessageStruct // Shared channel for all workers
	// retry holds messages a stopped worker was still publishing; the next
	// generation publishes them before anything else, so a rotation neither
	// loses nor reorders them.
	retry chan *MessageStruct
	wg    sync.WaitGroup
}

// PublishWorker handles publishing messages to AMQP
type PublishWorker struct {
	id            int
	config        *Config
	amqpURL       url.URL
	session       *Session
	messagesQueue chan *MessageStruct // Reference to shared channel
	retry         chan *MessageStruct // Reference to the pool's retry channel
	ctx           context.Context
	wg            sync.WaitGroup
}

// This should run in a new go co-routine.
func StartAMQP(config *Config, queue *ConfirmationQueue) {
	pool := newWorkerPool(config, queue)
	if err := pool.loadCredentials(); err != nil {
		log.Fatalln(err)
	}

	// Start worker pool
	pool.Start()

	// Monitor token file for changes if using tokens
	if pool.useToken {
		go pool.CheckTokenFile()
	}

	// Keep the main routine running. Only Stop ends the pool; a token
	// rotation replaces the workers without touching pool.ctx.
	<-pool.ctx.Done()
}

// loadCredentials puts the token from config.AmqpToken into the pool's URL,
// unless config.AmqpURL already carries credentials.
func (p *WorkerPool) loadCredentials() error {
	if p.url.User != nil {
		log.Debugln("Using credentials from AMQP URL, skipping token file")
		return nil
	}
	p.useToken = true
	tokenStat, err := os.Stat(p.config.AmqpToken)
	if err != nil {
		return fmt.Errorf("failed to stat token file: %w", err)
	}
	p.tokenAge = tokenStat.ModTime()
	tokenContents, err := readToken(p.config.AmqpToken)
	if err != nil {
		return fmt.Errorf("failed to read token, cannot recover: %w", err)
	}
	p.url.User = url.UserPassword("shoveler", tokenContents)
	return nil
}

// newWorkerPool creates a pool; Start launches its workers and feeder.
func newWorkerPool(config *Config, queue *ConfirmationQueue) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		config:        config,
		url:           copyURL(config.AmqpURL),
		queue:         queue,
		workers:       make([]*PublishWorker, 0, config.AmqpPublishWorkers),
		ctx:           ctx,
		cancel:        cancel,
		messagesQueue: make(chan *MessageStruct, 1000), // Shared buffered channel
		retry:         make(chan *MessageStruct, config.AmqpPublishWorkers+1),
	}
}

// workerCount is the number of publishing workers to run.
func (p *WorkerPool) workerCount() int {
	// In shoveler mode, only use 1 worker to preserve message ordering
	if p.config.Mode == "shoveler" {
		return 1
	}
	return p.config.AmqpPublishWorkers
}

// startWorkers starts a new generation of workers using the current p.url.
// The caller holds p.mu (or has exclusive access).
func (p *WorkerPool) startWorkers() {
	ctx, cancel := context.WithCancel(p.ctx)
	p.workersCancel = cancel
	p.workers = make([]*PublishWorker, 0, p.workerCount())
	for i := 0; i < p.workerCount(); i++ {
		worker := &PublishWorker{
			id:            i,
			config:        p.config,
			amqpURL:       *copyURL(p.url),
			messagesQueue: p.messagesQueue, // Share the pool's channel
			retry:         p.retry,
			ctx:           ctx,
		}
		p.workers = append(p.workers, worker)
		worker.Start()
	}
}

// stopWorkers stops the current generation of workers and waits for them.
// The caller holds p.mu (or has exclusive access).
func (p *WorkerPool) stopWorkers() {
	if p.workersCancel != nil {
		p.workersCancel()
	}
	for _, worker := range p.workers {
		worker.Stop()
	}
}

// Start initializes and starts all workers
func (p *WorkerPool) Start() {
	if p.config.Mode == "shoveler" {
		log.Infof("Starting AMQP worker pool with 1 worker (shoveler mode - message ordering required)")
	} else {
		log.Infof("Starting AMQP worker pool with %d workers", p.workerCount())
	}

	p.mu.Lock()
	p.startWorkers()
	p.mu.Unlock()

	// Start feeding messages to the shared queue. The feeder lives as long
	// as the pool, across token rotations.
	p.wg.Add(1)
	go p.feedMessages()
}

// Stop gracefully shuts down all workers. Messages still in the shared
// channel are not published; the shoveler is exiting.
func (p *WorkerPool) Stop() {
	log.Infoln("Stopping AMQP worker pool")
	p.cancel()

	p.mu.Lock()
	p.stopWorkers()
	p.mu.Unlock()

	p.wg.Wait()
	log.Infoln("AMQP worker pool stopped")
}

// Restart stops all workers and starts new ones with updated credentials.
// The feeder, the shared channel and the pool context are left running, and
// any message a worker was still publishing is handed to the new workers.
func (p *WorkerPool) Restart(newURL *url.URL) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ctx.Err() != nil {
		return // the pool is stopping
	}

	log.Infoln("Restarting AMQP worker pool with new credentials")

	p.stopWorkers()

	p.url = newURL

	// Start new workers with updated credentials
	p.startWorkers()

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
				// Shutting down: keep the message on disk for next time.
				p.queue.EnqueueToExchange(msgStruct.Message, msgStruct.Exchange)
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
				newURL := copyURL(p.url)
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
			if err := w.session.Close(); err != nil {
				log.Debugf("Worker %d: Error closing session: %v", w.id, err)
			}
		}
	}()

	for {
		// Once stopped, leave handed-back messages for the next generation.
		if w.ctx.Err() != nil {
			log.Debugf("Worker %d: Stopping", w.id)
			return
		}
		// Messages a previous generation of workers didn't finish go first.
		select {
		case msgStruct := <-w.retry:
			w.publishMessage(msgStruct)
			continue
		default:
		}

		select {
		case <-w.ctx.Done():
			log.Debugf("Worker %d: Stopping", w.id)
			return
		case msgStruct := <-w.retry:
			w.publishMessage(msgStruct)
		case msgStruct := <-w.messagesQueue:
			w.publishMessage(msgStruct)
		}
	}
}

// handBack returns a message this worker could not finish publishing, so
// the next generation of workers publishes it first.
func (w *PublishWorker) handBack(msgStruct *MessageStruct) {
	select {
	case w.retry <- msgStruct:
	default:
		// Cannot happen while each worker hands back at most one message;
		// don't block shutdown if it somehow does.
		log.Errorf("Worker %d: dropping a message that was being published during shutdown", w.id)
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
			w.handBack(msgStruct)
			return
		default:
			err := w.session.Push(w.ctx, exchange, msgStruct.Message)
			if err != nil {
				log.Warningf("Worker %d: Failed to push message: %v", w.id, err)
				// Random backoff between 1-5 seconds
				randSleep := rand.Intn(4000) + 1000
				select {
				case <-w.ctx.Done():
					w.handBack(msgStruct)
					return
				case <-time.After(time.Duration(randSleep) * time.Millisecond):
					continue
				}
			}
			// The broker confirmed the message
			MessagesPublished.Inc()
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
	pub             *publisher // the channel's confirm tracking
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	isReady         bool
	mu              sync.Mutex // guards isReady, connection, channel, pub
	closeOnce       sync.Once
	stopped         chan struct{} // closed when handleReconnect returns
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
		url:     url,
		done:    make(chan bool),
		stopped: make(chan struct{}),
	}
	go session.handleReconnect()
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect() {
	defer close(session.stopped)
	for {
		session.setReady(false)
		select {
		case <-session.done:
			return
		default:
		}
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
			// Close may have run before this connection was ready, so
			// it can't rely on Close to shut it.
			_ = conn.Close()
			return
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect() (*amqp.Connection, error) {
	log.Debugln("Connecting to URL:", session.url.Redacted())
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
		session.setReady(false)

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
	session.setReady(true)
	log.Debugln("Setup!")

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.mu.Lock()
	session.connection = connection
	session.mu.Unlock()
	session.notifyConnClose = make(chan *amqp.Error)
	connection.NotifyClose(session.notifyConnClose)
}

// setReady records whether the channel is usable for publishing.
func (session *Session) setReady(ready bool) {
	session.mu.Lock()
	session.isReady = ready
	session.mu.Unlock()
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.notifyChanClose = make(chan *amqp.Error)
	// Room for late confirms of resent messages, so the channel's reader
	// never blocks on this listener.
	confirms := make(chan amqp.Confirmation, 16)
	channel.NotifyClose(session.notifyChanClose)
	channel.NotifyPublish(confirms)
	session.mu.Lock()
	session.channel = channel
	session.pub = &publisher{channel: channel, confirms: confirms}
	session.mu.Unlock()
}

// publisher is one confirm-mode channel and the delivery tags assigned on it.
// The broker numbers a channel's publishes 1, 2, 3, ... and confirms them by
// that tag; lastTag mirrors the count. Only the session's single publishing
// goroutine (its worker) uses it.
type publisher struct {
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
	lastTag  uint64
}

// publish sends one message and records its delivery tag in lastTag.
func (p *publisher) publish(exchange string, data []byte) error {
	err := p.channel.Publish(
		exchange, // Exchange
		"",       // Routing key
		false,    // Mandatory
		false,    // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
	if err == nil {
		p.lastTag++
	}
	return err
}

// errConfirmTimeout means no confirm arrived in time; the message is resent.
var errConfirmTimeout = errors.New("no confirm from the broker")

// awaitConfirm waits for the confirm of one of tags: this message's
// publishes, including resends. Confirms for other tags belong to earlier
// messages (e.g. a late ack of a resend) and are skipped, so they can never
// confirm this one.
func awaitConfirm(ctx context.Context, done <-chan bool, confirms <-chan amqp.Confirmation,
	tags map[uint64]bool, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case confirm, ok := <-confirms:
			if !ok {
				return errNotConfirmed // the channel closed
			}
			if !tags[confirm.DeliveryTag] {
				continue
			}
			if !confirm.Ack {
				return errNotConfirmed
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return errShutdown
		case <-timer.C:
			return errConfirmTimeout
		}
	}
}

// errNotConfirmed means the broker refused the message (nack) or closed the
// channel before confirming it, e.g. a missing exchange or no permission.
var errNotConfirmed = errors.New("message not confirmed by the broker")

// Push publishes data and waits for the broker to confirm it. If no confirm
// arrives within resendDelay the message is re-sent. It returns an error if
// the broker refuses the message, the channel closes, or ctx is cancelled
// (e.g. the worker is being replaced on a token rotation), so the caller
// keeps the message and retries instead of counting it as published.
func (session *Session) Push(ctx context.Context, exchange string, data []byte) error {
	session.mu.Lock()
	ready, pub := session.isReady, session.pub
	session.mu.Unlock()
	if !ready {
		return errNotConnected
	}
	tags := map[uint64]bool{}
	for {
		if err := pub.publish(exchange, data); err != nil {
			log.Warningln("Push failed. Retrying...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-session.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		tags[pub.lastTag] = true
		err := awaitConfirm(ctx, session.done, pub.confirms, tags, resendDelay)
		if !errors.Is(err, errConfirmTimeout) {
			return err
		}
		log.Warningln("Push didn't confirm. Retrying...")
	}
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	// Always stop handleReconnect, even if the session never connected;
	// otherwise it keeps redialing with stale credentials forever.
	session.closeOnce.Do(func() { close(session.done) })
	session.mu.Lock()
	ready, channel, connection := session.isReady, session.channel, session.connection
	session.isReady = false
	session.mu.Unlock()
	if !ready {
		return errAlreadyClosed
	}
	err := channel.Close()
	if err != nil {
		return err
	}
	return connection.Close()
}
