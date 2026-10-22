package shoveler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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
	// retry holds messages a stopped worker hadn't had confirmed; the next
	// generation publishes them before anything else, so a rotation neither
	// loses nor reorders them. It holds every worker's whole window.
	retry chan *MessageStruct
	wg    sync.WaitGroup

	// held is a message the feeder had taken from the queue when the pool
	// stopped; saved on shutdown.
	held *MessageStruct
	// newSource opens a worker's AMQP session (tests replace it).
	newSource func(url.URL, int) channelSource
}

// PublishWorker handles publishing messages to AMQP
type PublishWorker struct {
	id            int
	config        *Config
	amqpURL       url.URL
	session       channelSource                    // its own AMQP session; created in run unless set
	window        int                              // most unconfirmed messages in flight
	messagesQueue chan *MessageStruct              // Reference to shared channel
	retry         chan *MessageStruct              // Reference to the pool's retry channel
	requeue       func(*MessageStruct)             // last resort if retry is full
	newSource     func(url.URL, int) channelSource // nil: newSession
	ctx           context.Context
	wg            sync.WaitGroup
}

// defaultConfirmWindow is the default amqp.confirm_window. A worker's
// throughput is capped at about window / broker round-trip time, so 4096
// allows ~40,000 msg/s at 100 ms: large enough not to need tuning. Messages
// are only held while unconfirmed (a few MB at typical packet sizes).
const defaultConfirmWindow = 4096

// confirmTimeout is how long a worker waits without any confirm while
// messages are in flight before it treats the channel as stuck and replaces
// it. A var so tests can shorten it.
var confirmTimeout = 30 * time.Second

// channelPoll is how often a worker checks for a usable channel while its
// session (re)connects.
const channelPoll = 100 * time.Millisecond

// After a worker gives up on a channel (the broker refused a message, closed
// the channel, or stopped confirming) it waits before republishing on a new
// one: channelRetryMin, doubling up to channelRetryMax, reset by an ack. A
// broker that keeps refusing (e.g. a missing exchange) is not hammered with
// channels. Vars so tests can shorten them.
var (
	channelRetryMin = time.Second
	channelRetryMax = 30 * time.Second
)

// This should run in a new go co-routine.
func StartAMQP(config *Config, queue *ConfirmationQueue) {
	pool := newWorkerPool(config, queue)
	if err := pool.loadCredentials(); err != nil {
		log.Fatalln(err)
	}
	if !registerPool(pool) {
		return // shutting down
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

// running holds the started worker pools so StopAMQP can save their
// in-flight messages on shutdown.
var running struct {
	sync.Mutex
	pools    []*WorkerPool
	stopping bool
}

// registerPool records a started pool; false if shutdown has begun.
func registerPool(p *WorkerPool) bool {
	running.Lock()
	defer running.Unlock()
	if running.stopping {
		return false
	}
	running.pools = append(running.pools, p)
	return true
}

// StopAMQP stops every AMQP worker pool and saves the messages they hold
// (taken from the queue but not yet confirmed by the broker) back to the
// front of the queue, so they are sent first after a restart. Each pool gets
// up to timeout. Close the queue after it returns.
func StopAMQP(timeout time.Duration) {
	running.Lock()
	running.stopping = true
	pools := running.pools
	running.Unlock()
	for _, p := range pools {
		p.saveAndStop(timeout)
	}
}

// ShutdownOnSignal waits for SIGTERM or SIGINT, then stops AMQP publishing,
// saves in-flight messages to the queue, closes the queue and exits, so a
// restart loses nothing that was queued.
func ShutdownOnSignal(queue *ConfirmationQueue) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	go func() {
		sig := <-signals
		log.Warningln("Received", sig.String()+"; saving queued messages and exiting")
		StopAMQP(10 * time.Second)
		if queue != nil {
			if err := queue.Close(); err != nil {
				log.Errorln("Failed to close the queue:", err)
			}
		}
		os.Exit(0)
	}()
}

// saveAndStop stops the pool and saves, in order, the messages its workers
// handed back, those in the shared channel, and the one the feeder held.
func (p *WorkerPool) saveAndStop(timeout time.Duration) {
	p.cancel()
	p.queue.Wake() // a feeder waiting on an empty queue returns
	done := make(chan struct{})
	go func() {
		p.mu.Lock()
		p.stopWorkers()
		p.mu.Unlock()
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Warningln("Timed out stopping the AMQP workers; saving the messages handed back so far")
	}

	var saved []*MessageStruct
	for len(p.retry) > 0 {
		saved = append(saved, <-p.retry)
	}
	for len(p.messagesQueue) > 0 {
		saved = append(saved, <-p.messagesQueue)
	}
	if p.held != nil {
		saved = append(saved, p.held)
		p.held = nil
	}
	if len(saved) > 0 {
		log.Infoln("Saving", len(saved), "unconfirmed messages to the queue")
		p.queue.SaveInFlight(saved)
	}
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
		retry:         make(chan *MessageStruct, max(config.AmqpPublishWorkers, 1)*(confirmWindow(config)+1)),
	}
}

// confirmWindow is the configured confirm window, or the default.
func confirmWindow(config *Config) int {
	if config.AmqpConfirmWindow > 0 {
		return config.AmqpConfirmWindow
	}
	return defaultConfirmWindow
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
			window:        confirmWindow(p.config),
			messagesQueue: p.messagesQueue, // Share the pool's channel
			retry:         p.retry,
			requeue: func(m *MessageStruct) {
				p.queue.SaveInFlight([]*MessageStruct{m})
			},
			newSource: p.newSource,
			ctx:       ctx,
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
			if errors.Is(err, ErrInterrupted) {
				return
			}
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
				// Shutting down: saveAndStop saves it after the others.
				p.held = msgStruct
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
	if w.session == nil {
		if w.newSource != nil {
			w.session = w.newSource(w.amqpURL, w.window)
		} else {
			w.session = newSession(w.amqpURL, w.window)
		}
	}
	// Closing waits for the broker's reply; by then this worker has handed
	// back its messages, so don't hold up a rotation or shutdown for it.
	defer func() {
		session := w.session
		go func() {
			if err := session.Close(); err != nil {
				log.Debugf("Worker %d: Error closing session: %v", w.id, err)
			}
		}()
	}()

	w.publishLoop()
}

// inflight is a message taken by the worker and not yet confirmed. tag is
// its delivery tag on the current channel, or 0 if it hasn't been
// published on it.
type inflight struct {
	msg *MessageStruct
	tag uint64
}

// publishLoop publishes with up to w.window messages awaiting confirms.
// Messages are counted as published only when the broker acks them. On a
// nack, a closed channel or a stuck one, every unconfirmed message is
// republished, in order, on the next channel before anything new, so
// ordering holds (duplicates are possible: delivery is at-least-once). When
// the worker is stopped, unconfirmed messages are handed back in order.
func (w *PublishWorker) publishLoop() {
	var pending []inflight
	var ch confirmChannel      // the channel pending was published on
	var dropped confirmChannel // the last channel given up on
	var lastProgress time.Time
	var retryAt time.Time // don't use a new channel before this
	retryDelay := channelRetryMin
	watchdog := time.NewTicker(confirmTimeout / 4)
	defer watchdog.Stop()

	// dropChannel abandons ch: everything pending goes out again on the
	// next channel.
	dropChannel := func(reason string) {
		if ch != nil {
			log.Warningf("Worker %d: %s; republishing %d unconfirmed messages on a new channel",
				w.id, reason, len(pending))
			// Closing waits for the broker's reply, which can take a while;
			// don't hold up the worker for it.
			go ch.close()
			dropped = ch
			retryAt = time.Now().Add(retryDelay)
			retryDelay = min(2*retryDelay, channelRetryMax)
		}
		ch = nil
		for i := range pending {
			pending[i].tag = 0
		}
	}

	// send publishes m on ch and tracks it; on failure it is kept for the
	// next channel.
	send := func(m *MessageStruct) {
		if len(pending) == 0 {
			lastProgress = time.Now()
		}
		tag, err := ch.publish(w.exchange(m), m.Message)
		pending = append(pending, inflight{msg: m, tag: tag})
		if err != nil {
			dropChannel("publish failed: " + err.Error())
		}
	}

	for {
		if w.ctx.Err() != nil {
			w.handBackAll(pending)
			log.Debugf("Worker %d: Stopping", w.id)
			return
		}

		if ch == nil {
			if wait := time.Until(retryAt); wait > 0 {
				sleepCtx(w.ctx, wait)
				continue
			}
			cur := w.session.current()
			// Until the session notices the close it may still offer the
			// channel just given up on; wait for a new one.
			if cur == nil || cur == dropped {
				if !sleepCtx(w.ctx, channelPoll) {
					continue
				}
				continue
			}
			ch = cur
			lastProgress = time.Now()
			// Republish everything unconfirmed, in order, before new messages.
			for i := range pending {
				tag, err := ch.publish(w.exchange(pending[i].msg), pending[i].msg.Message)
				if err != nil {
					dropChannel("republish failed: " + err.Error())
					break
				}
				pending[i].tag = tag
			}
			continue
		}

		// Room in the window: messages a previous worker generation didn't
		// finish go first.
		var retry, queue <-chan *MessageStruct
		if len(pending) < w.window {
			select {
			case m := <-w.retry:
				send(m)
				continue
			default:
			}
			retry, queue = w.retry, w.messagesQueue
		}

		select {
		case <-w.ctx.Done():
		case m := <-retry:
			send(m)
		case m := <-queue:
			send(m)
		case c, ok := <-ch.confirms():
			if !ok {
				dropChannel("channel closed")
				continue
			}
			i := 0
			for i < len(pending) && pending[i].tag != c.DeliveryTag {
				i++
			}
			if i == len(pending) || c.DeliveryTag == 0 {
				continue // not one of ours (e.g. a duplicate)
			}
			if !c.Ack {
				dropChannel("the broker refused a message")
				continue
			}
			pending = append(pending[:i], pending[i+1:]...)
			lastProgress = time.Now()
			retryDelay = channelRetryMin
			MessagesPublished.Inc()
		case <-watchdog.C:
			if len(pending) > 0 && time.Since(lastProgress) > confirmTimeout {
				dropChannel(fmt.Sprintf("no confirms for %s", confirmTimeout))
			}
		}
	}
}

// exchange is where m goes: its own exchange, or the configured default.
func (w *PublishWorker) exchange(m *MessageStruct) string {
	if m.Exchange != "" {
		return m.Exchange
	}
	return w.config.AmqpExchange
}

// handBackAll returns messages this worker took but didn't get confirmed,
// in order, so the next generation of workers publishes them first. If the
// retry channel is somehow full they go back to the durable queue rather
// than being dropped.
func (w *PublishWorker) handBackAll(pending []inflight) {
	for _, p := range pending {
		select {
		case w.retry <- p.msg:
		default:
			log.Warningf("Worker %d: retry channel full; returning a message to the queue", w.id)
			if w.requeue != nil {
				w.requeue(p.msg)
			}
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
	confirmBuffer   int           // capacity of each channel's confirm listener
	stopped         chan struct{} // closed when handleReconnect returns
}

var errAlreadyClosed = errors.New("already closed: not connected to the server")

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(url url.URL) *Session {
	return newSession(url, defaultConfirmWindow)
}

// newSession is New for a worker with up to window unconfirmed messages.
func newSession(url url.URL, window int) *Session {
	session := Session{
		url:     url,
		done:    make(chan bool),
		stopped: make(chan struct{}),
		// streadway's channel reader blocks if this listener's buffer can't
		// hold every outstanding confirm.
		confirmBuffer: max(1024, 2*window),
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

		if conn.IsClosed() {
			log.Warningln("Connection closed. Reconnecting...")
			return false
		}
		err := session.init(conn)

		if err != nil {
			log.Warningln("Failed to initialize channel. Retrying...", err)

			// Watch the connection while waiting: if it closed, retrying
			// the channel on it can never succeed.
			select {
			case <-session.done:
				return true
			case err := <-session.notifyConnClose:
				log.Warningln("Connection closed. Reconnecting...", err)
				return false
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
	// Buffered: the library blocks delivering the close notice until it is
	// received, and while it is blocked the connection can't finish closing,
	// so a channel being opened on it waits forever (seen as a stuck shoveler).
	session.notifyConnClose = make(chan *amqp.Error, 1)
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
	session.notifyChanClose = make(chan *amqp.Error, 1) // buffered, as notifyConnClose
	// The worker reads confirms as they come; the buffer keeps the
	// channel's reader from blocking while the worker is publishing.
	confirms := make(chan amqp.Confirmation, session.confirmBuffer)
	channel.NotifyClose(session.notifyChanClose)
	channel.NotifyPublish(confirms)
	session.mu.Lock()
	session.channel = channel
	session.pub = &publisher{channel: channel, confirmsChan: confirms}
	session.mu.Unlock()
}

// confirmChannel is a confirm-mode AMQP channel. Publishes are numbered 1,
// 2, 3, ... and confirmed by that delivery tag, in order; the confirms
// channel is closed when the AMQP channel closes.
type confirmChannel interface {
	publish(exchange string, data []byte) (uint64, error)
	confirms() <-chan amqp.Confirmation
	close() // abandon the channel; the session opens a new one
}

// channelSource provides a worker's current confirm channel, or nil while it
// (re)connects.
type channelSource interface {
	current() confirmChannel
	Close() error
}

// current returns the session's channel if it is ready to publish on.
func (session *Session) current() confirmChannel {
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.isReady || session.pub == nil {
		return nil
	}
	return session.pub
}

// publisher is one confirm-mode channel and the delivery tags assigned on
// it, mirroring the broker's numbering. Only the session's worker uses it.
type publisher struct {
	channel      *amqp.Channel
	confirmsChan chan amqp.Confirmation
	lastTag      uint64
}

// publish sends one message and returns its delivery tag.
func (p *publisher) publish(exchange string, data []byte) (uint64, error) {
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
	if err != nil {
		return 0, err
	}
	p.lastTag++
	return p.lastTag, nil
}

func (p *publisher) confirms() <-chan amqp.Confirmation { return p.confirmsChan }

// close closes the channel; the session's handleReInit opens a new one.
func (p *publisher) close() { _ = p.channel.Close() }

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
