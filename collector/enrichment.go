package collector

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// DNSResolver interface allows mocking DNS lookups in tests.
type DNSResolver interface {
	LookupAddr(ctx context.Context, addr string) ([]string, error)
}

// defaultDNSResolver wraps net.DefaultResolver.
type defaultDNSResolver struct{}

func (r *defaultDNSResolver) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	return net.DefaultResolver.LookupAddr(ctx, addr)
}

// RecordEnricher defines an enrichment stage for collector records.
type RecordEnricher interface {
	Name() string
	Enrich(ctx context.Context, record *CollectorRecord)
}

// EnrichedRecord is the output produced by the enrichment pipeline.
type EnrichedRecord struct {
	Record   *CollectorRecord
	Payload  []byte
	Exchange string
}

// EnrichmentDestination tells the pipeline where an enriched record should be sent.
type EnrichmentDestination struct {
	Results      chan<- EnrichedRecord
	WLCGExchange string
}

// enrichmentRequest is a unit of work for the enrichment worker pool.
// It carries the record to enrich and destination metadata.
type enrichmentRequest struct {
	record      *CollectorRecord
	destination EnrichmentDestination
}

// defaultEnrichmentQueueMaxSize is the default maximum number of pending enrichment requests.
// The queue grows lazily up to this limit, so we can tolerate large DNS backlogs without
// reserving the full backing store at startup.
const defaultEnrichmentQueueMaxSize = 1_000_000

// enrichmentQueuePageSize controls how many requests are stored in each lazily allocated page.
// With 32-byte queue entries on 64-bit platforms, each page is about 128 KiB.
const enrichmentQueuePageSize = 4096

// enrichmentWorkQueue is a bounded FIFO queue for enrichment requests.
// Storage is allocated in fixed-size pages so memory grows with backlog and empty
// pages can be reclaimed after the queue drains.
type enrichmentWorkQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	pages    [][]enrichmentRequest
	size     int
	closed   bool
	capacity int
}

func newEnrichmentWorkQueue(capacity int) *enrichmentWorkQueue {
	if capacity <= 0 {
		capacity = defaultEnrichmentQueueMaxSize
	}
	q := &enrichmentWorkQueue{capacity: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
	enrichmentQueueSize.Set(0)
	return q
}

func (q *enrichmentWorkQueue) enqueueLocked(req enrichmentRequest) {
	if len(q.pages) == 0 || len(q.pages[len(q.pages)-1]) == cap(q.pages[len(q.pages)-1]) {
		pageCapacity := enrichmentQueuePageSize
		if remaining := q.capacity - q.size; remaining < pageCapacity {
			pageCapacity = remaining
		}
		q.pages = append(q.pages, make([]enrichmentRequest, 0, pageCapacity))
	}

	lastPage := len(q.pages) - 1
	q.pages[lastPage] = append(q.pages[lastPage], req)
	q.size++
}

// Enqueue attempts to add a request to the queue without blocking.
// Returns (enqueued, closed): (true, false) on success, (false, true) when the
// queue is closed, and (false, false) when the buffer is full.
func (q *enrichmentWorkQueue) Enqueue(req enrichmentRequest) (enqueued bool, wasClosed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		enrichmentQueueSize.Set(float64(q.size))
		return false, true
	}

	if q.size >= q.capacity {
		enrichmentQueueSize.Set(float64(q.size))
		return false, false
	}

	q.enqueueLocked(req)
	enrichmentQueueSize.Set(float64(q.size))
	q.notEmpty.Signal()
	return true, false
}

// Dequeue blocks until a request is available or the queue is closed and drained.
// Returns (request, true) on success or (zero, false) when the queue is done.
func (q *enrichmentWorkQueue) Dequeue() (enrichmentRequest, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == 0 && !q.closed {
		q.notEmpty.Wait()
	}

	if q.size == 0 {
		enrichmentQueueSize.Set(0)
		return enrichmentRequest{}, false
	}

	page := q.pages[0]
	req := page[0]

	if len(page) == 1 {
		q.pages[0] = nil
		q.pages = q.pages[1:]
		if len(q.pages) == 0 {
			q.pages = nil
		}
	} else {
		q.pages[0] = page[1:]
	}

	q.size--
	enrichmentQueueSize.Set(float64(q.size))
	return req, true
}

// Close signals that no more items will be enqueued and unblocks waiting Dequeue calls.
func (q *enrichmentWorkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.notEmpty.Broadcast()
		enrichmentQueueSize.Set(float64(q.size))
	}
}

// NeedsEnrichment returns true when a record has pending asynchronous enrichments.
func (r *CollectorRecord) NeedsEnrichment() bool {
	return r.needsDNSEnrichment || r.needsServerDNS
}

// NeedsDNSEnrichment is kept for compatibility with older callers.
func (r *CollectorRecord) NeedsDNSEnrichment() bool {
	return r.NeedsEnrichment()
}

// EnqueueForEnrichment always routes records through the enrichment pipeline.
// Enqueue is non-blocking; when the queue is full the record is dropped, the
// drop counter is incremented in Prometheus, and a warning is logged only at the
// first drop and subsequent power-of-10 thresholds to avoid log flooding during
// overload events. The downstream destination (confirmation queue, disk-backed)
// should almost never be full, so the enrichment queue should rarely fill up.
func (c *Correlator) EnqueueForEnrichment(record *CollectorRecord, destination EnrichmentDestination) {
	if record == nil || destination.Results == nil {
		return
	}

	req := enrichmentRequest{record: record, destination: destination}
	if c.enrichmentQueue == nil {
		c.processEnrichmentRequest(req)
		return
	}

	enqueued, wasClosed := c.enrichmentQueue.Enqueue(req)
	if !enqueued {
		if wasClosed {
			c.logger.Debug("enrichment queue closed; dropping record")
		} else {
			enrichmentQueueDropped.Inc()
			n := atomic.AddInt64(&c.enrichmentDropCount, 1)
			// Log at Warn only on the first drop and at power-of-10 thresholds
			// to limit log volume during sustained overload.
			if isPowerOfTen(n) {
				c.logger.Warnf("enrichment queue full (capacity %d); %d records dropped total", c.enrichmentQueue.capacity, n)
			}
		}
	}
}

func (c *Correlator) registerEnricher(enricher RecordEnricher) {
	if enricher == nil {
		return
	}
	c.enrichers = append(c.enrichers, enricher)
}

func (c *Correlator) startEnrichmentWorkers() {
	if c.enrichmentWorkerCount <= 0 {
		c.enrichmentWorkerCount = 1
	}
	if c.enrichmentQueue == nil {
		c.enrichmentQueue = newEnrichmentWorkQueue(c.enrichmentQueueSize)
	}

	for i := 0; i < c.enrichmentWorkerCount; i++ {
		c.enrichmentWG.Add(1)
		go c.enrichmentWorker()
	}
	c.logger.Infof("Started %d enrichment workers", c.enrichmentWorkerCount)
}

func (c *Correlator) enrichmentWorker() {
	defer c.enrichmentWG.Done()

	for {
		req, ok := c.enrichmentQueue.Dequeue()
		if !ok {
			return
		}
		c.processEnrichmentRequest(req)
	}
}

func (c *Correlator) processEnrichmentRequest(req enrichmentRequest) {
	for _, enricher := range c.enrichers {
		enricher.Enrich(c.ctx, req.record)
	}

	enriched, err := c.buildEnrichedRecord(req.record, req.destination.WLCGExchange)
	if err != nil {
		c.logger.Errorf("failed to encode enriched record: %v", err)
		return
	}

	select {
	case req.destination.Results <- enriched:
	case <-c.ctx.Done():
	}
}

func (c *Correlator) buildEnrichedRecord(record *CollectorRecord, wlcgExchange string) (EnrichedRecord, error) {
	if IsWLCGPacket(record) {
		wlcgRecord, err := ConvertToWLCG(record)
		if err != nil {
			return EnrichedRecord{}, err
		}

		wlcgJSON, err := wlcgRecord.ToJSON()
		if err != nil {
			return EnrichedRecord{}, err
		}

		return EnrichedRecord{
			Record:   record,
			Payload:  wlcgJSON,
			Exchange: wlcgExchange,
		}, nil
	}

	recordJSON, err := record.ToJSON()
	if err != nil {
		return EnrichedRecord{}, err
	}

	return EnrichedRecord{
		Record:   record,
		Payload:  recordJSON,
		Exchange: "",
	}, nil
}

// enrichWithDNSSync performs synchronous DNS enrichment for an IP address.
// It only returns a hostname if the value is already in cache.
// Returns (hostname, needsLookup).
func (c *Correlator) enrichWithDNSSync(ipStr string) (string, bool) {
	if !c.enableDNSEnrichment || ipStr == "" {
		return "", false
	}

	if val, exists := c.dnsCache.Get(ipStr); exists {
		if hostname, ok := val.(string); ok {
			c.logger.Debugf("DNS cache hit: %s -> %s", ipStr, hostname)
			return hostname, false
		}
	}

	c.logger.Debugf("DNS cache miss: %s", ipStr)
	return "", true
}

// enrichWithDNSBlocking performs a direct reverse DNS lookup with timeout.
// This helper is primarily used in tests.
func (c *Correlator) enrichWithDNSBlocking(ipStr string) string {
	return c.lookupDNSHostname(c.ctx, ipStr)
}

func (c *Correlator) lookupDNSHostname(parentCtx context.Context, ipStr string) string {
	hostname, needsLookup := c.enrichWithDNSSync(ipStr)
	if hostname != "" || !needsLookup {
		return hostname
	}

	lookupCtx := parentCtx
	if lookupCtx == nil {
		lookupCtx = c.ctx
	}
	lookupCtx, cancel := context.WithTimeout(lookupCtx, c.dnsTimeout)
	defer cancel()

	hostname = c.performDNSLookup(lookupCtx, ipStr)
	if hostname != "" {
		c.dnsCache.Set(ipStr, hostname)
	}

	return hostname
}

// performDNSLookup does the actual reverse DNS lookup.
func (c *Correlator) performDNSLookup(ctx context.Context, ipStr string) string {
	if net.ParseIP(ipStr) == nil {
		return ""
	}

	names, err := c.dnsResolver.LookupAddr(ctx, ipStr)
	if err != nil || len(names) == 0 {
		c.logger.Debugf("DNS lookup failed for %s: %v", ipStr, err)
		return ""
	}

	hostname := strings.TrimSuffix(names[0], ".")
	c.logger.Debugf("DNS lookup success: %s -> %s", ipStr, hostname)
	return hostname
}

type dnsRecordEnricher struct {
	correlator *Correlator
}

func (d *dnsRecordEnricher) Name() string {
	return "dns"
}

func (d *dnsRecordEnricher) Enrich(ctx context.Context, record *CollectorRecord) {
	if record == nil {
		return
	}

	if record.needsDNSEnrichment {
		hostname := d.correlator.lookupDNSHostname(ctx, record.enrichmentIP)
		if hostname != "" {
			record.UserDomain = extractDomainFromHostname(hostname)
		}
		record.needsDNSEnrichment = false
		record.enrichmentIP = ""
	}

	if record.needsServerDNS {
		hostname := d.correlator.lookupDNSHostname(ctx, record.serverEnrichmentIP)
		if hostname != "" {
			record.ServerHostname = hostname
		}
		record.needsServerDNS = false
		record.serverEnrichmentIP = ""
	}
}

func extractDomainFromHostname(hostname string) string {
	parts := strings.Split(hostname, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

// isPowerOfTen returns true when n is a positive power of ten (1, 10, 100, …).
// Used to rate-limit warning logs so they are emitted at most O(log₁₀ n) times
// across any number of drop events.
func isPowerOfTen(n int64) bool {
	if n <= 0 {
		return false
	}
	for n > 1 {
		if n%10 != 0 {
			return false
		}
		n /= 10
	}
	return true
}
