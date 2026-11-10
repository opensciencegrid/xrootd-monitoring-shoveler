package collector

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// DNSResolver interface allows mocking DNS lookups in tests.
type DNSResolver interface {
	LookupAddr(ctx context.Context, addr string) ([]string, error)
	// LookupHost resolves a host name to addresses. It is the forward direction,
	// used to give an unqualified client name an address the site resolver can
	// work with. The name goes to the resolver as reported, and the system
	// resolver expands a bare "b9p04p7188" through the search list in
	// resolv.conf, exactly as `host b9p04p7188` would.
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// defaultDNSResolver wraps net.DefaultResolver.
type defaultDNSResolver struct{}

func (r *defaultDNSResolver) LookupAddr(ctx context.Context, addr string) ([]string, error) {
	return net.DefaultResolver.LookupAddr(ctx, addr)
}

func (r *defaultDNSResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
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
	page[0] = enrichmentRequest{}

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
	return r.needsDNSEnrichment || r.needsServerDNS || r.needsClientNameLookup
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
			if IsPowerOfTen(n) {
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
	// Find the VO first, so the drop filter and the WLCG rules below all match
	// the same value. Most records have no VO from the auth stream, so a VO rule
	// would have almost nothing to match otherwise.
	//
	// Does nothing when wlcg.enabled is off; the rules then see the packet's VO,
	// as upstream.
	c.resolveRecordVO(req.record)

	// Drop filter runs next. A matching record goes to neither exchange.
	if drop, reason := c.shouldDrop(req.record); drop {
		recordsDropped.WithLabelValues(reason).Inc()
		return
	}

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
	if c.matchesWLCG(record) {
		wlcgRecord, err := ConvertToWLCG(record, c.wlcgMetadata, c.scitags)
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

	// clientNames picks the records whose unqualified client name is looked up:
	// those the site resolver will place. nil looks up none.
	clientNames func(*CollectorRecord) bool
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
			record.clientHostname = hostname
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

	// An unqualified client name ("b9p04p7188") is one the reporting server got
	// from the site's DNS search list. The collector runs at the same site, so
	// the name is looked up the same way. The address feeds the "ip" method and
	// its PTR name feeds the "domain" method.
	if record.needsClientNameLookup {
		if d.clientNames != nil && d.clientNames(record) {
			if client := d.correlator.lookupClient(ctx, record.clientLookupName); client.addr != "" {
				record.clientIP = client.addr
				if client.hostname != "" {
					record.clientHostname = client.hostname
					record.UserDomain = extractDomainFromHostname(client.hostname)
				}
			}
		}
		record.needsClientNameLookup = false
		record.clientLookupName = ""
	}
}

// clientNameCacheKey namespaces forward lookups in the shared DNS cache, which
// is otherwise keyed by address. Without it a host named like an address could
// collide with a reverse entry.
func clientNameCacheKey(host string) string { return "name:" + host }

// clientLookup is what an unqualified client name resolved to: its address and
// the name its PTR record gives. Either may be empty.
type clientLookup struct {
	addr     string
	hostname string
}

// lookupClient resolves an unqualified client name to an address and the full
// name behind it, through the same cache and timeout as the reverse lookups.
// The whole answer is cached under the name, including a name DNS says does
// not exist and an address with no PTR record, so a busy worker node costs one
// lookup per cache lifetime. A timeout or other temporary error is retried on
// the next record, so a slow resolver at startup costs a few lookups instead
// of an hour of unplaced records.
func (c *Correlator) lookupClient(parentCtx context.Context, name string) clientLookup {
	if !c.enableDNSEnrichment || name == "" {
		return clientLookup{}
	}

	key := clientNameCacheKey(name)
	if val, exists := c.dnsCache.Get(key); exists {
		client, _ := val.(clientLookup)
		return client
	}

	lookupCtx := parentCtx
	if lookupCtx == nil {
		lookupCtx = c.ctx
	}
	lookupCtx, cancel := context.WithTimeout(lookupCtx, c.dnsTimeout)
	defer cancel()

	addrs, err := c.dnsResolver.LookupHost(lookupCtx, name)
	if err != nil || len(addrs) == 0 {
		c.logger.Debugf("client name lookup failed for %s: %v", name, err)
		if err == nil || isNameNotFound(err) {
			c.dnsCache.Set(key, clientLookup{})
		}
		return clientLookup{}
	}

	// First answer wins. A worker node with several addresses sits at one site,
	// so any of them places it; taking the first keeps the result stable for the
	// cache lifetime.
	client := clientLookup{addr: addrs[0]}

	ptrCtx, ptrCancel := context.WithTimeout(lookupCtx, c.dnsTimeout)
	defer ptrCancel()
	names, err := c.dnsResolver.LookupAddr(ptrCtx, client.addr)
	switch {
	case err == nil && len(names) > 0:
		client.hostname = strings.TrimSuffix(names[0], ".")
	case err != nil && !isNameNotFound(err):
		// The address is known but the PTR lookup failed for now: use the
		// address, and ask again on the next record.
		c.logger.Debugf("client PTR lookup failed for %s (%s): %v", name, client.addr, err)
		return client
	}

	c.logger.Debugf("client name lookup success: %s -> %s (%s)", name, client.addr, client.hostname)
	c.dnsCache.Set(key, client)
	return client
}

// isNameNotFound reports whether DNS answered that the name does not exist
// (NXDOMAIN), which is the one failure worth caching.
func isNameNotFound(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}

func extractDomainFromHostname(hostname string) string {
	parts := strings.Split(hostname, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

// IsPowerOfTen returns true when n is a positive power of ten (1, 10, 100, …).
// Used to rate-limit warning logs so they are emitted at most O(log₁₀ n) times
// across any number of drop events.
func IsPowerOfTen(n int64) bool {
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
