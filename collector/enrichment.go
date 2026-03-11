package collector

import (
	"context"
	"net"
	"strings"
	"sync"
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

// enrichmentWorkQueue is an in-memory non-blocking queue backed by a slice.
type enrichmentWorkQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []enrichmentRequest
	closed bool
}

func newEnrichmentWorkQueue() *enrichmentWorkQueue {
	q := &enrichmentWorkQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *enrichmentWorkQueue) Enqueue(req enrichmentRequest) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return false
	}

	q.items = append(q.items, req)
	q.cond.Signal()
	return true
}

func (q *enrichmentWorkQueue) Dequeue() (enrichmentRequest, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.items) == 0 {
		return enrichmentRequest{}, false
	}

	req := q.items[0]
	q.items[0] = enrichmentRequest{}
	q.items = q.items[1:]
	return req, true
}

func (q *enrichmentWorkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	q.cond.Broadcast()
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
// Enqueue is non-blocking and records are stored in an in-memory queue.
func (c *Correlator) EnqueueForEnrichment(record *CollectorRecord, destination EnrichmentDestination) {
	if record == nil || destination.Results == nil {
		return
	}

	req := enrichmentRequest{record: record, destination: destination}
	if c.enrichmentQueue == nil {
		c.processEnrichmentRequest(req)
		return
	}

	if ok := c.enrichmentQueue.Enqueue(req); !ok {
		c.logger.Debug("enrichment queue closed; dropping record")
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
		c.enrichmentQueue = newEnrichmentWorkQueue()
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
