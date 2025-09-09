package server

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector collects and aggregates metrics from all components
type MetricsCollector struct {
	// Ingestion metrics
	ledgersReceived   atomic.Uint64
	bytesReceived     atomic.Uint64
	lastIngestTime    atomic.Int64
	ingestStartTime   time.Time
	
	// Processing metrics
	ledgersProcessed  atomic.Uint64
	contractsFound    atomic.Uint64
	processingErrors  atomic.Uint64
	lastProcessTime   atomic.Int64
	
	// Stream metrics
	currentLedger     atomic.Uint32
	streamReconnects  atomic.Uint64
	
	// Performance metrics
	processingDuration *DurationTracker
	queueDepth        *GaugeTracker
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		ingestStartTime:    time.Now(),
		processingDuration: NewDurationTracker(),
		queueDepth:        NewGaugeTracker(),
	}
}

// RecordIngestion records ledger ingestion
func (m *MetricsCollector) RecordIngestion(ledgerSeq uint32, bytes int) {
	m.ledgersReceived.Add(1)
	m.bytesReceived.Add(uint64(bytes))
	m.lastIngestTime.Store(time.Now().UnixNano())
	
	// Update current ledger if newer
	for {
		current := m.currentLedger.Load()
		if ledgerSeq <= current {
			break
		}
		if m.currentLedger.CompareAndSwap(current, ledgerSeq) {
			break
		}
	}
}

// RecordProcessing records ledger processing
func (m *MetricsCollector) RecordProcessing(ledgerSeq uint32, contractsFound int, duration time.Duration, err error) {
	if err != nil {
		m.processingErrors.Add(1)
	} else {
		m.ledgersProcessed.Add(1)
		m.contractsFound.Add(uint64(contractsFound))
		m.processingDuration.Record(duration)
	}
	m.lastProcessTime.Store(time.Now().UnixNano())
}

// RecordReconnect records a stream reconnection
func (m *MetricsCollector) RecordReconnect() {
	m.streamReconnects.Add(1)
}

// UpdateQueueDepth updates the processing queue depth
func (m *MetricsCollector) UpdateQueueDepth(depth int) {
	m.queueDepth.Update(float64(depth))
}

// GetSnapshot returns a snapshot of current metrics
func (m *MetricsCollector) GetSnapshot() MetricsSnapshot {
	now := time.Now()
	uptime := now.Sub(m.ingestStartTime)
	
	ledgersReceived := m.ledgersReceived.Load()
	ledgersProcessed := m.ledgersProcessed.Load()
	
	var ingestRate, processRate float64
	if uptime.Seconds() > 0 {
		ingestRate = float64(ledgersReceived) / uptime.Seconds()
		processRate = float64(ledgersProcessed) / uptime.Seconds()
	}
	
	lastIngest := time.Unix(0, m.lastIngestTime.Load())
	lastProcess := time.Unix(0, m.lastProcessTime.Load())
	
	var ingestLag, processLag time.Duration
	if !lastIngest.IsZero() {
		ingestLag = now.Sub(lastIngest)
	}
	if !lastProcess.IsZero() {
		processLag = now.Sub(lastProcess)
	}
	
	return MetricsSnapshot{
		Timestamp:         now,
		Uptime:           uptime,
		
		// Ingestion
		LedgersReceived:  ledgersReceived,
		BytesReceived:    m.bytesReceived.Load(),
		IngestRate:       ingestRate,
		IngestLag:        ingestLag,
		
		// Processing
		LedgersProcessed: ledgersProcessed,
		ContractsFound:   m.contractsFound.Load(),
		ProcessingErrors: m.processingErrors.Load(),
		ProcessRate:      processRate,
		ProcessLag:       processLag,
		
		// Stream
		CurrentLedger:    m.currentLedger.Load(),
		StreamReconnects: m.streamReconnects.Load(),
		
		// Performance
		AvgProcessingDuration: m.processingDuration.Average(),
		P99ProcessingDuration: m.processingDuration.Percentile(0.99),
		CurrentQueueDepth:     int(m.queueDepth.Current()),
	}
}

// MetricsSnapshot represents a point-in-time snapshot of metrics
type MetricsSnapshot struct {
	Timestamp time.Time
	Uptime    time.Duration
	
	// Ingestion metrics
	LedgersReceived uint64
	BytesReceived   uint64
	IngestRate      float64
	IngestLag       time.Duration
	
	// Processing metrics
	LedgersProcessed uint64
	ContractsFound   uint64
	ProcessingErrors uint64
	ProcessRate      float64
	ProcessLag       time.Duration
	
	// Stream metrics
	CurrentLedger    uint32
	StreamReconnects uint64
	
	// Performance metrics
	AvgProcessingDuration time.Duration
	P99ProcessingDuration time.Duration
	CurrentQueueDepth     int
}

// DurationTracker tracks duration statistics
type DurationTracker struct {
	samples []time.Duration
	index   int
	count   int
	mu      sync.RWMutex
}

func NewDurationTracker() *DurationTracker {
	return &DurationTracker{
		samples: make([]time.Duration, 1000), // Keep last 1000 samples
	}
}

func (d *DurationTracker) Record(duration time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	d.samples[d.index] = duration
	d.index = (d.index + 1) % len(d.samples)
	if d.count < len(d.samples) {
		d.count++
	}
}

func (d *DurationTracker) Average() time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	if d.count == 0 {
		return 0
	}
	
	var sum time.Duration
	for i := 0; i < d.count; i++ {
		sum += d.samples[i]
	}
	
	return sum / time.Duration(d.count)
}

func (d *DurationTracker) Percentile(p float64) time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	if d.count == 0 {
		return 0
	}
	
	// Simple implementation - for production, use a proper percentile algorithm
	// This just returns the max value as an approximation of P99
	var max time.Duration
	for i := 0; i < d.count; i++ {
		if d.samples[i] > max {
			max = d.samples[i]
		}
	}
	
	return max
}

// GaugeTracker tracks gauge values
type GaugeTracker struct {
	current atomic.Value // float64
}

func NewGaugeTracker() *GaugeTracker {
	g := &GaugeTracker{}
	g.current.Store(float64(0))
	return g
}

func (g *GaugeTracker) Update(value float64) {
	g.current.Store(value)
}

func (g *GaugeTracker) Current() float64 {
	return g.current.Load().(float64)
}