package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// Collector manages all metrics for the Arrow service
type Collector struct {
	logger *logging.ComponentLogger
	
	// Counters
	ledgersProcessed   prometheus.Counter
	recordsStreamed    prometheus.Counter
	bytesTransferred   prometheus.Counter
	errorsTotal        prometheus.Counter
	
	// Gauges
	activeConnections  prometheus.Gauge
	memoryUsage        prometheus.Gauge
	batchSize          prometheus.Gauge
	
	// Histograms
	processingDuration prometheus.Histogram
	batchDuration      prometheus.Histogram
	conversionDuration prometheus.Histogram
	streamDuration     prometheus.Histogram
	
	// Summary
	recordSizeSummary  prometheus.Summary
	
	// Custom registerer
	registry           *prometheus.Registry
}

// NewCollector creates a new metrics collector
func NewCollector(logger *logging.ComponentLogger) *Collector {
	registry := prometheus.NewRegistry()
	
	c := &Collector{
		logger:   logger,
		registry: registry,
		
		// Initialize counters
		ledgersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stellar_arrow_ledgers_processed_total",
			Help: "Total number of ledgers processed",
		}),
		
		recordsStreamed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stellar_arrow_records_streamed_total",
			Help: "Total number of Arrow records streamed",
		}),
		
		bytesTransferred: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stellar_arrow_bytes_transferred_total",
			Help: "Total bytes transferred via Arrow Flight",
		}),
		
		errorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stellar_arrow_errors_total",
			Help: "Total number of errors",
		}),
		
		// Initialize gauges
		activeConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stellar_arrow_active_connections",
			Help: "Number of active Arrow Flight connections",
		}),
		
		memoryUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stellar_arrow_memory_usage_bytes",
			Help: "Current memory usage in bytes",
		}),
		
		batchSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stellar_arrow_batch_size",
			Help: "Current batch size configuration",
		}),
		
		// Initialize histograms
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "stellar_arrow_processing_duration_seconds",
			Help:    "Time spent processing ledgers",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
		}),
		
		batchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "stellar_arrow_batch_duration_seconds",
			Help:    "Time to build Arrow batches",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		}),
		
		conversionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "stellar_arrow_xdr_conversion_duration_seconds",
			Help:    "Time to convert XDR to Arrow format",
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 10), // 10μs to ~10ms
		}),
		
		streamDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "stellar_arrow_stream_duration_seconds",
			Help:    "Duration of Arrow Flight streams",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~1000s
		}),
		
		// Initialize summary
		recordSizeSummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "stellar_arrow_record_size_bytes",
			Help:       "Size of Arrow records in bytes",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}
	
	// Register all metrics
	registry.MustRegister(
		c.ledgersProcessed,
		c.recordsStreamed,
		c.bytesTransferred,
		c.errorsTotal,
		c.activeConnections,
		c.memoryUsage,
		c.batchSize,
		c.processingDuration,
		c.batchDuration,
		c.conversionDuration,
		c.streamDuration,
		c.recordSizeSummary,
	)
	
	// Register Go runtime metrics
	registry.MustRegister(prometheus.NewGoCollector())
	
	logger.Info().
		Msg("Metrics collector initialized")
	
	return c
}

// StartMetricsServer starts the Prometheus metrics HTTP server
func (c *Collector) StartMetricsServer(port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	
	c.logger.Info().
		Int("port", port).
		Msg("Starting Prometheus metrics server")
	
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			c.logger.Error().
				Err(err).
				Msg("Metrics server failed")
		}
	}()
	
	return nil
}

// RecordLedgerProcessed increments the ledger counter
func (c *Collector) RecordLedgerProcessed() {
	c.ledgersProcessed.Inc()
}

// RecordRecordStreamed increments the record counter and tracks size
func (c *Collector) RecordRecordStreamed(sizeBytes int64) {
	c.recordsStreamed.Inc()
	c.bytesTransferred.Add(float64(sizeBytes))
	c.recordSizeSummary.Observe(float64(sizeBytes))
}

// RecordError increments the error counter
func (c *Collector) RecordError() {
	c.errorsTotal.Inc()
}

// UpdateActiveConnections updates the active connections gauge
func (c *Collector) UpdateActiveConnections(count int) {
	c.activeConnections.Set(float64(count))
}

// UpdateMemoryUsage updates the memory usage gauge
func (c *Collector) UpdateMemoryUsage(bytes int64) {
	c.memoryUsage.Set(float64(bytes))
}

// UpdateBatchSize updates the batch size gauge
func (c *Collector) UpdateBatchSize(size int) {
	c.batchSize.Set(float64(size))
}

// TimeProcessing measures processing duration
func (c *Collector) TimeProcessing(f func()) {
	timer := prometheus.NewTimer(c.processingDuration)
	defer timer.ObserveDuration()
	f()
}

// TimeBatchBuilding measures batch building duration
func (c *Collector) TimeBatchBuilding(f func()) {
	timer := prometheus.NewTimer(c.batchDuration)
	defer timer.ObserveDuration()
	f()
}

// TimeXDRConversion measures XDR conversion duration
func (c *Collector) TimeXDRConversion(f func()) {
	timer := prometheus.NewTimer(c.conversionDuration)
	defer timer.ObserveDuration()
	f()
}

// RecordStreamDuration records the duration of a stream
func (c *Collector) RecordStreamDuration(duration time.Duration) {
	c.streamDuration.Observe(duration.Seconds())
}

// StreamMetrics tracks metrics for an individual stream
type StreamMetrics struct {
	StreamID          string
	StartTime         time.Time
	EndTime           time.Time
	RecordsStreamed   int64
	BytesTransferred  int64
	Errors            int64
	mu                sync.Mutex
}

// NewStreamMetrics creates metrics for a new stream
func NewStreamMetrics(streamID string) *StreamMetrics {
	return &StreamMetrics{
		StreamID:  streamID,
		StartTime: time.Now(),
	}
}

// IncrementRecords increments the record counter
func (sm *StreamMetrics) IncrementRecords(bytes int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.RecordsStreamed++
	sm.BytesTransferred += bytes
}

// IncrementErrors increments the error counter
func (sm *StreamMetrics) IncrementErrors() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Errors++
}

// Finalize marks the stream as complete
func (sm *StreamMetrics) Finalize() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.EndTime = time.Now()
}

// Duration returns the stream duration
func (sm *StreamMetrics) Duration() time.Duration {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.EndTime.IsZero() {
		return time.Since(sm.StartTime)
	}
	return sm.EndTime.Sub(sm.StartTime)
}

// PerformanceTracker tracks performance metrics over time
type PerformanceTracker struct {
	collector        *Collector
	logger           *logging.ComponentLogger
	intervalMetrics  map[string]*IntervalMetrics
	mu               sync.RWMutex
}

// IntervalMetrics tracks metrics over a time interval
type IntervalMetrics struct {
	StartTime        time.Time
	LedgersProcessed int64
	RecordsStreamed  int64
	BytesTransferred int64
	Errors           int64
}

// NewPerformanceTracker creates a new performance tracker
func NewPerformanceTracker(collector *Collector, logger *logging.ComponentLogger) *PerformanceTracker {
	return &PerformanceTracker{
		collector:       collector,
		logger:          logger,
		intervalMetrics: make(map[string]*IntervalMetrics),
	}
}

// StartInterval starts tracking a new interval
func (pt *PerformanceTracker) StartInterval(name string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.intervalMetrics[name] = &IntervalMetrics{
		StartTime: time.Now(),
	}
}

// RecordInterval records metrics for an interval
func (pt *PerformanceTracker) RecordInterval(name string, ledgers, records, bytes, errors int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	if interval, exists := pt.intervalMetrics[name]; exists {
		interval.LedgersProcessed += ledgers
		interval.RecordsStreamed += records
		interval.BytesTransferred += bytes
		interval.Errors += errors
	}
}

// LogInterval logs and resets interval metrics
func (pt *PerformanceTracker) LogInterval(name string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	if interval, exists := pt.intervalMetrics[name]; exists {
		duration := time.Since(interval.StartTime)
		
		pt.logger.Info().
			Str("interval", name).
			Dur("duration", duration).
			Int64("ledgers", interval.LedgersProcessed).
			Int64("records", interval.RecordsStreamed).
			Int64("bytes", interval.BytesTransferred).
			Float64("ledgers_per_sec", float64(interval.LedgersProcessed)/duration.Seconds()).
			Float64("records_per_sec", float64(interval.RecordsStreamed)/duration.Seconds()).
			Float64("mb_per_sec", float64(interval.BytesTransferred)/duration.Seconds()/1024/1024).
			Int64("errors", interval.Errors).
			Msg("Performance interval metrics")
		
		// Reset interval
		pt.intervalMetrics[name] = &IntervalMetrics{
			StartTime: time.Now(),
		}
	}
}