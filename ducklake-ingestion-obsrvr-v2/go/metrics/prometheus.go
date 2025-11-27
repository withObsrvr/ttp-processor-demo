// Package metrics provides Prometheus metrics for Bronze Copier V2.
package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Bronze Copier metrics.
type Metrics struct {
	// Counters
	LedgersProcessed *prometheus.CounterVec
	RowsWritten      *prometheus.CounterVec
	BatchesCompleted *prometheus.CounterVec
	ErrorsTotal      *prometheus.CounterVec

	// Gauges
	ActiveWorkers   prometheus.Gauge
	CurrentLedger   prometheus.Gauge
	PendingBatches  prometheus.Gauge
	BufferedLedgers prometheus.Gauge

	// Histograms
	FlushDuration     *prometheus.HistogramVec
	BatchDuration     prometheus.Histogram
	LedgerLatency     prometheus.Histogram
	ExtractionLatency *prometheus.HistogramVec

	// Summary
	ProcessingRate prometheus.Summary

	// Internal
	registry *prometheus.Registry
	mu       sync.RWMutex
	enabled  bool
}

// Config holds metrics configuration.
type Config struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"` // e.g., ":9090"
}

// ApplyDefaults sets default values for metrics config.
func (c *Config) ApplyDefaults() {
	if c.Address == "" {
		c.Address = ":9090"
	}
}

// New creates a new metrics instance.
func New(cfg Config) *Metrics {
	cfg.ApplyDefaults()

	m := &Metrics{
		enabled:  cfg.Enabled,
		registry: prometheus.NewRegistry(),
	}

	if !cfg.Enabled {
		return m
	}

	// Counters
	m.LedgersProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bronze",
			Name:      "ledgers_processed_total",
			Help:      "Total number of ledgers processed",
		},
		[]string{"network", "era"},
	)

	m.RowsWritten = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bronze",
			Name:      "rows_written_total",
			Help:      "Total rows written by table",
		},
		[]string{"table"},
	)

	m.BatchesCompleted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bronze",
			Name:      "batches_completed_total",
			Help:      "Total batches completed",
		},
		[]string{"status"}, // "success", "error"
	)

	m.ErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bronze",
			Name:      "errors_total",
			Help:      "Total errors by type",
		},
		[]string{"type"}, // "extraction", "flush", "audit", "source"
	)

	// Gauges
	m.ActiveWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "bronze",
			Name:      "workers_active",
			Help:      "Number of active worker goroutines",
		},
	)

	m.CurrentLedger = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "bronze",
			Name:      "current_ledger",
			Help:      "Current ledger being processed",
		},
	)

	m.PendingBatches = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "bronze",
			Name:      "pending_batches",
			Help:      "Number of batches waiting to be processed",
		},
	)

	m.BufferedLedgers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "bronze",
			Name:      "buffered_ledgers",
			Help:      "Number of ledgers buffered for processing",
		},
	)

	// Histograms
	m.FlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bronze",
			Name:      "flush_duration_seconds",
			Help:      "Time spent flushing data to DuckLake",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"table"},
	)

	m.BatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "bronze",
			Name:      "batch_duration_seconds",
			Help:      "Time to process a batch of ledgers",
			Buckets:   []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
		},
	)

	m.LedgerLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "bronze",
			Name:      "ledger_latency_seconds",
			Help:      "Latency from ledger close to ingestion",
			Buckets:   []float64{1, 5, 10, 30, 60, 300, 600, 1800, 3600},
		},
	)

	m.ExtractionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bronze",
			Name:      "extraction_duration_seconds",
			Help:      "Time to extract data from a ledger",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"table"},
	)

	// Summary
	m.ProcessingRate = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:  "bronze",
			Name:       "processing_rate_ledgers_per_second",
			Help:       "Ledger processing rate",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
	)

	// Register all metrics
	m.registry.MustRegister(
		m.LedgersProcessed,
		m.RowsWritten,
		m.BatchesCompleted,
		m.ErrorsTotal,
		m.ActiveWorkers,
		m.CurrentLedger,
		m.PendingBatches,
		m.BufferedLedgers,
		m.FlushDuration,
		m.BatchDuration,
		m.LedgerLatency,
		m.ExtractionLatency,
		m.ProcessingRate,
	)

	// Also register Go runtime metrics
	m.registry.MustRegister(prometheus.NewGoCollector())
	m.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return m
}

// Handler returns an HTTP handler for metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// StartServer starts a metrics HTTP server.
func (m *Metrics) StartServer(addr string) error {
	if !m.enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return http.ListenAndServe(addr, mux)
}

// IsEnabled returns true if metrics are enabled.
func (m *Metrics) IsEnabled() bool {
	return m.enabled
}

// Helper methods for common operations

// RecordLedgerProcessed increments the ledger counter.
func (m *Metrics) RecordLedgerProcessed(network, era string) {
	if m.enabled && m.LedgersProcessed != nil {
		m.LedgersProcessed.WithLabelValues(network, era).Inc()
	}
}

// RecordRowsWritten increments rows written for a table.
func (m *Metrics) RecordRowsWritten(table string, count int64) {
	if m.enabled && m.RowsWritten != nil {
		m.RowsWritten.WithLabelValues(table).Add(float64(count))
	}
}

// RecordBatchCompleted increments batch counter.
func (m *Metrics) RecordBatchCompleted(success bool) {
	if m.enabled && m.BatchesCompleted != nil {
		status := "success"
		if !success {
			status = "error"
		}
		m.BatchesCompleted.WithLabelValues(status).Inc()
	}
}

// RecordError increments error counter.
func (m *Metrics) RecordError(errorType string) {
	if m.enabled && m.ErrorsTotal != nil {
		m.ErrorsTotal.WithLabelValues(errorType).Inc()
	}
}

// SetActiveWorkers sets the active worker gauge.
func (m *Metrics) SetActiveWorkers(count int) {
	if m.enabled && m.ActiveWorkers != nil {
		m.ActiveWorkers.Set(float64(count))
	}
}

// SetCurrentLedger sets the current ledger gauge.
func (m *Metrics) SetCurrentLedger(seq uint32) {
	if m.enabled && m.CurrentLedger != nil {
		m.CurrentLedger.Set(float64(seq))
	}
}

// SetPendingBatches sets the pending batches gauge.
func (m *Metrics) SetPendingBatches(count int) {
	if m.enabled && m.PendingBatches != nil {
		m.PendingBatches.Set(float64(count))
	}
}

// RecordFlushDuration records flush duration for a table.
func (m *Metrics) RecordFlushDuration(table string, duration time.Duration) {
	if m.enabled && m.FlushDuration != nil {
		m.FlushDuration.WithLabelValues(table).Observe(duration.Seconds())
	}
}

// RecordBatchDuration records batch processing duration.
func (m *Metrics) RecordBatchDuration(duration time.Duration) {
	if m.enabled && m.BatchDuration != nil {
		m.BatchDuration.Observe(duration.Seconds())
	}
}

// RecordLedgerLatency records ingestion latency from ledger close.
func (m *Metrics) RecordLedgerLatency(closedAt time.Time) {
	if m.enabled && m.LedgerLatency != nil {
		latency := time.Since(closedAt)
		m.LedgerLatency.Observe(latency.Seconds())
	}
}

// RecordExtractionDuration records extraction duration for a table.
func (m *Metrics) RecordExtractionDuration(table string, duration time.Duration) {
	if m.enabled && m.ExtractionLatency != nil {
		m.ExtractionLatency.WithLabelValues(table).Observe(duration.Seconds())
	}
}

// RecordProcessingRate records the processing rate.
func (m *Metrics) RecordProcessingRate(rate float64) {
	if m.enabled && m.ProcessingRate != nil {
		m.ProcessingRate.Observe(rate)
	}
}
