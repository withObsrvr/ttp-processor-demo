package main

import (
	"fmt"

	"github.com/withObsrvr/flowctl-sdk/pkg/flowctl"
)

// TTPMetrics wraps flowctl StandardMetrics with domain-specific helpers
// This provides TTP-specific metric tracking while using flowctl's infrastructure
type TTPMetrics struct {
	base flowctl.Metrics
}

// NewTTPMetrics creates a new TTP metrics wrapper
func NewTTPMetrics(base flowctl.Metrics) *TTPMetrics {
	return &TTPMetrics{
		base: base,
	}
}

// RecordLedgerProcessed records a ledger being processed
func (m *TTPMetrics) RecordLedgerProcessed(network string, batchMode bool) {
	// Increment the base processed count
	m.base.IncrementProcessedCount()

	// Track dimensional metrics using custom counters
	counterKey := fmt.Sprintf("ledgers_processed_%s_batch_%t", network, batchMode)
	if current, ok := m.getCounter(counterKey); ok {
		m.base.AddCounter(counterKey, current+1)
	} else {
		m.base.AddCounter(counterKey, 1)
	}
}

// RecordEventExtracted records an event being extracted from a ledger
func (m *TTPMetrics) RecordEventExtracted(eventType, network string, batchMode bool) {
	counterKey := fmt.Sprintf("events_extracted_%s_%s_batch_%t", eventType, network, batchMode)
	if current, ok := m.getCounter(counterKey); ok {
		m.base.AddCounter(counterKey, current+1)
	} else {
		m.base.AddCounter(counterKey, 1)
	}
}

// RecordEventFiltered records an event being filtered out
func (m *TTPMetrics) RecordEventFiltered(eventType, filterReason, network string) {
	counterKey := fmt.Sprintf("events_filtered_%s_%s_%s", eventType, filterReason, network)
	if current, ok := m.getCounter(counterKey); ok {
		m.base.AddCounter(counterKey, current+1)
	} else {
		m.base.AddCounter(counterKey, 1)
	}
}

// RecordEventEmitted records an event being emitted (passed filter)
func (m *TTPMetrics) RecordEventEmitted(eventType, network string, batchMode bool) {
	// Increment success count for flowctl heartbeats
	m.base.IncrementSuccessCount()

	counterKey := fmt.Sprintf("events_emitted_%s_%s_batch_%t", eventType, network, batchMode)
	if current, ok := m.getCounter(counterKey); ok {
		m.base.AddCounter(counterKey, current+1)
	} else {
		m.base.AddCounter(counterKey, 1)
	}
}

// RecordProcessingError records a processing error
func (m *TTPMetrics) RecordProcessingError(severity, stage, network string) {
	// Increment error count for flowctl heartbeats
	m.base.IncrementErrorCount()

	counterKey := fmt.Sprintf("processing_errors_%s_%s_%s", severity, stage, network)
	if current, ok := m.getCounter(counterKey); ok {
		m.base.AddCounter(counterKey, current+1)
	} else {
		m.base.AddCounter(counterKey, 1)
	}
}

// RecordProcessingLatency records processing latency
func (m *TTPMetrics) RecordProcessingLatency(stage, network string, durationMs float64, batchMode bool) {
	// Record in base metrics for flowctl
	m.base.RecordProcessingLatency(durationMs)

	// Also track as gauge for dimensional tracking
	gaugeKey := fmt.Sprintf("processing_latency_ms_%s_%s_batch_%t", stage, network, batchMode)
	m.base.AddGauge(gaugeKey, durationMs)
}

// RecordBatchSize records batch size as a gauge
func (m *TTPMetrics) RecordBatchSize(network string, size float64) {
	gaugeKey := fmt.Sprintf("batch_size_%s", network)
	m.base.AddGauge(gaugeKey, size)
}

// SetCurrentBatchSize sets the current batch size gauge
func (m *TTPMetrics) SetCurrentBatchSize(network string, size float64) {
	gaugeKey := fmt.Sprintf("current_batch_size_%s", network)
	m.base.AddGauge(gaugeKey, size)
}

// RecordFilterRate records the filter rate percentage
func (m *TTPMetrics) RecordFilterRate(network string, rate float64) {
	gaugeKey := fmt.Sprintf("filter_rate_percent_%s", network)
	m.base.AddGauge(gaugeKey, rate)
}

// getCounter retrieves the current value of a counter from metrics
// This is a helper to read back counter values for increment operations
func (m *TTPMetrics) getCounter(name string) (int64, bool) {
	metrics := m.base.GetMetrics()
	if val, exists := metrics["counter_"+name]; exists {
		if intVal, ok := val.(int64); ok {
			return intVal, true
		}
	}
	return 0, false
}

// GetMetrics returns all metrics for health/flowctl reporting
func (m *TTPMetrics) GetMetrics() map[string]interface{} {
	return m.base.GetMetrics()
}
