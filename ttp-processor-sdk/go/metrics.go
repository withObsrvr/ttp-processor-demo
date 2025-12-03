package main

import (
	"fmt"
	"sync"
	"time"
)

// MetricType defines the type of metric
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
)

// Metric represents a single metric with labels
type Metric struct {
	Name      string
	Type      MetricType
	Value     float64
	Labels    map[string]string
	Timestamp time.Time
}

// MetricsCollector collects and aggregates metrics
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*Metric
	enabled bool
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(enabled bool) *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]*Metric),
		enabled: enabled,
	}
}

// metricKey generates a unique key for a metric with labels
func metricKey(name string, labels map[string]string) string {
	key := name
	for k, v := range labels {
		key += fmt.Sprintf(",%s=%s", k, v)
	}
	return key
}

// Inc increments a counter metric
func (mc *MetricsCollector) Inc(name string, labels map[string]string) {
	if !mc.enabled {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := metricKey(name, labels)
	if metric, exists := mc.metrics[key]; exists {
		metric.Value++
		metric.Timestamp = time.Now()
	} else {
		mc.metrics[key] = &Metric{
			Name:      name,
			Type:      MetricTypeCounter,
			Value:     1,
			Labels:    labels,
			Timestamp: time.Now(),
		}
	}
}

// Add adds a value to a counter metric
func (mc *MetricsCollector) Add(name string, value float64, labels map[string]string) {
	if !mc.enabled {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := metricKey(name, labels)
	if metric, exists := mc.metrics[key]; exists {
		metric.Value += value
		metric.Timestamp = time.Now()
	} else {
		mc.metrics[key] = &Metric{
			Name:      name,
			Type:      MetricTypeCounter,
			Value:     value,
			Labels:    labels,
			Timestamp: time.Now(),
		}
	}
}

// Set sets a gauge metric to a specific value
func (mc *MetricsCollector) Set(name string, value float64, labels map[string]string) {
	if !mc.enabled {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := metricKey(name, labels)
	mc.metrics[key] = &Metric{
		Name:      name,
		Type:      MetricTypeGauge,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	}
}

// Observe records a histogram observation
func (mc *MetricsCollector) Observe(name string, value float64, labels map[string]string) {
	if !mc.enabled {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := metricKey(name, labels)
	// For simplicity, we store the latest observation
	// In production, would calculate percentiles
	mc.metrics[key] = &Metric{
		Name:      name,
		Type:      MetricTypeHistogram,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	}
}

// Get retrieves a metric by name and labels
func (mc *MetricsCollector) Get(name string, labels map[string]string) (*Metric, bool) {
	if !mc.enabled {
		return nil, false
	}

	mc.mu.RLock()
	defer mc.mu.RUnlock()

	key := metricKey(name, labels)
	metric, exists := mc.metrics[key]
	return metric, exists
}

// GetAll returns all collected metrics
func (mc *MetricsCollector) GetAll() []*Metric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	result := make([]*Metric, 0, len(mc.metrics))
	for _, metric := range mc.metrics {
		result = append(result, metric)
	}
	return result
}

// Reset clears all metrics
func (mc *MetricsCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics = make(map[string]*Metric)
}

// FormatPrometheus formats metrics in Prometheus exposition format
func (mc *MetricsCollector) FormatPrometheus() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	output := ""
	for _, metric := range mc.metrics {
		// Format labels
		labelsStr := ""
		if len(metric.Labels) > 0 {
			labelPairs := make([]string, 0, len(metric.Labels))
			for k, v := range metric.Labels {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=\"%s\"", k, v))
			}
			labelsStr = "{" + join(labelPairs, ",") + "}"
		}

		// Format metric line
		output += fmt.Sprintf("%s%s %v %d\n",
			metric.Name,
			labelsStr,
			metric.Value,
			metric.Timestamp.UnixMilli())
	}
	return output
}

// join is a simple string join helper
func join(strings []string, sep string) string {
	if len(strings) == 0 {
		return ""
	}
	result := strings[0]
	for i := 1; i < len(strings); i++ {
		result += sep + strings[i]
	}
	return result
}

// ProcessorMetrics provides domain-specific metrics for the processor
type ProcessorMetrics struct {
	collector *MetricsCollector
}

// NewProcessorMetrics creates a new processor metrics instance
func NewProcessorMetrics(enabled bool) *ProcessorMetrics {
	return &ProcessorMetrics{
		collector: NewMetricsCollector(enabled),
	}
}

// RecordLedgerProcessed records a ledger being processed
func (pm *ProcessorMetrics) RecordLedgerProcessed(network string, batchMode bool) {
	labels := map[string]string{
		"network":    network,
		"batch_mode": fmt.Sprintf("%t", batchMode),
	}
	pm.collector.Inc("ttp_ledgers_processed_total", labels)
}

// RecordEventExtracted records an event being extracted
func (pm *ProcessorMetrics) RecordEventExtracted(eventType, network string, batchMode bool) {
	labels := map[string]string{
		"event_type": eventType,
		"network":    network,
		"batch_mode": fmt.Sprintf("%t", batchMode),
	}
	pm.collector.Inc("ttp_events_extracted_total", labels)
}

// RecordEventFiltered records an event being filtered out
func (pm *ProcessorMetrics) RecordEventFiltered(eventType, filterReason, network string) {
	labels := map[string]string{
		"event_type":    eventType,
		"filter_reason": filterReason,
		"network":       network,
	}
	pm.collector.Inc("ttp_events_filtered_total", labels)
}

// RecordEventEmitted records an event being emitted
func (pm *ProcessorMetrics) RecordEventEmitted(eventType, network string, batchMode bool) {
	labels := map[string]string{
		"event_type": eventType,
		"network":    network,
		"batch_mode": fmt.Sprintf("%t", batchMode),
	}
	pm.collector.Inc("ttp_events_emitted_total", labels)
}

// RecordProcessingError records a processing error
func (pm *ProcessorMetrics) RecordProcessingError(severity, stage, network string) {
	labels := map[string]string{
		"severity": severity,
		"stage":    stage,
		"network":  network,
	}
	pm.collector.Inc("ttp_processing_errors_total", labels)
}

// RecordProcessingLatency records processing latency
func (pm *ProcessorMetrics) RecordProcessingLatency(stage, network string, durationMs float64, batchMode bool) {
	labels := map[string]string{
		"stage":      stage,
		"network":    network,
		"batch_mode": fmt.Sprintf("%t", batchMode),
	}
	pm.collector.Observe("ttp_processing_duration_ms", durationMs, labels)
}

// RecordBatchSize records batch size
func (pm *ProcessorMetrics) RecordBatchSize(network string, size float64) {
	labels := map[string]string{
		"network": network,
	}
	pm.collector.Observe("ttp_batch_size", size, labels)
}

// SetCurrentBatchSize sets the current batch size gauge
func (pm *ProcessorMetrics) SetCurrentBatchSize(network string, size float64) {
	labels := map[string]string{
		"network": network,
	}
	pm.collector.Set("ttp_current_batch_size", size, labels)
}

// RecordFilterRate records the filter rate percentage
func (pm *ProcessorMetrics) RecordFilterRate(network string, rate float64) {
	labels := map[string]string{
		"network": network,
	}
	pm.collector.Set("ttp_filter_rate_percent", rate, labels)
}

// GetCollector returns the underlying metrics collector
func (pm *ProcessorMetrics) GetCollector() *MetricsCollector {
	return pm.collector
}

// FormatPrometheus formats metrics in Prometheus format
func (pm *ProcessorMetrics) FormatPrometheus() string {
	return pm.collector.FormatPrometheus()
}
