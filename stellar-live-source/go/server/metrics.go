package server

import (
	"sync"
	"time"
)

// DataSourceMetrics tracks comprehensive metrics for data source monitoring
type DataSourceMetrics struct {
	*EnterpriseMetrics

	// Data source specific metrics
	LocalLedgersFetched      int64
	HistoricalLedgersFetched int64
	HybridResponseCount      int64
	HistoricalAccessLatency  *LatencyTracker
	CacheHits                int64
	CacheHitRate             float64
	ArchiveFailures          map[string]int64
	DataLakeErrors           int64
	PrefetchHits             int64
	PrefetchMisses           int64
}

// NewDataSourceMetrics creates a new metrics instance with data source tracking
func NewDataSourceMetrics() *DataSourceMetrics {
	return &DataSourceMetrics{
		EnterpriseMetrics:       NewEnterpriseMetrics(),
		HistoricalAccessLatency: NewLatencyTracker(),
		ArchiveFailures:         make(map[string]int64),
	}
}

// LatencyTracker tracks latency distribution
type LatencyTracker struct {
	mu        sync.RWMutex
	latencies []time.Duration
	maxSize   int
}

// NewLatencyTracker creates a new latency tracker
func NewLatencyTracker() *LatencyTracker {
	return &LatencyTracker{
		latencies: make([]time.Duration, 0, 1000),
		maxSize:   1000,
	}
}

// Record adds a new latency measurement
func (lt *LatencyTracker) Record(latency time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.latencies = append(lt.latencies, latency)
	// Keep only the last maxSize measurements
	if len(lt.latencies) > lt.maxSize {
		lt.latencies = lt.latencies[len(lt.latencies)-lt.maxSize:]
	}
}

// P99 calculates the 99th percentile latency
func (lt *LatencyTracker) P99() time.Duration {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	if len(lt.latencies) == 0 {
		return 0
	}

	// Simple P99 calculation (could be optimized)
	sorted := make([]time.Duration, len(lt.latencies))
	copy(sorted, lt.latencies)
	
	// Sort latencies
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	index := int(float64(len(sorted)) * 0.99)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

// UpdateDataSourceMetrics updates metrics based on data source
func (m *DataSourceMetrics) UpdateDataSourceMetrics(source string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch source {
	case "local":
		m.LocalLedgersFetched++
	case "data_lake", "historical":
		m.HistoricalLedgersFetched++
		m.HistoricalAccessLatency.Record(latency)
	case "hybrid":
		m.HybridResponseCount++
	}

	// Update cache hit rate
	total := float64(m.LocalLedgersFetched + m.HistoricalLedgersFetched)
	if total > 0 && m.CacheHits > 0 {
		m.CacheHitRate = float64(m.CacheHits) / total
	}
}

// RecordArchiveFailure records a failure for a specific archive
func (m *DataSourceMetrics) RecordArchiveFailure(archiveURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ArchiveFailures[archiveURL]++
	m.DataLakeErrors++
}

// GetMetricsSummary returns a summary of current metrics
func (m *DataSourceMetrics) GetMetricsSummary() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	localTotal := m.LocalLedgersFetched
	historicalTotal := m.HistoricalLedgersFetched
	total := localTotal + historicalTotal

	summary := map[string]interface{}{
		"total_ledgers_fetched":      total,
		"local_ledgers_fetched":      localTotal,
		"historical_ledgers_fetched": historicalTotal,
		"hybrid_responses":           m.HybridResponseCount,
		"cache_hit_rate":             m.CacheHitRate,
		"data_lake_errors":           m.DataLakeErrors,
		"prefetch_efficiency":        m.calculatePrefetchEfficiency(),
		"historical_latency_p99":     m.HistoricalAccessLatency.P99().String(),
	}

	if total > 0 {
		summary["local_vs_historical_ratio"] = float64(localTotal) / float64(total)
	}

	return summary
}

func (m *DataSourceMetrics) calculatePrefetchEfficiency() float64 {
	total := m.PrefetchHits + m.PrefetchMisses
	if total == 0 {
		return 0.0
	}
	return float64(m.PrefetchHits) / float64(total)
}