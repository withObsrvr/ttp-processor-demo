package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Contract processing metrics
	contractsProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_contracts_processed_total",
		Help: "Total number of contract data entries processed",
	})
	
	entriesSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_entries_skipped_total",
		Help: "Total number of entries skipped due to filters",
	})
	
	batchesCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_batches_created_total",
		Help: "Total number of Arrow batches created",
	})
	
	bytesProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_bytes_processed_total",
		Help: "Total bytes of data processed",
	})
	
	processingErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_processing_errors_total",
		Help: "Total number of processing errors",
	})
	
	processingDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "contract_data_processing_duration_seconds",
		Help:    "Time taken to process a ledger",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})
	
	currentLedgerGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "contract_data_current_ledger",
		Help: "Current ledger being processed",
	})
	
	// Flowctl integration metrics
	flowctlRegistered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "contract_data_flowctl_registered",
		Help: "Whether the service is registered with flowctl (1=yes, 0=no)",
	})
	
	flowctlHeartbeatsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_flowctl_heartbeats_sent_total",
		Help: "Total number of heartbeats sent to flowctl",
	})
	
	flowctlHeartbeatErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_flowctl_heartbeat_errors_total",
		Help: "Total number of flowctl heartbeat errors",
	})
	
	// Session metrics
	activeSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "contract_data_active_sessions",
		Help: "Number of active processing sessions",
	})
	
	// Arrow Flight metrics
	flightClientsConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "contract_data_flight_clients_connected",
		Help: "Number of connected Arrow Flight clients",
	})
	
	flightRecordsStreamed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contract_data_flight_records_streamed_total",
		Help: "Total number of Arrow records streamed",
	})
)

// UpdatePrometheusProcessingMetrics updates Prometheus metrics from processing metrics
func UpdatePrometheusProcessingMetrics(pm ProcessingMetrics) {
	contractsProcessedTotal.Add(float64(pm.EntriesProcessed))
	entriesSkippedTotal.Add(float64(pm.EntriesSkipped))
	batchesCreatedTotal.Add(float64(pm.BatchesCreated))
	bytesProcessedTotal.Add(float64(pm.BytesProcessed))
	currentLedgerGauge.Set(float64(pm.CurrentLedger))
}

// IncrementPrometheusProcessingErrors increments the error counter
func IncrementPrometheusProcessingErrors() {
	processingErrorsTotal.Inc()
}

// ObservePrometheusProcessingDuration records processing time for a ledger
func ObservePrometheusProcessingDuration(seconds float64) {
	processingDurationHistogram.Observe(seconds)
}

// SetPrometheusFlowctlRegistered sets whether we're registered with flowctl
func SetPrometheusFlowctlRegistered(registered bool) {
	if registered {
		flowctlRegistered.Set(1)
	} else {
		flowctlRegistered.Set(0)
	}
}

// IncrementPrometheusFlowctlHeartbeats increments successful heartbeat counter
func IncrementPrometheusFlowctlHeartbeats() {
	flowctlHeartbeatsSent.Inc()
}

// IncrementPrometheusFlowctlHeartbeatErrors increments heartbeat error counter
func IncrementPrometheusFlowctlHeartbeatErrors() {
	flowctlHeartbeatErrors.Inc()
}

// SetPrometheusActiveSessions sets the number of active sessions
func SetPrometheusActiveSessions(count int) {
	activeSessions.Set(float64(count))
}

// SetPrometheusFlightClientsConnected sets the number of connected clients
func SetPrometheusFlightClientsConnected(count int) {
	flightClientsConnected.Set(float64(count))
}

// IncrementPrometheusFlightRecordsStreamed increments streamed records counter
func IncrementPrometheusFlightRecordsStreamed(count int64) {
	flightRecordsStreamed.Add(float64(count))
}