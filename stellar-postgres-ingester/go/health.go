package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// HealthServer provides health and metrics endpoints
type HealthServer struct {
	mu         sync.RWMutex
	port       int
	startTime  time.Time
	checkpoint *Checkpoint
	metrics    *IngestionMetrics
	server     *http.Server
}

// IngestionMetrics tracks real-time ingestion metrics
type IngestionMetrics struct {
	mu                    sync.RWMutex
	LedgersProcessed      uint64
	TransactionsProcessed uint64
	OperationsProcessed   uint64
	ErrorCount            uint64
	LastError             string
	LastErrorTime         time.Time
	LastLedgerTime        time.Time
	ProcessingRate        float64 // Ledgers per second
}

// HealthResponse is the JSON response for /health
type HealthResponse struct {
	Status            string  `json:"status"`
	Uptime            string  `json:"uptime"`
	LastLedger        uint32  `json:"last_ledger"`
	TotalLedgers      uint64  `json:"total_ledgers"`
	TotalTransactions uint64  `json:"total_transactions"`
	TotalOperations   uint64  `json:"total_operations"`
	ProcessingRate    float64 `json:"processing_rate_ledgers_per_sec"`
	ErrorCount        uint64  `json:"error_count"`
	LastError         string  `json:"last_error,omitempty"`
	LastErrorTime     string  `json:"last_error_time,omitempty"`
}

// NewHealthServer creates a new health server
func NewHealthServer(port int, checkpoint *Checkpoint) *HealthServer {
	return &HealthServer{
		port:       port,
		startTime:  time.Now(),
		checkpoint: checkpoint,
		metrics: &IngestionMetrics{
			LastLedgerTime: time.Now(),
		},
	}
}

// Start starts the health HTTP server
func (hs *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/metrics", hs.handleMetrics)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Health server error: %v\n", err)
		}
	}()

	return nil
}

// Stop gracefully stops the health server
func (hs *HealthServer) Stop() error {
	if hs.server != nil {
		return hs.server.Close()
	}
	return nil
}

// handleHealth handles /health endpoint
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	lastLedger, totalLedgers, totalTx, totalOps := hs.checkpoint.GetStats()

	hs.metrics.mu.RLock()
	errorCount := hs.metrics.ErrorCount
	lastError := hs.metrics.LastError
	lastErrorTime := hs.metrics.LastErrorTime
	processingRate := hs.metrics.ProcessingRate
	hs.metrics.mu.RUnlock()

	resp := HealthResponse{
		Status:            "healthy",
		Uptime:            time.Since(hs.startTime).String(),
		LastLedger:        lastLedger,
		TotalLedgers:      totalLedgers,
		TotalTransactions: totalTx,
		TotalOperations:   totalOps,
		ProcessingRate:    processingRate,
		ErrorCount:        errorCount,
	}

	if lastError != "" {
		resp.LastError = lastError
		resp.LastErrorTime = lastErrorTime.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleMetrics handles /metrics endpoint (Prometheus-compatible plain text)
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	lastLedger, totalLedgers, totalTx, totalOps := hs.checkpoint.GetStats()

	hs.metrics.mu.RLock()
	errorCount := hs.metrics.ErrorCount
	processingRate := hs.metrics.ProcessingRate
	hs.metrics.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_last_ledger Last processed ledger sequence\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_last_ledger gauge\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_last_ledger %d\n", lastLedger)

	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_total_ledgers Total ledgers processed\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_total_ledgers counter\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_total_ledgers %d\n", totalLedgers)

	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_total_transactions Total transactions processed\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_total_transactions counter\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_total_transactions %d\n", totalTx)

	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_total_operations Total operations processed\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_total_operations counter\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_total_operations %d\n", totalOps)

	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_processing_rate Processing rate (ledgers/sec)\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_processing_rate gauge\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_processing_rate %.2f\n", processingRate)

	fmt.Fprintf(w, "# HELP stellar_postgres_ingester_errors Total errors\n")
	fmt.Fprintf(w, "# TYPE stellar_postgres_ingester_errors counter\n")
	fmt.Fprintf(w, "stellar_postgres_ingester_errors %d\n", errorCount)
}

// UpdateMetrics updates the ingestion metrics
func (hs *HealthServer) UpdateMetrics(ledgersCount, txCount, opCount uint64) {
	hs.metrics.mu.Lock()
	defer hs.metrics.mu.Unlock()

	hs.metrics.LedgersProcessed += ledgersCount
	hs.metrics.TransactionsProcessed += txCount
	hs.metrics.OperationsProcessed += opCount

	// Calculate processing rate
	now := time.Now()
	elapsed := now.Sub(hs.metrics.LastLedgerTime).Seconds()
	if elapsed > 0 {
		hs.metrics.ProcessingRate = float64(ledgersCount) / elapsed
	}
	hs.metrics.LastLedgerTime = now
}

// RecordError records an error in metrics
func (hs *HealthServer) RecordError(err error) {
	hs.metrics.mu.Lock()
	defer hs.metrics.mu.Unlock()

	hs.metrics.ErrorCount++
	hs.metrics.LastError = err.Error()
	hs.metrics.LastErrorTime = time.Now()
}
