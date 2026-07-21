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
	mu          sync.RWMutex
	port        int
	startTime   time.Time
	checkpoint  *Checkpoint
	metrics     *IngestionMetrics
	server      *http.Server
	streamState string
	staleAfter  time.Duration
	now         func() time.Time
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
	StreamState       string  `json:"stream_state"`
	Uptime            string  `json:"uptime"`
	LastLedger        uint32  `json:"last_ledger"`
	TotalLedgers      uint64  `json:"total_ledgers"`
	TotalTransactions uint64  `json:"total_transactions"`
	TotalOperations   uint64  `json:"total_operations"`
	ProcessingRate    float64 `json:"processing_rate_ledgers_per_sec"`
	ErrorCount        uint64  `json:"error_count"`
	LastError         string  `json:"last_error,omitempty"`
	LastErrorTime     string  `json:"last_error_time,omitempty"`
	LastProgressTime  string  `json:"last_progress_time,omitempty"`
}

// NewHealthServer creates a new health server
func NewHealthServer(port int, checkpoint *Checkpoint) *HealthServer {
	return NewHealthServerWithStaleAfter(port, checkpoint, 2*time.Minute)
}

// NewHealthServerWithStaleAfter creates a health server with an explicit
// progress threshold. It is primarily useful for deterministic health tests.
func NewHealthServerWithStaleAfter(port int, checkpoint *Checkpoint, staleAfter time.Duration) *HealthServer {
	now := time.Now()
	return &HealthServer{
		port:        port,
		startTime:   now,
		checkpoint:  checkpoint,
		streamState: "starting",
		staleAfter:  staleAfter,
		now:         time.Now,
		metrics: &IngestionMetrics{
			LastLedgerTime: now,
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
	streamState := hs.streamState
	startTime := hs.startTime
	staleAfter := hs.staleAfter
	now := hs.now
	hs.mu.RUnlock()
	if now == nil {
		now = time.Now
	}

	lastLedger, totalLedgers, totalTx, totalOps := hs.checkpoint.GetStats()

	hs.metrics.mu.RLock()
	errorCount := hs.metrics.ErrorCount
	lastError := hs.metrics.LastError
	lastErrorTime := hs.metrics.LastErrorTime
	processingRate := hs.metrics.ProcessingRate
	lastLedgerTime := hs.metrics.LastLedgerTime
	hs.metrics.mu.RUnlock()

	status := "healthy"
	httpStatus := http.StatusOK
	if streamState == "reconnecting" || streamState == "connecting" || streamState == "starting" {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	} else if staleAfter > 0 && now().Sub(lastLedgerTime) > staleAfter {
		status = "degraded"
		streamState = "stale"
		httpStatus = http.StatusServiceUnavailable
	}

	resp := HealthResponse{
		Status:            status,
		StreamState:       streamState,
		Uptime:            now().Sub(startTime).String(),
		LastLedger:        lastLedger,
		TotalLedgers:      totalLedgers,
		TotalTransactions: totalTx,
		TotalOperations:   totalOps,
		ProcessingRate:    processingRate,
		ErrorCount:        errorCount,
		LastProgressTime:  lastLedgerTime.UTC().Format(time.RFC3339),
	}

	if lastError != "" {
		resp.LastError = lastError
		resp.LastErrorTime = lastErrorTime.Format(time.RFC3339)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp)
}

// handleMetrics handles /metrics endpoint (Prometheus-compatible plain text)
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
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
	now := hs.currentTime()
	hs.metrics.mu.Lock()
	defer hs.metrics.mu.Unlock()

	hs.metrics.LedgersProcessed += ledgersCount
	hs.metrics.TransactionsProcessed += txCount
	hs.metrics.OperationsProcessed += opCount

	// Calculate processing rate
	elapsed := now.Sub(hs.metrics.LastLedgerTime).Seconds()
	if elapsed > 0 {
		hs.metrics.ProcessingRate = float64(ledgersCount) / elapsed
	}
	hs.metrics.LastLedgerTime = now
}

func (hs *HealthServer) currentTime() time.Time {
	hs.mu.RLock()
	now := hs.now
	hs.mu.RUnlock()
	if now == nil {
		return time.Now()
	}
	return now()
}

func (hs *HealthServer) MarkStreamConnecting() {
	hs.mu.Lock()
	hs.streamState = "connecting"
	hs.mu.Unlock()
}

func (hs *HealthServer) MarkStreamConnected() {
	hs.mu.Lock()
	hs.streamState = "streaming"
	hs.mu.Unlock()
}

func (hs *HealthServer) MarkStreamError(err error) {
	hs.mu.Lock()
	hs.streamState = "reconnecting"
	hs.mu.Unlock()
	hs.RecordError(err)
}

// RecordError records an error in metrics
func (hs *HealthServer) RecordError(err error) {
	now := hs.currentTime()
	hs.metrics.mu.Lock()
	defer hs.metrics.mu.Unlock()

	hs.metrics.ErrorCount++
	hs.metrics.LastError = err.Error()
	hs.metrics.LastErrorTime = now
}
