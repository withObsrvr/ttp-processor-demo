package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthServer provides HTTP health check and metrics endpoints
type HealthServer struct {
	port   int
	stats  *ProcessorStats
	mu     sync.RWMutex
	server *http.Server
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status                  string `json:"status"`
	LastLedgerProcessed     int64  `json:"last_ledger_processed"`
	LastProcessedAt         string `json:"last_processed_at,omitempty"`
	LagSeconds              int64  `json:"lag_seconds"`
	TokensDiscovered        int64  `json:"tokens_discovered"`
	TokensUpdated           int64  `json:"tokens_updated"`
	TotalTokensInRegistry   int64  `json:"total_tokens_in_registry"`
	SEP41TokenCount         int64  `json:"sep41_token_count"`
	LPTokenCount            int64  `json:"lp_token_count"`
	SACTokenCount           int64  `json:"sac_token_count"`
	UnknownTokenCount       int64  `json:"unknown_token_count"`
	CyclesCompleted         int64  `json:"cycles_completed"`
	LastCycleDurationMs     int64  `json:"last_cycle_duration_ms"`
	LastError               string `json:"last_error,omitempty"`
	ConsecutiveErrorCount   int    `json:"consecutive_error_count"`
}

// NewHealthServer creates a new health server
func NewHealthServer(port int) *HealthServer {
	return &HealthServer{
		port: port,
		stats: &ProcessorStats{
			LastProcessedAt: time.Now(),
		},
	}
}

// UpdateStats updates the processor statistics
func (hs *HealthServer) UpdateStats(stats ProcessorStats) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.stats = &stats
}

// GetStats returns a copy of current stats
func (hs *HealthServer) GetStats() ProcessorStats {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	return *hs.stats
}

// Start starts the health check HTTP server
func (hs *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/metrics", hs.handleMetrics)
	mux.HandleFunc("/stats", hs.handleStats)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	log.Printf("🏥 Health server listening on :%d", hs.port)

	go func() {
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
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

// handleHealth handles the /health endpoint
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := hs.GetStats()

	// Calculate lag
	var lagSeconds int64
	if !stats.LastProcessedAt.IsZero() {
		lagSeconds = int64(time.Since(stats.LastProcessedAt).Seconds())
	}

	response := HealthResponse{
		Status:                  "healthy",
		LastLedgerProcessed:     stats.LastLedgerProcessed,
		LagSeconds:              lagSeconds,
		TokensDiscovered:        stats.TokensDiscovered,
		TokensUpdated:           stats.TokensUpdated,
		TotalTokensInRegistry:   stats.TotalTokensInRegistry,
		SEP41TokenCount:         stats.SEP41TokenCount,
		LPTokenCount:            stats.LPTokenCount,
		SACTokenCount:           stats.SACTokenCount,
		UnknownTokenCount:       stats.UnknownTokenCount,
		CyclesCompleted:         stats.CyclesCompleted,
		LastCycleDurationMs:     stats.LastCycleDuration.Milliseconds(),
		LastError:               stats.LastError,
		ConsecutiveErrorCount:   stats.ConsecutiveErrorCount,
	}

	if !stats.LastProcessedAt.IsZero() {
		response.LastProcessedAt = stats.LastProcessedAt.Format(time.RFC3339)
	}

	// Set status based on lag and errors
	if stats.ConsecutiveErrorCount > 5 {
		response.Status = "unhealthy"
	} else if lagSeconds > 300 { // 5 minutes
		response.Status = "degraded"
	} else if lagSeconds > 900 { // 15 minutes
		response.Status = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	if response.Status == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding health response: %v", err)
	}
}

// handleMetrics handles the /metrics endpoint (Prometheus format)
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := hs.GetStats()

	w.Header().Set("Content-Type", "text/plain")

	// Output Prometheus-compatible metrics
	fmt.Fprintf(w, "# HELP token_discovery_last_ledger_processed Last processed ledger sequence\n")
	fmt.Fprintf(w, "# TYPE token_discovery_last_ledger_processed gauge\n")
	fmt.Fprintf(w, "token_discovery_last_ledger_processed %d\n", stats.LastLedgerProcessed)

	fmt.Fprintf(w, "# HELP token_discovery_tokens_discovered_total Total tokens discovered\n")
	fmt.Fprintf(w, "# TYPE token_discovery_tokens_discovered_total counter\n")
	fmt.Fprintf(w, "token_discovery_tokens_discovered_total %d\n", stats.TokensDiscovered)

	fmt.Fprintf(w, "# HELP token_discovery_tokens_updated_total Total token updates\n")
	fmt.Fprintf(w, "# TYPE token_discovery_tokens_updated_total counter\n")
	fmt.Fprintf(w, "token_discovery_tokens_updated_total %d\n", stats.TokensUpdated)

	fmt.Fprintf(w, "# HELP token_discovery_registry_total Total tokens in registry by type\n")
	fmt.Fprintf(w, "# TYPE token_discovery_registry_total gauge\n")
	fmt.Fprintf(w, "token_discovery_registry_total{type=\"sep41\"} %d\n", stats.SEP41TokenCount)
	fmt.Fprintf(w, "token_discovery_registry_total{type=\"lp\"} %d\n", stats.LPTokenCount)
	fmt.Fprintf(w, "token_discovery_registry_total{type=\"sac\"} %d\n", stats.SACTokenCount)
	fmt.Fprintf(w, "token_discovery_registry_total{type=\"unknown\"} %d\n", stats.UnknownTokenCount)

	fmt.Fprintf(w, "# HELP token_discovery_cycles_total Total discovery cycles completed\n")
	fmt.Fprintf(w, "# TYPE token_discovery_cycles_total counter\n")
	fmt.Fprintf(w, "token_discovery_cycles_total %d\n", stats.CyclesCompleted)

	fmt.Fprintf(w, "# HELP token_discovery_last_cycle_duration_ms Duration of last discovery cycle in ms\n")
	fmt.Fprintf(w, "# TYPE token_discovery_last_cycle_duration_ms gauge\n")
	fmt.Fprintf(w, "token_discovery_last_cycle_duration_ms %d\n", stats.LastCycleDuration.Milliseconds())

	fmt.Fprintf(w, "# HELP token_discovery_consecutive_errors Consecutive error count\n")
	fmt.Fprintf(w, "# TYPE token_discovery_consecutive_errors gauge\n")
	fmt.Fprintf(w, "token_discovery_consecutive_errors %d\n", stats.ConsecutiveErrorCount)
}

// handleStats handles the /stats endpoint (detailed JSON stats)
func (hs *HealthServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := hs.GetStats()

	response := map[string]interface{}{
		"processor": map[string]interface{}{
			"last_ledger_processed":   stats.LastLedgerProcessed,
			"last_processed_at":       stats.LastProcessedAt.Format(time.RFC3339),
			"cycles_completed":        stats.CyclesCompleted,
			"last_cycle_duration_ms":  stats.LastCycleDuration.Milliseconds(),
			"consecutive_error_count": stats.ConsecutiveErrorCount,
			"last_error":              stats.LastError,
		},
		"discovery": map[string]interface{}{
			"tokens_discovered": stats.TokensDiscovered,
			"tokens_updated":    stats.TokensUpdated,
		},
		"registry": map[string]interface{}{
			"total":   stats.TotalTokensInRegistry,
			"sep41":   stats.SEP41TokenCount,
			"lp":      stats.LPTokenCount,
			"sac":     stats.SACTokenCount,
			"unknown": stats.UnknownTokenCount,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
