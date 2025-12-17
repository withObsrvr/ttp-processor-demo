package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthServer provides HTTP endpoints for monitoring
type HealthServer struct {
	server      *http.Server
	startTime   time.Time
	lastFlush   time.Time
	watermark   int64
	stats       FlushStats
	nextFlushAt time.Time
	mu          sync.RWMutex
}

// NewHealthServer creates a new health server
func NewHealthServer(port string, flushInterval time.Duration) *HealthServer {
	hs := &HealthServer{
		startTime:   time.Now(),
		nextFlushAt: time.Now().Add(flushInterval),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/metrics", hs.handleMetrics)

	hs.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return hs
}

// Start starts the health server
func (hs *HealthServer) Start() error {
	log.Printf("🏥 Health server listening on %s", hs.server.Addr)
	return hs.server.ListenAndServe()
}

// Stop stops the health server
func (hs *HealthServer) Stop() error {
	return hs.server.Close()
}

// UpdateStats updates the health server statistics
func (hs *HealthServer) UpdateStats(watermark int64, stats FlushStats, nextFlushAt time.Time) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.lastFlush = time.Now()
	hs.watermark = watermark
	hs.stats = stats
	hs.nextFlushAt = nextFlushAt
}

// handleHealth handles the /health endpoint
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	uptime := time.Since(hs.startTime)
	nextFlushIn := time.Until(hs.nextFlushAt)

	if nextFlushIn < 0 {
		nextFlushIn = 0
	}

	response := map[string]interface{}{
		"service":         "silver-cold-flusher",
		"status":          "healthy",
		"flush_count":     hs.stats.FlushCount,
		"last_flush_time": hs.lastFlush.Format(time.RFC3339),
		"last_watermark":  hs.watermark,
		"total_flushed":   hs.stats.TotalRows,
		"uptime_seconds":  int(uptime.Seconds()),
		"next_flush_in":   nextFlushIn.String(),
		"timestamp":       time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetrics handles the /metrics endpoint (Prometheus format)
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain")

	fmt.Fprintf(w, "# HELP flusher_flush_count Total number of flush operations\n")
	fmt.Fprintf(w, "# TYPE flusher_flush_count counter\n")
	fmt.Fprintf(w, "flusher_flush_count %d\n\n", hs.stats.FlushCount)

	fmt.Fprintf(w, "# HELP flusher_last_watermark Last ledger sequence watermark flushed\n")
	fmt.Fprintf(w, "# TYPE flusher_last_watermark gauge\n")
	fmt.Fprintf(w, "flusher_last_watermark %d\n\n", hs.watermark)

	fmt.Fprintf(w, "# HELP flusher_total_rows_flushed Total rows flushed across all operations\n")
	fmt.Fprintf(w, "# TYPE flusher_total_rows_flushed counter\n")
	fmt.Fprintf(w, "flusher_total_rows_flushed %d\n\n", hs.stats.TotalRows)

	fmt.Fprintf(w, "# HELP flusher_last_flush_timestamp_seconds Unix timestamp of last flush\n")
	fmt.Fprintf(w, "# TYPE flusher_last_flush_timestamp_seconds gauge\n")
	fmt.Fprintf(w, "flusher_last_flush_timestamp_seconds %d\n\n", hs.lastFlush.Unix())
}
