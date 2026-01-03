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
	duckdb      *DuckDBClient
	startTime   time.Time
	lastFlush   time.Time
	watermark   int64
	stats       FlushStats
	nextFlushAt time.Time
	mu          sync.RWMutex
}

// NewHealthServer creates a new health server
func NewHealthServer(port string, flushInterval time.Duration, duckdb *DuckDBClient) *HealthServer {
	hs := &HealthServer{
		duckdb:      duckdb,
		startTime:   time.Now(),
		nextFlushAt: time.Now().Add(flushInterval),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/metrics", hs.handleMetrics)
	mux.HandleFunc("/maintenance/recreate-silver", hs.handleRecreateSilver)
	mux.HandleFunc("/maintenance/merge", hs.handleMergeSilver)
	mux.HandleFunc("/maintenance/expire", hs.handleExpireSilver)
	mux.HandleFunc("/maintenance/cleanup", hs.handleCleanupSilver)
	mux.HandleFunc("/maintenance/full", hs.handleFullMaintenanceSilver)

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

// handleRecreateSilver handles the /maintenance/recreate-silver endpoint
// WARNING: This deletes ALL Silver data and recreates tables with partitioning
func (hs *HealthServer) handleRecreateSilver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to recreate Silver tables.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("⚠️  Maintenance endpoint called: /maintenance/recreate-silver")

	if err := hs.duckdb.RecreateAllSilverTables(); err != nil {
		log.Printf("ERROR: Failed to recreate Silver tables: %v", err)
		http.Error(w, fmt.Sprintf("Failed to recreate Silver tables: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Silver tables recreated with partitioning",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Silver tables recreated successfully")
}

// handleMergeSilver handles the /maintenance/merge endpoint
// Merges adjacent files for all Silver tables
func (hs *HealthServer) handleMergeSilver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to merge files.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🔧 Received request to merge Silver files")

	// Merge with max files to avoid memory issues
	if err := hs.duckdb.MergeAllSilverTables(DefaultMaxCompactedFiles); err != nil{
		log.Printf("ERROR: Failed to merge Silver files: %v", err)
		http.Error(w, fmt.Sprintf("Failed to merge files: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Silver files merged successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Silver file merge completed successfully")
}

// handleExpireSilver handles the /maintenance/expire endpoint
// Expires old snapshots to mark merged files for cleanup
func (hs *HealthServer) handleExpireSilver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to expire snapshots.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🗑️  Received request to expire Silver snapshots")

	if err := hs.duckdb.ExpireSilverSnapshots(); err != nil {
		log.Printf("ERROR: Failed to expire snapshots: %v", err)
		http.Error(w, fmt.Sprintf("Failed to expire snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Silver snapshots expired successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Silver snapshot expiration completed successfully")
}

// handleCleanupSilver handles the /maintenance/cleanup endpoint
// Cleans up orphaned files from S3
func (hs *HealthServer) handleCleanupSilver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to cleanup files.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🧹 Received request to cleanup Silver orphaned files")

	if err := hs.duckdb.CleanupSilverOrphanedFiles(); err != nil {
		log.Printf("ERROR: Failed to cleanup orphaned files: %v", err)
		http.Error(w, fmt.Sprintf("Failed to cleanup files: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Silver orphaned files cleaned up successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Silver cleanup completed successfully")
}

// handleFullMaintenanceSilver handles the /maintenance/full endpoint
// Runs full maintenance cycle: merge → expire → cleanup
func (hs *HealthServer) handleFullMaintenanceSilver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to run full maintenance.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🔧 Received request for full Silver maintenance cycle")

	if err := hs.duckdb.PerformSilverMaintenanceCycle(DefaultMaxCompactedFiles); err != nil {
		log.Printf("ERROR: Failed to complete maintenance cycle: %v", err)
		http.Error(w, fmt.Sprintf("Failed to complete maintenance: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Full Silver maintenance cycle completed successfully",
		"note":    "Files merged, snapshots expired, and orphaned files cleaned up",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Full Silver maintenance cycle completed successfully")
}
