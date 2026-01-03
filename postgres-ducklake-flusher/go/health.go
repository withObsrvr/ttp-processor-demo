package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// HealthServer provides HTTP health endpoints and maintenance operations
type HealthServer struct {
	flusher *Flusher
	duckdb  *DuckDBClient
	port    int
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status          string `json:"status"`
	FlushCount      int64  `json:"flush_count"`
	LastFlushTime   string `json:"last_flush_time"`
	LastWatermark   int64  `json:"last_watermark"`
	TotalFlushed    int64  `json:"total_flushed"`
	UptimeSeconds   int64  `json:"uptime_seconds"`
	NextFlushIn     string `json:"next_flush_in"`
}

// NewHealthServer creates a new health server
func NewHealthServer(flusher *Flusher, duckdb *DuckDBClient, port int) *HealthServer {
	return &HealthServer{
		flusher: flusher,
		duckdb:  duckdb,
		port:    port,
	}
}

// Start starts the health HTTP server
func (h *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/maintenance/recreate-bronze", h.handleRecreateBronze)
	mux.HandleFunc("/maintenance/merge", h.handleMergeBronze)
	mux.HandleFunc("/maintenance/expire", h.handleExpireBronze)
	mux.HandleFunc("/maintenance/cleanup", h.handleCleanupBronze)
	mux.HandleFunc("/maintenance/full", h.handleFullMaintenanceBronze)

	addr := fmt.Sprintf(":%d", h.port)
	log.Printf("Health server listening on %s", addr)

	return http.ListenAndServe(addr, mux)
}

// handleHealth handles the /health endpoint
func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Calculate uptime
	startTime := time.Now().Unix() - h.flusher.GetFlushCount()*int64(h.flusher.config.Service.FlushIntervalMinutes*60)
	if startTime < 0 {
		startTime = 0
	}
	uptime := time.Now().Unix() - startTime

	// Calculate next flush time
	lastFlush := h.flusher.GetLastFlushTime()
	var nextFlushIn string
	if lastFlush > 0 {
		nextFlush := time.Unix(lastFlush, 0).Add(time.Duration(h.flusher.config.Service.FlushIntervalMinutes) * time.Minute)
		timeUntilFlush := time.Until(nextFlush)
		if timeUntilFlush > 0 {
			nextFlushIn = timeUntilFlush.Round(time.Second).String()
		} else {
			nextFlushIn = "due now"
		}
	} else {
		nextFlushIn = fmt.Sprintf("%d minutes", h.flusher.config.Service.FlushIntervalMinutes)
	}

	// Format last flush time
	lastFlushTimeStr := "never"
	if lastFlush > 0 {
		lastFlushTimeStr = time.Unix(lastFlush, 0).Format(time.RFC3339)
	}

	response := HealthResponse{
		Status:          "healthy",
		FlushCount:      h.flusher.GetFlushCount(),
		LastFlushTime:   lastFlushTimeStr,
		LastWatermark:   h.flusher.GetLastWatermark(),
		TotalFlushed:    h.flusher.GetTotalFlushed(),
		UptimeSeconds:   uptime,
		NextFlushIn:     nextFlushIn,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetrics handles the /metrics endpoint (Prometheus-compatible text format)
func (h *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	fmt.Fprintf(w, "# HELP flusher_flush_count Total number of flush operations\n")
	fmt.Fprintf(w, "# TYPE flusher_flush_count counter\n")
	fmt.Fprintf(w, "flusher_flush_count %d\n", h.flusher.GetFlushCount())

	fmt.Fprintf(w, "# HELP flusher_last_watermark Last ledger sequence watermark flushed\n")
	fmt.Fprintf(w, "# TYPE flusher_last_watermark gauge\n")
	fmt.Fprintf(w, "flusher_last_watermark %d\n", h.flusher.GetLastWatermark())

	fmt.Fprintf(w, "# HELP flusher_total_rows_flushed Total rows flushed across all operations\n")
	fmt.Fprintf(w, "# TYPE flusher_total_rows_flushed counter\n")
	fmt.Fprintf(w, "flusher_total_rows_flushed %d\n", h.flusher.GetTotalFlushed())

	lastFlush := h.flusher.GetLastFlushTime()
	if lastFlush > 0 {
		fmt.Fprintf(w, "# HELP flusher_last_flush_timestamp_seconds Unix timestamp of last flush\n")
		fmt.Fprintf(w, "# TYPE flusher_last_flush_timestamp_seconds gauge\n")
		fmt.Fprintf(w, "flusher_last_flush_timestamp_seconds %d\n", lastFlush)
	}
}

// handleRecreateBronze handles the /maintenance/recreate-bronze endpoint
// WARNING: This deletes ALL Bronze data and recreates tables with partitioning
func (h *HealthServer) handleRecreateBronze(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to recreate Bronze tables.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("⚠️  Received request to recreate Bronze tables")
	log.Println("⚠️  This will DELETE ALL Bronze data!")

	// Execute recreation
	ctx := r.Context()
	if err := h.duckdb.RecreateAllBronzeTables(ctx); err != nil {
		log.Printf("ERROR: Failed to recreate Bronze tables: %v", err)
		http.Error(w, fmt.Sprintf("Failed to recreate Bronze tables: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Bronze tables recreated with partitioning",
		"note":    "All Bronze data has been deleted. New data will be partitioned by ledger_range.",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Bronze tables recreation completed successfully")
}

// handleMergeBronze handles the /maintenance/merge endpoint
// Merges adjacent files for all Bronze tables
func (h *HealthServer) handleMergeBronze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to merge files.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🔧 Received request to merge Bronze files")

	// Merge with max 1000 files to avoid memory issues
	ctx := r.Context()
	if err := h.duckdb.MergeAllBronzeTables(ctx, 1000); err != nil {
		log.Printf("ERROR: Failed to merge Bronze files: %v", err)
		http.Error(w, fmt.Sprintf("Failed to merge files: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Bronze files merged successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Bronze file merge completed successfully")
}

// handleExpireBronze handles the /maintenance/expire endpoint
// Expires old snapshots to mark merged files for cleanup
func (h *HealthServer) handleExpireBronze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to expire snapshots.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🗑️  Received request to expire Bronze snapshots")

	ctx := r.Context()
	if err := h.duckdb.ExpireBronzeSnapshots(ctx); err != nil {
		log.Printf("ERROR: Failed to expire snapshots: %v", err)
		http.Error(w, fmt.Sprintf("Failed to expire snapshots: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Bronze snapshots expired successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Bronze snapshot expiration completed successfully")
}

// handleCleanupBronze handles the /maintenance/cleanup endpoint
// Cleans up orphaned files from S3
func (h *HealthServer) handleCleanupBronze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to cleanup files.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🧹 Received request to cleanup Bronze orphaned files")

	ctx := r.Context()
	if err := h.duckdb.CleanupBronzeOrphanedFiles(ctx); err != nil {
		log.Printf("ERROR: Failed to cleanup orphaned files: %v", err)
		http.Error(w, fmt.Sprintf("Failed to cleanup files: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Bronze orphaned files cleaned up successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Bronze cleanup completed successfully")
}

// handleFullMaintenanceBronze handles the /maintenance/full endpoint
// Runs full maintenance cycle: merge → expire → cleanup
func (h *HealthServer) handleFullMaintenanceBronze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST to run full maintenance.", http.StatusMethodNotAllowed)
		return
	}

	log.Println("🔧 Received request for full Bronze maintenance cycle")

	ctx := r.Context()
	if err := h.duckdb.PerformBronzeMaintenanceCycle(ctx, 1000); err != nil {
		log.Printf("ERROR: Failed to complete maintenance cycle: %v", err)
		http.Error(w, fmt.Sprintf("Failed to complete maintenance: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{
		"status":  "success",
		"message": "Full Bronze maintenance cycle completed successfully",
		"note":    "Files merged, snapshots expired, and orphaned files cleaned up",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Println("✅ Full Bronze maintenance cycle completed successfully")
}
