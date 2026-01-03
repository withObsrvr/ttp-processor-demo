package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthServer provides HTTP health check endpoint
type HealthServer struct {
	port        int
	stats       *TransformerStats
	mu          sync.RWMutex
	server      *http.Server
	indexWriter *IndexWriter
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status                      string `json:"status"`
	LastLedgerProcessed         int64  `json:"last_ledger_processed"`
	LastProcessedAt             string `json:"last_processed_at,omitempty"`
	LagSeconds                  int64  `json:"lag_seconds"`
	TransformationsTotal        int64  `json:"transformations_total"`
	TransformationErrors        int64  `json:"transformation_errors"`
	LastTransformDurationMs     int64  `json:"last_transformation_duration_ms"`
}

// NewHealthServer creates a new health server
func NewHealthServer(port int) *HealthServer {
	return &HealthServer{
		port: port,
		stats: &TransformerStats{
			LastProcessedAt: time.Now(),
		},
	}
}

// UpdateStats updates the transformer statistics
func (hs *HealthServer) UpdateStats(stats TransformerStats) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.stats = &stats
}

// GetStats returns a copy of current stats
func (hs *HealthServer) GetStats() TransformerStats {
	hs.mu.RLock()
	defer hs.mu.RUnlock()
	return *hs.stats
}

// SetIndexWriter sets the index writer for maintenance operations
func (hs *HealthServer) SetIndexWriter(writer *IndexWriter) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.indexWriter = writer
}

// Start starts the health check HTTP server
func (hs *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/maintenance/merge", hs.handleMerge)
	mux.HandleFunc("/maintenance/expire", hs.handleExpire)
	mux.HandleFunc("/maintenance/cleanup", hs.handleCleanup)
	mux.HandleFunc("/maintenance/full", hs.handleFullMaintenance)
	mux.HandleFunc("/maintenance/recreate", hs.handleRecreateTable)
	mux.HandleFunc("/debug/metadata", hs.handleDebugMetadata)

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
		Status:                      "healthy",
		LastLedgerProcessed:         stats.LastLedgerProcessed,
		LagSeconds:                  lagSeconds,
		TransformationsTotal:        stats.TransformationsTotal,
		TransformationErrors:        stats.TransformationErrors,
		LastTransformDurationMs:     stats.LastTransformDuration.Milliseconds(),
	}

	if !stats.LastProcessedAt.IsZero() {
		response.LastProcessedAt = stats.LastProcessedAt.Format(time.RFC3339)
	}

	// Set status based on lag
	if lagSeconds > 300 { // 5 minutes
		response.Status = "degraded"
	}
	if lagSeconds > 900 { // 15 minutes
		response.Status = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding health response: %v", err)
	}
}

// handleMerge handles the /maintenance/merge endpoint
func (hs *HealthServer) handleMerge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	// Get file count before merge
	fileCountBefore, err := writer.GetFileCount()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get file count: %v", err), http.StatusInternalServerError)
		return
	}

	// Run merge (max 1000 files to avoid memory issues)
	if err := writer.MergeAdjacentFiles(1000); err != nil {
		http.Error(w, fmt.Sprintf("Merge failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Get file count after merge
	fileCountAfter, err := writer.GetFileCount()
	if err != nil {
		fileCountAfter = -1 // Signal that we couldn't get the count
	}

	response := map[string]interface{}{
		"status":            "success",
		"files_before":      fileCountBefore,
		"files_after":       fileCountAfter,
		"files_merged":      fileCountBefore - fileCountAfter,
		"max_compact_files": 1000,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleExpire handles the /maintenance/expire endpoint
func (hs *HealthServer) handleExpire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	// Retain 20 most recent snapshots (default)
	retainSnapshots := 20

	if err := writer.ExpireSnapshots(retainSnapshots); err != nil {
		http.Error(w, fmt.Sprintf("Expire failed: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status":           "success",
		"retain_snapshots": retainSnapshots,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleCleanup handles the /maintenance/cleanup endpoint
func (hs *HealthServer) handleCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	if err := writer.CleanupOrphanedFiles(); err != nil {
		http.Error(w, fmt.Sprintf("Cleanup failed: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status": "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleFullMaintenance handles the /maintenance/full endpoint
// This runs merge → expire → cleanup in sequence
func (hs *HealthServer) handleFullMaintenance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	// Get file count before maintenance
	fileCountBefore, err := writer.GetFileCount()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get file count: %v", err), http.StatusInternalServerError)
		return
	}

	// Run full maintenance cycle (merge 1000 files, retain 20 snapshots)
	if err := writer.PerformMaintenanceFullCycle(1000, 20); err != nil {
		http.Error(w, fmt.Sprintf("Full maintenance failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Get file count after maintenance
	fileCountAfter, err := writer.GetFileCount()
	if err != nil {
		fileCountAfter = -1
	}

	response := map[string]interface{}{
		"status":            "success",
		"files_before":      fileCountBefore,
		"files_after":       fileCountAfter,
		"files_merged":      fileCountBefore - fileCountAfter,
		"max_compact_files": 1000,
		"retain_snapshots":  20,
		"steps_completed":   []string{"merge", "expire", "cleanup"},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleRecreateTable handles the /maintenance/recreate endpoint
// This drops and recreates the table with proper CLUSTER BY ledger_sequence
// WARNING: This deletes all indexed data! Use only to fix schema issues.
func (hs *HealthServer) handleRecreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	// Recreate table with partitioning and purge old metadata
	if err := writer.RecreateTable(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to recreate table: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status":  "success",
		"message": "Table recreated with partitioning enabled and orphaned metadata purged",
		"warning": "All indexed data has been deleted. Restart transformer to re-index from checkpoint.",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleDebugMetadata shows metadata table contents for debugging
func (hs *HealthServer) handleDebugMetadata(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	writer := hs.indexWriter
	hs.mu.RUnlock()

	if writer == nil {
		http.Error(w, "Index writer not initialized", http.StatusServiceUnavailable)
		return
	}

	metadata, err := writer.GetMetadataDebugInfo()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metadata)
}
