package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// TransformerStats holds transformer statistics
type TransformerStats struct {
	LastLedgerProcessed   int64         `json:"last_ledger_processed"`
	LastProcessedAt       time.Time     `json:"last_processed_at"`
	LastTransformDuration time.Duration `json:"last_transform_duration_ms"`
	TransformationsTotal  int64         `json:"transformations_total"`
	TransformationErrors  int64         `json:"transformation_errors"`
	LagSeconds            int64         `json:"lag_seconds"`
}

// HealthServer provides HTTP health endpoint
type HealthServer struct {
	port        int
	server      *http.Server
	stats       TransformerStats
	indexWriter *IndexWriter
	mu          sync.RWMutex
	startTime   time.Time
}

// NewHealthServer creates a new health server
func NewHealthServer(port int) *HealthServer {
	return &HealthServer{
		port:      port,
		startTime: time.Now(),
	}
}

// SetIndexWriter sets the index writer for stats queries
func (hs *HealthServer) SetIndexWriter(writer *IndexWriter) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.indexWriter = writer
}

// UpdateStats updates transformer statistics
func (hs *HealthServer) UpdateStats(stats TransformerStats) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.stats = stats
}

// Start starts the health server
func (hs *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/maintenance/merge", hs.handleMerge)
	mux.HandleFunc("/maintenance/expire", hs.handleExpire)
	mux.HandleFunc("/maintenance/cleanup", hs.handleCleanup)
	mux.HandleFunc("/maintenance/full", hs.handleFullMaintenance)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	go func() {
		log.Printf("🏥 Health endpoint listening on :%d/health", hs.port)
		log.Printf("🔧 Maintenance endpoints: /maintenance/merge, /maintenance/expire, /maintenance/cleanup, /maintenance/full")
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	return nil
}

// handleHealth handles health check requests
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	stats := hs.stats
	indexWriter := hs.indexWriter
	hs.mu.RUnlock()

	// Get index stats
	var indexStats map[string]interface{}
	if indexWriter != nil {
		var err error
		indexStats, err = indexWriter.GetIndexStats()
		if err != nil {
			log.Printf("Error getting index stats: %v", err)
			indexStats = map[string]interface{}{
				"error": err.Error(),
			}
		}
	}

	// Build response
	response := map[string]interface{}{
		"status":      "healthy",
		"transformer": "contract-event-index",
		"checkpoint": map[string]interface{}{
			"last_ledger":  stats.LastLedgerProcessed,
			"last_updated": stats.LastProcessedAt.Format(time.RFC3339),
		},
		"index":          indexStats,
		"uptime_seconds": int64(time.Since(hs.startTime).Seconds()),
		"stats": map[string]interface{}{
			"transformations_total":     stats.TransformationsTotal,
			"transformation_errors":     stats.TransformationErrors,
			"last_transform_duration_ms": stats.LastTransformDuration.Milliseconds(),
			"lag_seconds":               stats.LagSeconds,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Stop stops the health server
func (hs *HealthServer) Stop() error {
	if hs.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return hs.server.Shutdown(ctx)
	}
	return nil
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

	fileCountBefore, err := writer.GetFileCount()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get file count: %v", err), http.StatusInternalServerError)
		return
	}

	if err := writer.MergeAdjacentFiles(1000); err != nil {
		http.Error(w, fmt.Sprintf("Merge failed: %v", err), http.StatusInternalServerError)
		return
	}

	fileCountAfter, err := writer.GetFileCount()

	// Calculate files merged, handling edge cases
	var filesMerged int64
	if err != nil {
		// If we can't get the count after, report as unknown
		fileCountAfter = -1
		filesMerged = 0
	} else if fileCountAfter > fileCountBefore {
		// New files were written concurrently during merge
		filesMerged = 0
	} else {
		filesMerged = fileCountBefore - fileCountAfter
	}

	response := map[string]interface{}{
		"status":            "success",
		"files_before":      fileCountBefore,
		"files_after":       fileCountAfter,
		"files_merged":      filesMerged,
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

	if err := writer.ExpireSnapshots(20); err != nil {
		http.Error(w, fmt.Sprintf("Expire failed: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status":           "success",
		"retain_snapshots": 20,
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

	fileCountBefore, err := writer.GetFileCount()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get file count: %v", err), http.StatusInternalServerError)
		return
	}

	if err := writer.PerformMaintenanceFullCycle(1000, 20); err != nil {
		http.Error(w, fmt.Sprintf("Full maintenance failed: %v", err), http.StatusInternalServerError)
		return
	}

	fileCountAfter, err := writer.GetFileCount()

	// Calculate files merged, handling edge cases
	var filesMerged int64
	if err != nil {
		// If we can't get the count after, report as unknown
		fileCountAfter = -1
		filesMerged = 0
	} else if fileCountAfter > fileCountBefore {
		// New files were written concurrently during maintenance
		filesMerged = 0
	} else {
		filesMerged = fileCountBefore - fileCountAfter
	}

	response := map[string]interface{}{
		"status":            "success",
		"files_before":      fileCountBefore,
		"files_after":       fileCountAfter,
		"files_merged":      filesMerged,
		"max_compact_files": 1000,
		"retain_snapshots":  20,
		"steps_completed":   []string{"merge", "expire", "cleanup"},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
