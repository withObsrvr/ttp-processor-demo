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

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", hs.port),
		Handler: mux,
	}

	go func() {
		log.Printf("🏥 Health endpoint listening on :%d/health", hs.port)
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
