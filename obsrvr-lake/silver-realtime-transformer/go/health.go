package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// HealthServer serves health and metrics endpoints
type HealthServer struct {
	transformer *RealtimeTransformer
	startTime   time.Time
}

// NewHealthServer creates a new health server
func NewHealthServer(transformer *RealtimeTransformer) *HealthServer {
	return &HealthServer{
		transformer: transformer,
		startTime:   time.Now(),
	}
}

// Start starts the health HTTP server
func (hs *HealthServer) Start(port string) error {
	http.HandleFunc("/health", hs.handleHealth)
	http.HandleFunc("/metrics", hs.handleMetrics)

	log.Printf("🏥 Health server listening on :%s", port)
	return http.ListenAndServe(":"+port, nil)
}

// handleHealth handles health check requests
func (hs *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := hs.transformer.GetStats()

	// Calculate latency (time since last transformation)
	latency := time.Since(stats.LastTransformationTime)

	// Determine health status
	status := "healthy"
	if stats.TransformationErrors > 0 && stats.TransformationsTotal > 0 {
		errorRate := float64(stats.TransformationErrors) / float64(stats.TransformationsTotal)
		if errorRate > 0.1 { // More than 10% errors
			status = "degraded"
		}
	}

	if latency > 30*time.Second && stats.TransformationsTotal > 0 {
		status = "stale" // No transformation in last 30 seconds
	}

	// Build stats map
	statsMap := map[string]interface{}{
		"transformations_total":           stats.TransformationsTotal,
		"transformation_errors":           stats.TransformationErrors,
		"last_ledger_sequence":            stats.LastLedgerSequence,
		"last_transformation_time":        stats.LastTransformationTime,
		"last_transformation_duration_ms": stats.LastTransformationDuration.Milliseconds(),
		"last_transformation_row_count":   stats.LastTransformationRowCount,
		"latency_seconds":                 latency.Seconds(),
	}

	// Build source info
	sourceInfo := map[string]interface{}{
		"mode": stats.SourceMode,
	}

	// Add backfill-specific info when in backfill mode
	if stats.SourceMode == "backfill" {
		sourceInfo["hot_min_ledger"] = stats.HotMinLedger
		sourceInfo["backfill_target"] = stats.BackfillTarget
		sourceInfo["backfill_progress_percent"] = stats.BackfillProgress
	} else if stats.SourceMode == "hot" {
		sourceInfo["hot_min_ledger"] = stats.HotMinLedger
	}

	response := map[string]interface{}{
		"status":         status,
		"service":        "silver-realtime-transformer",
		"timestamp":      time.Now(),
		"uptime_seconds": int(time.Since(hs.startTime).Seconds()),
		"source":         sourceInfo,
		"stats":          statsMap,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetrics handles metrics requests (Prometheus-style)
func (hs *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := hs.transformer.GetStats()

	w.Header().Set("Content-Type", "text/plain")

	// Prometheus-style metrics
	w.Write([]byte("# HELP silver_transformations_total Total number of transformations\n"))
	w.Write([]byte("# TYPE silver_transformations_total counter\n"))
	w.Write([]byte(formatMetric("silver_transformations_total", stats.TransformationsTotal)))

	w.Write([]byte("# HELP silver_transformation_errors Total number of transformation errors\n"))
	w.Write([]byte("# TYPE silver_transformation_errors counter\n"))
	w.Write([]byte(formatMetric("silver_transformation_errors", stats.TransformationErrors)))

	w.Write([]byte("# HELP silver_last_ledger_sequence Last processed ledger sequence\n"))
	w.Write([]byte("# TYPE silver_last_ledger_sequence gauge\n"))
	w.Write([]byte(formatMetric("silver_last_ledger_sequence", stats.LastLedgerSequence)))

	w.Write([]byte("# HELP silver_last_transformation_duration_ms Last transformation duration in milliseconds\n"))
	w.Write([]byte("# TYPE silver_last_transformation_duration_ms gauge\n"))
	w.Write([]byte(formatMetric("silver_last_transformation_duration_ms", stats.LastTransformationDuration.Milliseconds())))

	w.Write([]byte("# HELP silver_last_transformation_rows Last transformation row count\n"))
	w.Write([]byte("# TYPE silver_last_transformation_rows gauge\n"))
	w.Write([]byte(formatMetric("silver_last_transformation_rows", stats.LastTransformationRowCount)))

	latency := time.Since(stats.LastTransformationTime)
	w.Write([]byte("# HELP silver_latency_seconds Time since last transformation\n"))
	w.Write([]byte("# TYPE silver_latency_seconds gauge\n"))
	w.Write([]byte(formatMetric("silver_latency_seconds", latency.Seconds())))

	uptime := time.Since(hs.startTime)
	w.Write([]byte("# HELP silver_uptime_seconds Service uptime in seconds\n"))
	w.Write([]byte("# TYPE silver_uptime_seconds counter\n"))
	w.Write([]byte(formatMetric("silver_uptime_seconds", int(uptime.Seconds()))))

	// Source mode metrics (1 = hot, 0 = backfill)
	sourceHot := 0
	if stats.SourceMode == "hot" {
		sourceHot = 1
	}
	w.Write([]byte("# HELP silver_source_mode_hot Source mode (1=hot, 0=backfill)\n"))
	w.Write([]byte("# TYPE silver_source_mode_hot gauge\n"))
	w.Write([]byte(formatMetric("silver_source_mode_hot", sourceHot)))

	w.Write([]byte("# HELP silver_hot_min_ledger Minimum ledger in hot storage\n"))
	w.Write([]byte("# TYPE silver_hot_min_ledger gauge\n"))
	w.Write([]byte(formatMetric("silver_hot_min_ledger", stats.HotMinLedger)))

	// Backfill metrics (only relevant during backfill)
	w.Write([]byte("# HELP silver_backfill_target Target ledger for backfill completion\n"))
	w.Write([]byte("# TYPE silver_backfill_target gauge\n"))
	w.Write([]byte(formatMetric("silver_backfill_target", stats.BackfillTarget)))

	w.Write([]byte("# HELP silver_backfill_progress_percent Backfill progress percentage\n"))
	w.Write([]byte("# TYPE silver_backfill_progress_percent gauge\n"))
	w.Write([]byte(formatMetric("silver_backfill_progress_percent", stats.BackfillProgress)))
}

// formatMetric formats a metric in Prometheus format
func formatMetric(name string, value interface{}) []byte {
	return []byte(name + " " + formatValue(value) + "\n")
}

// formatValue converts a value to string for metrics
func formatValue(value interface{}) string {
	switch v := value.(type) {
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	default:
		return "0"
	}
}
