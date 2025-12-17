package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Prometheus metrics
	transformationsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bronze_silver_transformations_total",
		Help: "Total number of successful transformations",
	})

	transformationErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bronze_silver_transformation_errors_total",
		Help: "Total number of transformation errors",
	})

	transformationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bronze_silver_transformation_duration_seconds",
		Help:    "Duration of transformation cycles",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300},
	})

	lastLedgerSequence = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bronze_silver_last_ledger_sequence",
		Help: "Last processed ledger sequence",
	})

	tablesTransformedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bronze_silver_tables_transformed_total",
		Help: "Total number of rows transformed per table",
	}, []string{"table_name"})
)

// HealthServer manages the HTTP health and metrics endpoints
type HealthServer struct {
	transformer *Transformer
	port        string
	startTime   time.Time
}

// NewHealthServer creates a new health server
func NewHealthServer(transformer *Transformer, port string) *HealthServer {
	return &HealthServer{
		transformer: transformer,
		port:        port,
		startTime:   time.Now(),
	}
}

// Start starts the health and metrics HTTP server
func (h *HealthServer) Start() error {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", h.handleHealth)

	// Ready endpoint (for k8s readiness probes)
	mux.HandleFunc("/ready", h.handleReady)

	// Live endpoint (for k8s liveness probes)
	mux.HandleFunc("/live", h.handleLive)

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	addr := ":" + h.port
	log.Printf("🏥 Health server listening on %s", addr)

	return http.ListenAndServe(addr, mux)
}

// handleHealth returns detailed health information
func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := h.transformer.GetStats()

	health := map[string]interface{}{
		"status":    "healthy",
		"service":   "bronze-silver-transformer",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime_seconds": int64(time.Since(h.startTime).Seconds()),
		"stats": map[string]interface{}{
			"transformations_total":     stats.TransformationsTotal,
			"transformation_errors":     stats.TransformationErrors,
			"last_ledger_sequence":      stats.LastLedgerSequence,
			"last_transformation_time":  stats.LastTransformationTime,
			"last_transformation_duration_seconds": stats.LastTransformationDuration.Seconds(),
		},
		"config": map[string]interface{}{
			"transform_interval_minutes": h.transformer.config.Service.TransformIntervalMinutes,
			"bronze_schema":              h.transformer.config.DuckLake.BronzeSchema,
			"silver_schema":              h.transformer.config.DuckLake.SilverSchema,
			"catalog_name":               h.transformer.config.DuckLake.CatalogName,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleReady returns readiness status (for k8s)
func (h *HealthServer) handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ready")
}

// handleLive returns liveness status (for k8s)
func (h *HealthServer) handleLive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "live")
}
