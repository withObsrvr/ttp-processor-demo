package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	// Prometheus metrics
	recordsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "postgresql_consumer_records_received_total",
		Help: "Total number of records received from Arrow Flight",
	})
	
	recordsInsertedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "postgresql_consumer_records_inserted_total",
		Help: "Total number of records inserted into PostgreSQL",
	})
	
	batchesProcessedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "postgresql_consumer_batches_processed_total",
		Help: "Total number of batches processed",
	})
	
	processingErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "postgresql_consumer_errors_total",
		Help: "Total number of processing errors",
	})
	
	batchInsertDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "postgresql_consumer_batch_insert_duration_seconds",
		Help:    "Time taken to insert a batch into PostgreSQL",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
	})
	
	batchSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "postgresql_consumer_batch_size",
		Help:    "Size of batches being processed",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1 to 1024
	})
	
	databaseConnectionsGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "postgresql_consumer_database_connections",
		Help: "Current number of database connections",
	}, func() float64 {
		// This would be populated from db.Stats()
		return 0
	})
)

func init() {
	// Register all metrics
	prometheus.MustRegister(
		recordsReceivedTotal,
		recordsInsertedTotal,
		batchesProcessedTotal,
		processingErrorsTotal,
		batchInsertDuration,
		batchSizeHistogram,
		databaseConnectionsGauge,
	)
}

// runMetricsServer runs the HTTP metrics server
func (c *PostgreSQLConsumer) runMetricsServer(ctx context.Context) {
	mux := http.NewServeMux()
	
	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
	
	// Health check endpoint
	mux.HandleFunc("/health", c.healthHandler)
	
	// Ready check endpoint
	mux.HandleFunc("/ready", c.readyHandler)
	
	// Custom stats endpoint
	mux.HandleFunc("/stats", c.statsHandler)
	
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", c.cfg.MetricsPort),
		Handler: mux,
	}
	
	log.Info().
		Int("port", c.cfg.MetricsPort).
		Msg("Starting metrics server")
	
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()
	
	// Wait for shutdown
	<-ctx.Done()
	
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Failed to shutdown metrics server")
	}
}

// healthHandler handles health check requests
func (c *PostgreSQLConsumer) healthHandler(w http.ResponseWriter, r *http.Request) {
	// Check database connectivity
	if err := c.db.Ping(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	
	// Check Arrow Flight connectivity
	// This is a simplified check - in production you might want more sophisticated checks
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"service": "postgresql-consumer",
		"uptime": time.Since(c.metrics.StartTime).Seconds(),
	})
}

// readyHandler handles readiness check requests
func (c *PostgreSQLConsumer) readyHandler(w http.ResponseWriter, r *http.Request) {
	// Check if we're actively processing
	metrics := c.GetMetrics()
	ready := metrics.RecordsReceived > 0 || time.Since(metrics.StartTime) < 30*time.Second
	
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ready": false,
			"reason": "no data received",
		})
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ready": true,
	})
}

// statsHandler returns detailed statistics
func (c *PostgreSQLConsumer) statsHandler(w http.ResponseWriter, r *http.Request) {
	metrics := c.GetMetrics()
	
	// Get database stats
	dbStats := c.db.Stats()
	
	// Get ingestion stats from database
	ingestionStats, err := c.getIngestionStats()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get ingestion stats")
	}
	
	response := map[string]interface{}{
		"consumer": map[string]interface{}{
			"records_received":    metrics.RecordsReceived,
			"records_inserted":    metrics.RecordsInserted,
			"batches_processed":   metrics.BatchesProcessed,
			"errors":              metrics.ErrorCount,
			"last_processed_time": metrics.LastProcessedTime,
			"uptime_seconds":      time.Since(metrics.StartTime).Seconds(),
		},
		"database": map[string]interface{}{
			"open_connections": dbStats.OpenConnections,
			"in_use":           dbStats.InUse,
			"idle":             dbStats.Idle,
			"wait_count":       dbStats.WaitCount,
			"wait_duration":    dbStats.WaitDuration.String(),
		},
	}
	
	if ingestionStats != nil {
		response["ingestion"] = map[string]interface{}{
			"total_entries":     ingestionStats.TotalEntries,
			"unique_contracts":  ingestionStats.UniqueContracts,
			"min_ledger":        ingestionStats.MinLedger,
			"max_ledger":        ingestionStats.MaxLedger,
			"last_ingested":     ingestionStats.LastIngested,
			"completed_batches": ingestionStats.CompletedBatches,
		}
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// updatePrometheusMetrics updates Prometheus metrics
func (c *PostgreSQLConsumer) updatePrometheusMetrics() {
	metrics := c.GetMetrics()
	
	// Update counters
	recordsReceivedTotal.Add(float64(metrics.RecordsReceived))
	recordsInsertedTotal.Add(float64(metrics.RecordsInserted))
	batchesProcessedTotal.Add(float64(metrics.BatchesProcessed))
	processingErrorsTotal.Add(float64(metrics.ErrorCount))
}

// observeBatchInsert records batch insert timing
func (c *PostgreSQLConsumer) observeBatchInsert(duration time.Duration, batchSize int) {
	batchInsertDuration.Observe(duration.Seconds())
	batchSizeHistogram.Observe(float64(batchSize))
}