package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Prometheus metrics
	ledgersProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "silver_transformer_ledgers_processed_total",
		Help: "Total number of ledgers transformed",
	})

	transformLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "silver_transformer_latency_seconds",
		Help:    "Latency of transformation operations",
		Buckets: prometheus.DefBuckets,
	})

	lastProcessedSequence = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "silver_transformer_last_sequence",
		Help: "Last processed ledger sequence",
	})
)

func main() {
	// Load configuration
	config := LoadConfig()

	// Create transformer
	transformer, err := NewSilverTransformer(config)
	if err != nil {
		log.Fatalf("❌ Failed to create transformer: %v", err)
	}

	// Start health and metrics servers
	go startHealthServer(config.HealthPort, transformer)
	go startMetricsServer(config.MetricsPort)

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start transformer in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- transformer.Start()
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		log.Println("\n📨 Received shutdown signal")
		transformer.Stop()
		log.Println("✅ Graceful shutdown complete")
	case err := <-errChan:
		if err != nil {
			log.Fatalf("❌ Transformer error: %v", err)
		}
	}
}

// startHealthServer starts HTTP health check endpoint
func startHealthServer(port string, transformer *SilverTransformer) {
	mux := http.NewServeMux()

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ledgers, transactions, operations, currentSeq := transformer.GetStats()

		health := map[string]interface{}{
			"status": "healthy",
			"service": "silver-transformer",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"stats": map[string]interface{}{
				"ledgers_processed":       ledgers,
				"transactions_processed":  transactions,
				"operations_processed":    operations,
				"last_processed_sequence": currentSeq,
			},
			"config": map[string]interface{}{
				"bronze_query_url": transformer.config.BronzeQueryURL,
				"silver_path":      transformer.config.SilverPath,
				"bronze_schema":    transformer.config.BronzeSchema,
				"poll_interval":    transformer.config.PollInterval.String(),
				"batch_size":       transformer.config.BatchSize,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(health)
	})

	// Ready endpoint (for k8s readiness probes)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ready")
	})

	// Live endpoint (for k8s liveness probes)
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "live")
	})

	addr := ":" + port
	log.Printf("🏥 Health server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("❌ Health server error: %v", err)
	}
}

// startMetricsServer starts Prometheus metrics endpoint
func startMetricsServer(port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	addr := ":" + port
	log.Printf("📊 Metrics server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("❌ Metrics server error: %v", err)
	}
}
