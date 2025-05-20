package main

import (
	"net"
	"net/http"
	"os"
	"strings"

	// Import the generated protobuf code package for the service WE PROVIDE
	eventservice "github.com/withObsrvr/ttp-processor/gen/event_service"
	// Import the server implementation package
	"github.com/withObsrvr/ttp-processor/server"

	"encoding/json"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultPort                 = ":50051"
	defaultSourceServiceAddress = "localhost:50052" // Default address for stellar-live-source
	defaultHealthPort           = "8088"
)

func main() {
	// Initialize zap logger with production configuration
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize zap logger: " + err.Error())
	}
	defer logger.Sync() // Ensure all buffered logs are written

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	// Ensure port starts with ":"
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatal("failed to listen",
			zap.String("port", port),
			zap.Error(err))
	}

	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		logger.Fatal("NETWORK_PASSPHRASE environment variable not set")
	}

	sourceServiceAddr := os.Getenv("SOURCE_SERVICE_ADDRESS")
	if sourceServiceAddr == "" {
		sourceServiceAddr = defaultSourceServiceAddress
	}

	logger = logger.With(
		zap.String("port", port),
		zap.String("source_service", sourceServiceAddr),
		zap.String("network", networkPassphrase),
	)

	s := grpc.NewServer()
	eventServer, err := server.NewEventServer(networkPassphrase, sourceServiceAddr)
	if err != nil {
		logger.Fatal("failed to create event server",
			zap.Error(err))
	}
	// Register the EventServiceServer implementation
	eventservice.RegisterEventServiceServer(s, eventServer)

	logger.Info("TTP Processor Server starting",
		zap.String("address", lis.Addr().String()))

	// Set up health check endpoint
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = defaultHealthPort
	}

	// Start health check server
	go func() {
		healthAddr := fmt.Sprintf(":%s", healthPort)
		logger.Info("Starting health check server", zap.String("address", healthAddr))

		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			metrics := eventServer.GetMetrics()
			response := map[string]interface{}{
				"status": "healthy",
				"metrics": map[string]interface{}{
					"success_count":         metrics.SuccessCount,
					"error_count":           metrics.ErrorCount,
					"total_processed":       metrics.TotalProcessed,
					"total_events_emitted":  metrics.TotalEventsEmitted,
					"last_processed_ledger": metrics.LastProcessedLedger,
					"processing_latency":    metrics.ProcessingLatency.String(),
				},
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				logger.Error("Failed to encode health check response", zap.Error(err))
			}
		})

		if err := http.ListenAndServe(healthAddr, nil); err != nil {
			logger.Fatal("Failed to start health check server", zap.Error(err))
		}
	}()

	// Initialize flowctl integration if enabled
	enableFlowctl := os.Getenv("ENABLE_FLOWCTL")
	if strings.ToLower(enableFlowctl) == "true" {
		logger.Info("Initializing flowctl integration")
		flowctlController := server.NewFlowctlController(eventServer)
		if err := flowctlController.RegisterWithFlowctl(); err != nil {
			logger.Warn("Failed to register with flowctl", zap.Error(err))
		}

		// Make sure to properly shut down flowctl when the main server stops
		defer flowctlController.Stop()
	}

	if err := s.Serve(lis); err != nil {
		logger.Fatal("failed to serve",
			zap.Error(err))
	}
}
