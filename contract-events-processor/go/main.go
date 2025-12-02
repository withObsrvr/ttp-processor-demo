package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/withObsrvr/contract-events-processor/config"
	pb "github.com/withObsrvr/contract-events-processor/gen/contract_event_service"
	"github.com/withObsrvr/contract-events-processor/server"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	// Initialize zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize zap logger: " + err.Error())
	}
	defer logger.Sync()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal("failed to load configuration", zap.Error(err))
	}

	logger = logger.With(
		zap.String("port", cfg.Port),
		zap.String("live_source", cfg.LiveSourceAddress),
		zap.String("network", cfg.NetworkPassphrase),
	)

	// Create TCP listener
	lis, err := net.Listen("tcp", cfg.Port)
	if err != nil {
		logger.Fatal("failed to listen",
			zap.String("port", cfg.Port),
			zap.Error(err))
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create and register contract event server
	eventServer := server.NewContractEventServer(
		cfg.LiveSourceAddress,
		cfg.NetworkPassphrase,
		logger,
	)
	pb.RegisterContractEventServiceServer(grpcServer, eventServer)

	// Initialize flowctl integration if enabled
	enableFlowctl := os.Getenv("ENABLE_FLOWCTL")
	if strings.ToLower(enableFlowctl) == "true" {
		logger.Info("Initializing flowctl integration")
		flowctlController := server.NewFlowctlController(eventServer)
		if err := flowctlController.RegisterWithFlowctl(); err != nil {
			logger.Warn("Failed to register with flowctl", zap.Error(err))
		}
		defer flowctlController.Stop()
	}

	logger.Info("Contract Events Processor starting",
		zap.String("address", lis.Addr().String()))

	// Start health check server
	go func() {
		healthAddr := fmt.Sprintf(":%s", cfg.HealthPort)
		logger.Info("Starting health check server", zap.String("address", healthAddr))

		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			stats := eventServer.GetStats()
			response := map[string]interface{}{
				"status": "healthy",
				"stats":  stats,
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

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("failed to serve", zap.Error(err))
	}
}
