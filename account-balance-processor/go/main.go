package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	accountbalance "github.com/withobsrvr/account-balance-processor/gen/account_balance_service"
	"github.com/withobsrvr/account-balance-processor/server"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultPort                 = ":50054"
	defaultSourceServiceAddress = "localhost:50053" // stellar-live-source-datalake default port
	defaultHealthPort           = "8089"
)

func main() {
	// Initialize zap logger with production configuration
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize zap logger: " + err.Error())
	}
	defer logger.Sync()

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
	balanceServer, err := server.NewAccountBalanceServer(networkPassphrase, sourceServiceAddr)
	if err != nil {
		logger.Fatal("failed to create account balance server",
			zap.Error(err))
	}
	defer balanceServer.Close()

	// Register the AccountBalanceServiceServer implementation
	accountbalance.RegisterAccountBalanceServiceServer(s, balanceServer)

	logger.Info("Account Balance Processor Server starting",
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
			metrics := balanceServer.GetMetrics()
			response := map[string]interface{}{
				"status": "healthy",
				"metrics": map[string]interface{}{
					"success_count":          metrics.SuccessCount,
					"error_count":            metrics.ErrorCount,
					"total_processed":        metrics.TotalProcessed,
					"total_balances_emitted": metrics.TotalBalancesEmitted,
					"last_processed_ledger":  metrics.LastProcessedLedger,
					"processing_latency":     metrics.ProcessingLatency.String(),
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

	if err := s.Serve(lis); err != nil {
		logger.Fatal("failed to serve",
			zap.Error(err))
	}
}
