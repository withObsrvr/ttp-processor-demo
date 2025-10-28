package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	accountbalance "github.com/withobsrvr/account-balance-processor/gen/account_balance_service"
	"github.com/withobsrvr/account-balance-processor/config"
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

	// Parse command-line flags
	configPath := flag.String("config", "", "Path to quickstart.yaml config file")
	flag.Parse()

	var (
		port              string
		healthPort        string
		networkPassphrase string
		sourceServiceAddr string
	)

	// Try to load config file if provided
	if *configPath != "" {
		logger.Info("Loading configuration from file",
			zap.String("config_path", *configPath))

		cfg, err := config.LoadConfig(*configPath)
		if err != nil {
			logger.Fatal("Failed to load config file",
				zap.String("config_path", *configPath),
				zap.Error(err))
		}

		// Use config values
		port = fmt.Sprintf(":%d", cfg.Spec.Processor.GRPCPort)
		healthPort = strconv.Itoa(cfg.Spec.Processor.HealthPort)
		networkPassphrase = cfg.GetNetworkPassphrase()
		sourceServiceAddr = defaultSourceServiceAddress // Intentionally hardcoded until source address configuration is added

		logger.Info("Configuration loaded successfully",
			zap.String("pipeline_name", cfg.Metadata.Name),
			zap.String("network", cfg.Spec.Source.Network),
			zap.Uint32("start_ledger", cfg.Spec.Source.Ledgers.Start),
			zap.Uint32("end_ledger", cfg.Spec.Source.Ledgers.End))
	} else {
		// Backward compatibility: use environment variables
		logger.Info("No config file provided, using environment variables")

		port = os.Getenv("PORT")
		if port == "" {
			port = defaultPort
		}
		// Ensure port starts with ":"
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}

		networkPassphrase = os.Getenv("NETWORK_PASSPHRASE")
		if networkPassphrase == "" {
			logger.Fatal("NETWORK_PASSPHRASE environment variable not set")
		}

		sourceServiceAddr = os.Getenv("SOURCE_SERVICE_ADDRESS")
		if sourceServiceAddr == "" {
			sourceServiceAddr = defaultSourceServiceAddress
		}

		healthPort = os.Getenv("HEALTH_PORT")
		if healthPort == "" {
			healthPort = defaultHealthPort
		}
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatal("failed to listen",
			zap.String("port", port),
			zap.Error(err))
	}

	logger = logger.With(
		zap.String("port", port),
		zap.String("source_service", sourceServiceAddr),
		zap.String("network", networkPassphrase),
	)

	s := grpc.NewServer()
	balanceServer, err := server.NewAccountBalanceServer(networkPassphrase, sourceServiceAddr, *configPath)
	if err != nil {
		logger.Fatal("failed to create account balance server",
			zap.Error(err))
	}
	defer balanceServer.Close()

	// Register the AccountBalanceServiceServer implementation
	accountbalance.RegisterAccountBalanceServiceServer(s, balanceServer)

	logger.Info("Account Balance Processor Server starting",
		zap.String("address", lis.Addr().String()))

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
