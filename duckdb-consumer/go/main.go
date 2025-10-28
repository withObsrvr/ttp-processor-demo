package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	accountbalance "github.com/withobsrvr/duckdb-consumer/gen/account_balance_service"
	"github.com/withobsrvr/duckdb-consumer/config"
	"github.com/withobsrvr/duckdb-consumer/consumer"

	"go.uber.org/zap"
)

const (
	defaultBalanceServiceAddress = "localhost:50054" // account-balance-processor default port
	defaultDuckDBPath            = "./account_balances.duckdb"
	defaultBatchSize             = 1000
	defaultHealthPort            = "8090"
)

func main() {
	// Initialize zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize zap logger: " + err.Error())
	}
	defer logger.Sync()

	// Parse command-line flags
	configPath := flag.String("config", "", "Path to quickstart.yaml config file")
	flag.Parse()

	var (
		balanceServiceAddr string
		dbPath             string
		batchSize          int
		healthPort         string
		startLedger        uint32
		endLedger          uint32
		filterAssetCode    string
		filterAssetIssuer  string
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
		balanceServiceAddr = defaultBalanceServiceAddress // Intentionally hardcoded until source address configuration is added
		dbPath = cfg.Spec.Consumer.Database.Path
		batchSize = cfg.Spec.Consumer.BatchSize
		healthPort = strconv.Itoa(cfg.Spec.Consumer.HealthPort)
		startLedger = cfg.Spec.Source.Ledgers.Start
		endLedger = cfg.Spec.Source.Ledgers.End

		// Handle filtering from config
		if len(cfg.Spec.Filters.Assets) > 0 {
			// Use first asset for now (multi-asset support can be added later)
			filterAssetCode = cfg.Spec.Filters.Assets[0].Code
			filterAssetIssuer = cfg.Spec.Filters.Assets[0].Issuer
		} else if len(cfg.Spec.Filters.AssetCodeOnly) > 0 {
			filterAssetCode = cfg.Spec.Filters.AssetCodeOnly[0]
			filterAssetIssuer = "" // Any issuer
		} else if cfg.Spec.Filters.IndexAll {
			filterAssetCode = ""
			filterAssetIssuer = ""
		}

		logger.Info("Configuration loaded successfully",
			zap.String("pipeline_name", cfg.Metadata.Name),
			zap.String("database_path", dbPath),
			zap.Int("batch_size", batchSize))
	} else {
		// Backward compatibility: use environment variables
		logger.Info("No config file provided, using environment variables")

		balanceServiceAddr = getEnv("BALANCE_SERVICE_ADDRESS", defaultBalanceServiceAddress)
		dbPath = getEnv("DUCKDB_PATH", defaultDuckDBPath)
		batchSizeStr := getEnv("BATCH_SIZE", strconv.Itoa(defaultBatchSize))
		healthPort = getEnv("HEALTH_PORT", defaultHealthPort)

		batchSize, err = strconv.Atoi(batchSizeStr)
		if err != nil {
			logger.Fatal("invalid BATCH_SIZE", zap.String("value", batchSizeStr))
		}

		// Get request parameters
		startLedgerStr := getEnv("START_LEDGER", "")
		if startLedgerStr == "" {
			logger.Fatal("START_LEDGER environment variable is required")
		}
		startLedger64, err := strconv.ParseUint(startLedgerStr, 10, 32)
		if err != nil {
			logger.Fatal("invalid START_LEDGER", zap.String("value", startLedgerStr))
		}
		startLedger = uint32(startLedger64)

		endLedgerStr := getEnv("END_LEDGER", "0")
		endLedger64, err := strconv.ParseUint(endLedgerStr, 10, 32)
		if err != nil {
			logger.Fatal("invalid END_LEDGER", zap.String("value", endLedgerStr))
		}
		endLedger = uint32(endLedger64)

		filterAssetCode = getEnv("FILTER_ASSET_CODE", "")
		filterAssetIssuer = getEnv("FILTER_ASSET_ISSUER", "")
	}

	logger = logger.With(
		zap.String("balance_service", balanceServiceAddr),
		zap.String("duckdb_path", dbPath),
		zap.Int("batch_size", batchSize),
		zap.Uint32("start_ledger", startLedger),
		zap.Uint32("end_ledger", endLedger),
		zap.String("filter_asset_code", filterAssetCode),
		zap.String("filter_asset_issuer", filterAssetIssuer),
	)

	// Create consumer
	logger.Info("creating DuckDB consumer")
	duckdbConsumer, err := consumer.NewDuckDBConsumer(dbPath, balanceServiceAddr, batchSize)
	if err != nil {
		logger.Fatal("failed to create DuckDB consumer", zap.Error(err))
	}
	defer duckdbConsumer.Close()

	logger.Info("DuckDB consumer created successfully")

	// Start health check server
	go startHealthCheckServer(logger, duckdbConsumer, healthPort)

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumption in a goroutine
	errChan := make(chan error, 1)
	go func() {
		req := &accountbalance.StreamAccountBalancesRequest{
			StartLedger:       startLedger,
			EndLedger:         endLedger,
			FilterAssetCode:   filterAssetCode,
			FilterAssetIssuer: filterAssetIssuer,
		}
		logger.Info("starting balance consumption")
		err := duckdbConsumer.ConsumeBalances(ctx, req)
		errChan <- err
	}()

	// Wait for completion or signal
	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			logger.Error("consumption failed", zap.Error(err))
			os.Exit(1)
		}
		logger.Info("consumption completed successfully")
		printSummary(logger, duckdbConsumer)
		os.Exit(0)

	case sig := <-sigChan:
		logger.Info("received signal, shutting down gracefully", zap.String("signal", sig.String()))
		cancel()
		// Wait for consumption to finish
		<-errChan
		printSummary(logger, duckdbConsumer)
		os.Exit(0)
	}
}

func startHealthCheckServer(logger *zap.Logger, duckdbConsumer *consumer.DuckDBConsumer, port string) {
	healthAddr := fmt.Sprintf(":%s", port)
	logger.Info("starting health check server", zap.String("address", healthAddr))

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		metrics := duckdbConsumer.GetMetrics()
		response := map[string]interface{}{
			"status": "healthy",
			"metrics": map[string]interface{}{
				"total_balances_received": metrics.TotalBalancesReceived,
				"total_balances_written":  metrics.TotalBalancesWritten,
				"total_batches":           metrics.TotalBatches,
				"error_count":             metrics.ErrorCount,
				"last_write_latency":      metrics.LastWriteLatency.String(),
				"uptime_seconds":          time.Since(metrics.StartTime).Seconds(),
			},
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Error("failed to encode health check response", zap.Error(err))
		}
	})

	if err := http.ListenAndServe(healthAddr, nil); err != nil {
		logger.Error("health check server failed", zap.Error(err))
	}
}

func printSummary(logger *zap.Logger, duckdbConsumer *consumer.DuckDBConsumer) {
	metrics := duckdbConsumer.GetMetrics()
	duration := time.Since(metrics.StartTime)

	logger.Info("=== CONSUMPTION SUMMARY ===",
		zap.Int64("total_balances_received", metrics.TotalBalancesReceived),
		zap.Int64("total_balances_written", metrics.TotalBalancesWritten),
		zap.Int64("total_batches", metrics.TotalBatches),
		zap.Int64("error_count", metrics.ErrorCount),
		zap.Duration("total_duration", duration),
		zap.Float64("balances_per_second", float64(metrics.TotalBalancesWritten)/duration.Seconds()),
	)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	// Trim quotes if present
	return strings.Trim(value, "\"'")
}
