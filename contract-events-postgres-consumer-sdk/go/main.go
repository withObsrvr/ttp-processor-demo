package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	// Load configuration from environment
	config := consumer.DefaultConfig()
	config.ID = getEnv("COMPONENT_ID", "contract-events-postgres-consumer")
	config.Name = "Contract Events PostgreSQL Consumer"
	config.Description = "Stores Soroban contract events in PostgreSQL"
	config.Version = "2.0.0-sdk"
	config.Endpoint = getEnv("PORT", ":8090")
	config.HealthPort = parseInt(getEnv("HEALTH_PORT", "8089"))
	config.MaxConcurrent = parseInt(getEnv("MAX_CONCURRENT", "10"))

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		config.FlowctlConfig.Enabled = true
		config.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		config.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Connect to PostgreSQL
	dbConfig := &DBConfig{
		Host:     getEnv("POSTGRES_HOST", "localhost"),
		Port:     getEnv("POSTGRES_PORT", "5432"),
		DBName:   getEnv("POSTGRES_DB", "contract_events"),
		User:     getEnv("POSTGRES_USER", "postgres"),
		Password: getEnv("POSTGRES_PASSWORD", "postgres"),
		SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
	}

	logger.Info("Connecting to PostgreSQL",
		zap.String("host", dbConfig.Host),
		zap.String("port", dbConfig.Port),
		zap.String("database", dbConfig.DBName))

	db, err := connectDB(dbConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Validate schema exists
	if err := validateSchema(db); err != nil {
		log.Fatalf("Schema validation failed: %v\n\nPlease ensure contract_events table exists", err)
	}

	logger.Info("Schema validated successfully")

	// Create consumer
	cons, err := consumer.New(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Register consumer handler
	err = cons.OnConsume(func(ctx context.Context, event *flowctlv1.Event) error {
		// Only process stellar contract events
		if event.Type != "stellar.contract.events.v1" {
			logger.Debug("Skipping non-contract-events", zap.String("type", event.Type))
			return nil // Skip non-contract events
		}

		// Parse the ContractEventBatch from payload
		var batch stellarv1.ContractEventBatch
		if err := proto.Unmarshal(event.Payload, &batch); err != nil {
			logger.Error("Failed to unmarshal ContractEventBatch", zap.Error(err))
			return fmt.Errorf("failed to unmarshal batch: %w", err)
		}

		logger.Info("Processing contract event batch",
			zap.Int("count", len(batch.Events)),
			zap.Uint64("ledger", event.StellarCursor.GetLedgerSequence()))

		// Insert each event
		for _, contractEvent := range batch.Events {
			if err := insertEvent(db, contractEvent); err != nil {
				logger.Error("Failed to insert event", zap.Error(err))
				return fmt.Errorf("failed to insert event: %w", err)
			}
		}

		logger.Debug("Successfully processed event batch",
			zap.Int("count", len(batch.Events)))

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cons.Start(ctx); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	logger.Info("Contract Events PostgreSQL Consumer is running",
		zap.String("endpoint", config.Endpoint),
		zap.Int("health_port", config.HealthPort),
		zap.Int("max_concurrent", config.MaxConcurrent),
		zap.Bool("flowctl_enabled", config.FlowctlConfig.Enabled))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop the consumer gracefully
	logger.Info("Shutting down consumer...")
	if err := cons.Stop(); err != nil {
		log.Fatalf("Failed to stop consumer: %v", err)
	}

	logger.Info("Consumer stopped successfully")
}

type DBConfig struct {
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
	SSLMode  string
}

func connectDB(config *DBConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
		config.User,
		config.Password,
		config.DBName,
		config.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}

func validateSchema(db *sql.DB) error {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = 'contract_events'
		)
	`).Scan(&exists)

	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("table contract_events does not exist")
	}

	return nil
}

func insertEvent(db *sql.DB, event *stellarv1.ContractEvent) error {
	// Convert topics to JSONB (if JSON is present, otherwise use empty array)
	topicsJSON, err := convertTopicsToJSON(event.Topics)
	if err != nil {
		return fmt.Errorf("failed to convert topics: %w", err)
	}

	// Convert data to JSONB
	var dataJSON []byte
	if event.Data != nil && event.Data.Json != "" {
		dataJSON = []byte(event.Data.Json)
	}

	// Extract XDR arrays
	topicsXDR := make([]string, 0, len(event.Topics))
	for _, topic := range event.Topics {
		topicsXDR = append(topicsXDR, topic.XdrBase64)
	}

	var dataXDR *string
	if event.Data != nil && event.Data.XdrBase64 != "" {
		dataXDR = &event.Data.XdrBase64
	}

	// Convert closed_at from Unix timestamp to time.Time
	closedAt := time.Unix(event.Meta.LedgerClosedAt, 0)

	// Insert event
	_, err = db.Exec(`
		INSERT INTO contract_events (
			ledger_sequence,
			ledger_closed_at,
			tx_hash,
			tx_successful,
			tx_index,
			contract_id,
			event_type,
			event_index,
			operation_index,
			topics,
			event_data,
			topics_xdr,
			data_xdr
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`,
		event.Meta.LedgerSequence,
		closedAt,
		event.Meta.TxHash,
		event.Meta.TxSuccessful,
		event.Meta.TxIndex,
		event.ContractId,
		event.EventType,
		event.EventIndex,
		event.OperationIndex,
		topicsJSON,
		dataJSON,
		pqArray(topicsXDR),
		dataXDR,
	)

	return err
}

func convertTopicsToJSON(topics []*stellarv1.ScValue) ([]byte, error) {
	jsonTopics := make([]json.RawMessage, 0, len(topics))
	for _, topic := range topics {
		if topic.Json != "" {
			jsonTopics = append(jsonTopics, json.RawMessage(topic.Json))
		} else {
			jsonTopics = append(jsonTopics, json.RawMessage("null"))
		}
	}
	return json.Marshal(jsonTopics)
}

// PostgreSQL array helper
func pqArray(arr []string) interface{} {
	if len(arr) == 0 {
		return "{}"
	}
	result := "{"
	for i, item := range arr {
		if i > 0 {
			result += ","
		}
		// Escape quotes and backslashes
		escaped := item
		escaped = replaceAll(escaped, "\\", "\\\\")
		escaped = replaceAll(escaped, "\"", "\\\"")
		result += "\"" + escaped + "\""
	}
	result += "}"
	return result
}

func replaceAll(s, old, new string) string {
	result := ""
	for i := 0; i < len(s); i++ {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			result += new
			i += len(old) - 1
		} else {
			result += string(s[i])
		}
	}
	return result
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}
