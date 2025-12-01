package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/withObsrvr/contract-events-postgres-consumer/gen/contract_event_service"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	// gRPC service
	ServiceAddress string

	// PostgreSQL
	PostgresHost     string
	PostgresPort     string
	PostgresDB       string
	PostgresUser     string
	PostgresPassword string
	PostgresSSLMode  string

	// Streaming
	StartLedger uint32
	EndLedger   uint32

	// Filters
	ContractIDs   []string
	EventTypes    []string
	IncludeFailed bool
}

type Stats struct {
	EventsProcessed   int64
	EventsInserted    int64
	EventsFailed      int64
	LedgersProcessed  int64
	LastLedger        uint32
	LastProcessedTime time.Time
}

func main() {
	// Parse command line
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <start_ledger> <end_ledger>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Set end_ledger=0 for continuous streaming\n")
		os.Exit(1)
	}

	startLedger, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		log.Fatalf("Invalid start_ledger: %v", err)
	}

	endLedger, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		log.Fatalf("Invalid end_ledger: %v", err)
	}

	// Load configuration
	config := &Config{
		ServiceAddress:   getEnvOrDefault("CONTRACT_EVENTS_SERVICE_ADDRESS", "localhost:50053"),
		PostgresHost:     getEnvOrDefault("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnvOrDefault("POSTGRES_PORT", "5432"),
		PostgresDB:       getEnvOrDefault("POSTGRES_DB", "contract_events"),
		PostgresUser:     getEnvOrDefault("POSTGRES_USER", "postgres"),
		PostgresPassword: getEnvOrDefault("POSTGRES_PASSWORD", "postgres"),
		PostgresSSLMode:  getEnvOrDefault("POSTGRES_SSLMODE", "disable"),
		StartLedger:      uint32(startLedger),
		EndLedger:        uint32(endLedger),
		ContractIDs:      getEnvSlice("FILTER_CONTRACT_IDS"),
		EventTypes:       getEnvSlice("FILTER_EVENT_TYPES"),
		IncludeFailed:    getBoolEnv("INCLUDE_FAILED", false),
	}

	// Connect to PostgreSQL
	log.Printf("Connecting to PostgreSQL at %s:%s/%s", config.PostgresHost, config.PostgresPort, config.PostgresDB)
	db, err := connectDB(config)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Validate schema exists
	if err := validateSchema(db); err != nil {
		log.Fatalf("Schema validation failed: %v\n\nPlease run: psql -f config/schema/contract_events.sql", err)
	}

	log.Println("✓ Schema validated")

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stats := &ThreadSafeStats{}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping...")
		cancel()
	}()

	// Start health check server
	healthPort := getEnvOrDefault("HEALTH_PORT", "8089")
	go func() {
		healthAddr := fmt.Sprintf(":%s", healthPort)
		log.Printf("Starting health check server at %s", healthAddr)

		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			snapshot := stats.GetSnapshot()
			response := map[string]interface{}{
				"status": "healthy",
				"stats": map[string]interface{}{
					"events_processed":  snapshot.EventsProcessed,
					"events_inserted":   snapshot.EventsInserted,
					"events_failed":     snapshot.EventsFailed,
					"ledgers_processed": snapshot.LedgersProcessed,
					"last_ledger":       snapshot.LastLedger,
				},
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Printf("Failed to encode health check response: %v", err)
			}
		})

		if err := http.ListenAndServe(healthAddr, nil); err != nil {
			log.Fatalf("Failed to start health check server: %v", err)
		}
	}()

	// Initialize flowctl integration if enabled
	enableFlowctl := getEnvOrDefault("ENABLE_FLOWCTL", "")
	if strings.ToLower(enableFlowctl) == "true" {
		log.Println("Initializing flowctl integration")
		flowctlController := NewFlowctlController(stats)
		if err := flowctlController.RegisterWithFlowctl(); err != nil {
			log.Printf("Warning: Failed to register with flowctl: %v", err)
		}
		defer flowctlController.Stop()
	}

	// Start stats reporter
	go reportStats(stats)

	// Connect to contract events service
	log.Printf("Connecting to Contract Events service at %s", config.ServiceAddress)
	conn, err := grpc.NewClient(config.ServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewContractEventServiceClient(conn)

	// Create request
	request := &pb.GetContractEventsRequest{
		StartLedger:   config.StartLedger,
		EndLedger:     config.EndLedger,
		ContractIds:   config.ContractIDs,
		EventTypes:    config.EventTypes,
		IncludeFailed: config.IncludeFailed,
	}

	// Log filters
	if len(request.ContractIds) > 0 {
		log.Printf("Filtering by contract IDs: %v", request.ContractIds)
	}
	if len(request.EventTypes) > 0 {
		log.Printf("Filtering by event types: %v", request.EventTypes)
	}

	// Start streaming
	log.Printf("Starting event stream from ledger %d to %d", config.StartLedger, config.EndLedger)
	if config.EndLedger == 0 {
		log.Printf("Streaming continuously (end_ledger=0)")
	}

	stream, err := client.GetContractEvents(ctx, request)
	if err != nil {
		log.Fatalf("Failed to start stream: %v", err)
	}

	// Process events
	currentLedger := uint32(0)
	ledgerEvents := 0

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream completed")
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, graceful shutdown
				break
			}
			log.Fatalf("Error receiving event: %v", err)
		}

		stats.IncrementEventsProcessed()

		// Track ledger changes
		if event.Meta.LedgerSequence != currentLedger {
			if currentLedger > 0 {
				log.Printf("Ledger %d: %d events inserted", currentLedger, ledgerEvents)
			}
			currentLedger = event.Meta.LedgerSequence
			ledgerEvents = 0
			stats.IncrementLedgersProcessed()
			stats.SetLastLedger(currentLedger)
		}

		// Insert event
		if err := insertEvent(db, event); err != nil {
			log.Printf("Failed to insert event: %v", err)
			stats.IncrementEventsFailed()
			continue
		}

		stats.IncrementEventsInserted()
		ledgerEvents++
		stats.SetLastProcessedTime(time.Now())
	}

	// Final stats
	finalStats := stats.GetSnapshot()
	log.Println("\n" + strings.Repeat("=", 80))
	log.Printf("Final Statistics:")
	log.Printf("  Events Processed: %d", finalStats.EventsProcessed)
	log.Printf("  Events Inserted:  %d", finalStats.EventsInserted)
	log.Printf("  Events Failed:    %d", finalStats.EventsFailed)
	log.Printf("  Ledgers Processed: %d", finalStats.LedgersProcessed)
	log.Printf("  Last Ledger:      %d", finalStats.LastLedger)
	log.Println(strings.Repeat("=", 80))
}

func connectDB(config *Config) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		config.PostgresHost,
		config.PostgresPort,
		config.PostgresUser,
		config.PostgresPassword,
		config.PostgresDB,
		config.PostgresSSLMode,
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

func insertEvent(db *sql.DB, event *pb.ContractEvent) error {
	// Convert topics to JSONB
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

	// Insert (no schema management - just INSERT!)
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

func convertTopicsToJSON(topics []*pb.ScValue) ([]byte, error) {
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

func reportStats(stats *ThreadSafeStats) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		snapshot := stats.GetSnapshot()
		log.Printf("Stats: %d events processed, %d inserted, %d ledgers, last: %d",
			snapshot.EventsProcessed,
			snapshot.EventsInserted,
			snapshot.LedgersProcessed,
			snapshot.LastLedger,
		)
	}
}

// Helper functions

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvSlice(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	var result []string
	for _, item := range splitAndTrim(value, ",") {
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func getBoolEnv(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	switch value {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return defaultValue
	}
}

func splitAndTrim(s, sep string) []string {
	parts := []string{}
	current := ""
	for _, char := range s {
		if string(char) == sep {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else if char != ' ' && char != '\t' {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
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
