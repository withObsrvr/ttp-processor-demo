// Package main provides a PostgreSQL sink for flowctl pipelines.
// This version uses the flowctl-sdk consumer pattern to receive events
// from the pipeline and store ledger data in PostgreSQL with JSONB support.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"
	"google.golang.org/protobuf/proto"

	_ "github.com/lib/pq"
)

// PostgreSQLSink handles writing ledger events to PostgreSQL
type PostgreSQLSink struct {
	db *sql.DB
}

// Global sink instance (needed because consumer.Run() uses a callback)
var sink *PostgreSQLSink

// Use atomic counters to avoid race conditions
var eventsProcessed atomic.Int64
var ledgersProcessed atomic.Int64
var contractEventsProcessed atomic.Int64

// NewPostgreSQLSink creates a new PostgreSQL sink
func NewPostgreSQLSink(connStr string) (*PostgreSQLSink, error) {
	log.Printf("Connecting to PostgreSQL...")

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Create tables
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	log.Printf("PostgreSQL database ready")
	return &PostgreSQLSink{db: db}, nil
}

// initSchema creates the necessary tables with JSONB support
func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS ledgers (
			sequence INTEGER PRIMARY KEY,
			network VARCHAR(50),
			closed_at TIMESTAMPTZ,
			xdr_size INTEGER,
			inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS events (
			id VARCHAR(255) PRIMARY KEY,
			event_type VARCHAR(255) NOT NULL,
			ledger_sequence INTEGER,
			source_component VARCHAR(255),
			content_type VARCHAR(100),
			payload_size INTEGER,
			metadata JSONB,
			inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS contract_events (
			id VARCHAR(255) PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash VARCHAR(64),
			contract_id VARCHAR(56),
			event_type VARCHAR(50),
			event_index INTEGER,
			in_successful_tx BOOLEAN,
			topics_count INTEGER,
			topics JSONB,
			data JSONB,
			inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_ledgers_closed_at ON ledgers(closed_at);
		CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
		CREATE INDEX IF NOT EXISTS idx_events_ledger ON events(ledger_sequence);
		CREATE INDEX IF NOT EXISTS idx_contract_events_ledger ON contract_events(ledger_sequence);
		CREATE INDEX IF NOT EXISTS idx_contract_events_contract ON contract_events(contract_id);
		CREATE INDEX IF NOT EXISTS idx_contract_events_type ON contract_events(event_type);
	`)

	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Create GIN indexes for JSONB queries (PostgreSQL specific)
	ginIndexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_contract_events_topics_gin ON contract_events USING GIN (topics)",
		"CREATE INDEX IF NOT EXISTS idx_contract_events_data_gin ON contract_events USING GIN (data)",
		"CREATE INDEX IF NOT EXISTS idx_events_metadata_gin ON events USING GIN (metadata)",
	}

	for _, idx := range ginIndexes {
		if _, err := db.Exec(idx); err != nil {
			log.Printf("Warning: Failed to create GIN index: %v", err)
		}
	}

	log.Println("Database schema initialized")
	return nil
}

// InsertLedger inserts a ledger record
func (s *PostgreSQLSink) InsertLedger(ledger *stellarv1.RawLedger) error {
	_, err := s.db.Exec(`
		INSERT INTO ledgers (sequence, network, closed_at, xdr_size)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (sequence) DO UPDATE SET
			network = EXCLUDED.network,
			closed_at = EXCLUDED.closed_at,
			xdr_size = EXCLUDED.xdr_size
	`,
		ledger.Sequence,
		ledger.Network,
		time.Unix(ledger.ClosedAt, 0),
		len(ledger.LedgerCloseMetaXdr),
	)
	return err
}

// InsertEvent inserts a generic event record
func (s *PostgreSQLSink) InsertEvent(event *flowctlv1.Event) error {
	// Extract ledger sequence from metadata if available
	var ledgerSeq *int
	if seq, ok := event.Metadata["ledger_sequence"]; ok {
		var seqInt int
		if _, err := fmt.Sscanf(seq, "%d", &seqInt); err == nil {
			ledgerSeq = &seqInt
		}
	}

	// Serialize metadata to JSON
	metadataJSON, _ := json.Marshal(event.Metadata)

	_, err := s.db.Exec(`
		INSERT INTO events (id, event_type, ledger_sequence, source_component, content_type, payload_size, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			event_type = EXCLUDED.event_type,
			ledger_sequence = EXCLUDED.ledger_sequence,
			source_component = EXCLUDED.source_component,
			content_type = EXCLUDED.content_type,
			payload_size = EXCLUDED.payload_size,
			metadata = EXCLUDED.metadata
	`,
		event.Id,
		event.Type,
		ledgerSeq,
		event.SourceComponentId,
		event.ContentType,
		len(event.Payload),
		string(metadataJSON),
	)
	return err
}

// insertContractEventTx inserts a contract event using an existing transaction
func insertContractEventTx(tx *sql.Tx, ce *stellarv1.ContractEvent) error {
	var ledgerSeq uint32
	var txHash string
	if ce.Meta != nil {
		ledgerSeq = ce.Meta.LedgerSequence
		txHash = ce.Meta.TxHash
	}

	eventID := fmt.Sprintf("ce-%d-%s-%d", ledgerSeq, ce.ContractId, ce.EventIndex)

	// Extract topics as JSON array
	var topicsArray []interface{}
	for _, topic := range ce.Topics {
		if topic.Json != "" {
			var decoded interface{}
			if err := json.Unmarshal([]byte(topic.Json), &decoded); err == nil {
				topicsArray = append(topicsArray, decoded)
			} else {
				topicsArray = append(topicsArray, topic.Json)
			}
		} else {
			topicsArray = append(topicsArray, topic.XdrBase64)
		}
	}
	topicsJSON, _ := json.Marshal(topicsArray)

	// Extract data as JSON
	var dataJSON []byte
	if ce.Data != nil {
		if ce.Data.Json != "" {
			var decoded interface{}
			if err := json.Unmarshal([]byte(ce.Data.Json), &decoded); err == nil {
				dataJSON, _ = json.Marshal(decoded)
			} else {
				dataJSON = []byte(ce.Data.Json)
			}
		} else {
			dataJSON, _ = json.Marshal(ce.Data.XdrBase64)
		}
	}

	_, err := tx.Exec(`
		INSERT INTO contract_events (id, ledger_sequence, tx_hash, contract_id, event_type, event_index, in_successful_tx, topics_count, topics, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			ledger_sequence = EXCLUDED.ledger_sequence,
			tx_hash = EXCLUDED.tx_hash,
			contract_id = EXCLUDED.contract_id,
			event_type = EXCLUDED.event_type,
			event_index = EXCLUDED.event_index,
			in_successful_tx = EXCLUDED.in_successful_tx,
			topics_count = EXCLUDED.topics_count,
			topics = EXCLUDED.topics,
			data = EXCLUDED.data
	`,
		eventID,
		ledgerSeq,
		txHash,
		ce.ContractId,
		ce.EventType,
		ce.EventIndex,
		ce.InSuccessfulTx,
		len(ce.Topics),
		string(topicsJSON),
		string(dataJSON),
	)
	return err
}

// Close closes the database connection
func (s *PostgreSQLSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// handleEvent processes incoming events from the pipeline
func handleEvent(ctx context.Context, event *flowctlv1.Event) error {
	eventsProcessed.Add(1)

	switch event.Type {
	case "stellar.ledger.v1", "stellar.raw.ledger.v1":
		// Try to parse as RawLedger protobuf
		if event.ContentType == "application/protobuf" || event.ContentType == "" {
			var ledger stellarv1.RawLedger
			if err := proto.Unmarshal(event.Payload, &ledger); err != nil {
				log.Printf("Warning: Failed to parse RawLedger protobuf: %v", err)
				// Fall back to storing as generic event
				return sink.InsertEvent(event)
			}

			if err := sink.InsertLedger(&ledger); err != nil {
				return fmt.Errorf("failed to insert ledger: %w", err)
			}

			count := ledgersProcessed.Add(1)
			if count%100 == 0 {
				log.Printf("Processed %d ledgers (latest: %d)", count, ledger.Sequence)
			}
			return nil
		}

		// Otherwise store as generic event
		return sink.InsertEvent(event)

	case "stellar.contract.events.v1":
		// Parse as ContractEventBatch protobuf
		if event.ContentType == "application/protobuf" || event.ContentType == "" {
			var batch stellarv1.ContractEventBatch
			if err := proto.Unmarshal(event.Payload, &batch); err != nil {
				log.Printf("Warning: Failed to parse ContractEventBatch protobuf: %v", err)
				// Fall back to storing as generic event
				return sink.InsertEvent(event)
			}

			// Use transaction for batch insert to ensure atomicity
			tx, err := sink.db.Begin()
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			var insertedCount int64
			for _, ce := range batch.Events {
				if err := insertContractEventTx(tx, ce); err != nil {
					log.Printf("Warning: Failed to insert contract event: %v", err)
					continue
				}
				insertedCount++
			}

			if err := tx.Commit(); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			total := contractEventsProcessed.Add(insertedCount)
			if len(batch.Events) > 0 {
				log.Printf("Stored %d contract events (total: %d)", len(batch.Events), total)
			}
			return nil
		}

		// Otherwise store as generic event
		return sink.InsertEvent(event)

	default:
		// Store unknown event types as generic events
		log.Printf("Received unknown event type: %s", event.Type)
		return sink.InsertEvent(event)
	}
}

func main() {
	// Get configuration from environment
	pgHost := getEnv("POSTGRES_HOST", "localhost")
	pgPort := getEnv("POSTGRES_PORT", "5432")
	pgDB := getEnv("POSTGRES_DB", "stellar_events")
	pgUser := getEnv("POSTGRES_USER", "postgres")
	pgPassword := getEnv("POSTGRES_PASSWORD", "")
	pgSSLMode := getEnv("POSTGRES_SSLMODE", "disable")
	componentID := getEnv("FLOWCTL_COMPONENT_ID", "postgres-consumer")

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		pgHost, pgPort, pgDB, pgUser, pgPassword, pgSSLMode)

	// Set default port to 50053 to avoid conflict with stellar-live-source (50052)
	// consumer.Run() reads PORT from environment with default of :50052
	if os.Getenv("PORT") == "" {
		os.Setenv("PORT", ":50053")
	}

	log.Printf("Starting PostgreSQL Consumer (flowctl mode)")
	log.Printf("Database: %s@%s:%s/%s", pgUser, pgHost, pgPort, pgDB)
	log.Printf("Component ID: %s", componentID)
	log.Printf("Port: %s", os.Getenv("PORT"))

	// Initialize PostgreSQL
	var err error
	sink, err = NewPostgreSQLSink(connStr)
	if err != nil {
		log.Fatalf("Failed to initialize PostgreSQL: %v", err)
	}
	defer sink.Close()

	// Use consumer.Run() which handles all the gRPC setup, registration, and event loop
	// This blocks until the consumer is stopped (e.g., via SIGINT/SIGTERM)
	// Note: InputType accepts first type, but handleEvent processes both ledger and contract events
	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "PostgreSQL Consumer",
		InputType:    "stellar.contract.events.v1",
		OnEvent:      handleEvent,
		ComponentID:  componentID,
	})

	// Print summary (only reached on shutdown)
	log.Printf("=== Summary ===")
	log.Printf("Total events processed: %d", eventsProcessed.Load())
	log.Printf("Ledgers stored: %d", ledgersProcessed.Load())
	log.Printf("Contract events stored: %d", contractEventsProcessed.Load())
	log.Println("PostgreSQL consumer stopped")
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return strings.Trim(value, "\"'")
}
