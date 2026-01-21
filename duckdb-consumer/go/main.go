// Package main provides a DuckDB sink for flowctl pipelines.
// This version uses the flowctl-sdk consumer pattern to receive events
// from the pipeline and store ledger data in DuckDB.
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

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/withObsrvr/flowctl-sdk/pkg/consumer"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"
	"google.golang.org/protobuf/proto"

	_ "github.com/duckdb/duckdb-go/v2"
)

// DuckDBSink handles writing ledger events to DuckDB
type DuckDBSink struct {
	db *sql.DB
}

// Global sink instance (needed because consumer.Run() uses a callback)
var sink *DuckDBSink

// Use atomic counters to avoid race conditions
var eventsProcessed atomic.Int64
var ledgersProcessed atomic.Int64
var contractEventsProcessed atomic.Int64

// NewDuckDBSink creates a new DuckDB sink
func NewDuckDBSink(dbPath string) (*DuckDBSink, error) {
	log.Printf("Opening DuckDB database at: %s", dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// DuckDB doesn't handle concurrent writes well - use single connection
	db.SetMaxOpenConns(1)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DuckDB: %w", err)
	}

	// Create tables
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	log.Printf("DuckDB database ready")
	return &DuckDBSink{db: db}, nil
}

// initSchema creates the necessary tables
func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS ledgers (
			sequence INTEGER PRIMARY KEY,
			network VARCHAR,
			closed_at TIMESTAMP,
			xdr_size INTEGER,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS events (
			id VARCHAR PRIMARY KEY,
			event_type VARCHAR NOT NULL,
			ledger_sequence INTEGER,
			source_component VARCHAR,
			content_type VARCHAR,
			payload_size INTEGER,
			metadata JSON,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS contract_events (
			id VARCHAR PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash VARCHAR,
			contract_id VARCHAR,
			event_type VARCHAR,
			event_index INTEGER,
			in_successful_tx BOOLEAN,
			topics_count INTEGER,
			topics JSON,
			data JSON,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

	log.Println("Database schema initialized")
	return nil
}

// InsertLedger inserts a ledger record
func (s *DuckDBSink) InsertLedger(ledger *stellarv1.RawLedger) error {
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO ledgers (sequence, network, closed_at, xdr_size)
		VALUES (?, ?, ?, ?)
	`,
		ledger.Sequence,
		ledger.Network,
		time.Unix(ledger.ClosedAt, 0),
		len(ledger.LedgerCloseMetaXdr),
	)
	return err
}

// InsertEvent inserts a generic event record
func (s *DuckDBSink) InsertEvent(event *flowctlv1.Event) error {
	// Extract ledger sequence from metadata if available
	var ledgerSeq *int
	if seq, ok := event.Metadata["ledger_sequence"]; ok {
		var seqInt int
		if _, err := fmt.Sscanf(seq, "%d", &seqInt); err == nil {
			ledgerSeq = &seqInt
		}
	}

	// Serialize metadata to JSON
	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		log.Printf("Warning: failed to marshal event metadata for event %s: %v", event.Id, err)
		metadataJSON = []byte("{}")
	}

	_, err = s.db.Exec(`
		INSERT OR REPLACE INTO events (id, event_type, ledger_sequence, source_component, content_type, payload_size, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?)
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

// sqlExecutor is an interface satisfied by both *sql.DB and *sql.Tx
type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// insertContractEvent inserts a contract event using any SQL executor (DB or Tx)
func insertContractEvent(exec sqlExecutor, ce *stellarv1.ContractEvent) error {
	var ledgerSeq uint32
	var txHash string
	var txIndex uint32
	if ce.Meta != nil {
		ledgerSeq = ce.Meta.LedgerSequence
		txHash = ce.Meta.TxHash
		txIndex = ce.Meta.TxIndex
	}

	// Generate unique event ID using Stellar's TOID (Total Order ID) format
	// TOID encodes ledger sequence + transaction index + operation index
	// We use operation_index if available, otherwise 0, then append event_index
	var opIndex int32 = 0
	if ce.OperationIndex != nil {
		opIndex = *ce.OperationIndex
	}
	toidVal := toid.New(int32(ledgerSeq), int32(txIndex), opIndex).ToInt64()
	eventID := fmt.Sprintf("ce-%d-%d", toidVal, ce.EventIndex)

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

	_, err := exec.Exec(`
		INSERT OR REPLACE INTO contract_events (id, ledger_sequence, tx_hash, contract_id, event_type, event_index, in_successful_tx, topics_count, topics, data)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

// InsertContractEvent inserts a contract event record using the database connection
func (s *DuckDBSink) InsertContractEvent(ce *stellarv1.ContractEvent) error {
	return insertContractEvent(s.db, ce)
}

// insertContractEventTx inserts a contract event using an existing transaction
func insertContractEventTx(tx *sql.Tx, ce *stellarv1.ContractEvent) error {
	return insertContractEvent(tx, ce)
}

// Close closes the database connection
func (s *DuckDBSink) Close() error {
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
			var batchErr error
			for _, ce := range batch.Events {
				if err := insertContractEventTx(tx, ce); err != nil {
					log.Printf("Warning: Failed to insert contract event, rolling back batch: %v", err)
					batchErr = err
					break
				}
				insertedCount++
			}

			if batchErr != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					return fmt.Errorf("failed to rollback transaction after insert error: %v (original error: %w)", rbErr, batchErr)
				}
				return fmt.Errorf("contract event insertion failed: %w", batchErr)
			}

			if err := tx.Commit(); err != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					log.Printf("Warning: rollback failed after commit error (commitErr=%v, rollbackErr=%v)", err, rbErr)
				}
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
	dbPath := getEnv("DUCKDB_PATH", "./stellar_data.duckdb")
	componentID := getEnv("FLOWCTL_COMPONENT_ID", "duckdb-consumer")

	// Set default port to 50053 to avoid conflict with stellar-live-source (50052)
	// consumer.Run() reads PORT from environment with default of :50052
	if os.Getenv("PORT") == "" {
		os.Setenv("PORT", ":50053")
	}

	log.Printf("Starting DuckDB Consumer (flowctl mode)")
	log.Printf("Database path: %s", dbPath)
	log.Printf("Component ID: %s", componentID)
	log.Printf("Port: %s", os.Getenv("PORT"))

	// Initialize DuckDB
	var err error
	sink, err = NewDuckDBSink(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize DuckDB: %v", err)
	}
	defer sink.Close()

	// Use consumer.Run() which handles all the gRPC setup, registration, and event loop
	// This blocks until the consumer is stopped (e.g., via SIGINT/SIGTERM)
	// Note: InputType accepts first type, but handleEvent processes both ledger and contract events
	consumer.Run(consumer.ConsumerConfig{
		ConsumerName: "DuckDB Consumer",
		InputType:    "stellar.contract.events.v1",
		OnEvent:      handleEvent,
		ComponentID:  componentID,
	})

	// Print summary (only reached on shutdown)
	log.Printf("=== Summary ===")
	log.Printf("Total events processed: %d", eventsProcessed.Load())
	log.Printf("Ledgers stored: %d", ledgersProcessed.Load())
	log.Printf("Contract events stored: %d", contractEventsProcessed.Load())
	log.Println("DuckDB consumer stopped")
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return strings.Trim(value, "\"'")
}
