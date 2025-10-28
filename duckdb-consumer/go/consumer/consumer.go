package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	accountbalance "github.com/withobsrvr/duckdb-consumer/gen/account_balance_service"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// Batch size for bulk inserts
	defaultBatchSize = 1000

	// Schema for account_balances table
	createTableSQL = `
		CREATE TABLE IF NOT EXISTS account_balances (
			account_id VARCHAR NOT NULL,
			asset_code VARCHAR NOT NULL,
			asset_issuer VARCHAR NOT NULL,
			balance BIGINT NOT NULL,
			last_modified_ledger INTEGER NOT NULL,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (account_id, asset_code, asset_issuer)
		)
	`
)

// ConsumerMetrics tracks metrics for the DuckDB consumer
type ConsumerMetrics struct {
	mu                   sync.RWMutex
	TotalBalancesReceived int64
	TotalBalancesWritten  int64
	TotalBatches          int64
	ErrorCount            int64
	LastError             error
	LastErrorTime         time.Time
	StartTime             time.Time
	LastWriteLatency      time.Duration
	CurrentBatchSize      int
}

// NewConsumerMetrics creates a new metrics instance
func NewConsumerMetrics() *ConsumerMetrics {
	return &ConsumerMetrics{
		StartTime: time.Now(),
	}
}

// RecordBalanceReceived increments the received balance count
func (m *ConsumerMetrics) RecordBalanceReceived() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalBalancesReceived++
}

// RecordBatchWrite records a successful batch write
func (m *ConsumerMetrics) RecordBatchWrite(batchSize int, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalBalancesWritten += int64(batchSize)
	m.TotalBatches++
	m.LastWriteLatency = latency
	m.CurrentBatchSize = 0
}

// RecordError updates metrics when an error occurs
func (m *ConsumerMetrics) RecordError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ErrorCount++
	m.LastError = err
	m.LastErrorTime = time.Now()
}

// DuckDBConsumer consumes account balances and writes them to DuckDB
type DuckDBConsumer struct {
	db                   *sql.DB
	balanceServiceClient accountbalance.AccountBalanceServiceClient
	balanceServiceConn   *grpc.ClientConn
	logger               *zap.Logger
	metrics              *ConsumerMetrics
	batchSize            int
	dbPath               string
}

// NewDuckDBConsumer creates a new DuckDB consumer
func NewDuckDBConsumer(dbPath string, balanceServiceAddr string, batchSize int) (*DuckDBConsumer, error) {
	// Initialize zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize zap logger: %w", err)
	}

	// Open DuckDB database
	logger.Info("opening DuckDB database", zap.String("path", dbPath))
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DuckDB: %w", err)
	}

	// Create table
	logger.Info("creating account_balances table if not exists")
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Connect to account balance service
	logger.Info("connecting to account balance service", zap.String("address", balanceServiceAddr))
	conn, err := grpc.Dial(balanceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to balance service: %w", err)
	}
	logger.Info("successfully connected to account balance service")

	client := accountbalance.NewAccountBalanceServiceClient(conn)

	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	return &DuckDBConsumer{
		db:                   db,
		balanceServiceClient: client,
		balanceServiceConn:   conn,
		logger:               logger,
		metrics:              NewConsumerMetrics(),
		batchSize:            batchSize,
		dbPath:               dbPath,
	}, nil
}

// Close cleans up resources
func (c *DuckDBConsumer) Close() error {
	c.logger.Info("closing DuckDB consumer")
	var errs []error

	if c.balanceServiceConn != nil {
		if err := c.balanceServiceConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close balance service connection: %w", err))
		}
	}

	if c.db != nil {
		if err := c.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close DuckDB: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

// ConsumeBalances streams account balances and writes them to DuckDB
func (c *DuckDBConsumer) ConsumeBalances(ctx context.Context, req *accountbalance.StreamAccountBalancesRequest) error {
	logger := c.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.String("filter_asset_code", req.FilterAssetCode),
		zap.String("filter_asset_issuer", req.FilterAssetIssuer),
	)
	logger.Info("starting balance consumption")

	// Create a new context for the stream
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	// Start streaming balances
	stream, err := c.balanceServiceClient.StreamAccountBalances(streamCtx, req)
	if err != nil {
		logger.Error("failed to start balance stream", zap.Error(err))
		c.metrics.RecordError(err)
		return fmt.Errorf("failed to start balance stream: %w", err)
	}
	logger.Info("successfully initiated balance stream")

	// Batch buffer
	batch := make([]*accountbalance.AccountBalance, 0, c.batchSize)

	// Loop receiving balances
	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			logger.Info("context cancelled, stopping consumption")
			// Write any remaining balances
			if len(batch) > 0 {
				if err := c.writeBatch(batch); err != nil {
					logger.Error("failed to write final batch", zap.Error(err))
					return err
				}
			}
			return ctx.Err()
		default:
			// Continue
		}

		// Receive next balance
		balance, err := stream.Recv()
		if err == io.EOF {
			logger.Info("stream ended, writing final batch if any")
			// Write any remaining balances
			if len(batch) > 0 {
				if err := c.writeBatch(batch); err != nil {
					logger.Error("failed to write final batch", zap.Error(err))
					return err
				}
			}
			logger.Info("consumption completed successfully")
			return nil
		}
		if err != nil {
			// Check if cancelled
			if status.Code(err) == codes.Canceled && ctx.Err() != nil {
				logger.Info("stream cancelled due to context")
				// Write any remaining balances
				if len(batch) > 0 {
					if err := c.writeBatch(batch); err != nil {
						logger.Error("failed to write final batch", zap.Error(err))
						return err
					}
				}
				return ctx.Err()
			}
			logger.Error("error receiving balance", zap.Error(err))
			c.metrics.RecordError(err)
			return fmt.Errorf("error receiving balance: %w", err)
		}

		// Add to batch
		c.metrics.RecordBalanceReceived()
		batch = append(batch, balance)

		// Write batch if full
		if len(batch) >= c.batchSize {
			if err := c.writeBatch(batch); err != nil {
				logger.Error("failed to write batch", zap.Error(err))
				return err
			}
			// Clear batch
			batch = batch[:0]
		}
	}
}

// writeBatch writes a batch of balances to DuckDB
func (c *DuckDBConsumer) writeBatch(batch []*accountbalance.AccountBalance) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	c.logger.Info("writing batch", zap.Int("size", len(batch)))

	// Begin transaction
	tx, err := c.db.Begin()
	if err != nil {
		c.metrics.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Prepare statement using INSERT OR REPLACE for upsert behavior
	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO account_balances
		(account_id, asset_code, asset_issuer, balance, last_modified_ledger)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		c.metrics.RecordError(err)
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute for each balance
	for _, balance := range batch {
		_, err := stmt.Exec(
			balance.AccountId,
			balance.AssetCode,
			balance.AssetIssuer,
			balance.Balance,
			balance.LastModifiedLedger,
		)
		if err != nil {
			c.metrics.RecordError(err)
			return fmt.Errorf("failed to insert balance: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		c.metrics.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	latency := time.Since(start)
	c.metrics.RecordBatchWrite(len(batch), latency)
	c.logger.Info("batch written successfully",
		zap.Int("size", len(batch)),
		zap.Duration("latency", latency))

	return nil
}

// GetMetrics returns a copy of the current metrics
func (c *DuckDBConsumer) GetMetrics() *ConsumerMetrics {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()
	return c.metrics
}

// QueryBalances queries balances from DuckDB
func (c *DuckDBConsumer) QueryBalances(query string) (*sql.Rows, error) {
	return c.db.Query(query)
}
