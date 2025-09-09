package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

// DataSourceClient manages the connection to stellar-live-source-datalake
type DataSourceClient struct {
	config        *config.Config
	logger        *logging.ComponentLogger
	conn          *grpc.ClientConn
	client        rawledger.RawLedgerServiceClient
	
	// Connection state
	mu            sync.RWMutex
	connected     bool
	lastError     error
	reconnectChan chan struct{}
	
	// Metrics
	ledgersReceived uint64
	bytesReceived   uint64
	lastLedger      uint32
	startTime       time.Time
}

// NewDataSourceClient creates a new client for stellar-live-source-datalake
func NewDataSourceClient(cfg *config.Config, logger *logging.ComponentLogger) *DataSourceClient {
	return &DataSourceClient{
		config:        cfg,
		logger:        logger,
		reconnectChan: make(chan struct{}, 1),
		startTime:     time.Now(),
	}
}

// Connect establishes connection to the data source
func (c *DataSourceClient) Connect(ctx context.Context) error {
	c.logger.Info().
		Str("endpoint", c.config.SourceEndpoint).
		Msg("Connecting to stellar-live-source-datalake")
	
	// Set up connection with timeout
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	conn, err := grpc.DialContext(dialCtx, c.config.SourceEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		c.setConnectionState(false, err)
		return fmt.Errorf("failed to connect to data source: %w", err)
	}
	
	c.conn = conn
	c.client = rawledger.NewRawLedgerServiceClient(conn)
	c.setConnectionState(true, nil)
	
	c.logger.Info().
		Str("endpoint", c.config.SourceEndpoint).
		Msg("Successfully connected to data source")
	
	return nil
}

// StreamLedgers starts streaming raw ledgers from the data source
func (c *DataSourceClient) StreamLedgers(ctx context.Context, startLedger uint32, handler LedgerHandler) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return fmt.Errorf("not connected to data source")
	}
	client := c.client
	c.mu.RUnlock()
	
	// Create stream request
	req := &rawledger.StreamLedgersRequest{
		StartLedger: startLedger,
	}
	
	// Start streaming
	stream, err := client.StreamRawLedgers(ctx, req)
	if err != nil {
		c.setConnectionState(false, err)
		return fmt.Errorf("failed to start ledger stream: %w", err)
	}
	
	c.logger.Info().
		Uint32("start_ledger", startLedger).
		Msg("Started streaming ledgers")
	
	// Process stream
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			rawLedger, err := stream.Recv()
			if err == io.EOF {
				c.logger.Info().Msg("Ledger stream ended")
				return nil
			}
			if err != nil {
				// Check if it's a transient error
				if c.isRetriableError(err) {
					c.logger.Warn().
						Err(err).
						Msg("Transient error in ledger stream, will reconnect")
					c.setConnectionState(false, err)
					c.triggerReconnect()
					return err
				}
				return fmt.Errorf("error receiving ledger: %w", err)
			}
			
			// Update metrics
			c.updateMetrics(rawLedger)
			
			// Process the ledger
			if err := handler(ctx, rawLedger); err != nil {
				c.logger.Error().
					Err(err).
					Uint32("ledger", rawLedger.Sequence).
					Msg("Error processing ledger")
				// Continue processing other ledgers
			}
		}
	}
}

// LedgerHandler processes a raw ledger
type LedgerHandler func(ctx context.Context, ledger *rawledger.RawLedger) error

// Close closes the connection
func (c *DataSourceClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.conn != nil {
		c.connected = false
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		return err
	}
	
	return nil
}

// IsHealthy returns true if the client is connected and healthy
func (c *DataSourceClient) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// GetMetrics returns current ingestion metrics
func (c *DataSourceClient) GetMetrics() DataSourceMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	uptime := time.Since(c.startTime)
	rate := float64(c.ledgersReceived) / uptime.Seconds()
	
	return DataSourceMetrics{
		Connected:       c.connected,
		LedgersReceived: c.ledgersReceived,
		BytesReceived:   c.bytesReceived,
		LastLedger:      c.lastLedger,
		IngestRate:      rate,
		Uptime:          uptime,
		LastError:       c.lastError,
	}
}

// DataSourceMetrics contains metrics about data ingestion
type DataSourceMetrics struct {
	Connected       bool
	LedgersReceived uint64
	BytesReceived   uint64
	LastLedger      uint32
	IngestRate      float64
	Uptime          time.Duration
	LastError       error
}

// Reconnect attempts to reconnect to the data source
func (c *DataSourceClient) Reconnect(ctx context.Context) error {
	c.logger.Info().Msg("Attempting to reconnect to data source")
	
	// Close existing connection
	c.Close()
	
	// Exponential backoff for reconnection
	backoff := time.Second
	maxBackoff := time.Minute
	
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			c.logger.Debug().
				Int("attempt", attempt).
				Dur("backoff", backoff).
				Msg("Reconnection attempt")
			
			if err := c.Connect(ctx); err != nil {
				c.logger.Warn().
					Err(err).
					Int("attempt", attempt).
					Msg("Reconnection failed")
				
				// Increase backoff
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			
			c.logger.Info().
				Int("attempts", attempt).
				Msg("Successfully reconnected to data source")
			return nil
		}
	}
}

// Private helper methods

func (c *DataSourceClient) setConnectionState(connected bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
	c.lastError = err
}

func (c *DataSourceClient) updateMetrics(ledger *rawledger.RawLedger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ledgersReceived++
	c.bytesReceived += uint64(len(ledger.LedgerCloseMetaXdr))
	c.lastLedger = ledger.Sequence
}

func (c *DataSourceClient) isRetriableError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

func (c *DataSourceClient) triggerReconnect() {
	select {
	case c.reconnectChan <- struct{}{}:
	default:
		// Channel already has a signal
	}
}