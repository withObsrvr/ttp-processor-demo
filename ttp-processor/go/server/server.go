package server

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	// Import the generated protobuf code package for the service WE PROVIDE
	eventservice "github.com/withObsrvr/ttp-processor/gen/event_service"
	// Import the generated protobuf code package for the service WE CONSUME
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"

	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ProcessorMetrics tracks comprehensive metrics for the TTP processor
type ProcessorMetrics struct {
	mu sync.RWMutex
	// Core metrics
	SuccessCount        int64
	ErrorCount          int64
	TotalProcessed      int64
	TotalEventsEmitted  int64
	LastError           error
	LastErrorTime       time.Time
	StartTime           time.Time
	LastProcessedLedger uint32
	ProcessingLatency   time.Duration
}

// NewProcessorMetrics creates a new metrics instance
func NewProcessorMetrics() *ProcessorMetrics {
	return &ProcessorMetrics{
		StartTime: time.Now(),
	}
}

// GetMetrics returns a copy of the current metrics
func (m *ProcessorMetrics) GetMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make(map[string]interface{})
	result["success_count"] = m.SuccessCount
	result["error_count"] = m.ErrorCount
	result["total_processed"] = m.TotalProcessed
	result["total_events_emitted"] = m.TotalEventsEmitted
	result["uptime_seconds"] = time.Since(m.StartTime).Seconds()
	result["last_processed_ledger"] = m.LastProcessedLedger
	result["processing_latency_ms"] = m.ProcessingLatency.Milliseconds()
	
	if m.LastError != nil {
		result["last_error"] = m.LastError.Error()
		result["last_error_time"] = m.LastErrorTime.Format(time.RFC3339)
		result["seconds_since_last_error"] = time.Since(m.LastErrorTime).Seconds()
	}
	
	return result
}

// RecordSuccess updates metrics for successful ledger processing
func (m *ProcessorMetrics) RecordSuccess(ledgerSequence uint32, eventCount int, processingTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.SuccessCount++
	m.TotalProcessed++
	m.TotalEventsEmitted += int64(eventCount)
	m.LastProcessedLedger = ledgerSequence
	m.ProcessingLatency = processingTime
}

// RecordError updates metrics when an error occurs during processing
func (m *ProcessorMetrics) RecordError(err error, ledgerSequence uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.ErrorCount++
	m.LastError = err
	m.LastErrorTime = time.Now()
	m.LastProcessedLedger = ledgerSequence
}

// EventServer implements the EventServiceServer interface
type EventServer struct {
	// Embed the unimplemented server type for forward compatibility
	eventservice.UnimplementedEventServiceServer

	processor       *token_transfer.EventsProcessor  // The core TTP logic
	rawLedgerClient rawledger.RawLedgerServiceClient // gRPC client for the raw source service
	rawLedgerConn   *grpc.ClientConn                 // Connection to the raw source service
	logger          *zap.Logger                      // Structured logger
	metrics         *ProcessorMetrics                // Metrics tracking
}

// NewEventServer creates a new instance of the TTP processor server
func NewEventServer(passphrase string, sourceServiceAddr string) (*EventServer, error) {
	// Initialize zap logger with production configuration
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize zap logger")
	}

	processor := token_transfer.NewEventsProcessor(passphrase)

	logger.Info("connecting to raw ledger source",
		zap.String("source_address", sourceServiceAddr))

	// Set up a connection to the raw ledger source server.
	// Using insecure credentials for this example. Use secure credentials in production.
	conn, err := grpc.Dial(sourceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "did not connect to raw ledger source service at %s", sourceServiceAddr)
	}
	logger.Info("successfully connected to raw ledger source service")

	client := rawledger.NewRawLedgerServiceClient(conn)

	return &EventServer{
		processor:       processor,
		rawLedgerClient: client,
		rawLedgerConn:   conn,
		logger:          logger,
		metrics:         NewProcessorMetrics(),
	}, nil
}

// Close cleans up resources, like the connection to the source service.
func (s *EventServer) Close() error {
	s.logger.Info("closing connection to raw ledger source service")
	if s.rawLedgerConn != nil {
		return s.rawLedgerConn.Close()
	}
	return nil
}

// GetMetrics returns a copy of the current server metrics
func (s *EventServer) GetMetrics() ProcessorMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return *s.metrics
}

// matchesAccountFilter checks if a TTP event matches the account filter criteria
func matchesAccountFilter(event *token_transfer.TokenTransferEvent, accountIDs []string) bool {
	// If no account filter is specified, return all events
	if len(accountIDs) == 0 {
		return true
	}
	
	// Check contract address
	if event.Meta != nil && event.Meta.ContractAddress != "" {
		for _, accountID := range accountIDs {
			if event.Meta.ContractAddress == accountID {
				return true
			}
		}
	}
	
	// Check event-specific account fields
	switch e := event.Event.(type) {
	case *token_transfer.TokenTransferEvent_Transfer:
		if e.Transfer != nil {
			for _, accountID := range accountIDs {
				if e.Transfer.From == accountID || e.Transfer.To == accountID {
					return true
				}
			}
		}
	case *token_transfer.TokenTransferEvent_Mint:
		if e.Mint != nil {
			for _, accountID := range accountIDs {
				if e.Mint.To == accountID {
					return true
				}
			}
		}
	case *token_transfer.TokenTransferEvent_Burn:
		if e.Burn != nil {
			for _, accountID := range accountIDs {
				if e.Burn.From == accountID {
					return true
				}
			}
		}
	case *token_transfer.TokenTransferEvent_Clawback:
		if e.Clawback != nil {
			for _, accountID := range accountIDs {
				if e.Clawback.From == accountID {
					return true
				}
			}
		}
	case *token_transfer.TokenTransferEvent_Fee:
		if e.Fee != nil {
			for _, accountID := range accountIDs {
				if e.Fee.From == accountID {
					return true
				}
			}
		}
	}
	
	return false
}

// GetTTPEvents implements the gRPC GetTTPEvents method
func (s *EventServer) GetTTPEvents(req *eventservice.GetEventsRequest, stream eventservice.EventService_GetTTPEventsServer) error {
	// The context of the incoming stream from the *consumer* (e.g., Minecraft mod)
	ctx := stream.Context()
	logger := s.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.Strings("account_ids", req.AccountIds),
	)
	logger.Info("received GetTTPEvents request")

	// Create a request for the raw ledger source service
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
		EndLedger:   req.EndLedger,
	}

	// Create a new context for the *outgoing* call to the source service.
	// This allows us to cancel the call to the source if the downstream consumer disconnects.
	sourceCtx, cancelSourceStream := context.WithCancel(ctx)
	defer cancelSourceStream() // Ensure cancellation propagates if this function returns

	logger.Info("requesting raw ledger stream from source service")
	// Call the source service to get the stream of raw ledgers
	rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
	if err != nil {
		logger.Error("failed to connect to raw ledger source",
			zap.Error(err))
		// Update error metrics
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.LastError = err
		s.metrics.LastErrorTime = time.Now()
		s.metrics.mu.Unlock()
		return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
	}
	logger.Info("successfully initiated raw ledger stream")

	// Loop indefinitely, receiving raw ledgers from the source stream
	for {
		// Check if the consumer's context is cancelled first
		select {
		case <-ctx.Done():
			logger.Info("consumer context cancelled, stopping TTP event stream",
				zap.Error(ctx.Err()))
			// Cancel the upstream call to the source service
			cancelSourceStream()
			return status.FromContextError(ctx.Err()).Err()
		default:
			// Continue if consumer is still connected
		}

		// Receive the next raw ledger message from the source service stream
		rawLedgerMsg, err := rawLedgerStream.Recv()
		if err == io.EOF {
			// Source stream ended unexpectedly (should not happen for live source)
			logger.Error("raw ledger source stream ended unexpectedly")
			// Update error metrics
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.mu.Unlock()
			return status.Error(codes.Unavailable, "raw ledger source stream ended unexpectedly")
		}
		if err != nil {
			// Handle errors from the source stream (e.g., source service crashed)
			// Check if the error is due to the cancellation we initiated
			if status.Code(err) == codes.Canceled && ctx.Err() != nil {
				logger.Info("source stream cancelled due to consumer disconnection")
				return status.FromContextError(ctx.Err()).Err() // Return consumer's error
			}
			logger.Error("error receiving from raw ledger source stream",
				zap.Error(err))
			// Update error metrics
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.mu.Unlock()
			return status.Errorf(codes.Internal, "error receiving data from raw ledger source: %v", err)
		}

		ledgerLogger := logger.With(zap.Uint32("ledger_sequence", rawLedgerMsg.Sequence))

		// Check if we need to stop based on the consumer's requested endLedger
		// A non-zero endLedger indicates a bounded request.
		if req.EndLedger > 0 && rawLedgerMsg.Sequence > req.EndLedger {
			ledgerLogger.Info("reached end ledger requested by consumer")
			cancelSourceStream() // No need to get more from source
			return nil           // Successful completion of bounded stream
		}

		ledgerLogger.Debug("processing raw ledger from source")

		// Unmarshal the raw XDR bytes into a LedgerCloseMeta object
		var lcm xdr.LedgerCloseMeta
		_, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
		if err != nil {
			ledgerLogger.Error("failed to unmarshal XDR",
				zap.Error(err))
			// Update error metrics
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.LastProcessedLedger = rawLedgerMsg.Sequence
			s.metrics.mu.Unlock()
			// Decide how to handle - skip ledger or terminate? Terminating is safer.
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to unmarshal ledger %d XDR: %v", rawLedgerMsg.Sequence, err)
		}

		// Track start time before processing events
		processingStart := time.Now()
		
		// Now use the TTP processor to get events from this ledger
		events, err := s.processor.EventsFromLedger(lcm)
		
		// Calculate processing time
		processingTime := time.Since(processingStart)
		
		if err != nil {
			ledgerLogger.Error("failed to process TTP events",
				zap.Error(err))
			// Update error metrics
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.LastError = err
			s.metrics.LastErrorTime = time.Now()
			s.metrics.LastProcessedLedger = lcm.LedgerSequence()
			s.metrics.mu.Unlock()
			// Terminate if processing fails
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to process TTP events for ledger %d: %v", lcm.LedgerSequence(), err)
		}

		// Stream each generated TTP event to the *consumer*, applying account filter
		eventsSent := 0
		eventsFiltered := 0
		for i := range events {
			ttpEvent := events[i] // Create loop variable copy
			
			// Apply account filter
			if !matchesAccountFilter(ttpEvent, req.AccountIds) {
				eventsFiltered++
				continue
			}
			
			if err := stream.Send(ttpEvent); err != nil {
				ledgerLogger.Error("failed to send TTP event to consumer",
					zap.Error(err),
					zap.Int("event_index", i))
				// Update error metrics
				s.metrics.mu.Lock()
				s.metrics.ErrorCount++
				s.metrics.LastError = err
				s.metrics.LastErrorTime = time.Now()
				s.metrics.LastProcessedLedger = lcm.LedgerSequence()
				s.metrics.mu.Unlock()
				// Consumer likely disconnected. Cancel upstream source stream.
				cancelSourceStream()
				return status.Errorf(codes.Unavailable, "failed to send TTP event to consumer: %v", err)
			}
			eventsSent++
		}
		
		// Update metrics on successful processing
		s.metrics.mu.Lock()
		s.metrics.SuccessCount++
		s.metrics.TotalProcessed++
		s.metrics.LastProcessedLedger = lcm.LedgerSequence()
		s.metrics.ProcessingLatency = processingTime
		s.metrics.TotalEventsEmitted += int64(eventsSent)
		s.metrics.mu.Unlock()
		
		ledgerLogger.Info("finished processing ledger",
			zap.Int("events_sent", eventsSent),
			zap.Int("events_filtered", eventsFiltered),
			zap.Int("total_events", len(events)),
			zap.Duration("processing_time", processingTime))
	}
	// This part is theoretically unreachable
}
