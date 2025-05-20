package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	// Import the generated protobuf code package for the service WE PROVIDE
	jsonrpcservice "github.com/withObsrvr/ledger-jsonrpc-processor/gen/ledger_jsonrpc_service"
	ledgerjsonrpc "github.com/withObsrvr/ledger-jsonrpc-processor/gen/ingest/processors/ledger_jsonrpc"
	// Import the generated protobuf code package for the service WE CONSUME
	rawledger "github.com/withObsrvr/ledger-jsonrpc-processor/gen/raw_ledger_service"

	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// LedgerJsonRpcMetrics tracks comprehensive metrics for the processor
type LedgerJsonRpcMetrics struct {
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

// NewLedgerJsonRpcMetrics creates a new metrics instance
func NewLedgerJsonRpcMetrics() *LedgerJsonRpcMetrics {
	return &LedgerJsonRpcMetrics{
		StartTime: time.Now(),
	}
}

// GetMetrics returns a copy of the current metrics
func (m *LedgerJsonRpcMetrics) GetMetrics() map[string]interface{} {
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
func (m *LedgerJsonRpcMetrics) RecordSuccess(ledgerSequence uint32, eventCount int, processingTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.SuccessCount++
	m.TotalProcessed++
	m.TotalEventsEmitted += int64(eventCount)
	m.LastProcessedLedger = ledgerSequence
	m.ProcessingLatency = processingTime
}

// RecordError updates metrics when an error occurs during processing
func (m *LedgerJsonRpcMetrics) RecordError(err error, ledgerSequence uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.ErrorCount++
	m.LastError = err
	m.LastErrorTime = time.Now()
	m.LastProcessedLedger = ledgerSequence
}

// LedgerJsonRpcServer implements the LedgerJsonRpcServiceServer interface
type LedgerJsonRpcServer struct {
	// Embed the unimplemented server type for forward compatibility
	jsonrpcservice.UnimplementedLedgerJsonRpcServiceServer

	processor       *LedgerJsonRpcProcessor // The core processor logic
	rawLedgerClient rawledger.RawLedgerServiceClient // gRPC client for the raw source service
	rawLedgerConn   *grpc.ClientConn                 // Connection to the raw source service
	logger          *zap.Logger                      // Structured logger
	metrics         *LedgerJsonRpcMetrics            // Metrics tracking
	
	// Configuration
	retentionWindowDays int    // How many days of history to retain
	networkPassphrase   string // Stellar network passphrase
}

// LedgerJsonRpcProcessor handles the core conversion from ledger data to JSON-RPC responses
type LedgerJsonRpcProcessor struct {
	networkPassphrase string  // Stellar network passphrase
	retentionDays     int     // How many days of history to keep
	bloomFpRate       float64 // Bloom filter false positive rate
	enableJsonXdr     bool    // Enable JSON XDR parsing optimization
	logger            *zap.Logger
}

// NewLedgerJsonRpcProcessor creates a processor with the given configuration
func NewLedgerJsonRpcProcessor(passphrase string, retentionDays int, bloomFpRate float64, enableJsonXdr bool, logger *zap.Logger) *LedgerJsonRpcProcessor {
	return &LedgerJsonRpcProcessor{
		networkPassphrase: passphrase,
		retentionDays:     retentionDays,
		bloomFpRate:       bloomFpRate,
		enableJsonXdr:     enableJsonXdr,
		logger:            logger,
	}
}

// NewLedgerJsonRpcServer creates a new instance of the processor server
func NewLedgerJsonRpcServer(passphrase string, sourceServiceAddr string, retentionDays int, bloomFpRate float64, enableJsonXdr bool) (*LedgerJsonRpcServer, error) {
	// Initialize zap logger with production configuration
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize zap logger: %w", err)
	}

	processor := NewLedgerJsonRpcProcessor(passphrase, retentionDays, bloomFpRate, enableJsonXdr, logger)

	logger.Info("connecting to raw ledger source",
		zap.String("source_address", sourceServiceAddr))

	// Set up a connection to the raw ledger source server.
	// Using insecure credentials for this example. Use secure credentials in production.
	conn, err := grpc.Dial(sourceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("did not connect to raw ledger source service at %s: %w", sourceServiceAddr, err)
	}
	logger.Info("successfully connected to raw ledger source service")

	client := rawledger.NewRawLedgerServiceClient(conn)

	return &LedgerJsonRpcServer{
		processor:          processor,
		rawLedgerClient:    client,
		rawLedgerConn:      conn,
		logger:             logger,
		metrics:            NewLedgerJsonRpcMetrics(),
		retentionWindowDays: retentionDays,
		networkPassphrase:  passphrase,
	}, nil
}

// Close cleans up resources, like the connection to the source service.
func (s *LedgerJsonRpcServer) Close() error {
	s.logger.Info("closing connection to raw ledger source service")
	if s.rawLedgerConn != nil {
		return s.rawLedgerConn.Close()
	}
	return nil
}

// GetMetrics returns a copy of the current server metrics
func (s *LedgerJsonRpcServer) GetMetrics() LedgerJsonRpcMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return *s.metrics
}

// ProcessLedger processes a raw ledger and returns a JSON-RPC response based on the requested method
func (p *LedgerJsonRpcProcessor) ProcessLedger(lcm xdr.LedgerCloseMeta, method string, params *structpb.Struct) (*ledgerjsonrpc.LedgerJsonRpcEvent, error) {
	// Start with the event shell
	event := &ledgerjsonrpc.LedgerJsonRpcEvent{
		RequestCtx: &ledgerjsonrpc.RequestContext{
			Id:     "auto", // This would normally come from the client
			Method: method,
			Params: params,
		},
		LedgerMeta: &ledgerjsonrpc.LedgerMeta{
			Sequence: lcm.LedgerSequence(),
			ClosedAt: timestamppb.Now(), // This would be extracted from the ledger
			Hash:     "hash_placeholder", // This would be extracted from the ledger
		},
		ProcessedAt: timestamppb.Now(),
	}

	// The response structure always starts with these fields
	response := &ledgerjsonrpc.JsonRpcResponse{
		Jsonrpc: "2.0",
		Id:      "auto", // This would normally match the request
	}

	// Process according to method
	switch method {
	case "getLedgers":
		// Implementation for getLedgers method
		result, err := p.handleGetLedgers(lcm, params)
		if err != nil {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
				Error: &ledgerjsonrpc.JsonRpcError{
					Code:    -32603, // Internal error
					Message: "Failed to process getLedgers: " + err.Error(),
				},
			}
		} else {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Result{
				Result: result,
			}
		}

	case "getTransactions":
		// Implementation for getTransactions method
		result, err := p.handleGetTransactions(lcm, params)
		if err != nil {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
				Error: &ledgerjsonrpc.JsonRpcError{
					Code:    -32603, // Internal error
					Message: "Failed to process getTransactions: " + err.Error(),
				},
			}
		} else {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Result{
				Result: result,
			}
		}

	// Additional methods would be implemented here
	default:
		response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
			Error: &ledgerjsonrpc.JsonRpcError{
				Code:    -32601, // Method not found
				Message: "Method not found: " + method,
			},
		}
	}

	event.Response = response
	return event, nil
}

// handleGetLedgers implements the getLedgers JSON-RPC method
func (p *LedgerJsonRpcProcessor) handleGetLedgers(lcm xdr.LedgerCloseMeta, params *structpb.Struct) (*structpb.Struct, error) {
	// This would implement the full getLedgers method logic
	// For demonstration, returning a skeleton response
	result, err := structpb.NewStruct(map[string]interface{}{
		"ledgers": []interface{}{
			map[string]interface{}{
				"hash":            "example_hash",
				"sequence":        lcm.LedgerSequence(),
				"ledgerCloseTime": time.Now().Unix(),
				"headerXdr":       "base64_encoded_xdr",
				"metadataXdr":     "base64_encoded_xdr",
			},
		},
		"latestLedger":          lcm.LedgerSequence(),
		"latestLedgerCloseTime": time.Now().Unix(),
		"oldestLedger":          lcm.LedgerSequence() - 1000, // Example
		"oldestLedgerCloseTime": time.Now().Add(-time.Hour).Unix(),
		"cursor":                "example_cursor",
	})
	
	return result, err
}

// handleGetTransactions implements the getTransactions JSON-RPC method
func (p *LedgerJsonRpcProcessor) handleGetTransactions(lcm xdr.LedgerCloseMeta, params *structpb.Struct) (*structpb.Struct, error) {
	// This would implement the full getTransactions method logic
	// For demonstration, returning a skeleton response
	result, err := structpb.NewStruct(map[string]interface{}{
		"transactions": []interface{}{
			map[string]interface{}{
				"status":           "SUCCESS",
				"applicationOrder": 1,
				"feeBump":          false,
				"envelopeXdr":      "base64_encoded_xdr",
				"resultXdr":        "base64_encoded_xdr",
				"resultMetaXdr":    "base64_encoded_xdr",
				"ledger":           lcm.LedgerSequence(),
				"createdAt":        time.Now().Unix(),
			},
		},
		"latestLedger":                lcm.LedgerSequence(),
		"latestLedgerCloseTimestamp":  time.Now().Unix(),
		"oldestLedger":                lcm.LedgerSequence() - 1000, // Example
		"oldestLedgerCloseTimestamp":  time.Now().Add(-time.Hour).Unix(),
		"cursor":                      "example_cursor",
	})
	
	return result, err
}

// GetJsonRpcResponses implements the gRPC GetJsonRpcResponses method
func (s *LedgerJsonRpcServer) GetJsonRpcResponses(req *jsonrpcservice.GetJsonRpcRequest, stream jsonrpcservice.LedgerJsonRpcService_GetJsonRpcResponsesServer) error {
	// The context of the incoming stream from the *consumer*
	ctx := stream.Context()
	logger := s.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.String("method", req.Method),
	)
	logger.Info("received GetJsonRpcResponses request")

	// Convert the string map to a structpb
	paramsMap := make(map[string]interface{})
	for k, v := range req.Params {
		paramsMap[k] = v
	}
	
	params, err := structpb.NewStruct(paramsMap)
	if err != nil {
		logger.Error("failed to convert params to structpb",
			zap.Error(err))
		return status.Errorf(codes.InvalidArgument, "invalid params: %v", err)
	}

	// Create a request for the raw ledger source service
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
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
		s.metrics.RecordError(err, 0)
		return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
	}
	logger.Info("successfully initiated raw ledger stream")

	// Loop indefinitely, receiving raw ledgers from the source stream
	for {
		// Check if the consumer's context is cancelled first
		select {
		case <-ctx.Done():
			logger.Info("consumer context cancelled, stopping JSON-RPC event stream",
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
			s.metrics.RecordError(err, 0)
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
			s.metrics.RecordError(err, 0)
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
			s.metrics.RecordError(err, rawLedgerMsg.Sequence)
			// Decide how to handle - skip ledger or terminate? Terminating is safer.
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to unmarshal ledger %d XDR: %v", rawLedgerMsg.Sequence, err)
		}

		// Track start time before processing events
		processingStart := time.Now()
		
		// Process the ledger using the JSON-RPC processor
		jsonRpcEvent, err := s.processor.ProcessLedger(lcm, req.Method, params)
		
		// Calculate processing time
		processingTime := time.Since(processingStart)
		
		if err != nil {
			ledgerLogger.Error("failed to process JSON-RPC event",
				zap.Error(err))
			// Update error metrics
			s.metrics.RecordError(err, lcm.LedgerSequence())
			// Terminate if processing fails
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to process JSON-RPC event for ledger %d: %v", lcm.LedgerSequence(), err)
		}

		// Send the JSON-RPC event to the consumer
		if err := stream.Send(jsonRpcEvent); err != nil {
			ledgerLogger.Error("failed to send JSON-RPC event to consumer",
				zap.Error(err))
			// Update error metrics
			s.metrics.RecordError(err, lcm.LedgerSequence())
			// Consumer likely disconnected. Cancel upstream source stream.
			cancelSourceStream()
			return status.Errorf(codes.Unavailable, "failed to send JSON-RPC event to consumer: %v", err)
		}
		
		// Update metrics on successful processing
		s.metrics.RecordSuccess(lcm.LedgerSequence(), 1, processingTime)
		
		ledgerLogger.Info("finished processing ledger",
			zap.Duration("processing_time", processingTime))
	}
	// This part is theoretically unreachable
}

// GetJsonRpcMethod implements the gRPC GetJsonRpcMethod method
func (s *LedgerJsonRpcServer) GetJsonRpcMethod(ctx context.Context, req *jsonrpcservice.JsonRpcMethodRequest) (*ledgerjsonrpc.JsonRpcResponse, error) {
	// Implementation of a single method execution
	// This would handle one-off method calls for specific requests
	
	logger := s.logger.With(
		zap.String("method", req.Method),
		zap.String("id", req.Id),
	)
	logger.Info("received GetJsonRpcMethod request")
	
	// The response structure always starts with these fields
	response := &ledgerjsonrpc.JsonRpcResponse{
		Jsonrpc: "2.0",
		Id:      req.Id,
	}
	
	// Parse params if needed
	var paramsStruct *structpb.Struct
	
	if len(req.ParamsJson) > 0 {
		// In a real implementation, we would parse the JSON here
		// For this stub, we'll just use an empty struct
		var err error
		paramsStruct, err = structpb.NewStruct(map[string]interface{}{})
		if err != nil {
			logger.Error("failed to create empty struct",
				zap.Error(err))
		} else {
			logger.Debug("created params struct", zap.Any("params", paramsStruct))
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
				Error: &ledgerjsonrpc.JsonRpcError{
					Code:    -32602,
					Message: "Invalid params: " + err.Error(),
				},
			}
			return response, nil
		}
		
		// Normally we'd parse the JSON here
		// For this example, we'll just use empty params
	}
	
	// Process according to method
	switch req.Method {
	case "getHealth":
		// Example implementation of getHealth method
		metrics := s.GetMetrics()
		result, err := structpb.NewStruct(map[string]interface{}{
			"status":              "healthy",
			"latestLedger":        metrics.LastProcessedLedger,
			"oldestLedger":        metrics.LastProcessedLedger - 10000, // Example
			"ledgerRetentionWindow": s.retentionWindowDays,
		})
		
		if err != nil {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
				Error: &ledgerjsonrpc.JsonRpcError{
					Code:    -32603, // Internal error
					Message: "Failed to process getHealth: " + err.Error(),
				},
			}
		} else {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Result{
				Result: result,
			}
		}
		
	case "getNetwork":
		// Example implementation of getNetwork method
		result, err := structpb.NewStruct(map[string]interface{}{
			"passphrase":     s.networkPassphrase,
			"protocolVersion": 20, // Example
		})
		
		if err != nil {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
				Error: &ledgerjsonrpc.JsonRpcError{
					Code:    -32603, // Internal error
					Message: "Failed to process getNetwork: " + err.Error(),
				},
			}
		} else {
			response.Response = &ledgerjsonrpc.JsonRpcResponse_Result{
				Result: result,
			}
		}
		
	default:
		response.Response = &ledgerjsonrpc.JsonRpcResponse_Error{
			Error: &ledgerjsonrpc.JsonRpcError{
				Code:    -32601, // Method not found
				Message: "Method not found: " + req.Method,
			},
		}
	}
	
	return response, nil
}