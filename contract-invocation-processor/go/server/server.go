package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"
)

// ProcessorMetrics tracks comprehensive metrics for the processor
type ProcessorMetrics struct {
	mu                      sync.RWMutex
	TotalProcessed          int64     `json:"total_processed"`
	TotalInvocations        int64     `json:"total_invocations"`
	SuccessfulInvocations   int64     `json:"successful_invocations"`
	FailedInvocations       int64     `json:"failed_invocations"`
	ContractCalls           int64     `json:"contract_calls"`
	CreateContracts         int64     `json:"create_contracts"`
	UploadWasms             int64     `json:"upload_wasms"`
	ArchiveRestorations     int64     `json:"archive_restorations"`
	ErrorCount              int64     `json:"error_count"`
	LastError               error     `json:"-"`
	LastErrorTime           time.Time `json:"last_error_time"`
	LastProcessedLedger     uint32    `json:"last_processed_ledger"`
	ProcessingLatency       time.Duration `json:"processing_latency"`
	TotalEventsEmitted      int64     `json:"total_events_emitted"`
	StartTime               time.Time `json:"start_time"`
}

// ContractInvocationServer implements the gRPC service
type ContractInvocationServer struct {
	pb.UnimplementedContractInvocationServiceServer
	logger              *zap.Logger
	sourceServiceAddr   string
	networkPassphrase   string
	metrics             *ProcessorMetrics
	flowctlController   *FlowctlController
}

// NewContractInvocationServer creates a new server instance
func NewContractInvocationServer() (*ContractInvocationServer, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	sourceServiceAddr := os.Getenv("SOURCE_SERVICE_ADDRESS")
	if sourceServiceAddr == "" {
		sourceServiceAddr = "localhost:50052"
	}

	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		networkPassphrase = "Public Global Stellar Network ; September 2015"
	}

	server := &ContractInvocationServer{
		logger:            logger,
		sourceServiceAddr: sourceServiceAddr,
		networkPassphrase: networkPassphrase,
		metrics: &ProcessorMetrics{
			StartTime: time.Now(),
		},
	}

	// Initialize flowctl integration if enabled
	if os.Getenv("ENABLE_FLOWCTL") == "true" {
		server.flowctlController = NewFlowctlController(logger)
	}

	logger.Info("Contract Invocation Server initialized",
		zap.String("source_service_addr", sourceServiceAddr),
		zap.String("network_passphrase", networkPassphrase),
		zap.Bool("flowctl_enabled", server.flowctlController != nil),
	)

	return server, nil
}

// GetContractInvocations implements the main gRPC service method
func (s *ContractInvocationServer) GetContractInvocations(
	req *pb.GetInvocationsRequest,
	stream pb.ContractInvocationService_GetContractInvocationsServer,
) error {
	// The context of the incoming stream from the consumer
	ctx := stream.Context()
	logger := s.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.Int("filter_count", s.countRequestFilters(req)),
	)
	logger.Info("received GetContractInvocations request")

	// Create ledger processor for this request
	ledgerProcessor := NewLedgerProcessor(s.logger, s.networkPassphrase, s)

	// Set up connection to raw ledger source service
	conn, rawLedgerClient, err := s.connectToRawLedgerSource(ctx)
	if err != nil {
		s.updateErrorMetrics(err, 0)
		return err
	}
	defer conn.Close()

	// Create request for the raw ledger source service
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
	}

	// Create context for outgoing call to source service
	sourceCtx, cancelSourceStream := context.WithCancel(ctx)
	defer cancelSourceStream()

	logger.Info("requesting raw ledger stream from source service")
	rawLedgerStream, err := rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
	if err != nil {
		logger.Error("failed to connect to raw ledger source", zap.Error(err))
		s.updateErrorMetrics(err, 0)
		return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
	}
	logger.Info("successfully initiated raw ledger stream")

	// Process ledgers in streaming loop
	for {
		// Check if consumer context is cancelled
		select {
		case <-ctx.Done():
			logger.Info("consumer context cancelled, stopping contract invocation stream",
				zap.Error(ctx.Err()))
			cancelSourceStream()
			return status.FromContextError(ctx.Err()).Err()
		default:
			// Continue if consumer is still connected
		}

		// Receive next raw ledger from source stream
		rawLedgerMsg, err := rawLedgerStream.Recv()
		if err == io.EOF {
			logger.Error("raw ledger source stream ended unexpectedly")
			s.updateErrorMetrics(err, 0)
			return status.Error(codes.Unavailable, "raw ledger source stream ended unexpectedly")
		}
		if err != nil {
			// Handle errors from source stream
			if status.Code(err) == codes.Canceled && ctx.Err() != nil {
				logger.Info("source stream cancelled due to consumer disconnection")
				return status.FromContextError(ctx.Err()).Err()
			}
			logger.Error("error receiving from raw ledger source stream", zap.Error(err))
			s.updateErrorMetrics(err, 0)
			return status.Errorf(codes.Internal, "error receiving data from raw ledger source: %v", err)
		}

		ledgerLogger := logger.With(zap.Uint32("ledger_sequence", rawLedgerMsg.Sequence))

		// Check if we need to stop based on endLedger
		if req.EndLedger > 0 && rawLedgerMsg.Sequence > req.EndLedger {
			ledgerLogger.Info("reached end ledger requested by consumer")
			cancelSourceStream()
			return nil
		}

		ledgerLogger.Debug("processing raw ledger for contract invocations")

		// Process ledger to extract contract invocations
		processingStart := time.Now()
		events, err := ledgerProcessor.ProcessLedger(ctx, rawLedgerMsg, req)
		processingTime := time.Since(processingStart)

		if err != nil {
			ledgerLogger.Error("failed to process contract invocations", zap.Error(err))
			s.updateErrorMetrics(err, rawLedgerMsg.Sequence)
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to process contract invocations for ledger %d: %v", rawLedgerMsg.Sequence, err)
		}

		// Stream each contract invocation event to consumer
		eventsSent := 0
		for i, event := range events {
			if err := stream.Send(event); err != nil {
				ledgerLogger.Error("failed to send contract invocation event to consumer",
					zap.Error(err),
					zap.Int("event_index", i))
				s.updateErrorMetrics(err, rawLedgerMsg.Sequence)
				cancelSourceStream()
				return status.Errorf(codes.Unavailable, "failed to send contract invocation event to consumer: %v", err)
			}
			eventsSent++
		}

		// Update metrics on successful processing
		s.updateSuccessMetrics(rawLedgerMsg.Sequence, processingTime, int64(eventsSent))

		ledgerLogger.Info("finished processing ledger",
			zap.Int("events_sent", eventsSent),
			zap.Duration("processing_time", processingTime))
	}
}

// GetMetrics returns current processor metrics
func (s *ContractInvocationServer) GetMetrics() ProcessorMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return *s.metrics
}

// StartHealthCheckServer starts the health check HTTP server
func (s *ContractInvocationServer) StartHealthCheckServer(port int) error {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		metrics := s.GetMetrics()
		
		status := "healthy"
		if metrics.ErrorCount > 0 && time.Since(metrics.LastErrorTime) < 5*time.Minute {
			status = "degraded"
		}

		uptime := time.Since(metrics.StartTime)

		response := map[string]interface{}{
			"status": status,
			"uptime": uptime.String(),
			"metrics": map[string]interface{}{
				"total_processed":        metrics.TotalProcessed,
				"total_invocations":      metrics.TotalInvocations,
				"successful_invocations": metrics.SuccessfulInvocations,
				"failed_invocations":     metrics.FailedInvocations,
				"contract_calls":         metrics.ContractCalls,
				"create_contracts":       metrics.CreateContracts,
				"upload_wasms":           metrics.UploadWasms,
				"archive_restorations":   metrics.ArchiveRestorations,
				"error_count":            metrics.ErrorCount,
				"last_processed_ledger":  metrics.LastProcessedLedger,
				"processing_latency":     metrics.ProcessingLatency.String(),
				"total_events_emitted":   metrics.TotalEventsEmitted,
			},
			"configuration": map[string]interface{}{
				"source_service_addr": s.sourceServiceAddr,
				"network_passphrase":  s.networkPassphrase,
				"flowctl_enabled":     s.flowctlController != nil,
			},
			"phase": "Phase 1 - Project Structure Complete",
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%d", port)
	s.logger.Info("starting health check server", zap.String("address", addr))
	return http.ListenAndServe(addr, nil)
}

// connectToRawLedgerSource establishes connection to the raw ledger source service
func (s *ContractInvocationServer) connectToRawLedgerSource(ctx context.Context) (*grpc.ClientConn, rawledger.RawLedgerServiceClient, error) {
	s.logger.Info("connecting to raw ledger source",
		zap.String("source_address", s.sourceServiceAddr))

	// Set up connection with context for timeout handling
	conn, err := grpc.DialContext(ctx, s.sourceServiceAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "failed to connect to raw ledger source at %s: %v", s.sourceServiceAddr, err)
	}

	s.logger.Info("successfully connected to raw ledger source service")
	client := rawledger.NewRawLedgerServiceClient(conn)
	return conn, client, nil
}

// updateErrorMetrics updates metrics when an error occurs
func (s *ContractInvocationServer) updateErrorMetrics(err error, ledgerSequence uint32) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	
	s.metrics.ErrorCount++
	s.metrics.LastError = err
	s.metrics.LastErrorTime = time.Now()
	if ledgerSequence > 0 {
		s.metrics.LastProcessedLedger = ledgerSequence
	}
}

// updateSuccessMetrics updates metrics on successful processing
func (s *ContractInvocationServer) updateSuccessMetrics(ledgerSequence uint32, processingTime time.Duration, eventCount int64) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	
	s.metrics.TotalProcessed++
	s.metrics.TotalEventsEmitted += eventCount
	s.metrics.LastProcessedLedger = ledgerSequence
	s.metrics.ProcessingLatency = processingTime
	
	// Update invocation type metrics based on eventCount
	s.metrics.TotalInvocations += eventCount
	if eventCount > 0 {
		s.metrics.SuccessfulInvocations += eventCount
	}
}

// countRequestFilters counts the number of active filters in the request
func (s *ContractInvocationServer) countRequestFilters(req *pb.GetInvocationsRequest) int {
	count := 0
	if len(req.ContractIds) > 0 {
		count++
	}
	if len(req.FunctionNames) > 0 {
		count++
	}
	if len(req.InvokingAccounts) > 0 {
		count++
	}
	if req.SuccessfulOnly {
		count++
	}
	if req.TypeFilter != pb.InvocationTypeFilter_INVOCATION_TYPE_FILTER_ALL {
		count++
	}
	if req.ContentFilter != nil {
		count++
	}
	if req.TimeFilter != nil {
		count++
	}
	return count
}