package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	pb "github.com/withObsrvr/ttp-processor-demo/contract-data-processor/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ControlServer implements the gRPC control plane
type ControlServer struct {
	pb.UnimplementedControlServiceServer
	
	config   *config.Config
	logger   *logging.ComponentLogger
	
	// Session management
	mu       sync.RWMutex
	sessions map[string]*ProcessingSession
	
	// Components
	dataSource   *DataSourceClient
	coordinator  *ProcessingCoordinator
}

// ProcessingSession represents an active processing session
type ProcessingSession struct {
	ID             string
	Status         pb.GetStatusResponse_Status
	StartLedger    uint32
	EndLedger      uint32
	CurrentLedger  uint32
	BatchSize      uint32
	StartTime      time.Time
	LastUpdateTime time.Time
	ErrorMessage   string
	
	// Metrics
	EntriesProcessed uint64
	EntriesSkipped   uint64
	BatchesCreated   uint64
	BytesProcessed   uint64
	
	// Control
	CancelFunc context.CancelFunc
}

// NewControlServer creates a new control server
func NewControlServer(cfg *config.Config, logger *logging.ComponentLogger, dataSource *DataSourceClient, coordinator *ProcessingCoordinator) *ControlServer {
	return &ControlServer{
		config:      cfg,
		logger:      logger,
		sessions:    make(map[string]*ProcessingSession),
		dataSource:  dataSource,
		coordinator: coordinator,
	}
}

// StartProcessing starts a new processing session
func (s *ControlServer) StartProcessing(ctx context.Context, req *pb.StartProcessingRequest) (*pb.StartProcessingResponse, error) {
	s.logger.Info().
		Uint32("start_ledger", req.StartLedger).
		Uint32("end_ledger", req.EndLedger).
		Uint32("batch_size", req.BatchSize).
		Msg("Starting processing session")
	
	// Validate request
	if req.StartLedger == 0 {
		return nil, status.Error(codes.InvalidArgument, "start_ledger must be greater than 0")
	}
	
	if req.EndLedger != 0 && req.EndLedger < req.StartLedger {
		return nil, status.Error(codes.InvalidArgument, "end_ledger must be greater than start_ledger")
	}
	
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000 // Default batch size
	}
	
	// Create session
	sessionID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	
	session := &ProcessingSession{
		ID:             sessionID,
		Status:         pb.GetStatusResponse_STARTING,
		StartLedger:    req.StartLedger,
		EndLedger:      req.EndLedger,
		CurrentLedger:  req.StartLedger,
		BatchSize:      batchSize,
		StartTime:      time.Now(),
		LastUpdateTime: time.Now(),
		CancelFunc:     cancel,
	}
	
	// Store session
	s.mu.Lock()
	s.sessions[sessionID] = session
	s.mu.Unlock()
	
	// Start processing in background
	go s.runProcessingSession(ctx, session)
	
	return &pb.StartProcessingResponse{
		Success:   true,
		Message:   "Processing started successfully",
		SessionId: sessionID,
	}, nil
}

// StopProcessing stops an active processing session
func (s *ControlServer) StopProcessing(ctx context.Context, req *pb.StopProcessingRequest) (*pb.StopProcessingResponse, error) {
	s.logger.Info().
		Str("session_id", req.SessionId).
		Msg("Stopping processing session")
	
	s.mu.Lock()
	session, exists := s.sessions[req.SessionId]
	s.mu.Unlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	
	// Update status
	s.mu.Lock()
	session.Status = pb.GetStatusResponse_STOPPING
	session.LastUpdateTime = time.Now()
	s.mu.Unlock()
	
	// Cancel processing
	session.CancelFunc()
	
	return &pb.StopProcessingResponse{
		Success: true,
		Message: "Processing stop initiated",
	}, nil
}

// GetStatus returns the status of a processing session
func (s *ControlServer) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	s.mu.RLock()
	session, exists := s.sessions[req.SessionId]
	s.mu.RUnlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	
	return &pb.GetStatusResponse{
		Status:           session.Status,
		CurrentLedger:    session.CurrentLedger,
		TargetLedger:     session.EndLedger,
		EntriesProcessed: session.EntriesProcessed,
		ErrorMessage:     session.ErrorMessage,
	}, nil
}

// ConfigureFilters updates processing filters
func (s *ControlServer) ConfigureFilters(ctx context.Context, req *pb.ConfigureFiltersRequest) (*pb.ConfigureFiltersResponse, error) {
	s.logger.Info().
		Int("contract_ids", len(req.ContractIds)).
		Int("asset_codes", len(req.AssetCodes)).
		Int("asset_issuers", len(req.AssetIssuers)).
		Bool("include_deleted", req.IncludeDeleted).
		Msg("Configuring filters")
	
	// Update configuration
	s.mu.Lock()
	s.config.FilterContractIDs = req.ContractIds
	s.config.FilterAssetCodes = req.AssetCodes
	s.config.FilterAssetIssuers = req.AssetIssuers
	s.config.IncludeDeleted = req.IncludeDeleted
	s.mu.Unlock()
	
	// Update processor filters
	if err := s.coordinator.UpdateFilters(s.config); err != nil {
		return &pb.ConfigureFiltersResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to update filters: %v", err),
		}, nil
	}
	
	return &pb.ConfigureFiltersResponse{
		Success: true,
		Message: "Filters updated successfully",
	}, nil
}

// GetMetrics returns processing metrics
func (s *ControlServer) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	s.mu.RLock()
	session, exists := s.sessions[req.SessionId]
	s.mu.RUnlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	
	// Calculate processing rate
	duration := time.Since(session.StartTime).Seconds()
	processingRate := float64(session.EntriesProcessed) / duration
	
	return &pb.GetMetricsResponse{
		ContractsProcessed: session.EntriesProcessed,
		EntriesSkipped:     session.EntriesSkipped,
		BatchesCreated:     session.BatchesCreated,
		BytesProcessed:     session.BytesProcessed,
		ProcessingRate:     processingRate,
		LastUpdateTime:     session.LastUpdateTime.Unix(),
	}, nil
}

// runProcessingSession runs the processing loop for a session
func (s *ControlServer) runProcessingSession(ctx context.Context, session *ProcessingSession) {
	s.logger.Info().
		Str("session_id", session.ID).
		Uint32("start_ledger", session.StartLedger).
		Uint32("end_ledger", session.EndLedger).
		Msg("Starting processing session")
	
	// Update status
	s.updateSessionStatus(session, pb.GetStatusResponse_RUNNING, "")
	
	// Start processing
	err := s.coordinator.ProcessLedgers(ctx, session.StartLedger, session.EndLedger, func(metrics ProcessingMetrics) {
		// Update session metrics
		s.mu.Lock()
		session.CurrentLedger = metrics.CurrentLedger
		session.EntriesProcessed = metrics.EntriesProcessed
		session.EntriesSkipped = metrics.EntriesSkipped
		session.BatchesCreated = metrics.BatchesCreated
		session.BytesProcessed = metrics.BytesProcessed
		session.LastUpdateTime = time.Now()
		s.mu.Unlock()
	})
	
	if err != nil {
		if err == context.Canceled {
			s.updateSessionStatus(session, pb.GetStatusResponse_STOPPED, "Processing cancelled")
		} else {
			s.updateSessionStatus(session, pb.GetStatusResponse_ERROR, err.Error())
		}
	} else {
		s.updateSessionStatus(session, pb.GetStatusResponse_STOPPED, "Processing completed")
	}
	
	s.logger.Info().
		Str("session_id", session.ID).
		Uint64("entries_processed", session.EntriesProcessed).
		Msg("Processing session completed")
}

// updateSessionStatus updates the status of a session
func (s *ControlServer) updateSessionStatus(session *ProcessingSession, status pb.GetStatusResponse_Status, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	session.Status = status
	session.ErrorMessage = message
	session.LastUpdateTime = time.Now()
}

// ProcessingMetrics contains metrics from the processing coordinator
type ProcessingMetrics struct {
	CurrentLedger    uint32
	EntriesProcessed uint64
	EntriesSkipped   uint64
	BatchesCreated   uint64
	BytesProcessed   uint64
}