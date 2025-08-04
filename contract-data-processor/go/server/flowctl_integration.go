package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/withObsrvr/flowctl/proto"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FlowctlController manages integration with the flowctl control plane
type FlowctlController struct {
	config         *config.Config
	logger         *logging.ComponentLogger
	conn           *grpc.ClientConn
	client         proto.ControlPlaneClient
	serviceID      string
	
	// Metrics tracking
	metricsLock    sync.RWMutex
	lastMetrics    FlowctlMetrics
	
	// Lifecycle
	cancelFunc     context.CancelFunc
	wg             sync.WaitGroup
}

// FlowctlMetrics contains metrics to report to flowctl
type FlowctlMetrics struct {
	ContractsProcessed uint64
	EntriesSkipped     uint64
	BatchesCreated     uint64
	BytesProcessed     uint64
	ProcessingRate     float64
	CurrentLedger      uint32
	ErrorCount         uint64
	LastUpdateTime     time.Time
}

// NewFlowctlController creates a new flowctl controller
func NewFlowctlController(cfg *config.Config, logger *logging.ComponentLogger) *FlowctlController {
	return &FlowctlController{
		config: cfg,
		logger: logger,
		lastMetrics: FlowctlMetrics{
			LastUpdateTime: time.Now(),
		},
	}
}

// Start starts the flowctl integration
func (fc *FlowctlController) Start(ctx context.Context) error {
	if !fc.config.FlowctlEnabled {
		fc.logger.Info().Msg("Flowctl integration disabled")
		return nil
	}
	
	fc.logger.Info().
		Str("endpoint", fc.config.FlowctlEndpoint).
		Msg("Starting flowctl integration")
	
	// Create context for this controller
	ctx, cancel := context.WithCancel(ctx)
	fc.cancelFunc = cancel
	
	// Connect to flowctl
	if err := fc.connect(); err != nil {
		return fmt.Errorf("failed to connect to flowctl: %w", err)
	}
	
	// Register with control plane
	if err := fc.register(ctx); err != nil {
		fc.Close()
		return fmt.Errorf("failed to register with flowctl: %w", err)
	}
	
	// Start heartbeat loop
	fc.wg.Add(1)
	go func() {
		defer fc.wg.Done()
		fc.heartbeatLoop(ctx)
	}()
	
	return nil
}

// connect establishes connection to flowctl control plane
func (fc *FlowctlController) connect() error {
	conn, err := grpc.Dial(
		fc.config.FlowctlEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to dial flowctl: %w", err)
	}
	
	fc.conn = conn
	fc.client = proto.NewControlPlaneClient(conn)
	
	fc.logger.Info().
		Str("endpoint", fc.config.FlowctlEndpoint).
		Msg("Connected to flowctl control plane")
	
	return nil
}

// register registers the service with flowctl
func (fc *FlowctlController) register(ctx context.Context) error {
	req := &proto.RegisterServiceRequest{
		ServiceName:    fc.config.ServiceName,
		ServiceType:    proto.ServiceType_PROCESSOR,
		ServiceVersion: fc.config.ServiceVersion,
		InputEventTypes: []string{
			"raw_ledger_service.RawLedgerChunk",
			"stellar.LedgerCloseMeta",
		},
		OutputEventTypes: []string{
			"contract_data.ArrowBatch",
			"stellar.ContractDataEntry",
		},
		HealthEndpoint: fmt.Sprintf("http://localhost:%d/health", fc.config.HealthPort),
		Metadata: map[string]string{
			"processor_type":     "contract_data",
			"network_passphrase": fc.config.NetworkPassphrase,
			"grpc_address":       fc.config.GRPCAddress,
			"flight_address":     fc.config.FlightAddress,
			"batch_size":         fmt.Sprintf("%d", fc.config.BatchSize),
			"worker_count":       fmt.Sprintf("%d", fc.config.WorkerCount),
		},
	}
	
	// Add filter information to metadata
	if len(fc.config.FilterContractIDs) > 0 {
		req.Metadata["filter_contracts"] = fmt.Sprintf("%d", len(fc.config.FilterContractIDs))
	}
	if len(fc.config.FilterAssetCodes) > 0 {
		req.Metadata["filter_assets"] = fmt.Sprintf("%d", len(fc.config.FilterAssetCodes))
	}
	
	resp, err := fc.client.RegisterService(ctx, req)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	
	fc.serviceID = resp.ServiceId
	
	fc.logger.Info().
		Str("service_id", fc.serviceID).
		Bool("success", resp.Success).
		Str("message", resp.Message).
		Msg("Registered with flowctl control plane")
	
	return nil
}

// heartbeatLoop sends periodic heartbeats to flowctl
func (fc *FlowctlController) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(fc.config.FlowctlHeartbeatInterval)
	defer ticker.Stop()
	
	// Send initial heartbeat
	fc.sendHeartbeat(ctx)
	
	for {
		select {
		case <-ticker.C:
			fc.sendHeartbeat(ctx)
			
		case <-ctx.Done():
			fc.logger.Info().Msg("Stopping flowctl heartbeat loop")
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat to flowctl
func (fc *FlowctlController) sendHeartbeat(ctx context.Context) {
	fc.metricsLock.RLock()
	metrics := fc.lastMetrics
	fc.metricsLock.RUnlock()
	
	// Calculate rates
	uptime := time.Since(metrics.LastUpdateTime).Seconds()
	if uptime == 0 {
		uptime = 1
	}
	
	req := &proto.HeartbeatRequest{
		ServiceId: fc.serviceID,
		Timestamp: time.Now().Unix(),
		Metrics: map[string]float64{
			"contracts_processed":    float64(metrics.ContractsProcessed),
			"entries_skipped":        float64(metrics.EntriesSkipped),
			"batches_created":        float64(metrics.BatchesCreated),
			"bytes_processed":        float64(metrics.BytesProcessed),
			"processing_rate":        metrics.ProcessingRate,
			"current_ledger":         float64(metrics.CurrentLedger),
			"error_count":            float64(metrics.ErrorCount),
			"contracts_per_second":   float64(metrics.ContractsProcessed) / uptime,
			"bytes_per_second":       float64(metrics.BytesProcessed) / uptime,
		},
		Status: proto.ServiceStatus_HEALTHY,
		Metadata: map[string]string{
			"last_update": metrics.LastUpdateTime.Format(time.RFC3339),
		},
	}
	
	// Set status based on errors
	errorRate := float64(metrics.ErrorCount) / float64(metrics.ContractsProcessed+1)
	if errorRate > 0.1 { // More than 10% errors
		req.Status = proto.ServiceStatus_DEGRADED
		req.Metadata["error_rate"] = fmt.Sprintf("%.2f%%", errorRate*100)
	}
	
	resp, err := fc.client.Heartbeat(ctx, req)
	if err != nil {
		fc.logger.Error().
			Err(err).
			Msg("Failed to send heartbeat to flowctl")
		return
	}
	
	if !resp.Success {
		fc.logger.Warn().
			Str("message", resp.Message).
			Msg("Heartbeat not acknowledged by flowctl")
	} else {
		fc.logger.Debug().
			Uint64("contracts", metrics.ContractsProcessed).
			Uint32("ledger", metrics.CurrentLedger).
			Msg("Heartbeat sent to flowctl")
	}
}

// UpdateMetrics updates the metrics for flowctl reporting
func (fc *FlowctlController) UpdateMetrics(metrics FlowctlMetrics) {
	fc.metricsLock.Lock()
	defer fc.metricsLock.Unlock()
	
	fc.lastMetrics = metrics
	fc.lastMetrics.LastUpdateTime = time.Now()
}

// UpdateProcessingMetrics updates metrics from the processing coordinator
func (fc *FlowctlController) UpdateProcessingMetrics(pm ProcessingMetrics) {
	fc.metricsLock.Lock()
	defer fc.metricsLock.Unlock()
	
	fc.lastMetrics.ContractsProcessed = pm.EntriesProcessed
	fc.lastMetrics.EntriesSkipped = pm.EntriesSkipped
	fc.lastMetrics.BatchesCreated = pm.BatchesCreated
	fc.lastMetrics.BytesProcessed = pm.BytesProcessed
	fc.lastMetrics.CurrentLedger = pm.CurrentLedger
	fc.lastMetrics.LastUpdateTime = time.Now()
	
	// Calculate processing rate
	if fc.lastMetrics.ContractsProcessed > 0 {
		duration := time.Since(fc.lastMetrics.LastUpdateTime).Seconds()
		if duration > 0 {
			fc.lastMetrics.ProcessingRate = float64(fc.lastMetrics.ContractsProcessed) / duration
		}
	}
}

// ReportError increments the error counter
func (fc *FlowctlController) ReportError() {
	fc.metricsLock.Lock()
	defer fc.metricsLock.Unlock()
	
	fc.lastMetrics.ErrorCount++
}

// Stop stops the flowctl integration
func (fc *FlowctlController) Stop() {
	if fc.cancelFunc != nil {
		fc.cancelFunc()
	}
	
	// Wait for heartbeat loop to stop
	fc.wg.Wait()
	
	// Send final heartbeat with shutdown status
	if fc.client != nil && fc.serviceID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		req := &proto.HeartbeatRequest{
			ServiceId: fc.serviceID,
			Timestamp: time.Now().Unix(),
			Status:    proto.ServiceStatus_SHUTTING_DOWN,
			Metrics:   make(map[string]float64),
		}
		
		fc.client.Heartbeat(ctx, req)
	}
	
	fc.Close()
}

// Close closes the flowctl connection
func (fc *FlowctlController) Close() {
	if fc.conn != nil {
		fc.conn.Close()
		fc.conn = nil
		fc.client = nil
	}
}

// IsEnabled returns whether flowctl integration is enabled
func (fc *FlowctlController) IsEnabled() bool {
	return fc.config.FlowctlEnabled
}