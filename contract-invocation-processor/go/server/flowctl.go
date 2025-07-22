package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Note: These imports will be implemented when flowctl integration is available
	// For now, we'll use placeholder structures
)

// FlowctlController handles integration with the Obsrvr flowctl control plane
type FlowctlController struct {
	logger   *zap.Logger
	endpoint string
	conn     *grpc.ClientConn
	// client   pb.ControlPlaneClient // TODO: Import actual flowctl types
}

// ServiceInfo represents the service registration information
type ServiceInfo struct {
	ServiceType      string            `json:"service_type"`
	ServiceID        string            `json:"service_id"`
	Address          string            `json:"address"`
	InputEventTypes  []string          `json:"input_event_types"`
	OutputEventTypes []string          `json:"output_event_types"`
	HealthEndpoint   string            `json:"health_endpoint"`
	Metadata         map[string]string `json:"metadata"`
}

// ServiceHeartbeat represents a heartbeat message to the control plane
type ServiceHeartbeat struct {
	ServiceID string             `json:"service_id"`
	Timestamp time.Time          `json:"timestamp"`
	Metrics   map[string]float64 `json:"metrics"`
}

// NewFlowctlController creates a new flowctl controller
func NewFlowctlController(logger *zap.Logger) *FlowctlController {
	endpoint := os.Getenv("FLOWCTL_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:8080"
	}

	return &FlowctlController{
		logger:   logger,
		endpoint: endpoint,
	}
}

// Connect establishes connection to the flowctl control plane
func (fc *FlowctlController) Connect(ctx context.Context) error {
	fc.logger.Info("connecting to flowctl control plane", zap.String("endpoint", fc.endpoint))

	conn, err := grpc.DialContext(ctx, fc.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to flowctl: %w", err)
	}

	fc.conn = conn
	fc.logger.Info("connected to flowctl control plane successfully")
	return nil
}

// Register registers this service with the flowctl control plane
func (fc *FlowctlController) Register(ctx context.Context) error {
	if fc.conn == nil {
		return fmt.Errorf("not connected to flowctl control plane")
	}

	// TODO: Implement actual registration when flowctl types are available
	serviceInfo := ServiceInfo{
		ServiceType: "processor",
		ServiceID:   "contract-invocation-processor",
		Address:     "localhost:50054", // TODO: Get actual address
		InputEventTypes: []string{
			"raw_ledger_service.RawLedger",
		},
		OutputEventTypes: []string{
			"contract_invocation.ContractInvocationEvent",
		},
		HealthEndpoint: "http://localhost:8089/health",
		Metadata: map[string]string{
			"processor_type":     "contract_invocation",
			"protocol_support":   "23",
			"network_support":    "mainnet,testnet",
			"version":           "1.0.0",
			"implementation":    "go",
		},
	}

	fc.logger.Info("registering with flowctl control plane",
		zap.String("service_id", serviceInfo.ServiceID),
		zap.String("service_type", serviceInfo.ServiceType),
		zap.Strings("input_types", serviceInfo.InputEventTypes),
		zap.Strings("output_types", serviceInfo.OutputEventTypes),
	)

	// TODO: Actual registration call will be implemented when flowctl types are available
	// For now, just log the registration attempt
	fc.logger.Info("flowctl registration placeholder - will be implemented when flowctl types are available")

	return nil
}

// SendHeartbeat sends a heartbeat with current metrics to the control plane
func (fc *FlowctlController) SendHeartbeat(ctx context.Context, metrics map[string]float64) error {
	if fc.conn == nil {
		return fmt.Errorf("not connected to flowctl control plane")
	}

	heartbeat := ServiceHeartbeat{
		ServiceID: "contract-invocation-processor",
		Timestamp: time.Now(),
		Metrics:   metrics,
	}

	fc.logger.Debug("sending heartbeat to flowctl",
		zap.String("service_id", heartbeat.ServiceID),
		zap.Any("metrics", heartbeat.Metrics),
	)

	// TODO: Actual heartbeat call will be implemented when flowctl types are available
	// For now, just log the heartbeat attempt
	fc.logger.Debug("flowctl heartbeat placeholder - will be implemented when flowctl types are available")

	return nil
}

// StartHeartbeatLoop starts a goroutine that sends periodic heartbeats
func (fc *FlowctlController) StartHeartbeatLoop(ctx context.Context, metricsProvider func() map[string]float64) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				fc.logger.Info("stopping flowctl heartbeat loop")
				return
			case <-ticker.C:
				metrics := metricsProvider()
				if err := fc.SendHeartbeat(ctx, metrics); err != nil {
					fc.logger.Error("failed to send heartbeat", zap.Error(err))
				}
			}
		}
	}()

	fc.logger.Info("started flowctl heartbeat loop")
}

// Close closes the connection to the flowctl control plane
func (fc *FlowctlController) Close() error {
	if fc.conn != nil {
		fc.logger.Info("closing flowctl connection")
		return fc.conn.Close()
	}
	return nil
}

// Helper function to convert processor metrics to flowctl metrics format
func ConvertMetricsToFlowctl(metrics ProcessorMetrics) map[string]float64 {
	return map[string]float64{
		"total_processed":        float64(metrics.TotalProcessed),
		"total_invocations":      float64(metrics.TotalInvocations),
		"successful_invocations": float64(metrics.SuccessfulInvocations),
		"failed_invocations":     float64(metrics.FailedInvocations),
		"contract_calls":         float64(metrics.ContractCalls),
		"create_contracts":       float64(metrics.CreateContracts),
		"upload_wasms":           float64(metrics.UploadWasms),
		"archive_restorations":   float64(metrics.ArchiveRestorations),
		"error_count":            float64(metrics.ErrorCount),
		"last_processed_ledger":  float64(metrics.LastProcessedLedger),
		"processing_latency_ms":  float64(metrics.ProcessingLatency.Milliseconds()),
		"total_events_emitted":   float64(metrics.TotalEventsEmitted),
		"uptime_seconds":         float64(time.Since(metrics.StartTime).Seconds()),
	}
}