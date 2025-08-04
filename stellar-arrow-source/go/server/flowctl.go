package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	// Import the flowctl proto
	flowctlpb "github.com/withobsrvr/flowctl/proto"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// FlowctlController manages the interaction with the flowctl control plane
type FlowctlController struct {
	conn              *grpc.ClientConn
	client            flowctlpb.ControlPlaneClient
	serviceID         string
	heartbeatInterval time.Duration
	stopHeartbeat     chan struct{}
	arrowServer       *StellarArrowServer
	endpoint          string
	logger            *logging.ComponentLogger
}

// NewFlowctlController creates a new controller for flowctl integration
func NewFlowctlController(arrowServer *StellarArrowServer) *FlowctlController {
	logger := logging.NewComponentLogger("stellar-arrow-source-flowctl", "v1.0.0")
	
	// Default values
	interval := 10 * time.Second
	endpoint := "localhost:8080" // Default endpoint

	// Check for environment variables
	if intervalStr := os.Getenv("FLOWCTL_HEARTBEAT_INTERVAL"); intervalStr != "" {
		if parsedInterval, err := time.ParseDuration(intervalStr); err == nil {
			interval = parsedInterval
		}
	}

	if endpointEnv := os.Getenv("FLOWCTL_ENDPOINT"); endpointEnv != "" {
		endpoint = endpointEnv
	}

	logger.Info().
		Str("operation", "flowctl_controller_init").
		Str("endpoint", endpoint).
		Dur("heartbeat_interval", interval).
		Msg("Initializing flowctl controller for Arrow service")

	return &FlowctlController{
		heartbeatInterval: interval,
		stopHeartbeat:     make(chan struct{}),
		arrowServer:       arrowServer,
		endpoint:          endpoint,
		logger:            logger,
	}
}

// RegisterWithFlowctl registers the service with the flowctl control plane
func (fc *FlowctlController) RegisterWithFlowctl() error {
	// Connect to flowctl control plane
	var err error
	fc.conn, err = grpc.Dial(fc.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fc.logger.Error().
			Err(err).
			Str("endpoint", fc.endpoint).
			Msg("Failed to connect to flowctl control plane")
		return fmt.Errorf("failed to connect to flowctl control plane: %w", err)
	}

	// Create client
	fc.client = flowctlpb.NewControlPlaneClient(fc.conn)

	// Prepare service info
	network := os.Getenv("STELLAR_NETWORK")
	if network == "" {
		network = "testnet" // Default to testnet
	}

	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8088" // Default health port
	}
	healthEndpoint := fmt.Sprintf("http://localhost:%s/health", healthPort)

	// Create service info - simplified to match working services exactly
	serviceInfo := &flowctlpb.ServiceInfo{
		ServiceType: flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		// Phase 1: Use existing event types (compatibility mode)
		OutputEventTypes: []string{"raw_ledger_service.RawLedgerChunk"},
		HealthEndpoint:   healthEndpoint,
		MaxInflight:      100,
		Metadata: map[string]string{
			"network":      network,
			"ledger_type":  "stellar",
			// Simplified metadata for debugging
			"data_format":  "apache_arrow",
		},
	}

	// Register with control plane
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fc.logger.Info().
		Str("operation", "flowctl_registration").
		Str("endpoint", fc.endpoint).
		Interface("output_event_types", serviceInfo.OutputEventTypes).
		Str("compatibility_mode", "phase1").
		Msg("Registering Arrow service with flowctl control plane")
	
	// Attempt to register
	ack, err := fc.client.Register(ctx, serviceInfo)
	if err != nil {
		// If registration fails, use a simulated ID but log the error
		fc.serviceID = "sim-stellar-arrow-source-" + time.Now().Format("20060102150405")
		fc.logger.Warn().
			Err(err).
			Str("simulated_id", fc.serviceID).
			Msg("Failed to register with flowctl. Using simulated ID")
		
		// Start heartbeat loop anyway, it will use the simulated ID
		go fc.startHeartbeatLoop()
		return nil
	}
	
	// Use the service ID from the response
	fc.serviceID = ack.ServiceId
	fc.logger.Info().
		Str("operation", "flowctl_registration_success").
		Str("service_id", fc.serviceID).
		Bool("native_arrow", true).
		Msg("Successfully registered Arrow service with flowctl control plane")
	
	// Log topic names and connection info if provided
	if len(ack.TopicNames) > 0 {
		fc.logger.Info().
			Interface("topics", ack.TopicNames).
			Msg("Assigned flowctl topics")
	}
	if len(ack.ConnectionInfo) > 0 {
		fc.logger.Info().
			Interface("connection_info", ack.ConnectionInfo).
			Msg("Received flowctl connection info")
	}

	// Start heartbeat loop
	go fc.startHeartbeatLoop()

	return nil
}

// startHeartbeatLoop sends periodic heartbeats to the control plane
func (fc *FlowctlController) startHeartbeatLoop() {
	ticker := time.NewTicker(fc.heartbeatInterval)
	defer ticker.Stop()

	fc.logger.Info().
		Str("operation", "heartbeat_loop_start").
		Dur("interval", fc.heartbeatInterval).
		Str("service_id", fc.serviceID).
		Msg("Starting flowctl heartbeat loop for Arrow service")

	heartbeatCount := 0
	successCount := 0
	
	for {
		select {
		case <-ticker.C:
			heartbeatCount++
			fc.logger.Debug().
				Str("operation", "heartbeat_attempt").
				Str("service_id", fc.serviceID).
				Int("attempt", heartbeatCount).
				Int("success_count", successCount).
				Msg("Attempting to send heartbeat")
			
			// Send heartbeat (success/failure tracked in sendHeartbeat logs)
			fc.sendHeartbeat()
			
		case <-fc.stopHeartbeat:
			fc.logger.Info().
				Str("operation", "heartbeat_loop_stop").
				Str("service_id", fc.serviceID).
				Int("total_attempts", heartbeatCount).
				Msg("Stopping flowctl heartbeat loop")
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat to the control plane
func (fc *FlowctlController) sendHeartbeat() {
	// Create simple heartbeat matching the working services exactly
	heartbeat := &flowctlpb.ServiceHeartbeat{
		ServiceId: fc.serviceID,
		Timestamp: timestamppb.Now(),
		Metrics: map[string]float64{
			// Use exact same metrics as stellar-live-source-datalake for compatibility
			"success_count":            1.0,
			"error_count":              0.0,
			"total_processed":          1.0,
			"total_bytes_processed":    1024.0,
			"last_successful_sequence": 1.0,
		},
	}

	// Send the heartbeat with connection state checking
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Check connection state before sending
	state := fc.conn.GetState()
	fc.logger.Debug().
		Str("connection_state", state.String()).
		Str("service_id", fc.serviceID).
		Msg("Connection state before heartbeat")

	_, err := fc.client.Heartbeat(ctx, heartbeat)
	if err != nil {
		fc.logger.Error().
			Err(err).
			Str("service_id", fc.serviceID).
			Str("connection_state", fc.conn.GetState().String()).
			Msg("Failed to send heartbeat to flowctl")
		return
	}

	// Verify connection state after sending
	fc.logger.Info().
		Str("operation", "heartbeat_sent").
		Str("service_id", fc.serviceID).
		Str("connection_state", fc.conn.GetState().String()).
		Bool("arrow_service", true).
		Msg("Sent heartbeat to flowctl control plane")
}

// Stop stops the flowctl controller and closes connections
func (fc *FlowctlController) Stop() {
	fc.logger.Info().
		Str("operation", "flowctl_controller_stop").
		Str("service_id", fc.serviceID).
		Msg("Stopping flowctl controller")

	if fc.conn != nil {
		close(fc.stopHeartbeat)
		fc.conn.Close()
	}

	fc.logger.Info().
		Str("operation", "flowctl_controller_stopped").
		Msg("Flowctl controller stopped successfully")
}

// GetServiceID returns the current service ID
func (fc *FlowctlController) GetServiceID() string {
	return fc.serviceID
}

// GetHealthStatus returns flowctl-specific health information
func (fc *FlowctlController) GetHealthStatus() map[string]interface{} {
	return map[string]interface{}{
		"flowctl_connected": fc.conn != nil,
		"service_id":        fc.serviceID,
		"endpoint":          fc.endpoint,
		"heartbeat_active":  true, // Simplified for Phase 1
		"compatibility_mode": "phase1",
	}
}