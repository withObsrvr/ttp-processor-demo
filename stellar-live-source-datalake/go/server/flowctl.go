package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	// Import the flowctl proto
	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

// FlowctlController manages the interaction with the flowctl control plane
type FlowctlController struct {
	conn              *grpc.ClientConn
	client            flowctlpb.ControlPlaneClient
	serviceID         string
	heartbeatInterval time.Duration
	stopHeartbeat     chan struct{}
	rawLedgerServer   *RawLedgerServer
	endpoint          string
}

// NewFlowctlController creates a new controller for flowctl integration
func NewFlowctlController(rawLedgerServer *RawLedgerServer) *FlowctlController {
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

	return &FlowctlController{
		heartbeatInterval: interval,
		stopHeartbeat:     make(chan struct{}),
		rawLedgerServer:   rawLedgerServer,
		endpoint:          endpoint,
	}
}

// RegisterWithFlowctl registers the service with the flowctl control plane
func (fc *FlowctlController) RegisterWithFlowctl() error {
	// Connect to flowctl control plane
	var err error
	fc.conn, err = grpc.Dial(fc.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
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

	// Read component ID from environment (set by flowctl)
	componentID := os.Getenv("FLOWCTL_COMPONENT_ID")
	if componentID == "" {
		log.Println("Warning: FLOWCTL_COMPONENT_ID not set, using legacy mode")
	}

	// Create service info
	serviceInfo := &flowctlpb.ServiceInfo{
		ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_SOURCE,
		OutputEventTypes: []string{"raw_ledger_service.RawLedgerChunk"},
		HealthEndpoint:   healthEndpoint,
		MaxInflight:      100,
		Metadata: map[string]string{
			"component_id":  componentID, // Component ID from pipeline YAML
			"network":       network,
			"ledger_type":   "stellar",
			"storage_type":  os.Getenv("STORAGE_TYPE"),
			"bucket_name":   os.Getenv("BUCKET_NAME"),
		},
	}

	// Register with control plane
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Registering with flowctl control plane at %s", fc.endpoint)
	
	// Attempt to register
	ack, err := fc.client.Register(ctx, serviceInfo)
	if err != nil {
		// If registration fails, use a simulated ID but log the error
		fc.serviceID = "sim-stellar-source-" + time.Now().Format("20060102150405")
		log.Printf("Warning: Failed to register with flowctl. Using simulated ID: %s. Error: %v", fc.serviceID, err)
		
		// Start heartbeat loop anyway, it will use the simulated ID
		go fc.startHeartbeatLoop()
		return nil
	}
	
	// Use the service ID from the response
	fc.serviceID = ack.ServiceId
	log.Printf("Successfully registered with flowctl control plane - Service ID: %s", fc.serviceID)
	
	// Log topic names and connection info if provided
	if len(ack.TopicNames) > 0 {
		log.Printf("Assigned topics: %v", ack.TopicNames)
	}
	if len(ack.ConnectionInfo) > 0 {
		log.Printf("Connection info: %v", ack.ConnectionInfo)
	}

	// Start heartbeat loop
	go fc.startHeartbeatLoop()

	return nil
}

// startHeartbeatLoop sends periodic heartbeats to the control plane
func (fc *FlowctlController) startHeartbeatLoop() {
	ticker := time.NewTicker(fc.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fc.sendHeartbeat()
		case <-fc.stopHeartbeat:
			log.Println("Stopping flowctl heartbeat loop")
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat to the control plane
func (fc *FlowctlController) sendHeartbeat() {
	// Get current metrics from the server
	metrics := fc.rawLedgerServer.GetMetrics()

	// Create heartbeat message
	heartbeat := &flowctlpb.ServiceHeartbeat{
		ServiceId: fc.serviceID,
		Timestamp: timestamppb.Now(),
		Metrics: map[string]float64{
			"success_count":            float64(metrics.SuccessCount),
			"error_count":              float64(metrics.ErrorCount),
			"total_processed":          float64(metrics.TotalProcessed),
			"total_bytes_processed":    float64(metrics.TotalBytesProcessed),
			"last_successful_sequence": float64(metrics.LastSuccessfulSeq),
		},
	}

	// Send the heartbeat
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := fc.client.Heartbeat(ctx, heartbeat)
	if err != nil {
		log.Printf("Failed to send heartbeat to flowctl: %v", err)
		return
	}

	log.Printf("Sent heartbeat for service %s", fc.serviceID)
}

// Stop stops the flowctl controller and closes connections
func (fc *FlowctlController) Stop() {
	if fc.conn != nil {
		close(fc.stopHeartbeat)
		fc.conn.Close()
	}
}