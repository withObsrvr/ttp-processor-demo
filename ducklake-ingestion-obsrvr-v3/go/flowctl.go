package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	// Import the flowctl proto
	flowctlpb "github.com/withObsrvr/flowctl/proto"
)

// IngesterMetrics holds processing metrics for flowctl reporting
type IngesterMetrics struct {
	mu                    sync.RWMutex
	TotalLedgersProcessed uint64
	TotalTablesWritten    uint64
	LastLedgerSequence    uint32
	LedgersPerSecond      float64
	QualityChecksPassed   uint64
	QualityChecksFailed   uint64
	LastFlushDuration     float64 // in seconds
	TotalRows             uint64
}

// UpdateMetrics updates the ingester metrics
func (m *IngesterMetrics) UpdateMetrics(ledgerCount uint64, tableCount uint64, lastSeq uint32, ledgersPerSec float64, rowCount uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalLedgersProcessed += ledgerCount
	m.TotalTablesWritten += tableCount
	m.LastLedgerSequence = lastSeq
	m.LedgersPerSecond = ledgersPerSec
	m.TotalRows += rowCount
}

// UpdateQualityMetrics updates quality check metrics
func (m *IngesterMetrics) UpdateQualityMetrics(passed, failed uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.QualityChecksPassed = passed
	m.QualityChecksFailed = failed
}

// UpdateFlushDuration updates the last flush duration
func (m *IngesterMetrics) UpdateFlushDuration(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastFlushDuration = duration.Seconds()
}

// GetSnapshot returns a snapshot of current metrics
func (m *IngesterMetrics) GetSnapshot() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]float64{
		"total_ledgers_processed": float64(m.TotalLedgersProcessed),
		"total_tables_written":    float64(m.TotalTablesWritten),
		"last_ledger_sequence":    float64(m.LastLedgerSequence),
		"ledgers_per_second":      m.LedgersPerSecond,
		"quality_checks_passed":   float64(m.QualityChecksPassed),
		"quality_checks_failed":   float64(m.QualityChecksFailed),
		"last_flush_duration_sec": m.LastFlushDuration,
		"total_rows_written":      float64(m.TotalRows),
	}
}

// FlowctlController manages the interaction with the flowctl control plane
type FlowctlController struct {
	conn              *grpc.ClientConn
	client            flowctlpb.ControlPlaneClient
	serviceID         string
	heartbeatInterval time.Duration
	stopHeartbeat     chan struct{}
	metrics           *IngesterMetrics
	endpoint          string
}

// NewFlowctlController creates a new controller for flowctl integration
func NewFlowctlController(metrics *IngesterMetrics) *FlowctlController {
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
		metrics:           metrics,
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
		network = "mainnet" // Default to mainnet for this processor
	}

	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8082" // Default health port for processor
	}
	healthEndpoint := fmt.Sprintf("http://localhost:%s/health", healthPort)

	catalogName := os.Getenv("CATALOG_NAME")
	if catalogName == "" {
		catalogName = "unknown"
	}

	// Read component ID from environment (set by flowctl)
	componentID := os.Getenv("FLOWCTL_COMPONENT_ID")
	if componentID == "" {
		log.Println("Warning: FLOWCTL_COMPONENT_ID not set, using legacy mode")
	}

	// Create service info
	serviceInfo := &flowctlpb.ServiceInfo{
		ServiceType:      flowctlpb.ServiceType_SERVICE_TYPE_PROCESSOR,
		ComponentId:      componentID, // Component ID from pipeline YAML
		InputEventTypes:  []string{"raw_ledger_service.RawLedgerChunk"},
		OutputEventTypes: []string{}, // Writes to DuckDB, not producing events
		HealthEndpoint:   healthEndpoint,
		MaxInflight:      100,
		Metadata: map[string]string{
			"network":       network,
			"processor":     "ducklake-ingestion-obsrvr-v2",
			"catalog_name":  catalogName,
			"schema_name":   os.Getenv("SCHEMA_NAME"),
			"version":       "v2.3.0",
			"bronze_tables": "19",
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
		fc.serviceID = "sim-ducklake-processor-" + time.Now().Format("20060102150405")
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
	// Get current metrics snapshot
	metricsSnapshot := fc.metrics.GetSnapshot()

	// Create heartbeat message
	heartbeat := &flowctlpb.ServiceHeartbeat{
		ServiceId: fc.serviceID,
		Timestamp: timestamppb.Now(),
		Metrics:   metricsSnapshot,
	}

	// Send the heartbeat
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := fc.client.Heartbeat(ctx, heartbeat)
	if err != nil {
		log.Printf("Failed to send heartbeat to flowctl: %v", err)
		return
	}

	log.Printf("Sent heartbeat for service %s (ledgers: %.0f, ledgers/sec: %.2f)",
		fc.serviceID,
		metricsSnapshot["total_ledgers_processed"],
		metricsSnapshot["ledgers_per_second"])
}

// Stop stops the flowctl controller and closes connections
func (fc *FlowctlController) Stop() {
	if fc.conn != nil {
		close(fc.stopHeartbeat)
		fc.conn.Close()
	}
}
