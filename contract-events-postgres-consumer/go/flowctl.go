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
	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

// ThreadSafeStats provides thread-safe access to statistics
type ThreadSafeStats struct {
	mu                sync.RWMutex
	EventsProcessed   int64
	EventsInserted    int64
	EventsFailed      int64
	LedgersProcessed  int64
	LastLedger        uint32
	LastProcessedTime time.Time
}

func (ts *ThreadSafeStats) IncrementEventsProcessed() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.EventsProcessed++
}

func (ts *ThreadSafeStats) IncrementEventsInserted() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.EventsInserted++
}

func (ts *ThreadSafeStats) IncrementEventsFailed() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.EventsFailed++
}

func (ts *ThreadSafeStats) IncrementLedgersProcessed() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LedgersProcessed++
}

func (ts *ThreadSafeStats) SetLastLedger(ledger uint32) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LastLedger = ledger
}

func (ts *ThreadSafeStats) SetLastProcessedTime(t time.Time) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LastProcessedTime = t
}

func (ts *ThreadSafeStats) GetSnapshot() Stats {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return Stats{
		EventsProcessed:   ts.EventsProcessed,
		EventsInserted:    ts.EventsInserted,
		EventsFailed:      ts.EventsFailed,
		LedgersProcessed:  ts.LedgersProcessed,
		LastLedger:        ts.LastLedger,
		LastProcessedTime: ts.LastProcessedTime,
	}
}

// FlowctlController manages the interaction with the flowctl control plane
type FlowctlController struct {
	conn              *grpc.ClientConn
	client            flowctlpb.ControlPlaneClient
	serviceID         string
	heartbeatInterval time.Duration
	stopHeartbeat     chan struct{}
	stats             *ThreadSafeStats
	endpoint          string
}

// NewFlowctlController creates a new controller for flowctl integration
func NewFlowctlController(stats *ThreadSafeStats) *FlowctlController {
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
		stats:             stats,
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
		healthPort = "8089" // Default health port (different from processor)
	}
	healthEndpoint := fmt.Sprintf("http://localhost:%s/health", healthPort)

	// Create service info for SINK type
	serviceInfo := &flowctlpb.ServiceInfo{
		ServiceType:     flowctlpb.ServiceType_SERVICE_TYPE_SINK,
		InputEventTypes: []string{"contract_event_service.ContractEvent"},
		HealthEndpoint:  healthEndpoint,
		MaxInflight:     100,
		Metadata: map[string]string{
			"network":   network,
			"sink_type": "postgres",
			"database":  os.Getenv("POSTGRES_DB"),
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
		fc.serviceID = "sim-postgres-consumer-" + time.Now().Format("20060102150405")
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
	// Get current stats snapshot
	snapshot := fc.stats.GetSnapshot()

	// Create heartbeat message
	heartbeat := &flowctlpb.ServiceHeartbeat{
		ServiceId: fc.serviceID,
		Timestamp: timestamppb.Now(),
		Metrics: map[string]float64{
			"events_processed":  float64(snapshot.EventsProcessed),
			"events_inserted":   float64(snapshot.EventsInserted),
			"events_failed":     float64(snapshot.EventsFailed),
			"ledgers_processed": float64(snapshot.LedgersProcessed),
			"last_ledger":       float64(snapshot.LastLedger),
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
