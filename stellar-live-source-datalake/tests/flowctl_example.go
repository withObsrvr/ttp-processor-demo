package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ServiceRegistration represents the service registration data
type ServiceRegistration struct {
	ServiceType      string            `json:"service_type"`
	OutputEventTypes []string          `json:"output_event_types,omitempty"`
	InputEventTypes  []string          `json:"input_event_types,omitempty"`
	HealthEndpoint   string            `json:"health_endpoint"`
	MaxInflight      int               `json:"max_inflight"`
	Metadata         map[string]string `json:"metadata"`
}

// ServiceHeartbeat represents the heartbeat data
type ServiceHeartbeat struct {
	ServiceID  string             `json:"service_id"`
	Timestamp  time.Time          `json:"timestamp"`
	Metrics    map[string]float64 `json:"metrics"`
	IsHealthy  bool               `json:"is_healthy"`
}

func main() {
	log.Println("Starting flowctl integration example")

	// Prepare registration info
	serviceID := "stellar-source-" + time.Now().Format("20060102150405")
	
	registration := ServiceRegistration{
		ServiceType:      "SOURCE",
		OutputEventTypes: []string{"raw_ledger_service.RawLedgerChunk"},
		HealthEndpoint:   "http://localhost:8080/health",
		MaxInflight:      100,
		Metadata: map[string]string{
			"network":      "testnet",
			"ledger_type":  "stellar",
			"storage_type": os.Getenv("STORAGE_TYPE"),
			"bucket_name":  os.Getenv("BUCKET_NAME"),
		},
	}

	// Log registration details
	log.Printf("Registering with flowctl control plane")
	registrationURL := "http://localhost:8080/api/v1/services/register"
	registrationJSON, _ := json.MarshalIndent(registration, "", "  ")
	log.Printf("Registration URL: %s", registrationURL)
	log.Printf("Registration payload: %s", string(registrationJSON))
	log.Printf("Service registered with ID: %s", serviceID)

	// Start heartbeat loop in goroutine
	stopHeartbeat := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				heartbeat := ServiceHeartbeat{
					ServiceID: serviceID,
					Timestamp: time.Now(),
					IsHealthy: true,
					Metrics: map[string]float64{
						"success_count":       float64(100),
						"error_count":         float64(2),
						"total_processed":     float64(10240),
						"last_sequence":       float64(45678923),
					},
				}
				heartbeatJSON, _ := json.MarshalIndent(heartbeat, "", "  ")
				log.Printf("Sending heartbeat: %s", string(heartbeatJSON))
			case <-stopHeartbeat:
				log.Println("Stopping heartbeat loop")
				return
			}
		}
	}()

	// Wait for Ctrl+C to terminate
	log.Println("Service running. Press Ctrl+C to stop")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	// Cleanup
	close(stopHeartbeat)
	log.Println("Service stopped")
}