package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"

	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
	"github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server"
)

func main() {
	// Create gRPC server
	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Initialize the raw ledger server (handles all configuration validation)
	rawLedgerServer, err := server.NewRawLedgerServer()
	if err != nil {
		log.Fatalf("failed to create raw ledger server: %v", err)
	}

	// Start health check server in a separate goroutine
	healthPort := 8088
	if portStr := os.Getenv("HEALTH_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			healthPort = port
		}
	}
	go func() {
		if err := rawLedgerServer.StartHealthCheckServer(healthPort); err != nil {
			log.Printf("failed to start health check server: %v", err)
		}
	}()

	// Register with flowctl control plane if enabled
	if os.Getenv("ENABLE_FLOWCTL") == "true" {
		flowctlController := server.NewFlowctlController(rawLedgerServer)
		if err := flowctlController.RegisterWithFlowctl(); err != nil {
			log.Printf("Warning: Failed to register with flowctl control plane: %v", err)
			log.Printf("Service will continue without control plane integration")
		} else {
			log.Printf("Successfully registered with flowctl control plane")

			// Ensure flowctl controller is stopped gracefully on server shutdown
			defer flowctlController.Stop()
		}
	}

	// Register and start gRPC server
	s := grpc.NewServer()
	pb.RegisterRawLedgerServiceServer(s, rawLedgerServer)

	log.Printf("Server listening at %v", lis.Addr())
	log.Printf("Health check server listening at :%d", healthPort)
	// Note: After migration, BACKEND_TYPE env var might not be set, so we can't rely on it
	log.Printf("Server initialized successfully with configured backend")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
