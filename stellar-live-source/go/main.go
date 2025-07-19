package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	// Import the generated protobuf code package
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source/gen/raw_ledger_service"
	// Import the server implementation package
	"github.com/withObsrvr/ttp-processor-demo/stellar-live-source/server"

	"google.golang.org/grpc"
)

const (
	defaultPort      = ":50052"
	defaultHealthPort = "8088"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	// Ensure port starts with ":"
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}
	
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	rpcEndpoint := os.Getenv("RPC_ENDPOINT")
	if rpcEndpoint == "" {
		log.Fatalf("RPC_ENDPOINT environment variable not set")
	}

	s := grpc.NewServer()
	rawLedgerServer, err := server.NewRawLedgerServer(rpcEndpoint)
	if err != nil {
		log.Fatalf("failed to create raw ledger server: %v", err)
	}

	// Register the server implementation with the gRPC server
	rawledger.RegisterRawLedgerServiceServer(s, rawLedgerServer)

	log.Printf("Raw Ledger Source Server listening at %v", lis.Addr())
	log.Printf("Connecting to RPC endpoint: %s", rpcEndpoint)

	// Start health check server
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = defaultHealthPort
	}
	
	healthPortInt, err := strconv.Atoi(healthPort)
	if err != nil {
		log.Fatalf("invalid health port: %v", err)
	}
	
	go func() {
		if err := rawLedgerServer.StartHealthCheckServer(healthPortInt); err != nil {
			log.Fatalf("failed to start health check server: %v", err)
		}
	}()

	// Initialize flowctl integration if enabled
	enableFlowctl := os.Getenv("ENABLE_FLOWCTL")
	if strings.ToLower(enableFlowctl) == "true" {
		log.Println("Initializing flowctl integration")
		flowctlController := server.NewFlowctlController(rawLedgerServer)
		if err := flowctlController.RegisterWithFlowctl(); err != nil {
			log.Printf("Warning: Failed to register with flowctl: %v", err)
		}
		
		// Make sure to properly shut down flowctl when the main server stops
		defer flowctlController.Stop()
	}

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
