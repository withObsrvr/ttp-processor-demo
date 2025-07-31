package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
	"github.com/withobsrvr/contract-invocation-processor/server"
)

func main() {
	// Get port from environment or use default
	port := os.Getenv("PROCESSOR_PORT")
	if port == "" {
		port = "50054"
	}

	// Create gRPC server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Initialize the contract invocation server
	contractInvocationServer, err := server.NewContractInvocationServer()
	if err != nil {
		log.Fatalf("failed to create contract invocation server: %v", err)
	}

	// Start health check server in a separate goroutine
	healthPort := 8089
	if portStr := os.Getenv("HEALTH_PORT"); portStr != "" {
		// Parse port from environment if provided
		// For now, using default 8089
	}

	go func() {
		if err := contractInvocationServer.StartHealthCheckServer(healthPort); err != nil {
			log.Printf("Health check server failed: %v", err)
		}
	}()

	// Register and start gRPC server
	s := grpc.NewServer()
	pb.RegisterContractInvocationServiceServer(s, contractInvocationServer)

	log.Printf("Contract Invocation Processor listening at %v", lis.Addr())
	log.Printf("Health check server listening at :%d", healthPort)
	log.Printf("Ready to process contract invocation events")
	
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}