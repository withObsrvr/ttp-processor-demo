package main

import (
	"log"
	"net"
	"os"

	// Import the generated protobuf code package for the service WE PROVIDE
	eventservice "github.com/withObsrvr/ttp-processor/gen/event_service"
	// Import the server implementation package
	"github.com/withObsrvr/ttp-processor/server"

	"google.golang.org/grpc"
)

const defaultPort = ":50051"
const defaultSourceServiceAddress = "localhost:50052" // Default address for stellar-live-source

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		log.Fatalf("NETWORK_PASSPHRASE environment variable not set")
	}

	sourceServiceAddr := os.Getenv("SOURCE_SERVICE_ADDRESS")
	if sourceServiceAddr == "" {
		sourceServiceAddr = defaultSourceServiceAddress
	}

	s := grpc.NewServer()
	eventServer, err := server.NewEventServer(networkPassphrase, sourceServiceAddr)
	if err != nil {
		log.Fatalf("failed to create event server: %v", err)
	}
	// Register the EventServiceServer implementation
	eventservice.RegisterEventServiceServer(s, eventServer)

	log.Printf("TTP Processor Server listening at %v", lis.Addr())
	log.Printf("Connecting to raw ledger source at %s", sourceServiceAddr)
	log.Printf("Using network passphrase: %s", networkPassphrase)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
