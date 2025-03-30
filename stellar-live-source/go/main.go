package main

import (
	"log"
	"net"
	"os"

	// Import the generated protobuf code package
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"
	// Import the server implementation package
	"github.com/stellar/stellar-live-source/server"

	"google.golang.org/grpc"
)

const defaultPort = ":50052"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
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
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
