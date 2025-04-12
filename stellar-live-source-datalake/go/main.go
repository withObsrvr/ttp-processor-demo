package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
	"github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/server"
)

func main() {
	// Check required environment variables
	storageType := os.Getenv("STORAGE_TYPE")
	bucketName := os.Getenv("BUCKET_NAME")

	if storageType == "" || bucketName == "" {
		log.Fatalf("STORAGE_TYPE and BUCKET_NAME environment variables are required")
	}

	// Create gRPC server
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	rawLedgerServer := server.NewRawLedgerServer()
	pb.RegisterRawLedgerServiceServer(s, rawLedgerServer)

	log.Printf("Server listening at %v", lis.Addr())
	log.Printf("Reading ledgers from %s: %s", storageType, bucketName)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
