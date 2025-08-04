package main

import (
	"context"
	"fmt"
	"log"
	"time"

	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect directly to stellar-live-source-datalake
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "localhost:50053",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to stellar-live-source-datalake")

	// Create client
	client := rawledger.NewRawLedgerServiceClient(conn)

	// Test with different ledger starting points
	testStarts := []struct {
		start uint32
		desc  string
	}{
		{100201, "Starting from 100201"},
		{1, "Starting from ledger 1"},
		{806932, "Starting from 806932"},
	}

	for _, test := range testStarts {
		fmt.Printf("\n=== Testing %s ===\n", test.desc)

		stream, err := client.StreamRawLedgers(ctx, &rawledger.StreamLedgersRequest{
			StartLedger: test.start,
		})
		if err != nil {
			fmt.Printf("Failed to create stream: %v\n", err)
			continue
		}

		count := 0
		startTime := time.Now()
		
		for {
			ledger, err := stream.Recv()
			if err != nil {
				fmt.Printf("Stream error after %d ledgers: %v\n", count, err)
				break
			}
			
			count++
			fmt.Printf("Received ledger %d: sequence=%d, size=%d bytes\n", 
				count, ledger.Sequence, len(ledger.LedgerCloseMetaXdr))
			
			if count >= 5 {
				fmt.Println("Stopping after 5 ledgers...")
				break
			}
		}
		
		fmt.Printf("Duration: %v\n", time.Since(startTime))
	}
}