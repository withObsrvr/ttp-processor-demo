package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import the raw ledger service client
	rawledger "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/gen/raw_ledger_service"
)

func main() {
	// Connect to stellar-live-source-datalake
	endpoint := "localhost:50053"
	fmt.Printf("Connecting to stellar-live-source-datalake at %s...\n", endpoint)

	conn, err := grpc.Dial(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect to stellar data source: %v", err)
	}
	defer conn.Close()

	// Create gRPC client
	client := rawledger.NewRawLedgerServiceClient(conn)
	fmt.Println("Successfully connected!")

	// Test different ledger ranges to see what's available
	// Based on health check, last successful sequence was 806957
	testRanges := []struct {
		name        string
		startLedger uint32
		maxLedgers  int
	}{
		{"Near last known success (805,950-805,955)", 805950, 5},
		{"Just before circuit breaker opened (806,950-806,955)", 806950, 5},
		{"Early range that was processed (100,000-100,005)", 100000, 5},
		{"Another early range (100,200-100,205)", 100200, 5},
		{"Mid-range (500,000-500,005)", 500000, 5},
	}

	for _, testRange := range testRanges {
		fmt.Printf("\n=== Testing: %s ===\n", testRange.name)
		fmt.Printf("Requesting ledgers starting from %d (max %d ledgers)\n", testRange.startLedger, testRange.maxLedgers)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create request
		req := &rawledger.StreamLedgersRequest{
			StartLedger: testRange.startLedger,
		}

		// Start streaming
		stream, err := client.StreamRawLedgers(ctx, req)
		if err != nil {
			fmt.Printf("ERROR: Failed to start stream: %v\n", err)
			continue
		}

		// Receive ledgers
		receivedCount := 0
		firstLedger := uint32(0)
		lastLedger := uint32(0)

		startTime := time.Now()
		for receivedCount < testRange.maxLedgers {
			// Check for timeout
			select {
			case <-ctx.Done():
				fmt.Printf("TIMEOUT: Context deadline exceeded after %v\n", time.Since(startTime))
				goto next_range
			default:
			}

			rawLedgerMsg, err := stream.Recv()
			if err == io.EOF {
				fmt.Printf("Stream ended after %d ledgers\n", receivedCount)
				break
			}
			if err != nil {
				fmt.Printf("ERROR receiving ledger: %v\n", err)
				break
			}

			if receivedCount == 0 {
				firstLedger = rawLedgerMsg.Sequence
			}
			lastLedger = rawLedgerMsg.Sequence

			receivedCount++

			// Print details for first few ledgers
			if receivedCount <= 3 {
				fmt.Printf("  Ledger %d: XDR size = %d bytes\n",
					rawLedgerMsg.Sequence,
					len(rawLedgerMsg.LedgerCloseMetaXdr))
			}
		}

		if receivedCount > 0 {
			fmt.Printf("SUCCESS: Received %d ledgers (sequence %d to %d)\n", 
				receivedCount, firstLedger, lastLedger)
		} else {
			fmt.Printf("WARNING: No ledgers received for this range\n")
		}

		// Close the stream
		next_range:
		cancel()
	}

	fmt.Println("\n=== Test Summary ===")
	fmt.Println("Test completed. Check the results above to see which ledger ranges are available.")
}