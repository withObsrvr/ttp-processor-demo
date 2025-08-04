package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to Arrow Flight server
	serverAddr := "localhost:8815"
	fmt.Printf("Connecting to Arrow Flight server at %s...\n", serverAddr)

	// Connect with increased message size
	maxMsgSize := 100 * 1024 * 1024 // 100MB
	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Arrow Flight server: %v", err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	fmt.Println("Successfully connected to Arrow Flight server!")

	// Test different ledger ranges
	testRanges := []struct {
		name        string
		startLedger uint32
		endLedger   uint32
	}{
		{"Small range near known data", 806950, 806955},
		{"Early ledgers", 100000, 100005},
		{"Mid-range ledgers", 500000, 500005},
	}

	// allocator := memory.NewGoAllocator() // Not needed for this test

	for _, testRange := range testRanges {
		fmt.Printf("\n=== Testing: %s ===\n", testRange.name)
		fmt.Printf("Requesting ledgers %d to %d\n", testRange.startLedger, testRange.endLedger)

		// Create ticket for streaming
		ticketStr := fmt.Sprintf("stellar_ledger:%d:%d", testRange.startLedger, testRange.endLedger)
		ticket := &flight.Ticket{
			Ticket: []byte(ticketStr),
		}

		fmt.Printf("Using ticket: %s\n", ticketStr)

		// Start streaming with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		stream, err := client.DoGet(ctx, ticket)
		if err != nil {
			fmt.Printf("ERROR: Failed to start stream: %v\n", err)
			continue
		}

		// Create record reader
		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			fmt.Printf("ERROR: Failed to create record reader: %v\n", err)
			continue
		}
		defer reader.Release()

		// Process records
		recordCount := 0
		totalRows := int64(0)
		startTime := time.Now()

		for reader.Next() {
			record := reader.Record()
			recordCount++
			totalRows += record.NumRows()

			fmt.Printf("  Record %d: %d rows, %d columns\n", recordCount, record.NumRows(), record.NumCols())
			
			// Print column names for first record
			if recordCount == 1 {
				fmt.Println("  Columns:")
				for i := 0; i < int(record.NumCols()); i++ {
					col := record.Column(i)
					fmt.Printf("    - %s (%s)\n", record.ColumnName(i), col.DataType())
				}
			}

			// Don't hold onto the record
			record.Release()
		}

		if err := reader.Err(); err != nil {
			fmt.Printf("ERROR: Stream error: %v\n", err)
		}

		elapsed := time.Since(startTime)
		if recordCount > 0 {
			fmt.Printf("SUCCESS: Received %d records with %d total rows in %v\n", recordCount, totalRows, elapsed)
		} else {
			fmt.Printf("WARNING: No records received for this range\n")
		}
	}

	fmt.Println("\n=== Test Summary ===")
	fmt.Println("Arrow Flight connection test completed.")
}