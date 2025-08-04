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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "localhost:8815", 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to Arrow server")

	client := flight.NewClientFromConn(conn, nil)

	// Try to get flight info
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"stellar_ledger"},
	}

	info, err := client.GetFlightInfo(context.Background(), descriptor)
	if err != nil {
		log.Fatalf("Failed to get flight info: %v", err)
	}

	fmt.Printf("Flight info: %+v\n", info)

	// Try to start a stream
	ticket := &flight.Ticket{
		Ticket: []byte("stellar_ledger:100000:100010"),
	}

	fmt.Println("Starting DoGet...")
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		log.Fatalf("Failed to start stream: %v", err)
	}

	fmt.Println("Stream started successfully")
	
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Release()

	fmt.Println("Waiting for records...")
	if reader.Next() {
		fmt.Printf("Got record with %d rows\n", reader.Record().NumRows())
	} else {
		fmt.Println("No records received")
		if err := reader.Err(); err != nil {
			fmt.Printf("Reader error: %v\n", err)
		}
	}
}