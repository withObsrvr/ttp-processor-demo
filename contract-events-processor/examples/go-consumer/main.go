package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	pb "github.com/withObsrvr/contract-events-processor/gen/contract_event_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line arguments
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <start_ledger> <end_ledger>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  Set end_ledger=0 for continuous streaming\n")
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s 1000 2000    # Stream ledgers 1000-2000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s 1000 0       # Stream from 1000 continuously\n", os.Args[0])
		os.Exit(1)
	}

	startLedger, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		log.Fatalf("Invalid start_ledger: %v", err)
	}

	endLedger, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		log.Fatalf("Invalid end_ledger: %v", err)
	}

	// Get service address from environment or use default
	serviceAddress := getEnvOrDefault("CONTRACT_EVENTS_SERVICE_ADDRESS", "localhost:50053")

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived shutdown signal, stopping...")
		cancel()
	}()

	// Connect to contract events service
	log.Printf("Connecting to Contract Events service at %s", serviceAddress)
	conn, err := grpc.NewClient(serviceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewContractEventServiceClient(conn)

	// Create request
	request := &pb.GetContractEventsRequest{
		StartLedger: uint32(startLedger),
		EndLedger:   uint32(endLedger),
		// Optionally add filters from environment
		ContractIds:        getEnvSlice("FILTER_CONTRACT_IDS"),
		EventTypes:         getEnvSlice("FILTER_EVENT_TYPES"),
		IncludeFailed:      getBoolEnv("INCLUDE_FAILED", false),
		IncludeDiagnostics: getBoolEnv("INCLUDE_DIAGNOSTICS", false),
	}

	// Log filters if any
	if len(request.ContractIds) > 0 {
		log.Printf("Filtering by contract IDs: %v", request.ContractIds)
	}
	if len(request.EventTypes) > 0 {
		log.Printf("Filtering by event types: %v", request.EventTypes)
	}
	if request.IncludeFailed {
		log.Printf("Including events from failed transactions")
	}

	// Start streaming
	log.Printf("Requesting contract events from ledger %d to %d", startLedger, endLedger)
	if endLedger == 0 {
		log.Printf("Streaming continuously (end_ledger=0)")
	}

	stream, err := client.GetContractEvents(ctx, request)
	if err != nil {
		log.Fatalf("Failed to start stream: %v", err)
	}

	// Print header
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Contract Events Stream")
	fmt.Println(strings.Repeat("=", 80))

	eventCount := 0
	ledgerCount := make(map[uint32]int)

	// Receive and print events
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("\nStream completed")
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, graceful shutdown
				break
			}
			log.Fatalf("Error receiving event: %v", err)
		}

		eventCount++
		ledgerCount[event.Meta.LedgerSequence]++

		printEvent(event, eventCount)
	}

	// Print summary
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("Stream Summary:\n")
	fmt.Printf("  Total Events: %d\n", eventCount)
	fmt.Printf("  Ledgers Processed: %d\n", len(ledgerCount))
	if len(ledgerCount) > 0 {
		fmt.Printf("  Ledger Range: %d - %d\n", findMin(ledgerCount), findMax(ledgerCount))
	}
	fmt.Println(strings.Repeat("=", 80))
}

func printEvent(event *pb.ContractEvent, eventNum int) {
	fmt.Printf("\n--- Event #%d ---\n", eventNum)
	fmt.Printf("Ledger:       %d (closed at %d)\n", event.Meta.LedgerSequence, event.Meta.LedgerClosedAt)
	fmt.Printf("TX Hash:      %s\n", event.Meta.TxHash)
	fmt.Printf("TX Status:    %s\n", txStatus(event.Meta.TxSuccessful))
	fmt.Printf("Contract:     %s\n", event.ContractId)
	fmt.Printf("Event Type:   %s\n", event.EventType)
	fmt.Printf("Event Index:  %d\n", event.EventIndex)

	if event.OperationIndex != nil {
		fmt.Printf("Op Index:     %d\n", *event.OperationIndex)
	} else {
		fmt.Printf("Op Index:     (transaction-level event)\n")
	}

	// Print topics
	if len(event.Topics) > 0 {
		fmt.Printf("\nTopics (%d):\n", len(event.Topics))
		for i, topic := range event.Topics {
			if topic.Json != "" {
				var jsonData interface{}
				if err := json.Unmarshal([]byte(topic.Json), &jsonData); err == nil {
					fmt.Printf("  [%d] %v\n", i, jsonData)
				} else {
					fmt.Printf("  [%d] (XDR only)\n", i)
				}
			}
		}
	}

	// Print data
	if event.Data != nil && event.Data.Json != "" {
		fmt.Printf("\nData:\n")
		var jsonData interface{}
		if err := json.Unmarshal([]byte(event.Data.Json), &jsonData); err == nil {
			jsonBytes, _ := json.MarshalIndent(jsonData, "  ", "  ")
			fmt.Printf("  %s\n", string(jsonBytes))
		}
	}

	fmt.Println()
}

func txStatus(successful bool) string {
	if successful {
		return "✓ Successful"
	}
	return "✗ Failed"
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvSlice(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	// Split by comma
	var result []string
	for _, item := range splitAndTrim(value, ",") {
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func getBoolEnv(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	switch value {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return defaultValue
	}
}

func splitAndTrim(s, sep string) []string {
	parts := []string{}
	current := ""
	for _, char := range s {
		if string(char) == sep {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else if char != ' ' && char != '\t' {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func findMin(m map[uint32]int) uint32 {
	var min uint32
	first := true
	for k := range m {
		if first || k < min {
			min = k
			first = false
		}
	}
	return min
}

func findMax(m map[uint32]int) uint32 {
	var max uint32
	for k := range m {
		if k > max {
			max = k
		}
	}
	return max
}
