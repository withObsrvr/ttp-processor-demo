//go:build !js && !wasm
// +build !js,!wasm

// This file is only included when building for native platforms (not WebAssembly)

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/internal/client"
)

func main() {
	// For native execution, implement CLI behavior
	if len(os.Args) < 4 {
		fmt.Println("Usage: consumer_wasm <server_address> <start_ledger> <end_ledger> [accountId1] [accountId2] ...")
		fmt.Println("Example: consumer_wasm localhost:50051 100 200 GXXXXXXX GYYYYYY")
		os.Exit(1)
	}

	serverAddress := os.Args[1]
	startLedger, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		fmt.Printf("Error parsing startLedger: %v\n", err)
		os.Exit(1)
	}

	endLedger, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		fmt.Printf("Error parsing endLedger: %v\n", err)
		os.Exit(1)
	}
	
	// Optional account IDs for filtering
	accountIds := os.Args[4:]

	// Create client
	c, err := client.NewEventServiceClient(serverAddress)
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	// Process events
	ctx := context.Background()
	err = c.GetTTPEvents(ctx, uint32(startLedger), uint32(endLedger), accountIds, client.PrintEvent)
	if err != nil {
		fmt.Printf("Error getting events: %v\n", err)
		os.Exit(1)
	}
} 