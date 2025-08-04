package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/analytics"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: analytics-consumer <arrow-server> [analysis-type] [window-minutes]")
		fmt.Println("Example: analytics-consumer localhost:8815 payment-volume 60")
		fmt.Println("\nAnalysis types:")
		fmt.Println("  payment-volume    - Track payment volumes by asset")
		fmt.Println("  high-value        - Monitor high-value transactions")
		fmt.Println("  account-activity  - Track account activity patterns")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	analysisType := "payment-volume"
	windowMinutes := 60

	if len(os.Args) > 2 {
		analysisType = os.Args[2]
	}
	
	if len(os.Args) > 3 {
		if mins, err := strconv.Atoi(os.Args[3]); err == nil {
			windowMinutes = mins
		}
	}

	// Initialize
	logger := logging.NewComponentLogger("analytics-consumer", "1.0.0")
	allocator := memory.NewGoAllocator()
	
	logger.Info().
		Str("server", serverAddr).
		Str("analysis_type", analysisType).
		Int("window_minutes", windowMinutes).
		Msg("Starting analytics consumer")

	// Create analytics engine
	engine := analytics.NewAnalyticsEngine(allocator, logger)
	
	// Set up analytics based on type
	var aggregationID string
	var err error
	
	switch analysisType {
	case "payment-volume":
		agg, err := engine.CreateAggregation(
			"payment_volume",
			analytics.AggregationSum,
			time.Duration(windowMinutes)*time.Minute,
			[]string{"asset_code"},
			[]analytics.Filter{},
		)
		if err != nil {
			log.Fatalf("Failed to create payment volume aggregation: %v", err)
		}
		aggregationID = agg.ID
		
	case "high-value":
		agg, err := engine.CreateAggregation(
			"high_value_payments",
			analytics.AggregationCount,
			time.Duration(windowMinutes)*time.Minute,
			[]string{"source_account"},
			[]analytics.Filter{
				{
					Field:    "amount",
					Operator: analytics.FilterGreaterThan,
					Value:    int64(10000000000), // 1000 XLM
				},
			},
		)
		if err != nil {
			log.Fatalf("Failed to create high-value aggregation: %v", err)
		}
		aggregationID = agg.ID
		
	case "account-activity":
		agg, err := engine.CreateAggregation(
			"account_activity",
			analytics.AggregationCount,
			time.Duration(windowMinutes)*time.Minute,
			[]string{"source_account", "operation_type"},
			[]analytics.Filter{},
		)
		if err != nil {
			log.Fatalf("Failed to create account activity aggregation: %v", err)
		}
		aggregationID = agg.ID
		
	default:
		log.Fatalf("Unknown analysis type: %s", analysisType)
	}

	// Connect to Arrow Flight server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Arrow Flight server: %v", err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	// Create ticket for streaming (stream all ledgers)
	ticket := &flight.Ticket{
		Ticket: []byte("stellar_ledger:0:0"),
	}

	logger.Info().
		Msg("Starting Arrow Flight stream for analytics")

	// Start streaming
	ctx := context.Background()
	stream, err := client.DoGet(ctx, ticket)
	if err != nil {
		log.Fatalf("Failed to start stream: %v", err)
	}

	// Create record reader
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to create record reader: %v", err)
	}
	defer reader.Release()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Set up periodic results display
	resultsTicker := time.NewTicker(30 * time.Second)
	defer resultsTicker.Stop()

	// Process stream
	recordCount := int64(0)
	startTime := time.Now()

	go func() {
		for reader.Next() {
			record := reader.Record()
			recordCount += record.NumRows()
			
			// Process through analytics engine
			if err := engine.ProcessRecord(record); err != nil {
				logger.Error().Err(err).Msg("Failed to process record")
			}
		}
		
		if err := reader.Err(); err != nil {
			logger.Error().Err(err).Msg("Stream error")
		}
	}()

	// Main loop
	for {
		select {
		case <-resultsTicker.C:
			// Display current results
			results, err := engine.GetResults(aggregationID)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to get results")
				continue
			}
			
			displayResults(analysisType, results, logger)
			
		case <-sigChan:
			logger.Info().Msg("Received interrupt signal")
			
			// Display final results
			results, err := engine.GetResults(aggregationID)
			if err == nil {
				fmt.Println("\n=== Final Results ===")
				displayResults(analysisType, results, logger)
			}
			
			logger.Info().
				Int64("total_records", recordCount).
				Dur("duration", time.Since(startTime)).
				Msg("Analytics consumer stopped")
			return
		}
	}
}

func displayResults(analysisType string, results *analytics.AggregationResult, logger *logging.ComponentLogger) {
	fmt.Printf("\n=== %s Results ===\n", analysisType)
	fmt.Printf("Window: %s to %s\n", 
		results.WindowStart.Format("15:04:05"), 
		results.WindowEnd.Format("15:04:05"))
	fmt.Printf("Total rows analyzed: %d\n\n", results.TotalRows)
	
	switch analysisType {
	case "payment-volume":
		fmt.Println("Asset volumes:")
		for asset, group := range results.Groups {
			if volume, ok := group.Values["sum"].(int64); ok {
				fmt.Printf("  %s: %d stroops (%.2f XLM)\n", 
					asset, volume, float64(volume)/10000000)
			}
		}
		
	case "high-value":
		fmt.Println("High-value payment accounts:")
		for account, group := range results.Groups {
			fmt.Printf("  %s: %d payments\n", account[:8]+"...", group.Count)
		}
		
	case "account-activity":
		fmt.Println("Account activity:")
		for key, group := range results.Groups {
			fmt.Printf("  %s: %d operations\n", key, group.Count)
		}
	}
	
	fmt.Println()
}