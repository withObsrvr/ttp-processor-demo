package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/analytics"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Example of using Phase 3 analytics and Parquet storage
func main() {
	// Initialize
	allocator := memory.NewGoAllocator()
	logger := logging.NewComponentLogger("analytics-example", "1.0.0")
	
	// Create analytics engine
	analyticsEngine := analytics.NewAnalyticsEngine(allocator, logger)
	ttpAnalytics := analytics.NewTTPAnalytics(analyticsEngine, logger)
	
	// Set up Parquet storage
	parquetConfig := &storage.ParquetConfig{
		BasePath:        "./parquet-data",
		PartitionBy:     []string{"date"},
		PartitionFormat: "2006/01/02",
		MaxFileSize:     128 * 1024 * 1024, // 128MB
		Compression:     "zstd",
	}
	
	parquetWriter, err := storage.NewParquetWriter(parquetConfig, allocator, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer parquetWriter.Close()
	
	// Create archiver for automatic storage
	archiver := storage.NewParquetArchiver(parquetWriter, 5*time.Minute, 1000, logger)
	archiver.Start()
	defer archiver.Stop()
	
	// Start payment volume analysis
	pvResult, err := ttpAnalytics.AnalyzePaymentVolume(time.Hour, "")
	if err != nil {
		log.Fatal(err)
	}
	
	// Connect to Arrow Flight server
	conn, err := grpc.Dial("localhost:8815", 
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	
	client := flight.NewClientFromConn(conn, nil)
	
	// Create ticket for streaming
	ticket := &flight.Ticket{
		Ticket: []byte("stellar_ledger:100000:100100"),
	}
	
	// Start streaming
	stream, err := client.DoGet(context.Background(), ticket)
	if err != nil {
		log.Fatal(err)
	}
	
	// Create record reader
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Release()
	
	// Process stream
	recordCount := 0
	for reader.Next() {
		record := reader.Record()
		recordCount++
		
		// Real-time analytics
		if err := analyticsEngine.ProcessRecord(record); err != nil {
			logger.Error().Err(err).Msg("Failed to process record for analytics")
		}
		
		// Archive to Parquet
		archiver.AddRecord(record)
		
		// Log progress
		if recordCount%10 == 0 {
			// Get current analytics results
			if data, err := ttpAnalytics.GetPaymentVolumeResults(pvResult); err == nil {
				logger.Info().
					Int("records_processed", recordCount).
					Interface("volumes", data.Volumes).
					Msg("Analytics update")
			}
		}
	}
	
	if err := reader.Err(); err != nil {
		logger.Error().Err(err).Msg("Stream error")
	}
	
	// Final results
	finalData, err := ttpAnalytics.GetPaymentVolumeResults(pvResult)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get final results")
	} else {
		fmt.Printf("\nFinal Payment Volume Analysis:\n")
		fmt.Printf("Window: %s to %s\n", finalData.WindowStart.Format(time.RFC3339), 
			finalData.WindowEnd.Format(time.RFC3339))
		fmt.Printf("Volumes by asset:\n")
		for asset, volume := range finalData.Volumes {
			fmt.Printf("  %s: %d\n", asset, volume)
		}
	}
	
	// Get Parquet stats
	stats := parquetWriter.GetStats()
	fmt.Printf("\nParquet Storage Stats:\n")
	fmt.Printf("Files written: %d\n", stats.FilesWritten)
	fmt.Printf("Bytes written: %d\n", stats.BytesWritten)
	fmt.Printf("Records written: %d\n", stats.RecordsWritten)
}

// Example of querying analytics programmatically
func queryAnalytics(engine *analytics.AnalyticsEngine) {
	// High-value payment detection
	hvpAgg, err := engine.CreateAggregation(
		"high_value_payments",
		analytics.AggregationCount,
		30*time.Minute,
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
		log.Printf("Failed to create aggregation: %v", err)
		return
	}
	
	// Let it run for a while, then get results
	time.Sleep(5 * time.Second)
	
	results, err := engine.GetResults(hvpAgg.ID)
	if err != nil {
		log.Printf("Failed to get results: %v", err)
		return
	}
	
	fmt.Printf("\nHigh-Value Payments (>1000 XLM) in last 30 minutes:\n")
	for account, group := range results.Groups {
		fmt.Printf("  Account %s: %d payments\n", account, group.Count)
	}
}

// Example of custom analytics aggregation
func customAggregation() {
	// This would be called from within the main processing loop
	
	// Example: Track payment patterns by hour of day
	// Useful for identifying peak payment times
	
	// Example: Monitor cross-border payments
	// Group by source and destination to identify corridors
	
	// Example: Asset adoption metrics
	// Track new assets being used over time
}

// Example configuration for production
func productionConfig() {
	// Analytics for different use cases:
	
	// 1. Real-time monitoring (small windows)
	// - 5-minute payment volumes
	// - 1-minute high-value alerts
	// - 15-minute trend detection
	
	// 2. Business intelligence (larger windows)  
	// - Hourly summaries
	// - Daily reports
	// - Weekly trends
	
	// 3. Parquet partitioning strategies:
	// - By date for time-series analysis
	// - By ledger range for replay
	// - By asset for asset-specific analysis
}