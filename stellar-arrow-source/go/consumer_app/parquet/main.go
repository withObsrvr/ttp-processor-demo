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
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line arguments
	if len(os.Args) < 3 {
		fmt.Println("Usage: parquet-consumer <arrow-server> <output-path> [start-ledger] [end-ledger]")
		fmt.Println("Example: parquet-consumer localhost:8815 ./parquet-data 100000 100100")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	outputPath := os.Args[2]
	
	var startLedger, endLedger uint32
	if len(os.Args) > 3 {
		start, err := strconv.ParseUint(os.Args[3], 10, 32)
		if err != nil {
			log.Fatalf("Invalid start ledger: %v", err)
		}
		startLedger = uint32(start)
	}
	
	if len(os.Args) > 4 {
		end, err := strconv.ParseUint(os.Args[4], 10, 32)
		if err != nil {
			log.Fatalf("Invalid end ledger: %v", err)
		}
		endLedger = uint32(end)
	}

	// Initialize
	logger := logging.NewComponentLogger("parquet-consumer", "1.0.0")
	allocator := memory.NewGoAllocator()
	
	logger.Info().
		Str("server", serverAddr).
		Str("output_path", outputPath).
		Uint32("start_ledger", startLedger).
		Uint32("end_ledger", endLedger).
		Msg("Starting Parquet consumer")

	// Configure Parquet writer
	parquetConfig := &storage.ParquetConfig{
		BasePath:        outputPath,
		PartitionBy:     []string{"date"},
		PartitionFormat: getEnvOrDefault("PARTITION_FORMAT", "2006/01/02"),
		MaxFileSize:     int64(getEnvAsInt("MAX_FILE_SIZE", 256*1024*1024)), // 256MB
		Compression:     getEnvOrDefault("COMPRESSION", "zstd"),
	}

	// Create Parquet writer
	parquetWriter, err := storage.NewParquetWriter(parquetConfig, allocator, logger)
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}
	defer parquetWriter.Close()

	// Create archiver for automatic batching
	archiveInterval := time.Duration(getEnvAsInt("ARCHIVE_INTERVAL_MINUTES", 5)) * time.Minute
	archiveBufferSize := getEnvAsInt("ARCHIVE_BUFFER_SIZE", 1000)
	
	archiver := storage.NewParquetArchiver(parquetWriter, archiveInterval, archiveBufferSize, logger)
	archiver.Start()
	defer archiver.Stop()

	// Connect to Arrow Flight server with increased message size
	maxMsgSize := 100 * 1024 * 1024 // 100MB
	conn, err := grpc.Dial(serverAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Arrow Flight server: %v", err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)

	// Create ticket for streaming
	ticketStr := fmt.Sprintf("stellar_ledger:%d:%d", startLedger, endLedger)
	ticket := &flight.Ticket{
		Ticket: []byte(ticketStr),
	}

	logger.Info().
		Str("ticket", ticketStr).
		Msg("Starting Arrow Flight stream")

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

	// Process stream in goroutine
	done := make(chan bool)
	recordCount := int64(0)
	startTime := time.Now()

	go func() {
		logger.Info().Msg("Started processing stream")
		for reader.Next() {
			record := reader.Record()
			recordCount += record.NumRows()
			
			// Add to archiver for batched writing
			archiver.AddRecord(record)
			
			// Log progress periodically (or every record if buffer size is small)
			if recordCount%10000 == 0 || archiveBufferSize <= 10 {
				logger.Info().
					Int64("records_processed", recordCount).
					Int64("rows_in_batch", record.NumRows()).
					Float64("records_per_second", float64(recordCount)/time.Since(startTime).Seconds()).
					Msg("Processing progress")
			}
		}
		
		if err := reader.Err(); err != nil {
			logger.Error().Err(err).Msg("Stream error")
		}
		
		done <- true
	}()

	// Wait for completion or interrupt
	select {
	case <-done:
		logger.Info().
			Int64("total_records", recordCount).
			Dur("duration", time.Since(startTime)).
			Msg("Stream completed")
	case <-sigChan:
		logger.Info().Msg("Received interrupt signal")
	}

	// Get final stats
	stats := parquetWriter.GetStats()
	logger.Info().
		Int64("files_written", stats.FilesWritten).
		Int64("bytes_written", stats.BytesWritten).
		Int64("records_written", stats.RecordsWritten).
		Msg("Parquet consumer completed")
}

// Helper functions
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}