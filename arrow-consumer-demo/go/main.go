package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Initialize zerolog
	zerolog.TimeFieldFormat = time.RFC3339
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("component", "arrow-consumer-demo").
		Logger()

	// Parse command line arguments
	if len(os.Args) < 3 {
		logger.Fatal().Msg("Usage: arrow-consumer-demo <start_ledger> <end_ledger> [arrow_source_endpoint]")
	}

	startLedger, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		logger.Fatal().Err(err).Msg("Invalid start_ledger")
	}

	endLedger, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		logger.Fatal().Err(err).Msg("Invalid end_ledger")
	}

	endpoint := "localhost:8815"
	if len(os.Args) > 3 {
		endpoint = os.Args[3]
	}

	logger.Info().
		Uint64("start_ledger", startLedger).
		Uint64("end_ledger", endLedger).
		Str("endpoint", endpoint).
		Msg("Starting Arrow consumer demo")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info().Msg("Received interrupt signal, shutting down...")
		cancel()
	}()

	// Connect to Arrow Flight server
	client, err := connectToArrowServer(ctx, endpoint, &logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to Arrow server")
	}

	// Create flight descriptor for stellar ledger data
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"stellar_ledger"},
	}

	// Get flight info to retrieve schema
	logger.Info().Msg("Getting flight info...")
	info, err := client.GetFlightInfo(ctx, descriptor)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to get flight info")
	}

	logger.Info().
		Int("endpoints", len(info.Endpoint)).
		Int64("total_records", info.TotalRecords).
		Msg("Retrieved flight info")

	// Create ticket for the ledger range
	ticket := &flight.Ticket{
		Ticket: []byte(fmt.Sprintf("stellar_ledger:%d:%d", startLedger, endLedger)),
	}

	// Start streaming data
	logger.Info().Msg("Starting data stream...")
	stream, err := client.DoGet(ctx, ticket)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start data stream")
	}

	// Create record reader from stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create record reader")
	}
	defer reader.Release()

	recordCount := int64(0)
	batchCount := 0
	startTime := time.Now()
	lastLogTime := time.Now()

	// Read records from the stream
	for reader.Next() {
		record := reader.Record()
		batchCount++
		recordCount += record.NumRows()

		// Log batch details
		logger.Info().
			Int64("batch_rows", record.NumRows()).
			Int64("batch_cols", record.NumCols()).
			Int("batch_number", batchCount).
			Msg("Received Arrow batch")

		// Process the batch (example: extract ledger sequences)
		if record.NumCols() > 0 && record.NumRows() > 0 {
			// First column should be ledger_sequence
			col := record.Column(0)
			if col.DataType().ID() == arrow.UINT32 {
				// Get first and last values
				firstLedger := col.(*array.Uint32).Value(0)
				lastLedger := col.(*array.Uint32).Value(int(record.NumRows() - 1))
				
				logger.Info().
					Uint32("first_ledger", firstLedger).
					Uint32("last_ledger", lastLedger).
					Int64("batch_size", record.NumRows()).
					Msg("Processing ledger batch")
			}

			// Example: Show other columns
			for i := 0; i < int(record.NumCols()); i++ {
				field := record.Schema().Field(i)
				logger.Debug().
					Int("col_index", i).
					Str("col_name", field.Name).
					Str("col_type", field.Type.String()).
					Msg("Column info")
			}
		}

		// Release the record to free memory
		record.Release()

		// Log progress periodically
		if time.Since(lastLogTime) > 5*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(recordCount) / elapsed.Seconds()
			
			logger.Info().
				Int("batches", batchCount).
				Int64("records", recordCount).
				Float64("records_per_second", rate).
				Dur("elapsed", elapsed).
				Msg("Stream progress")
			
			lastLogTime = time.Now()
		}
	}

	// Check for errors
	if err := reader.Err(); err != nil {
		logger.Error().Err(err).Msg("Reader error")
	}

	// Final statistics
	elapsed := time.Since(startTime)
	rate := float64(recordCount) / elapsed.Seconds()

	logger.Info().
		Int("total_batches", batchCount).
		Int64("total_records", recordCount).
		Float64("records_per_second", rate).
		Dur("total_duration", elapsed).
		Msg("Arrow consumer demo completed")
}

// connectToArrowServer establishes connection to Arrow Flight server
func connectToArrowServer(ctx context.Context, endpoint string, logger *zerolog.Logger) (flight.Client, error) {
	logger.Info().
		Str("endpoint", endpoint).
		Msg("Connecting to Arrow Flight server...")

	// Create gRPC connection
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial Arrow server: %w", err)
	}

	// Create Flight client
	client := flight.NewClientFromConn(conn, nil)
	
	logger.Info().
		Str("endpoint", endpoint).
		Msg("Successfully connected to Arrow Flight server")

	return client, nil
}