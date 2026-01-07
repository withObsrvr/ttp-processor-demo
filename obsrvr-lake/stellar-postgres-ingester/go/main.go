package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s", cfg.Service.Name)
	log.Printf("Source endpoint: %s", cfg.Source.Endpoint)
	log.Printf("PostgreSQL: %s:%d/%s", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize checkpoint
	checkpoint, err := NewCheckpoint(cfg.Checkpoint.FilePath)
	if err != nil {
		log.Fatalf("Failed to initialize checkpoint: %v", err)
	}

	// Determine start ledger
	startLedger := cfg.Source.StartLedger
	if lastLedger := checkpoint.GetLastLedger(); lastLedger > 0 {
		startLedger = lastLedger + 1
		log.Printf("Resuming from checkpoint: ledger %d", startLedger)
	} else {
		log.Printf("Starting fresh from ledger %d", startLedger)
	}

	// Connect to PostgreSQL
	dbpool, err := pgxpool.New(ctx, cfg.GetPostgresConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer dbpool.Close()

	// Test database connection
	if err := dbpool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}
	log.Printf("Connected to PostgreSQL successfully")

	// Start health server
	healthServer := NewHealthServer(cfg.Service.HealthPort, checkpoint)
	if err := healthServer.Start(); err != nil {
		log.Fatalf("Failed to start health server: %v", err)
	}
	defer healthServer.Stop()
	log.Printf("Health server started on port %d", cfg.Service.HealthPort)

	// Create writer
	writer := NewWriter(dbpool, cfg, checkpoint, healthServer)

	// Connect to gRPC source
	conn, err := grpc.Dial(
		cfg.Source.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)), // 100MB
	)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC source: %v", err)
	}
	defer conn.Close()

	client := pb.NewRawLedgerServiceClient(conn)
	log.Printf("Connected to stellar-live-source-datalake at %s", cfg.Source.Endpoint)

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start streaming ledgers
	go func() {
		if err := streamLedgers(ctx, client, writer, startLedger, cfg.Source.EndLedger); err != nil {
			log.Printf("Stream error: %v", err)
			cancel()
		}
	}()

	// Wait for shutdown signal
	select {
	case <-sigChan:
		log.Println("Received shutdown signal")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}

	// Graceful shutdown
	log.Println("Shutting down gracefully...")
	cancel()
	time.Sleep(2 * time.Second) // Allow in-flight operations to complete

	// Save final checkpoint
	if err := checkpoint.Save(); err != nil {
		log.Printf("Warning: Failed to save final checkpoint: %v", err)
	}

	log.Println("Shutdown complete")
}

// streamLedgers streams ledgers from the gRPC source and processes them
func streamLedgers(ctx context.Context, client pb.RawLedgerServiceClient, writer *Writer, startLedger, endLedger uint32) error {
	req := &pb.StreamLedgersRequest{
		StartLedger: startLedger,
	}

	stream, err := client.StreamRawLedgers(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	log.Printf("Streaming ledgers from %d to %d (0 = continuous)", startLedger, endLedger)

	// Batch processing
	batch := make([]*pb.RawLedger, 0, writer.config.Postgres.BatchSize)
	lastCommit := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch before shutdown
			if len(batch) > 0 {
				if err := writer.WriteBatch(ctx, batch); err != nil {
					log.Printf("Error writing final batch: %v", err)
				}
			}
			return ctx.Err()

		default:
			ledger, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" || ctx.Err() != nil {
					log.Println("Stream ended")
					return nil
				}
				return fmt.Errorf("stream receive error: %w", err)
			}

			batch = append(batch, ledger)

			// Check if we should commit
			shouldCommit := len(batch) >= writer.config.Postgres.BatchSize ||
				time.Since(lastCommit).Seconds() >= float64(writer.config.Postgres.CommitIntervalSeconds)

			if shouldCommit {
				if err := writer.WriteBatch(ctx, batch); err != nil {
					log.Printf("Error writing batch: %v", err)
					writer.healthServer.RecordError(err)

					// Retry logic
					retries := 0
					for retries < writer.config.Postgres.MaxRetries {
						retries++
						log.Printf("Retrying batch write (attempt %d/%d)", retries, writer.config.Postgres.MaxRetries)
						time.Sleep(time.Second * time.Duration(retries))

						if err := writer.WriteBatch(ctx, batch); err == nil {
							log.Printf("Batch write succeeded on retry %d", retries)
							break
						}
					}

					if retries >= writer.config.Postgres.MaxRetries {
						return fmt.Errorf("failed to write batch after %d retries: %w", retries, err)
					}
				}

				// Reset batch
				batch = batch[:0]
				lastCommit = time.Now()
			}
		}
	}
}
