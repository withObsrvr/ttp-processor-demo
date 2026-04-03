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

	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s", cfg.Service.Name)
	log.Printf("Source: %s", cfg.Source.Endpoint)
	log.Printf("DuckLake catalog: %s", cfg.DuckLake.CatalogName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize DuckLake writer (DuckDB + ducklake extension + S3)
	writer, err := NewDuckLakeWriter(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize DuckLake: %v", err)
	}
	defer writer.Close()

	// Create processor
	processor := NewProcessor(cfg, writer)

	// Load checkpoint
	lastLedger, err := processor.LoadCheckpoint(ctx)
	if err != nil {
		log.Printf("Warning: checkpoint load failed: %v (starting fresh)", err)
	}
	startLedger := cfg.Source.StartLedger
	if lastLedger > 0 {
		startLedger = uint32(lastLedger) + 1
		log.Printf("Resuming from checkpoint: ledger %d", startLedger)
	}

	// Connect to gRPC source
	conn, err := grpc.Dial(
		cfg.Source.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC source: %v", err)
	}
	defer conn.Close()

	client := pb.NewRawLedgerServiceClient(conn)
	log.Printf("Connected to %s", cfg.Source.Endpoint)

	// Start periodic flush in background
	go processor.FlushLoop(ctx)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received...")
		cancel()
	}()

	// Stream and process ledgers — one at a time, real-time
	if err := streamAndProcess(ctx, client, processor, startLedger); err != nil {
		if ctx.Err() != nil {
			log.Println("Shutting down gracefully")
		} else {
			log.Fatalf("Stream error: %v", err)
		}
	}

	log.Println("Shutdown complete")
}

func streamAndProcess(ctx context.Context, client pb.RawLedgerServiceClient, processor *Processor, startLedger uint32) error {
	req := &pb.StreamLedgersRequest{
		StartLedger: startLedger,
	}

	stream, err := client.StreamRawLedgers(ctx, req)
	if err != nil {
		return fmt.Errorf("start stream: %w", err)
	}

	log.Printf("Streaming ledgers from %d (real-time, per-ledger processing)", startLedger)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rawLedger, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("stream recv: %w", err)
		}

		startTime := time.Now()

		if err := processor.ProcessLedger(ctx, rawLedger); err != nil {
			log.Printf("Error processing ledger %d: %v", rawLedger.Sequence, err)
			continue
		}

		duration := time.Since(startTime)
		if rawLedger.Sequence%100 == 0 {
			log.Printf("Processed ledger %d in %s", rawLedger.Sequence, duration.Round(time.Millisecond))
		}
	}
}
