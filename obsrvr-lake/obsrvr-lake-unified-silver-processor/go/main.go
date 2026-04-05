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

	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
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
	log.Printf("Bronze source: %s", cfg.Source.Endpoint)
	log.Printf("DuckLake catalog: %s", cfg.DuckLake.CatalogName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize DuckLake writer for silver schema
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

	// Connect to bronze processor's gRPC stream
	conn, err := grpc.NewClient(
		cfg.Source.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		log.Fatalf("Failed to connect to bronze source: %v", err)
	}
	defer conn.Close()

	client := pb.NewBronzeLedgerServiceClient(conn)
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

	// Stream and process
	if err := streamAndProcess(ctx, client, processor, startLedger); err != nil {
		if ctx.Err() != nil {
			log.Println("Shutting down gracefully")
		} else {
			log.Fatalf("Stream error: %v", err)
		}
	}

	log.Println("Shutdown complete")
}

func streamAndProcess(ctx context.Context, client pb.BronzeLedgerServiceClient, processor *Processor, startLedger uint32) error {
	req := &pb.StreamBronzeRequest{
		StartLedger: startLedger,
	}

	stream, err := client.StreamBronzeData(ctx, req)
	if err != nil {
		return fmt.Errorf("start stream: %w", err)
	}

	log.Printf("Streaming bronze data from ledger %d", startLedger)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		bronzeData, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("stream recv: %w", err)
		}

		startTime := time.Now()

		if err := processor.ProcessLedger(ctx, bronzeData); err != nil {
			log.Printf("Error processing ledger %d: %v", bronzeData.LedgerSequence, err)
			continue
		}

		duration := time.Since(startTime)
		if bronzeData.LedgerSequence%100 == 0 {
			log.Printf("Silver transform ledger %d in %s", bronzeData.LedgerSequence, duration.Round(time.Millisecond))
		}
	}
}
