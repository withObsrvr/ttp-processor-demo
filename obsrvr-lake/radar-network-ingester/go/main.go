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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-ingester/go/checkpoint"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-ingester/go/health"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-ingester/go/writer"
	pb "github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/gen/network_topology_service"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting %s", cfg.Service.Name)
	log.Printf("Source endpoint: %s", cfg.Source.Endpoint)
	log.Printf("PostgreSQL: %s:%d/%s", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize checkpoint
	cp, err := checkpoint.New(cfg.Checkpoint.FilePath)
	if err != nil {
		log.Fatalf("Failed to initialize checkpoint: %v", err)
	}

	// Determine start scan ID
	startScanID := cfg.Source.StartScanID
	if lastID := cp.GetLastScanID(); lastID > 0 {
		startScanID = lastID
		log.Printf("Resuming from checkpoint: scan_id=%d", startScanID)
	} else {
		log.Printf("Starting from scan_id=%d (0 = latest)", startScanID)
	}

	// Connect to PostgreSQL (stellar_hot)
	dbpool, err := pgxpool.New(ctx, cfg.GetPostgresConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer dbpool.Close()

	if err := dbpool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}
	log.Printf("Connected to PostgreSQL successfully")

	// Start health server
	hs := health.NewServer(cfg.Service.HealthPort, cp)
	if err := hs.Start(); err != nil {
		log.Fatalf("Failed to start health server: %v", err)
	}
	defer hs.Stop()
	log.Printf("Health server started on port %d", cfg.Service.HealthPort)

	// Create writer
	w := writer.New(dbpool)

	// Connect to gRPC source
	conn, err := grpc.Dial(
		cfg.Source.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)), // 100MB
	)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC source: %v", err)
	}
	defer conn.Close()

	client := pb.NewNetworkTopologyServiceClient(conn)
	log.Printf("Connected to radar-network-source at %s", cfg.Source.Endpoint)

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start streaming
	go func() {
		if err := streamSnapshots(ctx, client, w, cp, hs, startScanID, cfg.Postgres.MaxRetries); err != nil {
			log.Printf("Stream error: %v", err)
			cancel()
		}
	}()

	select {
	case <-sigChan:
		log.Println("Received shutdown signal")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}

	log.Println("Shutting down gracefully...")
	cancel()
	time.Sleep(2 * time.Second)

	if err := cp.Save(); err != nil {
		log.Printf("Warning: Failed to save final checkpoint: %v", err)
	}
	log.Println("Shutdown complete")
}

func streamSnapshots(
	ctx context.Context,
	client pb.NetworkTopologyServiceClient,
	w *writer.Writer,
	cp *checkpoint.Checkpoint,
	hs *health.Server,
	startScanID uint32,
	maxRetries int,
) error {
	req := &pb.StreamSnapshotsRequest{
		StartScanId: startScanID,
	}

	stream, err := client.StreamNetworkSnapshots(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	log.Printf("Streaming network snapshots from scan_id=%d", startScanID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		snapshot, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("stream receive error: %w", err)
		}

		// Write snapshot with retries
		var writeErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				log.Printf("Retrying write for scan_id=%d (attempt %d/%d)", snapshot.ScanId, attempt, maxRetries)
				time.Sleep(time.Second * time.Duration(attempt))
			}

			writeErr = w.WriteSnapshot(ctx, snapshot)
			if writeErr == nil {
				break
			}
			hs.RecordError(writeErr)
		}

		if writeErr != nil {
			return fmt.Errorf("failed to write scan_id=%d after %d retries: %w", snapshot.ScanId, maxRetries, writeErr)
		}

		cp.Update(snapshot.ScanId, uint64(len(snapshot.NodeMeasurements)), uint64(len(snapshot.OrgMeasurements)))
		if err := cp.Save(); err != nil {
			log.Printf("Warning: checkpoint save failed: %v", err)
		}
	}
}
