package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/checkpoint"
	pb "github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/gen/network_topology_service"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/health"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/server"
)

const (
	defaultPort       = ":50055"
	defaultHealthPort = 8095
)

func main() {
	// gRPC port
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	// Health port
	healthPort := defaultHealthPort
	if p := os.Getenv("HEALTH_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			healthPort = v
		}
	}

	// Database URL
	dbURL := os.Getenv("STELLARBEAT_DATABASE_URL")
	if dbURL == "" {
		log.Fatal("STELLARBEAT_DATABASE_URL is required")
	}

	// Checkpoint path
	cpPath := os.Getenv("CHECKPOINT_PATH")
	if cpPath == "" {
		cpPath = "/var/lib/radar-network-source/checkpoint.json"
	}

	ctx := context.Background()

	// Connect to Stellarbeat PostgreSQL (read-only)
	dbpool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to Stellarbeat database: %v", err)
	}
	defer dbpool.Close()

	if err := dbpool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping Stellarbeat database: %v", err)
	}
	log.Printf("Connected to Stellarbeat database")

	// Initialize checkpoint
	cp, err := checkpoint.New(cpPath)
	if err != nil {
		log.Fatalf("Failed to initialize checkpoint: %v", err)
	}
	if lastID := cp.GetLastScanID(); lastID > 0 {
		log.Printf("Resuming from checkpoint: scan_id=%d", lastID)
	}

	// Start health server
	hs := health.NewServer(healthPort, cp)
	if err := hs.Start(); err != nil {
		log.Fatalf("Failed to start health server: %v", err)
	}
	log.Printf("Health server listening on :%d", healthPort)

	// Create gRPC server
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", port, err)
	}

	srv := server.New(dbpool, cp, hs)
	s := grpc.NewServer()
	pb.RegisterNetworkTopologyServiceServer(s, srv)

	log.Printf("radar-network-source listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
