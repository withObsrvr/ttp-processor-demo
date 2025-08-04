package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/flight"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	pb "github.com/withObsrvr/ttp-processor-demo/contract-data-processor/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// HybridServer combines gRPC control plane and Arrow Flight data plane
type HybridServer struct {
	config       *config.Config
	logger       *logging.ComponentLogger
	
	// Servers
	grpcServer   *grpc.Server
	flightServer *flight.ContractDataFlightServer
	httpServer   *http.Server
	
	// Components
	dataSource     *DataSourceClient
	controlServer  *ControlServer
	coordinator    *ProcessingCoordinator
	healthServer   *health.Server
	flowctl        *FlowctlController
	
	// Lifecycle
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewHybridServer creates a new hybrid server
func NewHybridServer(cfg *config.Config, logger *logging.ComponentLogger) (*HybridServer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create components
	dataSource := NewDataSourceClient(cfg, logger)
	flightServer := flight.NewContractDataFlightServer(logger)
	coordinator := NewProcessingCoordinator(cfg, logger, dataSource, flightServer)
	controlServer := NewControlServer(cfg, logger, dataSource, coordinator)
	
	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024 * 64), // 64MB
		grpc.MaxSendMsgSize(1024 * 1024 * 64), // 64MB
	)
	
	// Register services
	pb.RegisterControlServiceServer(grpcServer, controlServer)
	
	// Health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	
	// Reflection for debugging
	reflection.Register(grpcServer)
	
	// HTTP server for metrics
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", handleHealth)
	
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HealthPort),
		Handler: mux,
	}
	
	// Create flowctl controller
	flowctl := NewFlowctlController(cfg, logger)
	
	return &HybridServer{
		config:         cfg,
		logger:         logger,
		grpcServer:     grpcServer,
		flightServer:   flightServer,
		httpServer:     httpServer,
		dataSource:     dataSource,
		controlServer:  controlServer,
		coordinator:    coordinator,
		healthServer:   healthServer,
		flowctl:        flowctl,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// Start starts all server components
func (s *HybridServer) Start() error {
	s.logger.Info().Msg("Starting hybrid server")
	
	// Connect to data source
	if err := s.dataSource.Connect(s.ctx); err != nil {
		return fmt.Errorf("failed to connect to data source: %w", err)
	}
	
	// Start gRPC control plane
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.startGRPC(); err != nil {
			s.logger.Error().Err(err).Msg("gRPC server error")
		}
	}()
	
	// Start Arrow Flight data plane
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.startFlight(); err != nil {
			s.logger.Error().Err(err).Msg("Flight server error")
		}
	}()
	
	// Start HTTP server for health/metrics
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.startHTTP(); err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("HTTP server error")
		}
	}()
	
	// Set health status
	s.healthServer.SetServingStatus("contractdata.v1.ControlService", grpc_health_v1.HealthCheckResponse_SERVING)
	s.healthServer.SetServingStatus("arrow.flight.protocol.FlightService", grpc_health_v1.HealthCheckResponse_SERVING)
	
	s.logger.Info().
		Str("grpc_address", s.config.GRPCAddress).
		Str("flight_address", s.config.FlightAddress).
		Int("health_port", s.config.HealthPort).
		Msg("Hybrid server started successfully")
	
	return nil
}

// Stop gracefully stops all server components
func (s *HybridServer) Stop() error {
	s.logger.Info().Msg("Stopping hybrid server")
	
	// Set health status to not serving
	s.healthServer.SetServingStatus("contractdata.v1.ControlService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.healthServer.SetServingStatus("arrow.flight.protocol.FlightService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	
	// Cancel context
	s.cancel()
	
	// Stop servers
	s.grpcServer.GracefulStop()
	
	// Stop HTTP server
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		s.logger.Error().Err(err).Msg("Error shutting down HTTP server")
	}
	
	// Disconnect from data source
	s.dataSource.Disconnect()
	
	// Wait for all goroutines
	s.wg.Wait()
	
	s.logger.Info().Msg("Hybrid server stopped")
	return nil
}

// startGRPC starts the gRPC control plane server
func (s *HybridServer) startGRPC() error {
	listener, err := net.Listen("tcp", s.config.GRPCAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.GRPCAddress, err)
	}
	
	s.logger.Info().
		Str("address", s.config.GRPCAddress).
		Msg("Starting gRPC control plane")
	
	return s.grpcServer.Serve(listener)
}

// startFlight starts the Arrow Flight data plane server
func (s *HybridServer) startFlight() error {
	s.logger.Info().
		Str("address", s.config.FlightAddress).
		Msg("Starting Arrow Flight data plane")
	
	return s.flightServer.Serve(s.config.FlightAddress)
}

// startHTTP starts the HTTP server for health/metrics
func (s *HybridServer) startHTTP() error {
	s.logger.Info().
		Int("port", s.config.HealthPort).
		Msg("Starting HTTP server for health/metrics")
	
	return s.httpServer.ListenAndServe()
}

// handleHealth handles HTTP health check requests
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"contract-data-processor"}`))
}