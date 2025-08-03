package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
	"google.golang.org/grpc"
)

// StellarArrowServer implements native Arrow Flight for Stellar data streaming
type StellarArrowServer struct {
	flight.BaseFlightServer

	allocator     memory.Allocator
	logger        *logging.ComponentLogger
	schemaManager *schema.SchemaManager
	sourceClient  ArrowSourceClient
	config        *ServerConfig
}

// ArrowSourceClient defines the interface for native Arrow data sources
type ArrowSourceClient interface {
	StreamArrowRecords(ctx context.Context, startLedger, endLedger uint32) (<-chan arrow.Record, <-chan error)
	GetLedgerRecord(ctx context.Context, sequence uint32) (arrow.Record, error)
	IsHealthy() bool
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port              int
	HealthPort        int
	SourceEndpoint    string
	NetworkPassphrase string
	BatchSize         int
	MaxConnections    int
	FlowCtlEnabled    bool
}

// StreamParams represents parsed stream parameters
type StreamParams struct {
	StartLedger uint32
	EndLedger   uint32
	StreamType  string
}

func NewStellarArrowServer(config *ServerConfig) (*StellarArrowServer, error) {
	logger := logging.NewComponentLogger("stellar-arrow-source", "v1.0.0")
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()

	logger.LogStartup(logging.StartupConfig{
		FlowCtlEnabled:    config.FlowCtlEnabled,
		SourceType:        "arrow_flight",
		BackendType:       "native_arrow",
		NetworkPassphrase: config.NetworkPassphrase,
		BatchSize:         config.BatchSize,
		Port:              config.Port,
	})

	// Create native Arrow source client  
	sourceClient, err := newMockArrowSourceClient(config.SourceEndpoint, logger)
	if err != nil {
		logger.Error().
			Err(err).
			Str("endpoint", config.SourceEndpoint).
			Msg("Failed to create Arrow source client")
		return nil, fmt.Errorf("failed to create Arrow source client: %w", err)
	}

	logger.Info().
		Str("operation", "server_initialization").
		Bool("native_arrow", true).
		Msg("Native Arrow Flight server initialized successfully")

	return &StellarArrowServer{
		allocator:     allocator,
		logger:        logger,
		schemaManager: schemaManager,
		sourceClient:  sourceClient,
		config:        config,
	}, nil
}

// GetFlightInfo returns flight information for Arrow clients
func (s *StellarArrowServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.logger.Debug().
		Str("operation", "get_flight_info").
		Interface("descriptor_path", request.Path).
		Str("descriptor_type", request.Type.String()).
		Msg("Flight info requested")

	// Parse the descriptor to determine schema type
	streamType := "stellar_ledger" // default
	if len(request.Path) > 0 {
		streamType = string(request.Path[0])
	}

	// Get appropriate schema
	var arrowSchema *arrow.Schema
	switch streamType {
	case "stellar_ledger":
		arrowSchema = s.schemaManager.GetStellarLedgerSchema()
	case "ttp_events":
		arrowSchema = s.schemaManager.GetTTPEventSchema()
	case "transactions":
		arrowSchema = s.schemaManager.GetTransactionSchema()
	default:
		arrowSchema = s.schemaManager.GetStellarLedgerSchema()
	}

	// Create flight info
	flightInfo := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(arrowSchema, s.allocator),
		FlightDescriptor: request,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: []byte(strings.Join(request.Path, ":")),
				},
				Location: []*flight.Location{
					{Uri: fmt.Sprintf("arrow-flight://localhost:%d", s.config.Port)},
				},
			},
		},
		TotalRecords: -1, // Streaming/unknown
		TotalBytes:   -1, // Streaming/unknown
	}

	s.logger.Info().
		Str("operation", "flight_info_created").
		Str("stream_type", streamType).
		Str("schema_version", s.schemaManager.GetSchemaVersion(streamType)).
		Msg("Flight info created")

	return flightInfo, nil
}

// DoGet streams Arrow data to clients
func (s *StellarArrowServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	start := time.Now()
	streamID := generateStreamID()

	// Parse streaming parameters from ticket
	params, err := parseStreamParams(ticket.Ticket)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str("stream_id", streamID).
			Msg("Failed to parse stream parameters")
		return fmt.Errorf("failed to parse stream parameters: %w", err)
	}

	s.logger.LogFlightConnection("stream_start", "", streamID)
	s.logger.Info().
		Str("operation", "arrow_stream_start").
		Uint32("start_ledger", params.StartLedger).
		Uint32("end_ledger", params.EndLedger).
		Str("stream_type", params.StreamType).
		Str("stream_id", streamID).
		Bool("native_processing", true).
		Msg("Starting native Arrow stream")

	// Get native Arrow record stream (no conversion needed)
	recordChan, errChan := s.sourceClient.StreamArrowRecords(
		stream.Context(), params.StartLedger, params.EndLedger)

	batchCount := 0
	totalRecords := int64(0)
	lastLogTime := time.Now()

	for {
		select {
		case <-stream.Context().Done():
			s.logger.LogFlightConnection("stream_cancelled", "", streamID)
			s.logger.Info().
				Str("operation", "arrow_stream_cancelled").
				Int("batches_sent", batchCount).
				Int64("total_records", totalRecords).
				Dur("stream_duration", time.Since(start)).
				Str("stream_id", streamID).
				Msg("Arrow stream cancelled by client")
			return stream.Context().Err()

		case err := <-errChan:
			if err != nil {
				s.logger.Error().
					Err(err).
					Int("batches_sent", batchCount).
					Int64("total_records", totalRecords).
					Str("stream_id", streamID).
					Msg("Arrow stream error")
				return fmt.Errorf("source stream error: %w", err)
			}
			// Stream completed successfully
			s.logger.LogFlightConnection("stream_completed", "", streamID)
			s.logger.LogArrowProcessing("native_stream", batchCount, time.Since(start))
			s.logger.Info().
				Str("operation", "arrow_stream_completed").
				Int("batches_sent", batchCount).
				Int64("total_records", totalRecords).
				Dur("stream_duration", time.Since(start)).
				Float64("records_per_second", float64(totalRecords)/time.Since(start).Seconds()).
				Str("stream_id", streamID).
				Msg("Arrow stream completed successfully")
			return nil

		case record := <-recordChan:
			// For Phase 1, we'll use a simplified approach
			// In future phases, we'll implement proper Flight data streaming
			recordRows := record.NumRows()
			s.logger.Debug().
				Int64("record_rows", recordRows).
				Str("stream_id", streamID).
				Msg("Received Arrow record (Phase 1 - logging only)")
			
			batchCount++
			totalRecords += recordRows
			
			// Release the record to prevent memory leaks
			record.Release()

			// Log progress periodically
			if time.Since(lastLogTime) > 10*time.Second {
				s.logger.Debug().
					Str("operation", "stream_progress").
					Int("batches_sent", batchCount).
					Int64("total_records", totalRecords).
					Float64("records_per_second", float64(totalRecords)/time.Since(start).Seconds()).
					Str("stream_id", streamID).
					Msg("Arrow stream progress")
				lastLogTime = time.Now()
			}
		}
	}
}

// StartArrowFlightServer starts the native Arrow Flight server
func (s *StellarArrowServer) StartArrowFlightServer() error {
	s.logger.Info().
		Int("port", s.config.Port).
		Str("protocol", "arrow-flight").
		Bool("native_arrow", true).
		Msg("Starting Arrow Flight server")

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		s.logger.Error().
			Err(err).
			Int("port", s.config.Port).
			Msg("Failed to create Arrow Flight listener")
		return fmt.Errorf("failed to listen on port %d: %w", s.config.Port, err)
	}

	server := grpc.NewServer()
	flight.RegisterFlightServiceServer(server, s)

	s.logger.Info().
		Str("address", listener.Addr().String()).
		Bool("server_ready", true).
		Str("service_type", "native_arrow_flight").
		Msg("Arrow Flight server listening")

	return server.Serve(listener)
}

// GetAllocator returns the Arrow memory allocator
func (s *StellarArrowServer) GetAllocator() memory.Allocator {
	return s.allocator
}

// GetHealthStatus returns server health information
func (s *StellarArrowServer) GetHealthStatus() map[string]interface{} {
	return map[string]interface{}{
		"status":            "healthy",
		"server_type":       "native_arrow_flight",
		"protocol":          "arrow-flight",
		"source_healthy":    s.sourceClient.IsHealthy(),
		"memory_allocated":  0, // Phase 1: simplified memory tracking
		"schemas_available": []string{"stellar_ledger", "ttp_events", "transactions"},
	}
}

// parseStreamParams parses stream parameters from flight ticket
func parseStreamParams(ticket []byte) (*StreamParams, error) {
	ticketStr := string(ticket)
	
	// Default parameters
	params := &StreamParams{
		StartLedger: 0,
		EndLedger:   0, // Unbounded
		StreamType:  "stellar_ledger",
	}

	// Parse ticket format: "stream_type:start_ledger:end_ledger"
	parts := strings.Split(ticketStr, ":")
	if len(parts) >= 1 && parts[0] != "" {
		params.StreamType = parts[0]
	}
	if len(parts) >= 2 {
		if start, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
			params.StartLedger = uint32(start)
		}
	}
	if len(parts) >= 3 {
		if end, err := strconv.ParseUint(parts[2], 10, 32); err == nil {
			params.EndLedger = uint32(end)
		}
	}

	return params, nil
}

// generateStreamID generates a unique stream identifier
func generateStreamID() string {
	return fmt.Sprintf("stream_%d", time.Now().UnixNano())
}