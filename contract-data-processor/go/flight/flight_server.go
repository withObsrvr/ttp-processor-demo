package flight

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/processor"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ContractDataFlightServer implements Apache Arrow Flight server for contract data
type ContractDataFlightServer struct {
	flight.BaseFlightServer
	
	logger        *logging.ComponentLogger
	allocator     memory.Allocator
	schemaManager *schema.SchemaManager
	
	// Active streams
	mu      sync.RWMutex
	streams map[string]*DataStream
}

// DataStream represents an active data stream
type DataStream struct {
	ID            string
	Schema        *arrow.Schema
	BatchChannel  chan arrow.Record
	ErrorChannel  chan error
	CancelFunc    context.CancelFunc
	CreatedAt     time.Time
	EntriesServed uint64
	BytesServed   uint64
}

// StreamDescriptor describes available streams
type StreamDescriptor struct {
	Path        []string
	Description string
	Schema      *arrow.Schema
}

// NewContractDataFlightServer creates a new Flight server
func NewContractDataFlightServer(logger *logging.ComponentLogger) *ContractDataFlightServer {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager(allocator)
	
	return &ContractDataFlightServer{
		logger:        logger,
		allocator:     allocator,
		schemaManager: schemaManager,
		streams:       make(map[string]*DataStream),
	}
}

// GetFlightInfo returns information about available data streams
func (s *ContractDataFlightServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.logger.Debug().
		Interface("descriptor", desc).
		Msg("GetFlightInfo called")
	
	// Get contract data schema
	contractSchema := s.schemaManager.GetContractDataSchema()
	
	// Create endpoint
	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{
			Ticket: []byte("contract-data-stream"),
		},
	}
	
	// Create flight info
	info := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(contractSchema.Schema, s.allocator),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     -1, // Unknown/streaming
		TotalBytes:       -1, // Unknown/streaming
	}
	
	return info, nil
}

// DoGet streams contract data to clients
func (s *ContractDataFlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	streamID := string(tkt.Ticket)
	
	s.logger.Info().
		Str("stream_id", streamID).
		Msg("DoGet called for stream")
	
	// Get or create stream
	s.mu.Lock()
	dataStream, exists := s.streams[streamID]
	if !exists {
		// Create new stream
		ctx, cancel := context.WithCancel(context.Background())
		dataStream = &DataStream{
			ID:           streamID,
			Schema:       s.schemaManager.GetContractDataSchema().Schema,
			BatchChannel: make(chan arrow.Record, 100),
			ErrorChannel: make(chan error, 1),
			CancelFunc:   cancel,
			CreatedAt:    time.Now(),
		}
		s.streams[streamID] = dataStream
	}
	s.mu.Unlock()
	
	// Send schema as first message
	writer := flight.NewRecordWriter(stream, flight.WithSchema(dataStream.Schema))
	defer writer.Close()
	
	// Stream data
	for {
		select {
		case record, ok := <-dataStream.BatchChannel:
			if !ok {
				// Channel closed, stream is done
				return nil
			}
			
			// Write record
			if err := writer.Write(record); err != nil {
				s.logger.Error().
					Err(err).
					Str("stream_id", streamID).
					Msg("Failed to write record")
				return err
			}
			
			// Update metrics
			dataStream.EntriesServed += uint64(record.NumRows())
			dataStream.BytesServed += calculateRecordSize(record)
			
			// Release record after sending
			record.Release()
			
		case err := <-dataStream.ErrorChannel:
			return err
			
		case <-stream.Context().Done():
			// Client disconnected
			s.logger.Info().
				Str("stream_id", streamID).
				Msg("Client disconnected from stream")
			return nil
		}
	}
}

// ListFlights lists available data streams
func (s *ContractDataFlightServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	s.logger.Debug().Msg("ListFlights called")
	
	// Get contract data schema
	contractSchema := s.schemaManager.GetContractDataSchema()
	
	// Create flight info for contract data stream
	info := &flight.FlightInfo{
		Schema: flight.SerializeSchema(contractSchema.Schema, s.allocator),
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.FlightDescriptor_PATH,
			Path: []string{"contract", "data"},
		},
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{
				Ticket: []byte("contract-data-stream"),
			},
		}},
		TotalRecords: -1, // Streaming
		TotalBytes:   -1, // Streaming
	}
	
	if err := stream.Send(info); err != nil {
		return err
	}
	
	return nil
}

// GetSchema returns the schema for a given stream
func (s *ContractDataFlightServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	s.logger.Debug().
		Interface("descriptor", desc).
		Msg("GetSchema called")
	
	// Get contract data schema
	contractSchema := s.schemaManager.GetContractDataSchema()
	
	return &flight.SchemaResult{
		Schema: flight.SerializeSchema(contractSchema.Schema, s.allocator),
	}, nil
}

// AddBatch adds a new batch to a stream
func (s *ContractDataFlightServer) AddBatch(streamID string, record arrow.Record) error {
	s.mu.RLock()
	stream, exists := s.streams[streamID]
	s.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("stream %s not found", streamID)
	}
	
	select {
	case stream.BatchChannel <- record:
		return nil
	default:
		// Channel full, drop oldest batch
		select {
		case oldRecord := <-stream.BatchChannel:
			oldRecord.Release()
		default:
		}
		
		// Try again
		select {
		case stream.BatchChannel <- record:
			return nil
		default:
			return fmt.Errorf("failed to add batch to stream %s", streamID)
		}
	}
}

// CloseStream closes a data stream
func (s *ContractDataFlightServer) CloseStream(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if stream, exists := s.streams[streamID]; exists {
		stream.CancelFunc()
		close(stream.BatchChannel)
		close(stream.ErrorChannel)
		delete(s.streams, streamID)
		
		s.logger.Info().
			Str("stream_id", streamID).
			Uint64("entries_served", stream.EntriesServed).
			Uint64("bytes_served", stream.BytesServed).
			Msg("Closed data stream")
	}
}

// GetStreamMetrics returns metrics for a stream
func (s *ContractDataFlightServer) GetStreamMetrics(streamID string) (map[string]interface{}, error) {
	s.mu.RLock()
	stream, exists := s.streams[streamID]
	s.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}
	
	return map[string]interface{}{
		"entries_served": stream.EntriesServed,
		"bytes_served":   stream.BytesServed,
		"created_at":     stream.CreatedAt,
		"duration":       time.Since(stream.CreatedAt).Seconds(),
	}, nil
}

// Serve starts the Flight server
func (s *ContractDataFlightServer) Serve(address string) error {
	listener, err := flight.NewFlightServer(&flight.ServerOpts{
		Address: address,
	})
	if err != nil {
		return fmt.Errorf("failed to create flight server: %w", err)
	}
	
	listener.RegisterFlightService(s)
	
	s.logger.Info().
		Str("address", address).
		Msg("Starting Arrow Flight server")
	
	return listener.Serve()
}

// calculateRecordSize estimates the size of an Arrow record in bytes
func calculateRecordSize(record arrow.Record) uint64 {
	var size uint64
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		size += uint64(col.Len()) * uint64(col.DataType().Layout().Buffers[0].ByteWidth)
	}
	return size
}

// Unimplemented methods
func (s *ContractDataFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not implemented")
}

func (s *ContractDataFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return status.Error(codes.Unimplemented, "DoExchange not implemented")
}

func (s *ContractDataFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	return status.Error(codes.Unimplemented, "DoAction not implemented")
}

func (s *ContractDataFlightServer) ListActions(stream flight.FlightService_ListActionsServer) error {
	return status.Error(codes.Unimplemented, "ListActions not implemented")
}