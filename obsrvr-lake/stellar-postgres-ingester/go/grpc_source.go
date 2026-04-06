package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// EventTypeBronzeLedgerCommitted is the flowctl event type for bronze ledger commits
	EventTypeBronzeLedgerCommitted = "stellar.bronze.ledger_committed"

	// subscriberBufferSize is the channel buffer per subscriber
	subscriberBufferSize = 100
)

// BronzeBatchInfo contains metadata about a committed bronze batch
type BronzeBatchInfo struct {
	StartLedger uint32
	EndLedger   uint32
	ClosedAt    time.Time
	TxCount     uint64
	OpCount     uint64
}

// BronzeSourceServer implements flowctl.v1.SourceService for the bronze ingester.
// It broadcasts ledger-committed events to downstream consumers (e.g., silver transformer).
type BronzeSourceServer struct {
	flowctlv1.UnimplementedSourceServiceServer

	mu          sync.RWMutex
	subscribers map[uint64]chan *flowctlv1.Event
	nextID      uint64
	checkpoint  *Checkpoint
}

// NewBronzeSourceServer creates a new source server
func NewBronzeSourceServer(checkpoint *Checkpoint) *BronzeSourceServer {
	return &BronzeSourceServer{
		subscribers: make(map[uint64]chan *flowctlv1.Event),
		checkpoint:  checkpoint,
	}
}

// Register registers the service with a gRPC server
func (s *BronzeSourceServer) Register(srv *grpc.Server) {
	flowctlv1.RegisterSourceServiceServer(srv, s)
}

// GetInfo returns component information
func (s *BronzeSourceServer) GetInfo(_ context.Context, _ *emptypb.Empty) (*flowctlv1.ComponentInfo, error) {
	return &flowctlv1.ComponentInfo{
		Id:               "stellar-postgres-ingester",
		Name:             "Stellar PostgreSQL Bronze Ingester",
		Description:      "Ingests raw Stellar ledger data and writes to bronze PostgreSQL",
		Version:          "1.0.0",
		Type:             flowctlv1.ComponentType_COMPONENT_TYPE_SOURCE,
		OutputEventTypes: []string{EventTypeBronzeLedgerCommitted},
	}, nil
}

// HealthCheck returns health status
func (s *BronzeSourceServer) HealthCheck(_ context.Context, _ *flowctlv1.HealthCheckRequest) (*flowctlv1.HealthCheckResponse, error) {
	s.mu.RLock()
	subCount := len(s.subscribers)
	s.mu.RUnlock()

	return &flowctlv1.HealthCheckResponse{
		Status:  flowctlv1.HealthStatus_HEALTH_STATUS_HEALTHY,
		Message: fmt.Sprintf("%d active subscribers", subCount),
	}, nil
}

// StreamEvents streams ledger-committed events to a subscriber.
// The subscriber receives an Event each time a bronze batch is committed to PostgreSQL.
func (s *BronzeSourceServer) StreamEvents(req *flowctlv1.StreamRequest, stream grpc.ServerStreamingServer[flowctlv1.Event]) error {
	// Parse optional start_ledger from params
	var startLedger uint64
	if req.Params != nil {
		if v, ok := req.Params["start_ledger"]; ok {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid start_ledger %q: %w", v, err)
			}
			startLedger = n
		}
	}

	// Register subscriber
	ch := make(chan *flowctlv1.Event, subscriberBufferSize)
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.subscribers[id] = ch
	s.mu.Unlock()

	log.Printf("[grpc-source] Subscriber %d connected (start_ledger=%d)", id, startLedger)

	// Cleanup on disconnect
	defer func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
		log.Printf("[grpc-source] Subscriber %d disconnected", id)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			// Filter by start_ledger if specified
			if startLedger > 0 && event.StellarCursor != nil {
				if event.StellarCursor.LedgerSequence < startLedger {
					continue
				}
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

// GetState returns the current ingestion state (last checkpoint)
func (s *BronzeSourceServer) GetState(_ context.Context, _ *flowctlv1.StateRequest) (*flowctlv1.StateResponse, error) {
	lastLedger, _, _, _ := s.checkpoint.GetStats()
	return &flowctlv1.StateResponse{
		State: map[string]string{
			"last_ledger": fmt.Sprintf("%d", lastLedger),
		},
		LastUpdatedUnix: time.Now().Unix(),
	}, nil
}

// SaveState is a no-op for this source (checkpoint is managed internally)
func (s *BronzeSourceServer) SaveState(_ context.Context, _ *flowctlv1.SaveStateRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// Broadcast sends a ledger-committed event to all subscribers.
// Called by the writer after a successful batch commit.
func (s *BronzeSourceServer) Broadcast(batch BronzeBatchInfo) {
	// Build metadata payload
	meta := map[string]interface{}{
		"start_ledger": batch.StartLedger,
		"end_ledger":   batch.EndLedger,
		"tx_count":     batch.TxCount,
		"op_count":     batch.OpCount,
	}
	payload, _ := json.Marshal(meta)

	event := &flowctlv1.Event{
		Id:                fmt.Sprintf("bronze-batch-%d-%d", batch.StartLedger, batch.EndLedger),
		Type:              EventTypeBronzeLedgerCommitted,
		Payload:           payload,
		ContentType:       "application/json",
		SourceComponentId: "stellar-postgres-ingester",
		Timestamp:         timestamppb.New(batch.ClosedAt),
		StellarCursor: &flowctlv1.StellarCursor{
			LedgerSequence: uint64(batch.EndLedger),
		},
		Metadata: map[string]string{
			"start_ledger": fmt.Sprintf("%d", batch.StartLedger),
			"end_ledger":   fmt.Sprintf("%d", batch.EndLedger),
		},
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, ch := range s.subscribers {
		select {
		case ch <- event:
		default:
			log.Printf("[grpc-source] WARNING: Subscriber %d buffer full, dropping batch %d-%d",
				id, batch.StartLedger, batch.EndLedger)
		}
	}
}

// SubscriberCount returns the number of active subscribers
func (s *BronzeSourceServer) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}
