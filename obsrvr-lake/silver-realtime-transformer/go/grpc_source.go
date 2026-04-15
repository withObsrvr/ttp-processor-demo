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
	// EventTypeSilverCheckpointAdvanced is emitted after a successful silver transform commit.
	EventTypeSilverCheckpointAdvanced = "stellar.silver.checkpoint_advanced"
	// silverSubscriberBufferSize is the channel buffer per downstream subscriber.
	silverSubscriberBufferSize = 100
)

type SilverBatchInfo struct {
	StartLedger uint32
	EndLedger   uint32
	ClosedAt    time.Time
	RowCount    uint64
}

// SilverSourceServer implements flowctl SourceService for downstream serving consumers.
type SilverSourceServer struct {
	flowctlv1.UnimplementedSourceServiceServer

	mu          sync.RWMutex
	subscribers map[uint64]chan *flowctlv1.Event
	nextID      uint64
	checkpoint  *CheckpointManager
}

func NewSilverSourceServer(checkpoint *CheckpointManager) *SilverSourceServer {
	return &SilverSourceServer{
		subscribers: make(map[uint64]chan *flowctlv1.Event),
		checkpoint:  checkpoint,
	}
}

func (s *SilverSourceServer) Register(srv *grpc.Server) {
	flowctlv1.RegisterSourceServiceServer(srv, s)
}

func (s *SilverSourceServer) GetInfo(_ context.Context, _ *emptypb.Empty) (*flowctlv1.ComponentInfo, error) {
	return &flowctlv1.ComponentInfo{
		Id:               "silver-realtime-transformer",
		Name:             "Silver Realtime Transformer",
		Description:      "Transforms bronze data into silver hot tables and emits downstream checkpoint events",
		Version:          "1.0.0",
		Type:             flowctlv1.ComponentType_COMPONENT_TYPE_SOURCE,
		OutputEventTypes: []string{EventTypeSilverCheckpointAdvanced},
	}, nil
}

func (s *SilverSourceServer) HealthCheck(_ context.Context, _ *flowctlv1.HealthCheckRequest) (*flowctlv1.HealthCheckResponse, error) {
	s.mu.RLock()
	subCount := len(s.subscribers)
	s.mu.RUnlock()
	return &flowctlv1.HealthCheckResponse{
		Status:  flowctlv1.HealthStatus_HEALTH_STATUS_HEALTHY,
		Message: fmt.Sprintf("%d active subscribers", subCount),
	}, nil
}

func (s *SilverSourceServer) StreamEvents(req *flowctlv1.StreamRequest, stream grpc.ServerStreamingServer[flowctlv1.Event]) error {
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

	ch := make(chan *flowctlv1.Event, silverSubscriberBufferSize)
	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.subscribers[id] = ch
	s.mu.Unlock()

	log.Printf("[silver-grpc-source] Subscriber %d connected (start_ledger=%d)", id, startLedger)
	defer func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
		log.Printf("[silver-grpc-source] Subscriber %d disconnected", id)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if startLedger > 0 && event.StellarCursor != nil && event.StellarCursor.LedgerSequence < startLedger {
				continue
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

func (s *SilverSourceServer) GetState(_ context.Context, _ *flowctlv1.StateRequest) (*flowctlv1.StateResponse, error) {
	lastLedger, err := s.checkpoint.Load()
	if err != nil {
		return nil, err
	}
	return &flowctlv1.StateResponse{
		State:           map[string]string{"last_ledger": fmt.Sprintf("%d", lastLedger)},
		LastUpdatedUnix: time.Now().Unix(),
	}, nil
}

func (s *SilverSourceServer) SaveState(_ context.Context, _ *flowctlv1.SaveStateRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *SilverSourceServer) Broadcast(batch SilverBatchInfo) {
	meta := map[string]any{
		"start_ledger": batch.StartLedger,
		"end_ledger":   batch.EndLedger,
		"row_count":    batch.RowCount,
	}
	payload, _ := json.Marshal(meta)
	event := &flowctlv1.Event{
		Id:                fmt.Sprintf("silver-batch-%d-%d", batch.StartLedger, batch.EndLedger),
		Type:              EventTypeSilverCheckpointAdvanced,
		Payload:           payload,
		ContentType:       "application/json",
		SourceComponentId: "silver-realtime-transformer",
		Timestamp:         timestamppb.New(batch.ClosedAt),
		StellarCursor:     &flowctlv1.StellarCursor{LedgerSequence: uint64(batch.EndLedger)},
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
			log.Printf("[silver-grpc-source] WARNING: Subscriber %d buffer full, dropping batch %d-%d", id, batch.StartLedger, batch.EndLedger)
		}
	}
}

func (s *SilverSourceServer) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}
