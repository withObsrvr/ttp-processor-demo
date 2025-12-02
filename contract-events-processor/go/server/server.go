package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	pb "github.com/withObsrvr/contract-events-processor/gen/contract_event_service"
	rawledger "github.com/withObsrvr/contract-events-processor/gen/raw_ledger_service"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Metrics holds server statistics
type Metrics struct {
	LedgersProcessed     uint64
	EventsSent           uint64
	EventsFiltered       uint64
	TotalEventsFound     uint64
	LastProcessedLedger  uint32
	ProcessingLatency    time.Duration
}

// ContractEventServer implements the ContractEventService gRPC server
type ContractEventServer struct {
	pb.UnimplementedContractEventServiceServer

	liveSourceAddress string
	networkPassphrase string
	extractor         *ContractEventExtractor
	logger            *zap.Logger

	mu      sync.RWMutex
	metrics Metrics
}

// NewContractEventServer creates a new gRPC server instance
func NewContractEventServer(liveSourceAddress, networkPassphrase string, logger *zap.Logger) *ContractEventServer {
	return &ContractEventServer{
		liveSourceAddress: liveSourceAddress,
		networkPassphrase: networkPassphrase,
		extractor:         NewContractEventExtractor(networkPassphrase, logger),
		logger:            logger,
	}
}

// GetContractEvents implements the streaming RPC
func (s *ContractEventServer) GetContractEvents(
	req *pb.GetContractEventsRequest,
	stream pb.ContractEventService_GetContractEventsServer,
) error {
	s.logger.Info("Starting contract events stream",
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.Strings("contract_ids", req.ContractIds),
		zap.Strings("event_types", req.EventTypes),
		zap.Bool("include_failed", req.IncludeFailed))

	// Connect to stellar-live-source
	conn, err := grpc.Dial(s.liveSourceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.logger.Error("Failed to connect to stellar-live-source", zap.Error(err))
		return fmt.Errorf("failed to connect to stellar-live-source: %w", err)
	}
	defer conn.Close()

	client := rawledger.NewRawLedgerServiceClient(conn)

	// Stream raw ledgers
	// Note: stellar-live-source streams indefinitely from StartLedger
	// We'll handle EndLedger filtering ourselves if specified
	ledgerReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
	}

	ledgerStream, err := client.StreamRawLedgers(stream.Context(), ledgerReq)
	if err != nil {
		s.logger.Error("Failed to start ledger stream", zap.Error(err))
		return fmt.Errorf("failed to start ledger stream: %w", err)
	}

	// Process each ledger
	for {
		startTime := time.Now()
		rawLedger, err := ledgerStream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				s.logger.Info("Ledger stream completed")
				return nil
			}
			s.logger.Error("Error receiving ledger", zap.Error(err))
			return fmt.Errorf("error receiving ledger: %w", err)
		}

		// Check if we've reached the end ledger (if specified)
		if req.EndLedger > 0 && rawLedger.Sequence > req.EndLedger {
			s.logger.Info("Reached end ledger, stopping stream",
				zap.Uint32("end_ledger", req.EndLedger))
			return nil
		}

		// Unmarshal LedgerCloseMeta from XDR
		var ledgerCloseMeta xdr.LedgerCloseMeta
		if err := ledgerCloseMeta.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
			s.logger.Error("Failed to unmarshal ledger",
				zap.Uint32("ledger", rawLedger.Sequence),
				zap.Error(err))
			continue
		}

		// Extract events
		events, err := s.extractor.ExtractEvents(ledgerCloseMeta)
		if err != nil {
			s.logger.Error("Failed to extract events",
				zap.Uint32("ledger", rawLedger.Sequence),
				zap.Error(err))
			continue
		}

		// Track metrics
		s.mu.Lock()
		s.metrics.LedgersProcessed++
		s.metrics.LastProcessedLedger = rawLedger.Sequence
		s.metrics.TotalEventsFound += uint64(len(events))
		s.mu.Unlock()

		// Filter and send events
		eventsSent := 0
		eventsFiltered := 0
		for _, event := range events {
			if s.shouldIncludeEvent(event, req) {
				pbEvent, err := s.convertToProto(event)
				if err != nil {
					s.logger.Warn("Failed to convert event to proto",
						zap.String("contract_id", event.ContractID),
						zap.Error(err))
					continue
				}

				if err := stream.Send(pbEvent); err != nil {
					s.logger.Error("Failed to send event", zap.Error(err))
					return fmt.Errorf("failed to send event: %w", err)
				}
				eventsSent++
			} else {
				eventsFiltered++
			}
		}

		// Update metrics
		s.mu.Lock()
		s.metrics.EventsSent += uint64(eventsSent)
		s.metrics.EventsFiltered += uint64(eventsFiltered)
		s.metrics.ProcessingLatency = time.Since(startTime)
		s.mu.Unlock()
	}
}

// shouldIncludeEvent applies filtering logic
func (s *ContractEventServer) shouldIncludeEvent(event *ExtractedEvent, req *pb.GetContractEventsRequest) bool {
	// Filter 1: Contract IDs (OR logic)
	if len(req.ContractIds) > 0 {
		found := false
		for _, id := range req.ContractIds {
			if event.ContractID == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter 2: Event Types (OR logic)
	if len(req.EventTypes) > 0 {
		found := false
		for _, et := range req.EventTypes {
			if event.EventType == et {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter 3: Failed Transactions
	if !req.IncludeFailed && !event.TxSuccessful {
		return false
	}

	return true
}

// convertToProto converts an ExtractedEvent to protobuf format
func (s *ContractEventServer) convertToProto(event *ExtractedEvent) (*pb.ContractEvent, error) {
	// Convert topics to ScValue format
	pbTopics := make([]*pb.ScValue, 0, len(event.Topics))
	for i := range event.Topics {
		var jsonStr string
		if i < len(event.TopicsDecoded) && event.TopicsDecoded[i] != nil {
			jsonBytes, err := json.Marshal(event.TopicsDecoded[i])
			if err == nil {
				jsonStr = string(jsonBytes)
			}
		}

		xdrStr := ""
		if i < len(event.TopicsXDR) {
			xdrStr = event.TopicsXDR[i]
		}

		pbTopics = append(pbTopics, &pb.ScValue{
			XdrBase64: xdrStr,
			Json:      jsonStr,
		})
	}

	// Convert data to ScValue format
	var dataJsonStr string
	if event.DataDecoded != nil {
		jsonBytes, err := json.Marshal(event.DataDecoded)
		if err == nil {
			dataJsonStr = string(jsonBytes)
		}
	}

	pbData := &pb.ScValue{
		XdrBase64: event.DataXDR,
		Json:      dataJsonStr,
	}

	// Create EventMeta
	meta := &pb.EventMeta{
		LedgerSequence: event.LedgerSequence,
		LedgerClosedAt: event.LedgerClosedAt,
		TxHash:         event.TxHash,
		TxSuccessful:   event.TxSuccessful,
		TxIndex:        event.TxIndex,
	}

	// Create ContractEvent
	pbEvent := &pb.ContractEvent{
		Meta:            meta,
		ContractId:      event.ContractID,
		EventType:       event.EventType,
		Topics:          pbTopics,
		Data:            pbData,
		InSuccessfulTx:  event.TxSuccessful,
		EventIndex:      event.EventIndex,
	}

	// Set operation index if present
	if event.OperationIndex != nil {
		pbEvent.OperationIndex = event.OperationIndex
	}

	return pbEvent, nil
}

// GetStats returns server statistics (legacy method)
func (s *ContractEventServer) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"ledgers_processed":     s.metrics.LedgersProcessed,
		"events_sent":           s.metrics.EventsSent,
		"events_filtered":       s.metrics.EventsFiltered,
		"total_events_found":    s.metrics.TotalEventsFound,
		"last_processed_ledger": s.metrics.LastProcessedLedger,
		"processing_latency_ms": s.metrics.ProcessingLatency.Milliseconds(),
	}
}

// GetMetrics returns structured metrics for flowctl integration
func (s *ContractEventServer) GetMetrics() Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.metrics
}
