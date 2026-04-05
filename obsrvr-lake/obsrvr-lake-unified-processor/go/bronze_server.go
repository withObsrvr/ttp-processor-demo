package main

import (
	"log"
	"sync"

	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
)

// BronzeServer implements the BronzeLedgerService gRPC server.
// It maintains a set of channel-based subscribers and broadcasts
// bronze data to all connected consumers.
type BronzeServer struct {
	pb.UnimplementedBronzeLedgerServiceServer

	mu          sync.RWMutex
	subscribers map[uint64]chan *pb.BronzeLedgerData
	nextID      uint64
}

func NewBronzeServer() *BronzeServer {
	return &BronzeServer{
		subscribers: make(map[uint64]chan *pb.BronzeLedgerData),
	}
}

// StreamBronzeData implements the gRPC service — sends bronze data to a subscriber.
func (s *BronzeServer) StreamBronzeData(req *pb.StreamBronzeRequest, stream pb.BronzeLedgerService_StreamBronzeDataServer) error {
	ch := make(chan *pb.BronzeLedgerData, 100) // Buffer 100 ledgers

	s.mu.Lock()
	id := s.nextID
	s.nextID++
	s.subscribers[id] = ch
	s.mu.Unlock()

	log.Printf("Bronze subscriber %d connected (start_ledger=%d)", id, req.StartLedger)

	defer func() {
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
		log.Printf("Bronze subscriber %d disconnected", id)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case data, ok := <-ch:
			if !ok {
				return nil
			}
			// Only send ledgers >= requested start
			if data.LedgerSequence < req.StartLedger {
				continue
			}
			if err := stream.Send(data); err != nil {
				return err
			}
		}
	}
}

// Broadcast sends bronze data to all connected subscribers.
func (s *BronzeServer) Broadcast(data *pb.BronzeLedgerData) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, ch := range s.subscribers {
		select {
		case ch <- data:
		default:
			log.Printf("Warning: subscriber %d channel full, dropping ledger %d", id, data.LedgerSequence)
		}
	}
}
