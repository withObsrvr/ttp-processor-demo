package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BronzeStreamClient connects to the ingester's flowctl SourceService
// and receives ledger-committed events via gRPC streaming.
type BronzeStreamClient struct {
	endpoint string
	conn     *grpc.ClientConn
	client   flowctlv1.SourceServiceClient
}

// NewBronzeStreamClient creates a new gRPC stream client
func NewBronzeStreamClient(endpoint string) (*BronzeStreamClient, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to bronze source at %s: %w", endpoint, err)
	}

	client := flowctlv1.NewSourceServiceClient(conn)
	return &BronzeStreamClient{
		endpoint: endpoint,
		conn:     conn,
		client:   client,
	}, nil
}

// Close closes the gRPC connection
func (c *BronzeStreamClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// LedgerBatchEvent represents a received ledger batch notification
type LedgerBatchEvent struct {
	StartLedger uint32
	EndLedger   uint32
	TxCount     uint64
	OpCount     uint64
}

// StreamLedgerEvents opens a streaming connection and sends batch events to the returned channel.
// It blocks until the context is cancelled or the stream errors.
// On transient errors it reconnects automatically with backoff.
func (c *BronzeStreamClient) StreamLedgerEvents(ctx context.Context, startLedger int64) (<-chan LedgerBatchEvent, <-chan error) {
	eventCh := make(chan LedgerBatchEvent, 100)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)

		backoff := time.Second
		maxBackoff := 30 * time.Second

		for {
			if ctx.Err() != nil {
				return
			}

			err := c.streamOnce(ctx, startLedger, eventCh)
			if ctx.Err() != nil {
				return
			}

			log.Printf("[grpc-client] Stream disconnected: %v, reconnecting in %v", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}()

	return eventCh, errCh
}

func (c *BronzeStreamClient) streamOnce(ctx context.Context, startLedger int64, eventCh chan<- LedgerBatchEvent) error {
	params := map[string]string{}
	if startLedger > 0 {
		params["start_ledger"] = fmt.Sprintf("%d", startLedger)
	}

	stream, err := c.client.StreamEvents(ctx, &flowctlv1.StreamRequest{
		Params: params,
	})
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}

	log.Printf("[grpc-client] Connected to bronze source at %s (start_ledger=%d)", c.endpoint, startLedger)

	for {
		event, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream recv: %w", err)
		}

		batch, err := parseEvent(event)
		if err != nil {
			log.Printf("[grpc-client] Warning: failed to parse event: %v", err)
			continue
		}

		select {
		case eventCh <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func parseEvent(event *flowctlv1.Event) (LedgerBatchEvent, error) {
	batch := LedgerBatchEvent{}

	meta := event.GetMetadata()
	if meta == nil {
		return batch, fmt.Errorf("event has no metadata")
	}

	if v, ok := meta["start_ledger"]; ok {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return batch, fmt.Errorf("invalid start_ledger: %w", err)
		}
		batch.StartLedger = uint32(n)
	}

	if v, ok := meta["end_ledger"]; ok {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return batch, fmt.Errorf("invalid end_ledger: %w", err)
		}
		batch.EndLedger = uint32(n)
	}

	return batch, nil
}
