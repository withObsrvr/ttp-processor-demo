package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SilverCheckpointEvent struct {
	StartLedger uint32
	EndLedger   uint32
}

type SilverStreamClient struct {
	endpoint string
	conn     *grpc.ClientConn
	client   flowctlv1.SourceServiceClient
}

func NewSilverStreamClient(endpoint string) (*SilverStreamClient, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to silver source at %s: %w", endpoint, err)
	}
	return &SilverStreamClient{
		endpoint: endpoint,
		conn:     conn,
		client:   flowctlv1.NewSourceServiceClient(conn),
	}, nil
}

func (c *SilverStreamClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *SilverStreamClient) StreamCheckpointEvents(ctx context.Context, startLedger int64) <-chan SilverCheckpointEvent {
	eventCh := make(chan SilverCheckpointEvent, 100)
	go func() {
		defer close(eventCh)
		var nextStart atomic.Int64
		nextStart.Store(startLedger)
		backoff := time.Second
		maxBackoff := 30 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			err := c.streamOnce(ctx, nextStart.Load(), eventCh, &nextStart)
			if ctx.Err() != nil {
				return
			}
			log.Printf("[serving-grpc-client] Stream disconnected: %v, reconnecting in %v", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}()
	return eventCh
}

func (c *SilverStreamClient) streamOnce(ctx context.Context, startLedger int64, eventCh chan<- SilverCheckpointEvent, nextStart *atomic.Int64) error {
	params := map[string]string{}
	if startLedger > 0 {
		params["start_ledger"] = fmt.Sprintf("%d", startLedger)
	}
	stream, err := c.client.StreamEvents(ctx, &flowctlv1.StreamRequest{Params: params})
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	log.Printf("[serving-grpc-client] Connected to silver source at %s (start_ledger=%d)", c.endpoint, startLedger)
	for {
		event, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream recv: %w", err)
		}
		checkpoint, err := parseSilverEvent(event)
		if err != nil {
			log.Printf("[serving-grpc-client] Warning: failed to parse event: %v", err)
			continue
		}
		if int64(checkpoint.EndLedger) > nextStart.Load() {
			nextStart.Store(int64(checkpoint.EndLedger))
		}
		select {
		case eventCh <- checkpoint:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func parseSilverEvent(event *flowctlv1.Event) (SilverCheckpointEvent, error) {
	var out SilverCheckpointEvent
	meta := event.GetMetadata()
	if meta == nil {
		return out, fmt.Errorf("event has no metadata")
	}
	v, ok := meta["start_ledger"]
	if !ok {
		return out, fmt.Errorf("event missing start_ledger")
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return out, fmt.Errorf("invalid start_ledger: %w", err)
	}
	out.StartLedger = uint32(n)
	v, ok = meta["end_ledger"]
	if !ok {
		return out, fmt.Errorf("event missing end_ledger")
	}
	n, err = strconv.ParseUint(v, 10, 32)
	if err != nil {
		return out, fmt.Errorf("invalid end_ledger: %w", err)
	}
	out.EndLedger = uint32(n)
	return out, nil
}
