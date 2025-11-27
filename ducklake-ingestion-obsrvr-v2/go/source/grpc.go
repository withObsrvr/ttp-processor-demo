package source

import (
	"context"
	"fmt"
	"log"

	"github.com/stellar/go/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCSource connects to stellar-live-source-datalake via gRPC streaming.
// This is the legacy source mode, kept for backward compatibility.
type GRPCSource struct {
	client            pb.RawLedgerServiceClient
	conn              *grpc.ClientConn
	networkPassphrase string
}

// NewGRPCSource creates a new gRPC-based ledger source.
func NewGRPCSource(cfg GRPCConfig, networkPassphrase string) (*GRPCSource, error) {
	cfg.ApplyDefaults()

	log.Printf("[source:grpc] connecting to %s", cfg.Endpoint)

	conn, err := grpc.Dial(
		cfg.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxMessageSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC endpoint: %w", err)
	}

	client := pb.NewRawLedgerServiceClient(conn)

	log.Printf("[source:grpc] connected successfully")

	return &GRPCSource{
		client:            client,
		conn:              conn,
		networkPassphrase: networkPassphrase,
	}, nil
}

// GetLedger fetches a single ledger by sequence number.
// Note: The gRPC service uses streaming, so this creates a stream and reads one ledger.
func (s *GRPCSource) GetLedger(ctx context.Context, seq uint32) (*xdr.LedgerCloseMeta, error) {
	stream, err := s.client.StreamRawLedgers(ctx, &pb.StreamLedgersRequest{
		StartLedger: seq,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start stream: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive ledger %d: %w", seq, err)
	}

	// Verify we got the right ledger
	if resp.Sequence != seq {
		return nil, fmt.Errorf("expected ledger %d, got %d", seq, resp.Sequence)
	}

	// Parse the XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(resp.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ledger %d: %w", seq, err)
	}

	return &lcm, nil
}

// GetLedgerRange returns a channel that streams ledgers in the given range.
// The gRPC service streams continuously, so we filter to the requested range.
func (s *GRPCSource) GetLedgerRange(ctx context.Context, start, end uint32) (<-chan LedgerResult, error) {
	stream, err := s.client.StreamRawLedgers(ctx, &pb.StreamLedgersRequest{
		StartLedger: start,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start stream: %w", err)
	}

	ch := make(chan LedgerResult, 100)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				ch <- LedgerResult{Err: ctx.Err()}
				return
			default:
			}

			resp, err := stream.Recv()
			if err != nil {
				ch <- LedgerResult{Err: fmt.Errorf("stream receive error: %w", err)}
				return
			}

			// Check if we've passed the end (for bounded ranges)
			if end > 0 && resp.Sequence > end {
				log.Printf("[source:grpc] completed range %d-%d", start, end)
				return
			}

			// Parse the XDR
			var lcm xdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(resp.LedgerCloseMetaXdr); err != nil {
				ch <- LedgerResult{Err: fmt.Errorf("failed to unmarshal ledger %d: %w", resp.Sequence, err)}
				return
			}

			ch <- LedgerResult{Ledger: &lcm}
		}
	}()

	return ch, nil
}

// PrepareRange is a no-op for gRPC source (streaming handles it).
func (s *GRPCSource) PrepareRange(ctx context.Context, start, end uint32) error {
	log.Printf("[source:grpc] prepare range %d-%d (no-op for gRPC)", start, end)
	return nil
}

// Close releases resources associated with the source.
func (s *GRPCSource) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
