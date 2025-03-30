package server

import (
	"context"
	"encoding/base64"
	"log"
	"time"

	// Import the generated protobuf code package
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"

	"github.com/stellar/stellar-rpc/client"
	"github.com/stellar/stellar-rpc/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RawLedgerServer implements the RawLedgerServiceServer interface
type RawLedgerServer struct {
	// Embed the unimplemented server type for forward compatibility
	rawledger.UnimplementedRawLedgerServiceServer
	rpcClient *client.Client
}

// NewRawLedgerServer creates a new instance of the server
func NewRawLedgerServer(rpcEndpoint string) (*RawLedgerServer, error) {
	rpcClient := client.NewClient(rpcEndpoint, nil)
	// Optional: Add a health check or initial connection test here
	log.Printf("Attempting to connect to RPC endpoint: %s", rpcEndpoint)
	// Example check (GetLatestLedger is usually lightweight)
	_, err := rpcClient.GetLatestLedger(context.Background())
	if err != nil {
		log.Printf("Warning: Initial connection test to RPC endpoint failed: %v", err)
		// Decide if this should be a fatal error or just a warning
		// return nil, errors.Wrap(err, "failed initial RPC connection test")
	} else {
		log.Printf("Successfully connected to RPC endpoint.")
	}

	return &RawLedgerServer{
		rpcClient: rpcClient,
	}, nil
}

// StreamRawLedgers implements the gRPC StreamRawLedgers method
func (s *RawLedgerServer) StreamRawLedgers(req *rawledger.StreamLedgersRequest, stream rawledger.RawLedgerService_StreamRawLedgersServer) error {
	ctx := stream.Context()
	log.Printf("Received StreamRawLedgers request: StartLedger=%d", req.StartLedger)

	// Create initial GetLedgers request for continuous polling
	getLedgersReq := protocol.GetLedgersRequest{
		StartLedger: uint32(req.StartLedger),
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1, // Fetch one ledger at a time initially
		},
	}

	// --- Continuous Polling Loop ---
	for {
		// Check if context is cancelled (client disconnected)
		select {
		case <-ctx.Done():
			err := ctx.Err()
			log.Printf("Context cancelled in StreamRawLedgers: %v. Stopping stream.", err)
			return status.FromContextError(err).Err() // Return gRPC status code
		default:
			// Continue execution
		}

		log.Printf("Requesting ledgers from RPC starting with cursor '%s' / ledger %d", getLedgersReq.Pagination.Cursor, getLedgersReq.StartLedger)

		// Get ledgers from RPC
		resp, err := s.rpcClient.GetLedgers(ctx, getLedgersReq)
		if err != nil {
			// Handle specific gRPC/RPC errors if needed
			log.Printf("Error getting ledgers from RPC: %v. Retrying after delay...", err)
			time.Sleep(5 * time.Second) // Backoff delay
			continue                    // Retry the request
		}

		if len(resp.Ledgers) == 0 {
			log.Printf("No new ledgers found (Latest: %d). Waiting...", resp.LatestLedger)
			// No ledgers found, wait before polling again
			time.Sleep(5 * time.Second) // Adjust polling interval as needed
			// Keep the same cursor/startLedger for the next request
			continue
		}

		lastProcessedSeq := uint32(0)
		// Process each ledger received in the batch
		for _, ledgerInfo := range resp.Ledgers {
			if ledgerInfo.Sequence < req.StartLedger {
				log.Printf("Skipping ledger %d (before start ledger %d)", ledgerInfo.Sequence, req.StartLedger)
				continue
			}
			log.Printf("Processing ledger %d from RPC", ledgerInfo.Sequence)

			// Decode the base64 Ledger Metadata into raw XDR bytes
			rawXdrBytes, err := base64.StdEncoding.DecodeString(ledgerInfo.LedgerMetadata)
			if err != nil {
				log.Printf("Error decoding base64 metadata for ledger %d: %v", ledgerInfo.Sequence, err)
				// Decide how to handle: skip ledger or terminate stream?
				// Returning error will terminate the stream for the client.
				return status.Errorf(codes.Internal, "failed to decode ledger metadata for sequence %d: %v", ledgerInfo.Sequence, err)
			}

			// Create the message to stream
			rawLedgerMsg := &rawledger.RawLedger{
				Sequence:           ledgerInfo.Sequence,
				LedgerCloseMetaXdr: rawXdrBytes,
			}

			// Send the raw ledger data over the gRPC stream
			if err := stream.Send(rawLedgerMsg); err != nil {
				log.Printf("Error sending RawLedger message to stream for ledger %d: %v", ledgerInfo.Sequence, err)
				// Client likely disconnected or stream broken
				return status.Errorf(codes.Unavailable, "failed to send data to client: %v", err)
			}
			lastProcessedSeq = ledgerInfo.Sequence
		}

		// --- Update request for next iteration ---
		// If we received ledgers, use the returned cursor for the next request.
		// Reset StartLedger to 0 as cursor takes precedence.
		getLedgersReq.Pagination.Cursor = resp.Cursor
		getLedgersReq.StartLedger = 0

		// Optional: Adjust Limit for potentially faster catch-up, e.g., fetch 10 next time?
		// getLedgersReq.Pagination.Limit = 10

		// Naive delay to avoid hitting RPC rate limits and allow network time
		// A more sophisticated approach might use the latestLedger sequence
		if lastProcessedSeq > 0 && lastProcessedSeq >= (resp.LatestLedger-1) {
			log.Printf("Processed up to ledger %d (Latest: %d), waiting before next poll...", lastProcessedSeq, resp.LatestLedger)
			time.Sleep(5 * time.Second) // Adjust wait time
		} else {
			// If catching up, potentially poll faster or immediately
			// time.Sleep(500 * time.Millisecond)
		}
	}
	// This part is theoretically unreachable in an infinite loop
	// log.Println("Finished StreamRawLedgers loop (should not happen for live stream)")
	// return nil
}
