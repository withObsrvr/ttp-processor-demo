package server

import (
	"bytes"
	"context"
	"io"
	"log"

	// Import the generated protobuf code package for the service WE PROVIDE
	eventservice "github.com/withObsrvr/ttp-processor/gen/event_service"
	// Import the generated protobuf code package for the service WE CONSUME
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"

	"github.com/stellar/go/ingest/processors/token_transfer"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// EventServer implements the EventServiceServer interface
type EventServer struct {
	// Embed the unimplemented server type for forward compatibility
	eventservice.UnimplementedEventServiceServer

	processor       *token_transfer.EventsProcessor  // The core TTP logic
	rawLedgerClient rawledger.RawLedgerServiceClient // gRPC client for the raw source service
	rawLedgerConn   *grpc.ClientConn                 // Connection to the raw source service
}

// NewEventServer creates a new instance of the TTP processor server
func NewEventServer(passphrase string, sourceServiceAddr string) (*EventServer, error) {
	processor := token_transfer.NewEventsProcessor(passphrase)

	log.Printf("Attempting to connect to raw ledger source at %s", sourceServiceAddr)
	// Set up a connection to the raw ledger source server.
	// Using insecure credentials for this example. Use secure credentials in production.
	conn, err := grpc.Dial(sourceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "did not connect to raw ledger source service at %s", sourceServiceAddr)
	}
	log.Printf("Successfully connected to raw ledger source service.")

	client := rawledger.NewRawLedgerServiceClient(conn)

	return &EventServer{
		processor:       processor,
		rawLedgerClient: client,
		rawLedgerConn:   conn, // Store the connection to close it later if needed
	}, nil
}

// Close cleans up resources, like the connection to the source service.
func (s *EventServer) Close() error {
	log.Println("Closing connection to raw ledger source service...")
	if s.rawLedgerConn != nil {
		return s.rawLedgerConn.Close()
	}
	return nil
}

// GetTTPEvents implements the gRPC GetTTPEvents method
func (s *EventServer) GetTTPEvents(req *eventservice.GetEventsRequest, stream eventservice.EventService_GetTTPEventsServer) error {
	// The context of the incoming stream from the *consumer* (e.g., Minecraft mod)
	ctx := stream.Context()
	log.Printf("Received GetTTPEvents request: StartLedger=%d, EndLedger=%d", req.StartLedger, req.EndLedger)

	// Create a request for the raw ledger source service
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
	}

	// Create a new context for the *outgoing* call to the source service.
	// This allows us to cancel the call to the source if the downstream consumer disconnects.
	sourceCtx, cancelSourceStream := context.WithCancel(ctx)
	defer cancelSourceStream() // Ensure cancellation propagates if this function returns

	log.Printf("Requesting raw ledger stream from source service starting at ledger %d", sourceReq.StartLedger)
	// Call the source service to get the stream of raw ledgers
	rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
	if err != nil {
		log.Printf("Error calling StreamRawLedgers on source service: %v", err)
		return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
	}
	log.Printf("Successfully initiated raw ledger stream from source service.")

	// Loop indefinitely, receiving raw ledgers from the source stream
	for {
		// Check if the consumer's context is cancelled first
		select {
		case <-ctx.Done():
			log.Printf("Consumer context cancelled. Stopping TTP event stream. Error: %v", ctx.Err())
			// Cancel the upstream call to the source service
			cancelSourceStream()
			return status.FromContextError(ctx.Err()).Err()
		default:
			// Continue if consumer is still connected
		}

		// Receive the next raw ledger message from the source service stream
		rawLedgerMsg, err := rawLedgerStream.Recv()
		if err == io.EOF {
			// Source stream ended unexpectedly (should not happen for live source)
			log.Println("Raw ledger source stream ended (EOF). Closing TTP stream.")
			return status.Error(codes.Unavailable, "raw ledger source stream ended unexpectedly")
		}
		if err != nil {
			// Handle errors from the source stream (e.g., source service crashed)
			// Check if the error is due to the cancellation we initiated
			if status.Code(err) == codes.Canceled && ctx.Err() != nil {
				log.Printf("Source stream cancelled as expected due to consumer disconnection.")
				return status.FromContextError(ctx.Err()).Err() // Return consumer's error
			}
			log.Printf("Error receiving from raw ledger source stream: %v", err)
			return status.Errorf(codes.Internal, "error receiving data from raw ledger source: %v", err)
		}

		// Check if we need to stop based on the consumer's requested endLedger
		// A non-zero endLedger indicates a bounded request.
		if req.EndLedger > 0 && rawLedgerMsg.Sequence > req.EndLedger {
			log.Printf("Reached end ledger %d requested by consumer. Closing TTP stream.", req.EndLedger)
			cancelSourceStream() // No need to get more from source
			return nil           // Successful completion of bounded stream
		}

		log.Printf("Processing raw ledger %d received from source", rawLedgerMsg.Sequence)

		// Unmarshal the raw XDR bytes into a LedgerCloseMeta object
		var lcm xdr.LedgerCloseMeta
		_, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
		if err != nil {
			log.Printf("Error unmarshaling XDR for ledger %d: %v", rawLedgerMsg.Sequence, err)
			// Decide how to handle - skip ledger or terminate? Terminating is safer.
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to unmarshal ledger %d XDR: %v", rawLedgerMsg.Sequence, err)
		}

		// Now use the TTP processor to get events from this ledger
		events, err := s.processor.EventsFromLedger(lcm)
		if err != nil {
			log.Printf("Error processing TTP events from ledger %d: %v", lcm.LedgerSequence(), err)
			// Terminate if processing fails
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to process TTP events for ledger %d: %v", lcm.LedgerSequence(), err)
		}

		// Stream each generated TTP event to the *consumer*
		for i := range events {
			ttpEvent := events[i] // Create loop variable copy
			if err := stream.Send(ttpEvent); err != nil {
				log.Printf("Error sending TTP event to consumer stream for ledger %d: %v", lcm.LedgerSequence(), err)
				// Consumer likely disconnected. Cancel upstream source stream.
				cancelSourceStream()
				return status.Errorf(codes.Unavailable, "failed to send TTP event to consumer: %v", err)
			}
		}
		log.Printf("Finished processing ledger %d, %d TTP events sent.", lcm.LedgerSequence(), len(events))
	}
	// This part is theoretically unreachable
}
