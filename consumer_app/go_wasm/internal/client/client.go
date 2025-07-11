package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import the generated protobuf code
	eventpb "github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/event_service"
	assetpb "github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/asset"
	ttpb "github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/processors/token_transfer"
)

// EventServiceClient represents a client for the EventService
type EventServiceClient struct {
	conn   *grpc.ClientConn
	client eventpb.EventServiceClient
}

// Event callback function type
type EventCallback func(event *ttpb.TokenTransferEvent)

// NewEventServiceClient creates a new client for the EventService
func NewEventServiceClient(serverAddress string) (*EventServiceClient, error) {
	// Only use mock client if explicitly requested with "mock"
	if serverAddress == "mock" {
		log.Println("Using mock TokenTransferServiceClient")
		return &EventServiceClient{
			conn:   nil, // No actual connection
			client: &mockEventServiceClient{},
		}, nil
	}

	// For all other addresses, use the real client
	log.Printf("Connecting to real gRPC server at %s", serverAddress)
	conn, err := grpc.Dial(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	client := eventpb.NewEventServiceClient(conn)
	return &EventServiceClient{
		conn:   conn,
		client: client,
	}, nil
}

// Close closes the client connection
func (c *EventServiceClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetTTPEvents gets TTP events from the server
func (c *EventServiceClient) GetTTPEvents(
	ctx context.Context,
	startLedger uint32,
	endLedger uint32,
	accountIds []string,
	callback EventCallback,
) error {
	req := &eventpb.GetEventsRequest{
		StartLedger: startLedger,
		EndLedger:   endLedger,
		AccountIds:  accountIds,
	}

	if len(accountIds) > 0 {
		log.Printf("Requesting events from ledger %d to %d for accounts %v from server", startLedger, endLedger, accountIds)
	} else {
		log.Printf("Requesting events from ledger %d to %d (all accounts) from server", startLedger, endLedger)
	}
	stream, err := c.client.GetTTPEvents(ctx, req)
	if err != nil {
		log.Printf("Error calling GetTTPEvents: %v", err)
		return fmt.Errorf("error calling GetTTPEvents: %w", err)
	}
	log.Println("Successfully connected to server, receiving events...")

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("Finished receiving events (EOF)")
			break
		}
		if err != nil {
			log.Printf("Error receiving event: %v", err)
			return fmt.Errorf("error receiving event: %w", err)
		}

		callback(event)
	}

	return nil
}

// PrintEvent prints an event to stdout
func PrintEvent(event *ttpb.TokenTransferEvent) {
	// Format and print event information
	fmt.Println("Received TTP event:")
	
	meta := event.GetMeta()
	if meta != nil {
		fmt.Printf("  Ledger: %d\n", meta.GetLedgerSequence())
		fmt.Printf("  Transaction Hash: %s\n", meta.GetTxHash())
		fmt.Printf("  Contract Address: %s\n", meta.GetContractAddress())
		
		if closedAt := meta.GetClosedAt(); closedAt != nil {
			ts := time.Unix(closedAt.GetSeconds(), int64(closedAt.GetNanos()))
			fmt.Printf("  Closed At: %s\n", ts.Format(time.RFC3339))
		}
	}

	// Print event type and details
	fmt.Println("  Event Type:", getEventType(event))
	printEventDetails(event)
	fmt.Println("-------------------")
}

// getEventType determines the type of event
func getEventType(event *ttpb.TokenTransferEvent) string {
	switch event.Event.(type) {
	case *ttpb.TokenTransferEvent_Transfer:
		return "Transfer"
	case *ttpb.TokenTransferEvent_Mint:
		return "Mint"
	case *ttpb.TokenTransferEvent_Burn:
		return "Burn"
	case *ttpb.TokenTransferEvent_Clawback:
		return "Clawback"
	case *ttpb.TokenTransferEvent_Fee:
		return "Fee"
	default:
		return "Unknown"
	}
}

// printEventDetails prints the details of an event based on its type
func printEventDetails(event *ttpb.TokenTransferEvent) {
	switch e := event.Event.(type) {
	case *ttpb.TokenTransferEvent_Transfer:
		transfer := e.Transfer
		fmt.Printf("  From: %s\n", transfer.GetFrom())
		fmt.Printf("  To: %s\n", transfer.GetTo())
		fmt.Printf("  Amount: %s\n", transfer.GetAmount())
		printAsset(transfer.GetAsset())
	
	case *ttpb.TokenTransferEvent_Mint:
		mint := e.Mint
		fmt.Printf("  To: %s\n", mint.GetTo())
		fmt.Printf("  Amount: %s\n", mint.GetAmount())
		printAsset(mint.GetAsset())
	
	case *ttpb.TokenTransferEvent_Burn:
		burn := e.Burn
		fmt.Printf("  From: %s\n", burn.GetFrom())
		fmt.Printf("  Amount: %s\n", burn.GetAmount())
		printAsset(burn.GetAsset())
	
	case *ttpb.TokenTransferEvent_Clawback:
		clawback := e.Clawback
		fmt.Printf("  From: %s\n", clawback.GetFrom())
		fmt.Printf("  Amount: %s\n", clawback.GetAmount())
		printAsset(clawback.GetAsset())
	
	case *ttpb.TokenTransferEvent_Fee:
		fee := e.Fee
		fmt.Printf("  From: %s\n", fee.GetFrom())
		fmt.Printf("  Amount: %s\n", fee.GetAmount())
		printAsset(fee.GetAsset())
	}
}

// printAsset prints asset information
func printAsset(asset *assetpb.Asset) {
	if asset == nil {
		return
	}

	switch at := asset.AssetType.(type) {
	case *assetpb.Asset_Native:
		fmt.Println("  Asset: Native (XLM)")
	case *assetpb.Asset_IssuedAsset:
		fmt.Printf("  Asset: %s (Issuer: %s)\n", at.IssuedAsset.GetAssetCode(), at.IssuedAsset.GetIssuer())
	}
}

// Mock implementation for testing
type mockEventServiceClient struct{}

func (c *mockEventServiceClient) GetTTPEvents(ctx context.Context, in *eventpb.GetEventsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ttpb.TokenTransferEvent], error) {
	return &mockTokenTransferStreamClient{
		startLedger: in.StartLedger,
		endLedger:   in.EndLedger,
		accountIds:  in.AccountIds,
		currentPos:  0,
	}, nil
}

// Mock stream client that returns predefined events
type mockTokenTransferStreamClient struct {
	grpc.ClientStream
	startLedger uint32
	endLedger   uint32
	accountIds  []string
	currentPos  int
}

func (m *mockTokenTransferStreamClient) Recv() (*ttpb.TokenTransferEvent, error) {
	// If we've sent all events, return EOF
	if m.currentPos >= 3 || (m.endLedger != 0 && m.startLedger+uint32(m.currentPos) > m.endLedger) {
		return nil, io.EOF
	}

	// Create a mock event
	currentLedger := m.startLedger + uint32(m.currentPos)
	var event *ttpb.TokenTransferEvent

	switch m.currentPos {
	case 0:
		// First event is a transfer
		event = &ttpb.TokenTransferEvent{
			Meta: &ttpb.EventMeta{
				LedgerSequence:   currentLedger,
				TxHash:           fmt.Sprintf("tx_hash_%d", currentLedger),
				TransactionIndex: 1,
				ContractAddress:  "GCONTRACT123456789",
				ClosedAt: &timestamp.Timestamp{
					Seconds: time.Now().Unix(),
					Nanos:   0,
				},
			},
			Event: &ttpb.TokenTransferEvent_Transfer{
				Transfer: &ttpb.Transfer{
					From:   "GFROM123456789",
					To:     "GTO123456789",
					Amount: "100.0",
					Asset: &assetpb.Asset{
						AssetType: &assetpb.Asset_Native{
							Native: true,
						},
					},
				},
			},
		}
	case 1:
		// Second event is a mint
		event = &ttpb.TokenTransferEvent{
			Meta: &ttpb.EventMeta{
				LedgerSequence:   currentLedger,
				TxHash:           fmt.Sprintf("tx_hash_%d", currentLedger),
				TransactionIndex: 2,
				ContractAddress:  "GCONTRACT123456789",
				ClosedAt: &timestamp.Timestamp{
					Seconds: time.Now().Unix(),
					Nanos:   0,
				},
			},
			Event: &ttpb.TokenTransferEvent_Mint{
				Mint: &ttpb.Mint{
					To:     "GTO123456789",
					Amount: "50.0",
					Asset: &assetpb.Asset{
						AssetType: &assetpb.Asset_IssuedAsset{
							IssuedAsset: &assetpb.IssuedAsset{
								AssetCode: "USD",
								Issuer:    "GISSUER123456789",
							},
						},
					},
				},
			},
		}
	case 2:
		// Third event is a fee
		event = &ttpb.TokenTransferEvent{
			Meta: &ttpb.EventMeta{
				LedgerSequence:   currentLedger,
				TxHash:           fmt.Sprintf("tx_hash_%d", currentLedger),
				TransactionIndex: 3,
				ContractAddress:  "GCONTRACT123456789",
				ClosedAt: &timestamp.Timestamp{
					Seconds: time.Now().Unix(),
					Nanos:   0,
				},
			},
			Event: &ttpb.TokenTransferEvent_Fee{
				Fee: &ttpb.Fee{
					From:   "GFROM123456789",
					Amount: "0.00001",
					Asset: &assetpb.Asset{
						AssetType: &assetpb.Asset_Native{
							Native: true,
						},
					},
				},
			},
		}
	}

	m.currentPos++
	return event, nil
}