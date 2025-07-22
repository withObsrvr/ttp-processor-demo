// Example Go consumer for Contract Invocation Processor
// This is a placeholder implementation showing how to connect and consume events

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
)

type ContractInvocationClient struct {
	conn   *grpc.ClientConn
	client pb.ContractInvocationServiceClient
}

func NewContractInvocationClient(serverAddr string) (*ContractInvocationClient, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	client := pb.NewContractInvocationServiceClient(conn)

	return &ContractInvocationClient{
		conn:   conn,
		client: client,
	}, nil
}

func (c *ContractInvocationClient) Close() error {
	return c.conn.Close()
}

func (c *ContractInvocationClient) GetContractInvocations(ctx context.Context, req *pb.GetInvocationsRequest) error {
	stream, err := c.client.GetContractInvocations(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get invocations: %w", err)
	}

	log.Printf("Connected to contract invocation processor, waiting for events...")

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream ended")
			break
		}
		if err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		// Process the event
		c.processEvent(event)
	}

	return nil
}

func (c *ContractInvocationClient) processEvent(event *pb.ContractInvocationEvent) {
	log.Printf("Received contract invocation event:")
	log.Printf("  Ledger: %d", event.Meta.LedgerSequence)
	log.Printf("  TxHash: %s", event.Meta.TxHash)
	log.Printf("  Successful: %t", event.Meta.Successful)

	switch inv := event.InvocationType.(type) {
	case *pb.ContractInvocationEvent_ContractCall:
		log.Printf("  Type: Contract Call")
		log.Printf("  Contract: %s", inv.ContractCall.ContractId)
		log.Printf("  Function: %s", inv.ContractCall.FunctionName)
		log.Printf("  Invoker: %s", inv.ContractCall.InvokingAccount)
		log.Printf("  Arguments: %d", len(inv.ContractCall.Arguments))

	case *pb.ContractInvocationEvent_CreateContract:
		log.Printf("  Type: Create Contract")
		log.Printf("  Contract: %s", inv.CreateContract.ContractId)
		log.Printf("  Creator: %s", inv.CreateContract.CreatorAccount)

	case *pb.ContractInvocationEvent_UploadWasm:
		log.Printf("  Type: Upload WASM")
		log.Printf("  Uploader: %s", inv.UploadWasm.UploaderAccount)
		log.Printf("  WASM Size: %d bytes", inv.UploadWasm.WasmSize)
	}

	log.Println("---")
}

func main() {
	// Connect to the contract invocation processor
	client, err := NewContractInvocationClient("localhost:50054")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Example 1: Stream all contract invocations from ledger 1000000
	log.Println("=== Example 1: All contract invocations ===")
	req1 := &pb.GetInvocationsRequest{
		StartLedger: 1000000,
		EndLedger:   1000010, // Bounded range for example
	}

	if err := client.GetContractInvocations(ctx, req1); err != nil {
		log.Printf("Example 1 failed: %v", err)
	}

	// Example 2: Filter by contract ID
	log.Println("\n=== Example 2: Specific contract only ===")
	req2 := &pb.GetInvocationsRequest{
		StartLedger: 1000000,
		EndLedger:   1000010,
		ContractIds: []string{"CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"},
	}

	if err := client.GetContractInvocations(ctx, req2); err != nil {
		log.Printf("Example 2 failed: %v", err)
	}

	// Example 3: Filter by function name and successful only
	log.Println("\n=== Example 3: Specific functions, successful only ===")
	req3 := &pb.GetInvocationsRequest{
		StartLedger:    1000000,
		EndLedger:      1000010,
		FunctionNames:  []string{"transfer", "mint"},
		SuccessfulOnly: true,
		TypeFilter:     pb.InvocationTypeFilter_INVOCATION_TYPE_FILTER_CONTRACT_CALL,
	}

	if err := client.GetContractInvocations(ctx, req3); err != nil {
		log.Printf("Example 3 failed: %v", err)
	}

	log.Println("\nNote: These examples require the contract invocation processor to be running.")
	log.Println("Start the processor with: make run")
}