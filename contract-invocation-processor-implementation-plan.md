# Contract Invocation Processor Implementation Plan

## Executive Summary

This document outlines the complete implementation plan for creating a new **Contract Invocation Processor** within the ttp-processor-demo directory. The processor will migrate and enhance the existing `processor_contract_invocation` from cdp-pipeline-workflow, adopting the proven architecture patterns from ttp-processor while ensuring full Protocol 23 compatibility.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Implementation Steps](#implementation-steps)
3. [Protocol 23 Compatibility](#protocol-23-compatibility)
4. [Testing Strategy](#testing-strategy)
5. [Deployment and Integration](#deployment-and-integration)
6. [Future Enhancements](#future-enhancements)

## Architecture Overview

### Target Architecture

The new Contract Invocation Processor will follow the proven ttp-processor pattern:

```
Raw Ledger Source → Contract Invocation Processor → Consumer Applications
     (gRPC)              (gRPC Service)                  (gRPC Clients)
```

### Key Components

1. **contract-invocation-processor/** - Main processor service
2. **Consumer applications** - Go, Node.js, Rust WASM clients
3. **Proto definitions** - gRPC service and message definitions
4. **Integration layer** - Flowctl control plane integration

### Data Flow

```
RawLedger (XDR) → ContractInvocationProcessor → ContractInvocationEvent → Consumers
```

## Implementation Steps

### Phase 1: Project Structure and Proto Definitions

#### 1.1 Create Directory Structure

```
ttp-processor-demo/
├── contract-invocation-processor/
│   ├── go/
│   │   ├── main.go
│   │   ├── server/
│   │   │   ├── server.go
│   │   │   └── flowctl.go
│   │   └── gen/ (generated)
│   ├── protos/
│   │   ├── contract_invocation_service/
│   │   │   └── contract_invocation_service.proto
│   │   └── contract_invocation/
│   │       └── contract_invocation_event.proto
│   ├── Makefile
│   └── README.md
└── consumer_app/
    ├── contract_invocation_node/
    ├── contract_invocation_go_wasm/
    └── contract_invocation_rust_wasm/
```

#### 1.2 Define Proto Messages

**contract_invocation_event.proto**

```protobuf
syntax = "proto3";

package contract_invocation;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/stellar/go/processors/contract_invocation";

// Main contract invocation event message
message ContractInvocationEvent {
  EventMeta meta = 1;
  oneof invocation_type {
    ContractCall contract_call = 2;
    CreateContract create_contract = 3;
    UploadWasm upload_wasm = 4;
  }
}

// Event metadata
message EventMeta {
  uint32 ledger_sequence = 1;
  google.protobuf.Timestamp closed_at = 2;
  string tx_hash = 3;
  uint32 tx_index = 4;
  uint32 op_index = 5;
  bool successful = 6;
  // Protocol 23 specific fields
  string data_source = 7; // "live" or "archive"
  repeated ArchiveRestoration archive_restorations = 8;
}

// Main contract call invocation
message ContractCall {
  string contract_id = 1;
  string invoking_account = 2;
  string function_name = 3;
  repeated ScValue arguments = 4;
  repeated DiagnosticEvent diagnostic_events = 5;
  repeated ContractToContractCall contract_calls = 6;
  repeated StateChange state_changes = 7;
  repeated TtlExtension ttl_extensions = 8;
}

// Create contract operation
message CreateContract {
  string contract_id = 1;
  string creator_account = 2;
  oneof source {
    string source_account = 3; // For create_stellar_asset
    bytes wasm_hash = 4;       // For create_contract
  }
}

// Upload WASM operation
message UploadWasm {
  string uploader_account = 1;
  bytes wasm_hash = 2;
  uint32 wasm_size = 3;
}

// ScValue for Soroban values
message ScValue {
  ScValueType type = 1;
  oneof value {
    bool bool_value = 2;
    uint32 u32_value = 3;
    int32 i32_value = 4;
    uint64 u64_value = 5;
    int64 i64_value = 6;
    string string_value = 7;
    bytes bytes_value = 8;
    ScValueVector vec_value = 9;
    ScValueMap map_value = 10;
    string address_value = 11;
  }
}

enum ScValueType {
  SC_VALUE_TYPE_VOID = 0;
  SC_VALUE_TYPE_BOOL = 1;
  SC_VALUE_TYPE_U32 = 2;
  SC_VALUE_TYPE_I32 = 3;
  SC_VALUE_TYPE_U64 = 4;
  SC_VALUE_TYPE_I64 = 5;
  SC_VALUE_TYPE_STRING = 6;
  SC_VALUE_TYPE_BYTES = 7;
  SC_VALUE_TYPE_VEC = 8;
  SC_VALUE_TYPE_MAP = 9;
  SC_VALUE_TYPE_ADDRESS = 10;
}

message ScValueVector {
  repeated ScValue values = 1;
}

message ScValueMap {
  repeated ScValueMapEntry entries = 1;
}

message ScValueMapEntry {
  ScValue key = 1;
  ScValue value = 2;
}

// Diagnostic event from contract execution
message DiagnosticEvent {
  string contract_id = 1;
  repeated ScValue topics = 2;
  ScValue data = 3;
  bool in_successful_contract_call = 4;
}

// Contract-to-contract call
message ContractToContractCall {
  string from_contract = 1;
  string to_contract = 2;
  string function = 3;
  bool successful = 4;
}

// State change in contract storage
message StateChange {
  string contract_id = 1;
  ScValue key = 2;
  ScValue old_value = 3;
  ScValue new_value = 4;
  StateChangeOperation operation = 5;
}

enum StateChangeOperation {
  STATE_CHANGE_OPERATION_CREATE = 0;
  STATE_CHANGE_OPERATION_UPDATE = 1;
  STATE_CHANGE_OPERATION_DELETE = 2;
}

// TTL extension for contract data
message TtlExtension {
  string contract_id = 1;
  uint32 old_ttl = 2;
  uint32 new_ttl = 3;
  ScValue key = 4;
}

// Protocol 23 specific: Archive restoration
message ArchiveRestoration {
  string contract_id = 1;
  ScValue key = 2;
  uint32 restored_at_ledger = 3;
}
```

**contract_invocation_service.proto**

```protobuf
syntax = "proto3";

package contract_invocation_service;

import "contract_invocation/contract_invocation_event.proto";

option go_package = "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service";

// Service for streaming contract invocation events
service ContractInvocationService {
  rpc GetContractInvocations(GetInvocationsRequest) returns (stream contract_invocation.ContractInvocationEvent);
}

// Request message for GetContractInvocations
message GetInvocationsRequest {
  uint32 start_ledger = 1;
  uint32 end_ledger = 2;   // If 0 or < start_ledger, indicates live stream
  
  // Filtering options
  repeated string contract_ids = 3;      // Filter by specific contracts
  repeated string function_names = 4;    // Filter by function names
  repeated string invoking_accounts = 5; // Filter by invoking accounts
  bool successful_only = 6;              // Only successful invocations
  InvocationTypeFilter type_filter = 7;  // Filter by invocation type
}

enum InvocationTypeFilter {
  INVOCATION_TYPE_FILTER_ALL = 0;
  INVOCATION_TYPE_FILTER_CONTRACT_CALL = 1;
  INVOCATION_TYPE_FILTER_CREATE_CONTRACT = 2;
  INVOCATION_TYPE_FILTER_UPLOAD_WASM = 3;
}
```

#### 1.3 Create Makefile

```makefile
# Contract Invocation Processor Makefile

.PHONY: init gen-proto build run clean test

# Go module initialization
init:
	cd go && go mod init github.com/withobsrvr/contract-invocation-processor
	cd go && go mod tidy

# Generate Go code from proto files
gen-proto:
	mkdir -p go/gen
	protoc --go_out=go/gen --go-grpc_out=go/gen \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		-I protos \
		protos/contract_invocation/*.proto \
		protos/contract_invocation_service/*.proto

# Build the binary
build: gen-proto
	cd go && CGO_ENABLED=0 go build -o ../contract-invocation-processor .

# Run the processor
run: build
	NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015" \
	SOURCE_SERVICE_ADDRESS=localhost:50052 \
	ENABLE_FLOWCTL=true \
	FLOWCTL_ENDPOINT=localhost:8080 \
	./contract-invocation-processor

# Clean build artifacts
clean:
	rm -rf go/gen
	rm -f contract-invocation-processor

# Run tests
test:
	cd go && go test ./...

# Build consumer applications
build-consumers:
	$(MAKE) -C ../consumer_app/contract_invocation_node build
	$(MAKE) -C ../consumer_app/contract_invocation_go_wasm build
```

### Phase 2: Core Server Implementation

#### 2.1 Main Entry Point

**go/main.go**

```go
package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
	"github.com/withobsrvr/contract-invocation-processor/server"
)

func main() {
	// Create gRPC server
	lis, err := net.Listen("tcp", ":50054")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Initialize the contract invocation server
	contractInvocationServer, err := server.NewContractInvocationServer()
	if err != nil {
		log.Fatalf("failed to create contract invocation server: %v", err)
	}

	// Start health check server in a separate goroutine
	healthPort := 8089
	if portStr := os.Getenv("HEALTH_PORT"); portStr != "" {
		// Parse port from environment if provided
	}

	go func() {
		if err := contractInvocationServer.StartHealthCheckServer(healthPort); err != nil {
			log.Printf("Health check server failed: %v", err)
		}
	}()

	// Register and start gRPC server
	s := grpc.NewServer()
	pb.RegisterContractInvocationServiceServer(s, contractInvocationServer)

	log.Printf("Contract Invocation Processor listening at %v", lis.Addr())
	log.Printf("Health check server listening at :%d", healthPort)
	
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
```

#### 2.2 Core Server Implementation

**go/server/server.go**

```go
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cipb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation"
	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
	rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"
)

// Constants for Protocol 23
const (
	PROTOCOL_23_ACTIVATION_LEDGER = 3000000 // Update with actual activation ledger
)

// ProcessorMetrics tracks comprehensive metrics for the processor
type ProcessorMetrics struct {
	mu                      sync.RWMutex
	TotalProcessed          int64
	TotalInvocations        int64
	SuccessfulInvocations   int64
	FailedInvocations       int64
	ContractCalls           int64
	CreateContracts         int64
	UploadWasms             int64
	ArchiveRestorations     int64
	ErrorCount              int64
	LastError               error
	LastErrorTime           time.Time
	LastProcessedLedger     uint32
	ProcessingLatency       time.Duration
	TotalEventsEmitted      int64
}

// ContractInvocationServer implements the gRPC service
type ContractInvocationServer struct {
	pb.UnimplementedContractInvocationServiceServer
	logger              *zap.Logger
	sourceServiceAddr   string
	networkPassphrase   string
	metrics             *ProcessorMetrics
	flowctlController   *FlowctlController
}

// NewContractInvocationServer creates a new server instance
func NewContractInvocationServer() (*ContractInvocationServer, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	sourceServiceAddr := os.Getenv("SOURCE_SERVICE_ADDRESS")
	if sourceServiceAddr == "" {
		sourceServiceAddr = "localhost:50052"
	}

	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		networkPassphrase = "Public Global Stellar Network ; September 2015"
	}

	server := &ContractInvocationServer{
		logger:            logger,
		sourceServiceAddr: sourceServiceAddr,
		networkPassphrase: networkPassphrase,
		metrics:           &ProcessorMetrics{},
	}

	// Initialize flowctl integration if enabled
	if os.Getenv("ENABLE_FLOWCTL") == "true" {
		server.flowctlController = NewFlowctlController(logger)
	}

	return server, nil
}

// GetContractInvocations implements the gRPC service method
func (s *ContractInvocationServer) GetContractInvocations(
	req *pb.GetInvocationsRequest,
	stream pb.ContractInvocationService_GetContractInvocationsServer,
) error {
	ctx := stream.Context()
	logger := s.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
	)
	logger.Info("received GetContractInvocations request")

	// Create source request
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
	}

	sourceCtx, cancelSourceStream := context.WithCancel(ctx)
	defer cancelSourceStream()

	// Connect to source service
	conn, err := grpc.DialContext(sourceCtx, s.sourceServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return status.Errorf(codes.Unavailable,
			"failed to connect to source service at %s: %v", s.sourceServiceAddr, err)
	}
	defer conn.Close()

	sourceClient := rawledger.NewRawLedgerServiceClient(conn)
	sourceStream, err := sourceClient.StreamRawLedgers(sourceCtx, sourceReq)
	if err != nil {
		return status.Errorf(codes.Internal,
			"failed to start source stream: %v", err)
	}

	logger.Info("connected to source service, starting to process ledgers")

	// Process ledgers
	for {
		select {
		case <-ctx.Done():
			logger.Info("client disconnected")
			return ctx.Err()
		default:
		}

		rawLedger, err := sourceStream.Recv()
		if err != nil {
			logger.Error("error receiving from source stream", zap.Error(err))
			return status.Errorf(codes.Internal, "source stream error: %v", err)
		}

		processingStart := time.Now()
		
		// Process the ledger
		events, err := s.processLedger(rawLedger, req)
		if err != nil {
			logger.Error("failed to process ledger",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Error(err))
			s.updateErrorMetrics(err, rawLedger.Sequence)
			continue
		}

		// Stream events to client
		eventsSent := 0
		for _, event := range events {
			if err := stream.Send(event); err != nil {
				logger.Error("failed to send event to client", zap.Error(err))
				return status.Errorf(codes.Internal, "failed to send event: %v", err)
			}
			eventsSent++
		}

		// Update metrics
		processingTime := time.Since(processingStart)
		s.updateSuccessMetrics(rawLedger.Sequence, processingTime, int64(eventsSent))

		logger.Debug("processed ledger",
			zap.Uint32("sequence", rawLedger.Sequence),
			zap.Int("events_sent", eventsSent),
			zap.Duration("processing_time", processingTime))

		// Check if we've reached the end ledger
		if req.EndLedger > 0 && rawLedger.Sequence >= req.EndLedger {
			logger.Info("reached end ledger, stopping stream")
			return nil
		}
	}
}

// processLedger processes a single ledger and extracts contract invocation events
func (s *ContractInvocationServer) processLedger(
	rawLedger *rawledger.RawLedger,
	req *pb.GetInvocationsRequest,
) ([]*cipb.ContractInvocationEvent, error) {
	var events []*cipb.ContractInvocationEvent

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LedgerCloseMeta: %w", err)
	}

	// Validate Protocol 23 compatibility
	if err := s.validateProtocol23Compatibility(lcm); err != nil {
		return nil, fmt.Errorf("protocol 23 validation failed: %w", err)
	}

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		s.networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		// Process each operation
		for opIndex, op := range tx.Envelope.Operations() {
			event := s.processOperation(tx, opIndex, op, lcm, req)
			if event != nil {
				events = append(events, event)
			}
		}
	}

	return events, nil
}

// processOperation processes a single operation and creates events if applicable
func (s *ContractInvocationServer) processOperation(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	lcm xdr.LedgerCloseMeta,
	req *pb.GetInvocationsRequest,
) *cipb.ContractInvocationEvent {
	
	switch op.Body.Type {
	case xdr.OperationTypeInvokeHostFunction:
		return s.processInvokeHostFunction(tx, opIndex, op, lcm, req)
	case xdr.OperationTypeCreateAccount:
		// Handle other Soroban operations if needed
		return nil
	default:
		return nil
	}
}

// processInvokeHostFunction processes InvokeHostFunction operations
func (s *ContractInvocationServer) processInvokeHostFunction(
	tx ingest.LedgerTransaction,
	opIndex int,
	op xdr.Operation,
	lcm xdr.LedgerCloseMeta,
	req *pb.GetInvocationsRequest,
) *cipb.ContractInvocationEvent {
	
	invokeOp := op.Body.MustInvokeHostFunctionOp()

	// Get invoking account
	var invokingAccount xdr.AccountId
	if op.SourceAccount != nil {
		invokingAccount = op.SourceAccount.ToAccountId()
	} else {
		invokingAccount = tx.Envelope.SourceAccount().ToAccountId()
	}

	// Determine success
	successful := s.isOperationSuccessful(tx, opIndex)

	// Filter by success if requested
	if req.SuccessfulOnly && !successful {
		return nil
	}

	// Create base metadata
	meta := &cipb.EventMeta{
		LedgerSequence: lcm.LedgerSequence(),
		ClosedAt:       timestamppb.New(time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)),
		TxHash:         tx.Result.TransactionHash.HexString(),
		TxIndex:        uint32(tx.Index),
		OpIndex:        uint32(opIndex),
		Successful:     successful,
	}

	// Add Protocol 23 specific metadata
	if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
		meta.DataSource = s.determineDataSource(lcm)
		meta.ArchiveRestorations = s.extractArchiveRestorations(tx, opIndex)
	}

	// Process based on host function type
	switch invokeOp.HostFunction.Type {
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		contractCall := s.processContractCall(invokeOp, tx, opIndex, invokingAccount, req)
		if contractCall != nil {
			s.metrics.mu.Lock()
			s.metrics.ContractCalls++
			s.metrics.mu.Unlock()
			
			return &cipb.ContractInvocationEvent{
				Meta: meta,
				InvocationType: &cipb.ContractInvocationEvent_ContractCall{
					ContractCall: contractCall,
				},
			}
		}

	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		createContract := s.processCreateContract(invokeOp, invokingAccount, req)
		if createContract != nil {
			s.metrics.mu.Lock()
			s.metrics.CreateContracts++
			s.metrics.mu.Unlock()
			
			return &cipb.ContractInvocationEvent{
				Meta: meta,
				InvocationType: &cipb.ContractInvocationEvent_CreateContract{
					CreateContract: createContract,
				},
			}
		}

	case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
		uploadWasm := s.processUploadWasm(invokingAccount, req)
		if uploadWasm != nil {
			s.metrics.mu.Lock()
			s.metrics.UploadWasms++
			s.metrics.mu.Unlock()
			
			return &cipb.ContractInvocationEvent{
				Meta: meta,
				InvocationType: &cipb.ContractInvocationEvent_UploadWasm{
					UploadWasm: uploadWasm,
				},
			}
		}
	}

	return nil
}

// Helper methods for processing specific invocation types
func (s *ContractInvocationServer) processContractCall(
	invokeOp xdr.InvokeHostFunctionOp,
	tx ingest.LedgerTransaction,
	opIndex int,
	invokingAccount xdr.AccountId,
	req *pb.GetInvocationsRequest,
) *cipb.ContractCall {
	
	invokeContract := invokeOp.HostFunction.MustInvokeContract()
	
	// Extract contract ID
	contractIDBytes := invokeContract.ContractAddress.ContractId
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
	if err != nil {
		s.logger.Error("failed to encode contract ID", zap.Error(err))
		return nil
	}

	// Apply contract ID filter
	if len(req.ContractIds) > 0 {
		found := false
		for _, filterID := range req.ContractIds {
			if filterID == contractID {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	// Extract function name
	functionName := string(invokeContract.FunctionName)
	
	// Apply function name filter
	if len(req.FunctionNames) > 0 {
		found := false
		for _, filterName := range req.FunctionNames {
			if filterName == functionName {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	// Apply invoking account filter
	invokingAccountStr := invokingAccount.Address()
	if len(req.InvokingAccounts) > 0 {
		found := false
		for _, filterAccount := range req.InvokingAccounts {
			if filterAccount == invokingAccountStr {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}

	// Extract arguments
	arguments := make([]*cipb.ScValue, len(invokeContract.Args))
	for i, arg := range invokeContract.Args {
		arguments[i] = s.convertScValToProto(arg)
	}

	return &cipb.ContractCall{
		ContractId:       contractID,
		InvokingAccount:  invokingAccountStr,
		FunctionName:     functionName,
		Arguments:        arguments,
		DiagnosticEvents: s.extractDiagnosticEvents(tx, opIndex),
		ContractCalls:    s.extractContractCalls(tx, opIndex),
		StateChanges:     s.extractStateChanges(tx, opIndex),
		TtlExtensions:    s.extractTtlExtensions(tx, opIndex),
	}
}

// Additional helper methods continue...
// (Implementation of other processing methods, utility functions, etc.)

// GetMetrics returns current processor metrics
func (s *ContractInvocationServer) GetMetrics() ProcessorMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return *s.metrics
}

func (s *ContractInvocationServer) updateSuccessMetrics(sequence uint32, latency time.Duration, eventsSent int64) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	s.metrics.TotalProcessed++
	s.metrics.LastProcessedLedger = sequence
	s.metrics.ProcessingLatency = latency
	s.metrics.TotalEventsEmitted += eventsSent
}

func (s *ContractInvocationServer) updateErrorMetrics(err error, sequence uint32) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	s.metrics.ErrorCount++
	s.metrics.LastError = err
	s.metrics.LastErrorTime = time.Now()
	s.metrics.LastProcessedLedger = sequence
}

// Protocol 23 validation methods
func (s *ContractInvocationServer) validateProtocol23Compatibility(lcm xdr.LedgerCloseMeta) error {
	if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
		// Implement actual Protocol 23 validation
		if err := s.validateDualBucketListHash(lcm); err != nil {
			return fmt.Errorf("dual bucket list validation failed: %w", err)
		}
		if err := s.validateHotArchiveBuckets(lcm); err != nil {
			return fmt.Errorf("hot archive validation failed: %w", err)
		}
	}
	return nil
}

func (s *ContractInvocationServer) validateDualBucketListHash(lcm xdr.LedgerCloseMeta) error {
	// TODO: Implement actual dual bucket list validation
	s.logger.Debug("validating dual bucket list hash",
		zap.Uint32("sequence", lcm.LedgerSequence()))
	return nil
}

func (s *ContractInvocationServer) validateHotArchiveBuckets(lcm xdr.LedgerCloseMeta) error {
	// TODO: Implement actual hot archive validation
	s.logger.Debug("validating hot archive buckets",
		zap.Uint32("sequence", lcm.LedgerSequence()))
	return nil
}

// Health check server
func (s *ContractInvocationServer) StartHealthCheckServer(port int) error {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		metrics := s.GetMetrics()
		
		status := "healthy"
		if metrics.ErrorCount > 0 && time.Since(metrics.LastErrorTime) < 5*time.Minute {
			status = "degraded"
		}

		response := map[string]interface{}{
			"status": status,
			"metrics": map[string]interface{}{
				"total_processed":        metrics.TotalProcessed,
				"total_invocations":      metrics.TotalInvocations,
				"successful_invocations": metrics.SuccessfulInvocations,
				"failed_invocations":     metrics.FailedInvocations,
				"contract_calls":         metrics.ContractCalls,
				"create_contracts":       metrics.CreateContracts,
				"upload_wasms":           metrics.UploadWasms,
				"archive_restorations":   metrics.ArchiveRestorations,
				"error_count":            metrics.ErrorCount,
				"last_processed_ledger":  metrics.LastProcessedLedger,
				"processing_latency":     metrics.ProcessingLatency.String(),
				"total_events_emitted":   metrics.TotalEventsEmitted,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%d", port)
	s.logger.Info("starting health check server", zap.String("address", addr))
	return http.ListenAndServe(addr, nil)
}
```

### Phase 3: Protocol 23 Compatibility

#### 3.1 Archive State Tracking

Implement Protocol 23 specific features for tracking archive state:

```go
// Protocol 23 helper methods
func (s *ContractInvocationServer) determineDataSource(lcm xdr.LedgerCloseMeta) string {
	// Logic to determine if data comes from live state or hot archive
	if lcm.V == 1 && lcm.V1 != nil {
		// Check for hot archive buckets presence
		return "live" // or "archive" based on actual analysis
	}
	return "live"
}

func (s *ContractInvocationServer) extractArchiveRestorations(
	tx ingest.LedgerTransaction,
	opIndex int,
) []*cipb.ArchiveRestoration {
	var restorations []*cipb.ArchiveRestoration
	
	// Check for automatic archive restorations in Soroban meta
	if tx.UnsafeMeta.V == 3 {
		sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
		if sorobanMeta != nil {
			// Extract restoration information from transaction meta
			// This would need to be implemented based on actual Protocol 23 structures
		}
	}
	
	return restorations
}
```

#### 3.2 Enhanced State Change Tracking

```go
func (s *ContractInvocationServer) extractStateChanges(
	tx ingest.LedgerTransaction,
	opIndex int,
) []*cipb.StateChange {
	var changes []*cipb.StateChange
	
	txChanges, err := tx.GetChanges()
	if err != nil {
		s.logger.Error("failed to get transaction changes", zap.Error(err))
		return changes
	}
	
	for _, change := range txChanges {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}
		
		stateChange := s.processLedgerEntryChange(change)
		if stateChange != nil {
			changes = append(changes, stateChange)
		}
	}
	
	return changes
}

func (s *ContractInvocationServer) processLedgerEntryChange(
	change ingest.Change,
) *cipb.StateChange {
	switch change.LedgerEntryChangeType() {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
			return s.createStateChangeFromContractData(
				*change.Post.Data.ContractData,
				nil,
				&change.Post.Data.ContractData.Val,
				cipb.StateChangeOperation_STATE_CHANGE_OPERATION_CREATE,
			)
		}
		
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		if change.Pre != nil && change.Post != nil &&
			change.Pre.Data.Type == xdr.LedgerEntryTypeContractData &&
			change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
			
			return s.createStateChangeFromContractData(
				*change.Post.Data.ContractData,
				&change.Pre.Data.ContractData.Val,
				&change.Post.Data.ContractData.Val,
				cipb.StateChangeOperation_STATE_CHANGE_OPERATION_UPDATE,
			)
		}
		
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
			return s.createStateChangeFromContractData(
				*change.Pre.Data.ContractData,
				&change.Pre.Data.ContractData.Val,
				nil,
				cipb.StateChangeOperation_STATE_CHANGE_OPERATION_DELETE,
			)
		}
	}
	
	return nil
}
```

### Phase 4: Consumer Applications

#### 4.1 Node.js Consumer

**consumer_app/contract_invocation_node/src/client.ts**

```typescript
import * as grpc from '@grpc/grpc-js';
import { ContractInvocationServiceClient } from '../gen/contract_invocation_service/contract_invocation_service_grpc_pb';
import { GetInvocationsRequest } from '../gen/contract_invocation_service/contract_invocation_service_pb';
import { ContractInvocationEvent } from '../gen/contract_invocation/contract_invocation_event_pb';

export class ContractInvocationClient {
    private client: ContractInvocationServiceClient;

    constructor(serverAddress: string) {
        this.client = new ContractInvocationServiceClient(
            serverAddress,
            grpc.credentials.createInsecure()
        );
    }

    getContractInvocations(
        startLedger: number,
        endLedger: number,
        onEvent: (event: ContractInvocationEvent) => void,
        filters?: {
            contractIds?: string[];
            functionNames?: string[];
            invokingAccounts?: string[];
            successfulOnly?: boolean;
        }
    ): void {
        const request = new GetInvocationsRequest();
        request.setStartLedger(startLedger);
        request.setEndLedger(endLedger);
        
        if (filters) {
            if (filters.contractIds) {
                request.setContractIdsList(filters.contractIds);
            }
            if (filters.functionNames) {
                request.setFunctionNamesList(filters.functionNames);
            }
            if (filters.invokingAccounts) {
                request.setInvokingAccountsList(filters.invokingAccounts);
            }
            if (filters.successfulOnly !== undefined) {
                request.setSuccessfulOnly(filters.successfulOnly);
            }
        }

        const stream = this.client.getContractInvocations(request);

        stream.on('data', (event: ContractInvocationEvent) => {
            onEvent(event);
        });

        stream.on('error', (error) => {
            console.error('Stream error:', error);
        });

        stream.on('end', () => {
            console.log('Stream ended');
        });
    }
}
```

#### 4.2 Go WASM Consumer

**consumer_app/contract_invocation_go_wasm/internal/client/client.go**

```go
package client

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cipb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation"
	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
)

type ContractInvocationCallback func(*cipb.ContractInvocationEvent) error

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

func (c *ContractInvocationClient) GetContractInvocations(
	ctx context.Context,
	startLedger uint32,
	endLedger uint32,
	callback ContractInvocationCallback,
	filters *pb.GetInvocationsRequest,
) error {
	req := &pb.GetInvocationsRequest{
		StartLedger: startLedger,
		EndLedger:   endLedger,
	}

	if filters != nil {
		req.ContractIds = filters.ContractIds
		req.FunctionNames = filters.FunctionNames
		req.InvokingAccounts = filters.InvokingAccounts
		req.SuccessfulOnly = filters.SuccessfulOnly
		req.TypeFilter = filters.TypeFilter
	}

	stream, err := c.client.GetContractInvocations(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get invocations: %w", err)
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("stream error: %w", err)
		}

		if err := callback(event); err != nil {
			log.Printf("callback error: %v", err)
		}
	}

	return nil
}
```

### Phase 5: Testing Strategy

#### 5.1 Unit Tests

```go
// go/server/server_test.go
package server

import (
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContractInvocationServer_processLedger(t *testing.T) {
	server, err := NewContractInvocationServer()
	require.NoError(t, err)

	// Create test LedgerCloseMeta with contract invocation
	testLedger := createTestLedgerWithContractInvocation()
	
	// Create test request
	req := &pb.GetInvocationsRequest{
		StartLedger: 100,
		EndLedger:   101,
	}

	// Process the ledger
	events, err := server.processLedger(testLedger, req)
	
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.NotNil(t, events[0].GetContractCall())
}

func TestContractInvocationServer_Protocol23Compatibility(t *testing.T) {
	server, err := NewContractInvocationServer()
	require.NoError(t, err)

	// Test Protocol 23 ledger
	testLedger := createTestProtocol23Ledger()
	
	err = server.validateProtocol23Compatibility(testLedger)
	assert.NoError(t, err)
}

func createTestLedgerWithContractInvocation() *xdr.LedgerCloseMeta {
	// Create a test ledger with contract invocation
	// Implementation details...
}

func createTestProtocol23Ledger() xdr.LedgerCloseMeta {
	// Create a test Protocol 23 ledger
	// Implementation details...
}
```

#### 5.2 Integration Tests

```go
// go/integration_test.go
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation_service"
)

func TestContractInvocationService_Integration(t *testing.T) {
	// Start test server
	go main()
	time.Sleep(2 * time.Second)

	// Connect to service
	conn, err := grpc.Dial("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewContractInvocationServiceClient(conn)

	// Test streaming
	req := &pb.GetInvocationsRequest{
		StartLedger: 1000000,
		EndLedger:   1000001,
	}

	stream, err := client.GetContractInvocations(context.Background(), req)
	require.NoError(t, err)

	eventCount := 0
	for {
		event, err := stream.Recv()
		if err != nil {
			break
		}
		eventCount++
		assert.NotNil(t, event.Meta)
	}

	t.Logf("Received %d events", eventCount)
}
```

### Phase 6: Deployment and Integration

#### 6.1 Docker Configuration

**Dockerfile**

```dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go/ .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o contract-invocation-processor .

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/contract-invocation-processor .

EXPOSE 50054 8089

CMD ["./contract-invocation-processor"]
```

#### 6.2 Flowctl Integration

**flowctl-pipeline.yaml**

```yaml
apiVersion: flow.obsrvr.dev/v1
kind: Pipeline
metadata:
  name: contract-invocation-pipeline
  namespace: default
spec:
  components:
    sources:
      - id: stellar-data-source
        type: stellar-live-source-datalake
        config:
          backend_type: ARCHIVE
          archive_storage_type: GCS
          archive_bucket_name: stellar-ledgers
          
    processors:
      - id: contract-invocation-processor
        type: contract-invocation-processor
        inputEventTypes:
          - raw_ledger_service.RawLedger
        outputEventTypes:
          - contract_invocation.ContractInvocationEvent
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
          enable_protocol_23: true
          
    sinks:
      - id: event-logger
        type: stdout-sink
        inputEventTypes:
          - contract_invocation.ContractInvocationEvent
          
  connections:
    - from: stellar-data-source
      to: contract-invocation-processor
    - from: contract-invocation-processor
      to: event-logger
```

## Testing Strategy

### Unit Testing
- **XDR Processing**: Test XDR unmarshaling and validation
- **Event Extraction**: Test contract invocation event creation
- **Filtering**: Test request filters (contract IDs, function names, accounts)
- **Protocol 23**: Test Protocol 23 specific features

### Integration Testing
- **gRPC Service**: Test end-to-end gRPC streaming
- **Source Integration**: Test connection to stellar-live-source services
- **Error Handling**: Test failure scenarios and recovery
- **Performance**: Load testing with high-volume data

### Consumer Testing
- **Multi-language Clients**: Test Go, Node.js, and WASM consumers
- **Real-time Streaming**: Test live data streaming capabilities
- **Filtering**: Test client-side filtering options

## Future Enhancements

### Phase 7: Advanced Features

1. **Enhanced Filtering**
   - Event content filtering (specific topics, data patterns)
   - Time-based filtering (recent events only)
   - Performance optimizations for large-scale filtering

2. **Analytics Integration**
   - Metrics collection for contract usage patterns
   - Performance analytics for contract execution
   - Cost analysis for contract operations

3. **Archive Integration**
   - Historical data queries against archive storage
   - Efficient retrieval of old contract invocations
   - Cross-ledger analysis capabilities

4. **Machine Learning Features**
   - Pattern detection in contract usage
   - Anomaly detection for suspicious contract behavior
   - Predictive analytics for contract performance

### Phase 8: Ecosystem Integration

1. **Multi-chain Support**
   - Extend to other Stellar-compatible networks
   - Cross-chain contract invocation tracking
   - Unified interface for multiple networks

2. **External Integrations**
   - Webhook support for external systems
   - Database connectors for direct storage
   - Message queue integration for scalable processing

3. **Developer Tools**
   - CLI tools for contract monitoring
   - Dashboard for contract analytics
   - Documentation and examples

## Conclusion

This implementation plan provides a comprehensive roadmap for creating a production-ready Contract Invocation Processor that follows the proven patterns established by the TTP processor while adding Protocol 23 compatibility and advanced contract tracking capabilities. The modular architecture ensures maintainability and extensibility for future enhancements.

The processor will provide real-time streaming of contract invocation events with comprehensive filtering capabilities, making it an essential tool for developers building on the Stellar network and monitoring Soroban smart contract activity.