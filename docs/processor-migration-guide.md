# Processor Migration Guide: CDP Pipeline to gRPC Streaming

A step-by-step guide for converting CDP pipeline processors to gRPC streaming architecture, using ContractInvocationProcessor as a practical example.

## ContractInvocationProcessor Analysis

The ContractInvocationProcessor is actually a **comprehensive Soroban Smart Contract Execution Analyzer**, not just an event processor. It handles:

### Scope of Analysis:
- **Contract Invocations**: Function calls with decoded arguments and return values
- **Contract State Changes**: Key-value modifications during execution
- **Diagnostic Events**: Events emitted during contract execution  
- **Contract Lifecycle**: Creation, deployment, and destruction
- **Cross-Contract Calls**: Contract-to-contract interactions
- **Execution Context**: Success/failure, gas usage, error details
- **Raw XDR Decoding**: Deep blockchain data extraction and transformation

### Better Service Names:
- **SorobanExecutionAnalyzer** - Comprehensive contract execution analysis
- **ContractInteractionService** - Contract calls, state, and events
- **SorobanContractProcessor** - Complete Soroban contract processing

This is enterprise-grade smart contract analytics, not simple event streaming.

## Overview

This guide shows how to migrate processors from the CDP pipeline workflow's message-based architecture to the TTP processor's gRPC streaming architecture.

### Architecture Comparison

**CDP Pipeline Architecture:**
```go
type Processor interface {
    Process(context.Context, Message) error
    Subscribe(Processor)
}

// Pipeline processing
msg := Message{Payload: ledgerData}
processor.Process(ctx, msg) // Forwards to downstream processors
```

**gRPC Streaming Architecture:**
```go
service ContractInvocationService {
    rpc GetContractInvocations(GetEventsRequest) returns (stream ContractInvocationEvent);
}

// Streaming processing
stream.Send(contractInvocationEvent) // Direct to consumer
```

## Migration Steps

### Step 1: Define Protocol Buffer Schema

First, create the protobuf definitions for your processor's events.

**File: `protos/soroban_execution_service/soroban_execution_service.proto`**

```protobuf
syntax = "proto3";

package soroban_execution.v1;

option go_package = "github.com/your-org/soroban-execution-processor/gen/soroban_execution_service";

import "google/protobuf/timestamp.proto";

// Comprehensive Soroban smart contract execution analysis service
service SorobanExecutionService {
    rpc GetContractExecutions(GetExecutionsRequest) returns (stream ContractExecutionAnalysis);
    rpc GetContractInvocations(GetExecutionsRequest) returns (stream ContractInvocation);
    rpc GetContractStateChanges(GetExecutionsRequest) returns (stream ContractStateChange);
    rpc GetContractEvents(GetExecutionsRequest) returns (stream ContractDiagnosticEvent);
}

message GetExecutionsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;    // 0 for unbounded stream
    
    // Optional filters
    repeated string contract_ids = 3;      // Filter by specific contracts
    repeated string function_names = 4;    // Filter by function names
    repeated string invoking_accounts = 5; // Filter by calling accounts
    bool successful_only = 6;              // Only successful executions
}

// Comprehensive contract execution analysis (main aggregated view)
message ContractExecutionAnalysis {
    // Ledger context
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Execution metadata
    ExecutionContext execution_context = 5;
    
    // Contract interaction details
    ContractInvocation invocation = 6;
    
    // State and event changes
    repeated ContractStateChange state_changes = 7;
    repeated ContractDiagnosticEvent diagnostic_events = 8;
    
    // Cross-contract interactions
    repeated ContractInteraction sub_invocations = 9;
    
    // Resource usage
    ResourceUsage resource_usage = 10;
    
    // Raw data for backward compatibility
    string raw_execution_data = 11;
}

message ExecutionContext {
    bool successful = 1;
    string execution_type = 2; // "invoke_contract", "create_contract", "restore_contract"
    string error_code = 3;
    string error_message = 4;
    uint32 instruction_count = 5;
    uint64 cpu_instructions = 6;
    uint64 memory_bytes = 7;
}

message ContractInvocation {
    string contract_id = 1;
    string invoking_account = 2;
    string function_name = 3;
    repeated ContractArgument arguments = 4;
    ContractReturnValue return_value = 5;
    
    // Contract metadata
    string contract_wasm_hash = 6;
    ContractType contract_type = 7;
}

enum ContractType {
    CONTRACT_TYPE_UNSPECIFIED = 0;
    CONTRACT_TYPE_WASM = 1;
    CONTRACT_TYPE_STELLAR_ASSET = 2;
}

message ContractArgument {
    string name = 1;
    string scval_type = 2;        // ScVal type (e.g., "Symbol", "U64", "Instance")
    string decoded_value = 3;     // Human-readable value
    string raw_xdr = 4;          // Raw XDR for precise reconstruction
    
    // For complex types
    repeated ContractArgument nested_args = 5; // For maps, vectors, tuples
}

message ContractReturnValue {
    string scval_type = 1;
    string decoded_value = 2;
    string raw_xdr = 3;
    bool is_void = 4;
}

message ContractStateChange {
    string contract_id = 1;
    StateChangeType change_type = 2;
    string key = 3;
    string key_type = 4;           // Type of the key (Symbol, etc.)
    string old_value = 5;
    string new_value = 6;
    string value_type = 7;         // Type of the value
    uint32 old_ttl = 8;           // Time-to-live before change
    uint32 new_ttl = 9;           // Time-to-live after change
}

enum StateChangeType {
    STATE_CHANGE_TYPE_UNSPECIFIED = 0;
    STATE_CHANGE_TYPE_CREATED = 1;
    STATE_CHANGE_TYPE_UPDATED = 2;
    STATE_CHANGE_TYPE_DELETED = 3;
    STATE_CHANGE_TYPE_TTL_EXTENDED = 4;
}

message ContractDiagnosticEvent {
    string contract_id = 1;
    repeated EventTopic topics = 2;
    EventData data = 3;
    bool in_successful_contract_call = 4;
    EventType event_type = 5;
}

message EventTopic {
    string scval_type = 1;
    string decoded_value = 2;
    string raw_xdr = 3;
}

message EventData {
    string scval_type = 1;
    string decoded_value = 2;
    string raw_xdr = 3;
}

enum EventType {
    EVENT_TYPE_UNSPECIFIED = 0;
    EVENT_TYPE_CONTRACT = 1;
    EVENT_TYPE_SYSTEM = 2;
    EVENT_TYPE_DIAGNOSTIC = 3;
}

message ContractInteraction {
    string from_contract = 1;
    string to_contract = 2;
    string function_name = 3;
    repeated ContractArgument arguments = 4;
    ContractReturnValue return_value = 5;
    bool successful = 6;
}

message ResourceUsage {
    uint64 cpu_instructions = 1;
    uint64 memory_bytes = 2;
    uint64 ledger_read_bytes = 3;
    uint64 ledger_write_bytes = 4;
    uint64 transaction_size_bytes = 5;
    uint32 events_and_return_value_size = 6;
}

// Specialized messages for individual stream endpoints

message ContractInvocation {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    string contract_id = 4;
    string invoking_account = 5;
    string function_name = 6;
    repeated ContractArgument arguments = 7;
    ContractReturnValue return_value = 8;
    bool successful = 9;
    string error_message = 10;
}

message ContractStateChange {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    string contract_id = 4;
    StateChangeType change_type = 5;
    string key = 6;
    string old_value = 7;
    string new_value = 8;
    uint32 old_ttl = 9;
    uint32 new_ttl = 10;
}

message ContractDiagnosticEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    string contract_id = 4;
    repeated EventTopic topics = 5;
    EventData data = 6;
    bool in_successful_contract_call = 7;
    EventType event_type = 8;
}
```

### Step 2: Extract Core Processing Logic

Extract the processing logic from the CDP processor into a reusable component.

**File: `go/processor/contract_invocation_extractor.go`**

```go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/stellar/go/xdr"
    "go.uber.org/zap"
    
    pb "github.com/your-org/contract-invocation-processor/gen/contract_invocation_service"
)

// ContractInvocationExtractor extracts contract invocation data from ledgers
type ContractInvocationExtractor struct {
    networkPassphrase string
    logger           *zap.Logger
}

// CDP's ContractInvocation struct for data extraction
type ContractInvocation struct {
    ContractID        string                 `json:"contract_id"`
    InvokingAccount   string                 `json:"invoking_account"`
    TransactionHash   string                 `json:"transaction_hash"`
    FunctionName      string                 `json:"function_name"`
    Arguments         []ContractArgument     `json:"arguments"`
    DiagnosticEvents  []DiagnosticEvent      `json:"diagnostic_events"`
    Successful        bool                   `json:"successful"`
    Result            *ContractResult        `json:"result,omitempty"`
    StateChanges      []StateChange          `json:"state_changes"`
    TTLExtensions     []TTLExtension         `json:"ttl_extensions"`
    Ledger            uint32                 `json:"ledger"`
    Timestamp         time.Time              `json:"timestamp"`
}

func NewContractInvocationExtractor(networkPassphrase string, logger *zap.Logger) *ContractInvocationExtractor {
    return &ContractInvocationExtractor{
        networkPassphrase: networkPassphrase,
        logger:           logger,
    }
}

// ExtractInvocations processes a ledger and extracts contract invocations
func (c *ContractInvocationExtractor) ExtractInvocations(lcm xdr.LedgerCloseMeta) ([]*ContractInvocation, error) {
    var invocations []*ContractInvocation
    
    // Get ledger header for timestamp
    var ledgerHeader xdr.LedgerHeader
    var txProcessing []xdr.TransactionResultMeta
    var txSet []xdr.TransactionEnvelope
    
    switch lcm.V {
    case 0:
        ledgerHeader = lcm.V0.LedgerHeader
        txProcessing = lcm.V0.TxProcessing
        // Extract transaction set from V0
        txSet = c.extractTxSetV0(lcm.V0)
    case 1:
        ledgerHeader = lcm.V1.LedgerHeader
        txProcessing = lcm.V1.TxProcessing
        // Extract transaction set from V1
        txSet = c.extractTxSetV1(lcm.V1)
    case 2:
        // Protocol 23+ V2 format
        ledgerHeader = lcm.V2.LedgerHeader
        txProcessing = lcm.V2.TxProcessing
        txSet = c.extractTxSetV2(lcm.V2)
    default:
        return nil, fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
    }
    
    ledgerSequence := uint32(ledgerHeader.LedgerSeq)
    ledgerCloseTime := time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0)
    
    // Process each transaction
    for i, txMeta := range txProcessing {
        if i >= len(txSet) {
            c.logger.Warn("Transaction processing count exceeds transaction set count",
                zap.Int("tx_processing_count", len(txProcessing)),
                zap.Int("tx_set_count", len(txSet)))
            break
        }
        
        tx := txSet[i]
        txHash := txMeta.TxResult.TransactionHash.HexString()
        
        // Extract contract invocations from this transaction
        txInvocations := c.extractTransactionInvocations(tx, txMeta, ledgerSequence, ledgerCloseTime, txHash)
        invocations = append(invocations, txInvocations...)
    }
    
    return invocations, nil
}

func (c *ContractInvocationExtractor) extractTransactionInvocations(
    tx xdr.TransactionEnvelope,
    txMeta xdr.TransactionResultMeta,
    ledgerSequence uint32,
    ledgerCloseTime time.Time,
    txHash string,
) []*ContractInvocation {
    var invocations []*ContractInvocation
    
    // Check if transaction was successful
    if !txMeta.TxResult.Result.Successful() {
        return invocations
    }
    
    // Extract source account
    sourceAccount := tx.SourceAccount().ToAccountId().Address()
    
    // Process operations for contract invocations
    operations := tx.Operations()
    for _, op := range operations {
        if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
            invocation := c.extractHostFunctionInvocation(
                op, txMeta, sourceAccount, txHash, ledgerSequence, ledgerCloseTime)
            if invocation != nil {
                invocations = append(invocations, invocation)
            }
        }
    }
    
    return invocations
}

func (c *ContractInvocationExtractor) extractHostFunctionInvocation(
    op xdr.Operation,
    txMeta xdr.TransactionResultMeta,
    sourceAccount string,
    txHash string,
    ledgerSequence uint32,
    ledgerCloseTime time.Time,
) *ContractInvocation {
    hostFn := op.Body.InvokeHostFunctionOp
    
    // Extract contract invocation details based on host function type
    switch hostFn.HostFunction.Type {
    case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
        return c.extractContractInvocation(hostFn, txMeta, sourceAccount, txHash, ledgerSequence, ledgerCloseTime)
    case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
        return c.extractContractCreation(hostFn, txMeta, sourceAccount, txHash, ledgerSequence, ledgerCloseTime)
    default:
        c.logger.Debug("Unsupported host function type",
            zap.String("type", hostFn.HostFunction.Type.String()))
        return nil
    }
}

func (c *ContractInvocationExtractor) extractContractInvocation(
    hostFn *xdr.InvokeHostFunctionOp,
    txMeta xdr.TransactionResultMeta,
    sourceAccount string,
    txHash string,
    ledgerSequence uint32,
    ledgerCloseTime time.Time,
) *ContractInvocation {
    invokeContract := hostFn.HostFunction.InvokeContract
    
    // Extract contract address
    contractAddress := invokeContract.ContractAddress
    contractId := contractAddress.ContractId().HexString()
    
    // Extract function name
    functionName := string(invokeContract.FunctionName)
    
    // Extract arguments
    var arguments []ContractArgument
    for i, arg := range invokeContract.Args {
        arguments = append(arguments, ContractArgument{
            Name:   fmt.Sprintf("arg_%d", i),
            Type:   arg.Type.String(),
            Value:  c.decodeScVal(arg),
            RawXDR: c.encodeXDR(arg),
        })
    }
    
    // Extract diagnostic events from transaction meta
    diagnosticEvents := c.extractDiagnosticEvents(txMeta)
    
    // Extract state changes
    stateChanges := c.extractStateChanges(txMeta)
    
    // Extract TTL extensions
    ttlExtensions := c.extractTTLExtensions(txMeta)
    
    // Determine if invocation was successful
    successful := c.isInvocationSuccessful(txMeta)
    
    return &ContractInvocation{
        ContractID:       contractId,
        InvokingAccount:  sourceAccount,
        TransactionHash:  txHash,
        FunctionName:     functionName,
        Arguments:        arguments,
        DiagnosticEvents: diagnosticEvents,
        Successful:       successful,
        StateChanges:     stateChanges,
        TTLExtensions:    ttlExtensions,
        Ledger:           ledgerSequence,
        Timestamp:        ledgerCloseTime,
    }
}

// Helper methods for data extraction
func (c *ContractInvocationExtractor) extractDiagnosticEvents(txMeta xdr.TransactionResultMeta) []DiagnosticEvent {
    var events []DiagnosticEvent
    
    // Extract diagnostic events from transaction meta
    for _, opMeta := range txMeta.TxResult.Result.Results {
        if opMeta.Tr != nil && opMeta.Tr.InvokeHostFunctionResult != nil {
            for _, event := range opMeta.Tr.InvokeHostFunctionResult.Events {
                events = append(events, DiagnosticEvent{
                    Topics:                   c.extractTopics(event.Event.V0.Topics),
                    Data:                     c.decodeScVal(event.Event.V0.Data),
                    InSuccessfulContractCall: event.InSuccessfulContractCall,
                    ContractId:               event.Event.V0.ContractId.HexString(),
                })
            }
        }
    }
    
    return events
}

func (c *ContractInvocationExtractor) extractStateChanges(txMeta xdr.TransactionResultMeta) []StateChange {
    var changes []StateChange
    
    // Extract state changes from ledger entry changes
    for _, change := range txMeta.TxChanges {
        if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryState {
            // Process state changes
            changes = append(changes, StateChange{
                Key:        c.extractLedgerKey(change.State.Data),
                ChangeType: "updated",
                // Extract old/new values based on change type
            })
        }
    }
    
    return changes
}

func (c *ContractInvocationExtractor) extractTTLExtensions(txMeta xdr.TransactionResultMeta) []TTLExtension {
    var extensions []TTLExtension
    
    // Extract TTL extensions from transaction meta
    // Implementation depends on specific XDR structure
    
    return extensions
}

// ConvertToProtobuf converts CDP ContractInvocation to protobuf event
func (c *ContractInvocationExtractor) ConvertToProtobuf(invocation *ContractInvocation) (*pb.ContractInvocationEvent, error) {
    // Convert arguments
    var pbArgs []*pb.ContractArgument
    for _, arg := range invocation.Arguments {
        pbArgs = append(pbArgs, &pb.ContractArgument{
            Name:   arg.Name,
            Type:   arg.Type,
            Value:  arg.Value,
            RawXdr: arg.RawXDR,
        })
    }
    
    // Convert diagnostic events
    var pbEvents []*pb.DiagnosticEvent
    for _, event := range invocation.DiagnosticEvents {
        pbEvents = append(pbEvents, &pb.DiagnosticEvent{
            Topics:                   event.Topics,
            Data:                     event.Data,
            InSuccessfulContractCall: event.InSuccessfulContractCall,
            ContractId:               event.ContractId,
        })
    }
    
    // Convert state changes
    var pbStateChanges []*pb.StateChange
    for _, change := range invocation.StateChanges {
        pbStateChanges = append(pbStateChanges, &pb.StateChange{
            Key:        change.Key,
            OldValue:   change.OldValue,
            NewValue:   change.NewValue,
            ChangeType: change.ChangeType,
        })
    }
    
    // Convert TTL extensions
    var pbTTLExtensions []*pb.TTLExtension
    for _, ext := range invocation.TTLExtensions {
        pbTTLExtensions = append(pbTTLExtensions, &pb.TTLExtension{
            Key:    ext.Key,
            OldTtl: ext.OldTTL,
            NewTtl: ext.NewTTL,
        })
    }
    
    // Serialize original data for backward compatibility
    rawData, err := json.Marshal(invocation)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal raw invocation data: %w", err)
    }
    
    return &pb.ContractInvocationEvent{
        LedgerSequence:    invocation.Ledger,
        LedgerCloseTime:   c.timeToTimestamp(invocation.Timestamp),
        TransactionHash:   invocation.TransactionHash,
        ContractId:        invocation.ContractID,
        InvokingAccount:   invocation.InvokingAccount,
        FunctionName:      invocation.FunctionName,
        Arguments:         pbArgs,
        DiagnosticEvents:  pbEvents,
        StateChanges:      &pb.ContractState{Changes: pbStateChanges, TtlExtensions: pbTTLExtensions},
        Successful:        invocation.Successful,
        RawInvocationData: string(rawData),
    }, nil
}

// Helper methods for XDR processing
func (c *ContractInvocationExtractor) decodeScVal(val xdr.ScVal) string {
    // Implement ScVal decoding logic
    return val.String()
}

func (c *ContractInvocationExtractor) encodeXDR(val interface{}) string {
    // Implement XDR encoding logic
    return fmt.Sprintf("%v", val)
}

func (c *ContractInvocationExtractor) extractTopics(topics []xdr.ScVal) []string {
    var result []string
    for _, topic := range topics {
        result = append(result, c.decodeScVal(topic))
    }
    return result
}

func (c *ContractInvocationExtractor) isInvocationSuccessful(txMeta xdr.TransactionResultMeta) bool {
    return txMeta.TxResult.Result.Successful()
}

func (c *ContractInvocationExtractor) extractLedgerKey(data xdr.LedgerEntryData) string {
    // Extract ledger key based on entry type
    return fmt.Sprintf("%v", data)
}

func (c *ContractInvocationExtractor) timeToTimestamp(t time.Time) *timestamppb.Timestamp {
    return &timestamppb.Timestamp{
        Seconds: t.Unix(),
        Nanos:   int32(t.Nanosecond()),
    }
}

// Transaction set extraction methods for different LCM versions
func (c *ContractInvocationExtractor) extractTxSetV0(v0 *xdr.LedgerCloseMetaV0) []xdr.TransactionEnvelope {
    // Implement V0 transaction set extraction
    return v0.TxSet.Txs
}

func (c *ContractInvocationExtractor) extractTxSetV1(v1 *xdr.LedgerCloseMetaV1) []xdr.TransactionEnvelope {
    // Implement V1 transaction set extraction
    if len(v1.TxSet.V1TxSet.Phases) > 0 {
        return v1.TxSet.V1TxSet.Phases[0].V0Components[0].TxsMaybeDiscountedFee.Txs
    }
    return nil
}

func (c *ContractInvocationExtractor) extractTxSetV2(v2 *xdr.LedgerCloseMetaV2) []xdr.TransactionEnvelope {
    // Implement V2 transaction set extraction (Protocol 23+)
    if len(v2.TxSet.V1TxSet.Phases) > 0 {
        return v2.TxSet.V1TxSet.Phases[0].V0Components[0].TxsMaybeDiscountedFee.Txs
    }
    return nil
}
```

### Step 3: Create gRPC Service Implementation

Now implement the gRPC streaming service using the extracted logic.

**File: `go/server/server.go`**

```go
package server

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "sync"
    "time"

    // Import the generated protobuf code for the service WE PROVIDE
    sorobanservice "github.com/your-org/soroban-execution-processor/gen/soroban_execution_service"
    // Import the generated protobuf code for the service WE CONSUME
    rawledger "github.com/stellar/stellar-live-source/gen/raw_ledger_service"

    "github.com/your-org/soroban-execution-processor/go/processor"
    "github.com/stellar/go/xdr"
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// SorobanExecutionMetrics tracks comprehensive processing metrics
type SorobanExecutionMetrics struct {
    mu sync.RWMutex
    
    // Core metrics
    SuccessCount          int64
    ErrorCount            int64
    TotalLedgersProcessed int64
    LastError             error
    LastErrorTime         time.Time
    StartTime             time.Time
    LastProcessedLedger   uint32
    ProcessingLatency     time.Duration
    
    // Soroban-specific metrics
    TotalContractInvocations int64
    TotalStateChanges        int64
    TotalDiagnosticEvents    int64
    TotalCrossContractCalls  int64
    SuccessfulInvocations    int64
    FailedInvocations        int64
    
    // Resource usage aggregates
    TotalCPUInstructions     uint64
    TotalMemoryBytes         uint64
    TotalLedgerReadBytes     uint64
    TotalLedgerWriteBytes    uint64
    
    // Contract type breakdown
    WasmContractInvocations       int64
    StellarAssetContractInvocations int64
}

// SorobanExecutionServer implements the comprehensive SorobanExecutionServiceServer interface
type SorobanExecutionServer struct {
    sorobanservice.UnimplementedSorobanExecutionServiceServer

    analyzer        *processor.SorobanExecutionAnalyzer  // Renamed and enhanced
    rawLedgerClient rawledger.RawLedgerServiceClient
    rawLedgerConn   *grpc.ClientConn
    logger          *zap.Logger
    metrics         *SorobanExecutionMetrics
}

// NewSorobanExecutionServer creates a comprehensive Soroban execution analysis server
func NewSorobanExecutionServer(passphrase string, sourceServiceAddr string) (*SorobanExecutionServer, error) {
    logger, err := zap.NewProduction()
    if err != nil {
        return nil, fmt.Errorf("failed to initialize zap logger: %w", err)
    }

    analyzer := processor.NewSorobanExecutionAnalyzer(passphrase, logger)

    logger.Info("connecting to raw ledger source",
        zap.String("source_address", sourceServiceAddr))

    conn, err := grpc.Dial(sourceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to raw ledger source service at %s: %w", sourceServiceAddr, err)
    }
    logger.Info("successfully connected to raw ledger source service")

    client := rawledger.NewRawLedgerServiceClient(conn)

    return &SorobanExecutionServer{
        analyzer:        analyzer,
        rawLedgerClient: client,
        rawLedgerConn:   conn,
        logger:          logger,
        metrics: &SorobanExecutionMetrics{
            StartTime: time.Now(),
        },
    }, nil
}

// Close cleans up resources
func (s *SorobanExecutionServer) Close() error {
    s.logger.Info("closing connection to raw ledger source service")
    if s.rawLedgerConn != nil {
        return s.rawLedgerConn.Close()
    }
    return nil
}

// GetContractExecutions implements comprehensive contract execution analysis streaming
func (s *SorobanExecutionServer) GetContractExecutions(
    req *sorobanservice.GetExecutionsRequest,
    stream sorobanservice.SorobanExecutionService_GetContractExecutionsServer,
) error {
    return s.streamExecutionData(req, "comprehensive", func(analysis *processor.ContractExecutionAnalysis) error {
        pbAnalysis, err := s.analyzer.ConvertExecutionAnalysisToProtobuf(analysis)
        if err != nil {
            return err
        }
        return stream.Send(pbAnalysis)
    })
}

// GetContractInvocations implements contract invocation-only streaming
func (s *SorobanExecutionServer) GetContractInvocations(
    req *sorobanservice.GetExecutionsRequest,
    stream sorobanservice.SorobanExecutionService_GetContractInvocationsServer,
) error {
    return s.streamExecutionData(req, "invocations", func(analysis *processor.ContractExecutionAnalysis) error {
        for _, invocation := range analysis.Invocations {
            pbInvocation, err := s.analyzer.ConvertInvocationToProtobuf(invocation)
            if err != nil {
                return err
            }
            if err := stream.Send(pbInvocation); err != nil {
                return err
            }
        }
        return nil
    })
}

// GetContractStateChanges implements state change-only streaming
func (s *SorobanExecutionServer) GetContractStateChanges(
    req *sorobanservice.GetExecutionsRequest,
    stream sorobanservice.SorobanExecutionService_GetContractStateChangesServer,
) error {
    return s.streamExecutionData(req, "state_changes", func(analysis *processor.ContractExecutionAnalysis) error {
        for _, stateChange := range analysis.StateChanges {
            pbStateChange, err := s.analyzer.ConvertStateChangeToProtobuf(stateChange)
            if err != nil {
                return err
            }
            if err := stream.Send(pbStateChange); err != nil {
                return err
            }
        }
        return nil
    })
}

// GetContractEvents implements diagnostic event-only streaming
func (s *SorobanExecutionServer) GetContractEvents(
    req *sorobanservice.GetExecutionsRequest,
    stream sorobanservice.SorobanExecutionService_GetContractEventsServer,
) error {
    return s.streamExecutionData(req, "events", func(analysis *processor.ContractExecutionAnalysis) error {
        for _, event := range analysis.DiagnosticEvents {
            pbEvent, err := s.analyzer.ConvertDiagnosticEventToProtobuf(event)
            if err != nil {
                return err
            }
            if err := stream.Send(pbEvent); err != nil {
                return err
            }
        }
        return nil
    })
}

// streamExecutionData is the core streaming implementation shared by all endpoints
func (s *SorobanExecutionServer) streamExecutionData(
    req *sorobanservice.GetExecutionsRequest,
    streamType string,
    sendFunc func(*processor.ContractExecutionAnalysis) error,
) error {
    ctx := context.Background() // This will be fixed to use the stream context
    logger := s.logger.With(
        zap.Uint32("start_ledger", req.StartLedger),
        zap.Uint32("end_ledger", req.EndLedger),
        zap.String("stream_type", streamType),
    )
    logger.Info("received Soroban execution analysis request")

    // Create a request for the raw ledger source service
    sourceReq := &rawledger.StreamLedgersRequest{
        StartLedger: req.StartLedger,
    }

    // Create a new context for the outgoing call to the source service
    sourceCtx, cancelSourceStream := context.WithCancel(ctx)
    defer cancelSourceStream()

    logger.Info("requesting raw ledger stream from source service")
    rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
    if err != nil {
        logger.Error("failed to connect to raw ledger source", zap.Error(err))
        s.recordError(err, 0)
        return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
    }
    logger.Info("successfully initiated raw ledger stream")

    // Process ledgers
    for {
        // Check if the consumer's context is cancelled first
        select {
        case <-ctx.Done():
            logger.Info("consumer context cancelled, stopping execution analysis stream",
                zap.Error(ctx.Err()))
            cancelSourceStream()
            return status.FromContextError(ctx.Err()).Err()
        default:
            // Continue processing
        }

        // Receive the next raw ledger message
        rawLedgerMsg, err := rawLedgerStream.Recv()
        if err == io.EOF {
            logger.Error("raw ledger source stream ended unexpectedly")
            s.recordError(err, 0)
            return status.Error(codes.Unavailable, "raw ledger source stream ended unexpectedly")
        }
        if err != nil {
            if status.Code(err) == codes.Canceled && ctx.Err() != nil {
                logger.Info("source stream cancelled due to consumer disconnection")
                return status.FromContextError(ctx.Err()).Err()
            }
            logger.Error("error receiving from raw ledger source stream", zap.Error(err))
            s.recordError(err, 0)
            return status.Errorf(codes.Internal, "error receiving data from raw ledger source: %v", err)
        }

        ledgerLogger := logger.With(zap.Uint32("ledger_sequence", rawLedgerMsg.Sequence))

        // Check if we need to stop based on the consumer's requested endLedger
        if req.EndLedger > 0 && rawLedgerMsg.Sequence > req.EndLedger {
            ledgerLogger.Info("reached end ledger requested by consumer")
            cancelSourceStream()
            return nil
        }

        ledgerLogger.Debug("processing raw ledger from source")

        // Process this ledger
        if err := s.processLedgerForStream(rawLedgerMsg, req, sendFunc, ledgerLogger); err != nil {
            ledgerLogger.Error("failed to process ledger", zap.Error(err))
            s.recordError(err, rawLedgerMsg.Sequence)
            // Continue processing other ledgers rather than terminating
            continue
        }
    }
}

func (s *SorobanExecutionServer) processLedgerForStream(
    rawLedgerMsg *rawledger.RawLedger,
    req *sorobanservice.GetExecutionsRequest,
    sendFunc func(*processor.ContractExecutionAnalysis) error,
    logger *zap.Logger,
) error {
    processingStart := time.Now()

    // Unmarshal the raw XDR bytes into a LedgerCloseMeta object
    var lcm xdr.LedgerCloseMeta
    _, err := xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
    if err != nil {
        return fmt.Errorf("failed to unmarshal ledger %d XDR: %w", rawLedgerMsg.Sequence, err)
    }

    // Perform comprehensive contract execution analysis
    analyses, err := s.analyzer.AnalyzeContractExecutions(lcm, req)
    if err != nil {
        return fmt.Errorf("failed to analyze contract executions for ledger %d: %w", lcm.LedgerSequence(), err)
    }

    // Stream each analysis result using the provided send function
    analysesSent := 0
    for _, analysis := range analyses {
        if err := sendFunc(analysis); err != nil {
            return fmt.Errorf("failed to send execution analysis to consumer: %w", err)
        }
        analysesSent++
    }

    // Record comprehensive metrics
    processingTime := time.Since(processingStart)
    s.recordAnalysisSuccess(lcm.LedgerSequence(), analyses, processingTime)

    logger.Info("finished processing ledger",
        zap.Int("analyses_sent", analysesSent),
        zap.Duration("processing_time", processingTime))

    return nil
}

func (s *SorobanExecutionServer) recordAnalysisSuccess(
    ledgerSeq uint32,
    analyses []*processor.ContractExecutionAnalysis,
    processingTime time.Duration,
) {
    s.metrics.mu.Lock()
    defer s.metrics.mu.Unlock()

    s.metrics.SuccessCount++
    s.metrics.TotalLedgersProcessed++
    s.metrics.LastProcessedLedger = ledgerSeq
    s.metrics.ProcessingLatency = processingTime

    // Aggregate Soroban-specific metrics
    for _, analysis := range analyses {
        s.metrics.TotalContractInvocations += int64(len(analysis.Invocations))
        s.metrics.TotalStateChanges += int64(len(analysis.StateChanges))
        s.metrics.TotalDiagnosticEvents += int64(len(analysis.DiagnosticEvents))
        s.metrics.TotalCrossContractCalls += int64(len(analysis.CrossContractCalls))
        
        for _, invocation := range analysis.Invocations {
            if invocation.Successful {
                s.metrics.SuccessfulInvocations++
            } else {
                s.metrics.FailedInvocations++
            }
            
            // Track contract type breakdown
            switch invocation.ContractType {
            case "wasm":
                s.metrics.WasmContractInvocations++
            case "stellar_asset":
                s.metrics.StellarAssetContractInvocations++
            }
        }
        
        // Aggregate resource usage
        if analysis.ResourceUsage != nil {
            s.metrics.TotalCPUInstructions += analysis.ResourceUsage.CPUInstructions
            s.metrics.TotalMemoryBytes += analysis.ResourceUsage.MemoryBytes
            s.metrics.TotalLedgerReadBytes += analysis.ResourceUsage.LedgerReadBytes
            s.metrics.TotalLedgerWriteBytes += analysis.ResourceUsage.LedgerWriteBytes
        }
    }
}

func (s *SorobanExecutionServer) recordError(err error, ledgerSeq uint32) {
    s.metrics.mu.Lock()
    defer s.metrics.mu.Unlock()

    s.metrics.ErrorCount++
    s.metrics.LastError = err
    s.metrics.LastErrorTime = time.Now()
    if ledgerSeq > 0 {
        s.metrics.LastProcessedLedger = ledgerSeq
    }
}

// GetMetrics returns current comprehensive processing metrics
func (s *SorobanExecutionServer) GetMetrics() SorobanExecutionMetrics {
    s.metrics.mu.RLock()
    defer s.metrics.mu.RUnlock()
    return *s.metrics
}
```

### Step 4: Create Service Entry Point

**File: `go/main.go`**

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    sorobanservice "github.com/your-org/soroban-execution-processor/gen/soroban_execution_service"
    "github.com/your-org/soroban-execution-processor/go/server"
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/reflection"
)

func main() {
    // Configuration
    networkPassphrase := getEnvOrDefault("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015")
    sourceServiceAddr := getEnvOrDefault("SOURCE_SERVICE_ADDR", "localhost:8080")
    grpcPort := getEnvOrDefault("GRPC_PORT", "8080")
    healthPort := getEnvOrDefault("HEALTH_PORT", "8088")

    logger, err := zap.NewProduction()
    if err != nil {
        log.Fatalf("Failed to initialize logger: %v", err)
    }
    defer logger.Sync()

    logger.Info("Starting Soroban Execution Analyzer",
        zap.String("network_passphrase", networkPassphrase),
        zap.String("source_service_addr", sourceServiceAddr),
        zap.String("grpc_port", grpcPort),
        zap.String("health_port", healthPort))

    // Create the comprehensive Soroban execution server
    executionServer, err := server.NewSorobanExecutionServer(networkPassphrase, sourceServiceAddr)
    if err != nil {
        logger.Fatal("Failed to create Soroban execution server", zap.Error(err))
    }
    defer executionServer.Close()

    // Start gRPC server with multiple service endpoints
    grpcServer := grpc.NewServer()
    sorobanservice.RegisterSorobanExecutionServiceServer(grpcServer, executionServer)
    reflection.Register(grpcServer)

    grpcListener, err := net.Listen("tcp", ":"+grpcPort)
    if err != nil {
        logger.Fatal("Failed to listen on gRPC port", zap.Error(err))
    }

    // Start comprehensive health check server
    go startHealthServer(executionServer, healthPort, logger)

    // Start gRPC server in goroutine
    go func() {
        logger.Info("Starting Soroban execution analysis gRPC server", 
            zap.String("address", grpcListener.Addr().String()))
        if err := grpcServer.Serve(grpcListener); err != nil {
            logger.Fatal("Failed to serve gRPC", zap.Error(err))
        }
    }()

    // Wait for interrupt signal
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    <-c

    logger.Info("Shutting down Soroban execution analyzer...")

    // Graceful shutdown
    grpcServer.GracefulStop()
    logger.Info("Servers stopped")
}

func startHealthServer(executionServer *server.SorobanExecutionServer, port string, logger *zap.Logger) {
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        metrics := executionServer.GetMetrics()
        
        status := "healthy"
        if metrics.ErrorCount > 0 && time.Since(metrics.LastErrorTime) < 5*time.Minute {
            status = "degraded"
        }

        response := map[string]interface{}{
            "status": status,
            "service": "soroban-execution-analyzer",
            "endpoints": []string{
                "GetContractExecutions", "GetContractInvocations", 
                "GetContractStateChanges", "GetContractEvents",
            },
            "metrics": map[string]interface{}{
                // Core metrics
                "success_count":             metrics.SuccessCount,
                "error_count":               metrics.ErrorCount,
                "total_ledgers_processed":   metrics.TotalLedgersProcessed,
                "last_processed_ledger":     metrics.LastProcessedLedger,
                "processing_latency_ms":     metrics.ProcessingLatency.Milliseconds(),
                "uptime_seconds":           time.Since(metrics.StartTime).Seconds(),
                
                // Soroban-specific metrics
                "total_contract_invocations": metrics.TotalContractInvocations,
                "total_state_changes":        metrics.TotalStateChanges,
                "total_diagnostic_events":    metrics.TotalDiagnosticEvents,
                "successful_invocations":     metrics.SuccessfulInvocations,
                "failed_invocations":         metrics.FailedInvocations,
                
                // Resource usage
                "total_cpu_instructions":     metrics.TotalCPUInstructions,
                "total_memory_bytes":         metrics.TotalMemoryBytes,
                "total_ledger_read_bytes":    metrics.TotalLedgerReadBytes,
                "total_ledger_write_bytes":   metrics.TotalLedgerWriteBytes,
                
                // Contract type breakdown
                "wasm_contract_invocations":        metrics.WasmContractInvocations,
                "stellar_asset_contract_invocations": metrics.StellarAssetContractInvocations,
            },
        }

        if metrics.LastError != nil {
            response["last_error"] = metrics.LastError.Error()
            response["last_error_time"] = metrics.LastErrorTime.Format(time.RFC3339)
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(response)
    })

    addr := ":" + port
    logger.Info("Starting comprehensive health check server", zap.String("address", addr))
    if err := http.ListenAndServe(addr, nil); err != nil {
        logger.Error("Health server failed", zap.Error(err))
    }
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

### Step 5: Build Configuration

**File: `Makefile`**

```makefile
.PHONY: build gen-proto run test clean

SERVICE_NAME = soroban-execution-analyzer

build:
	go build -o $(SERVICE_NAME) ./go

gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		protos/*/*.proto

run: build
	./$(SERVICE_NAME)

test:
	go test -v ./...

clean:
	rm -f $(SERVICE_NAME)
	rm -rf gen/

docker-build:
	docker build -t $(SERVICE_NAME):latest .

docker-run:
	docker run -p 8080:8080 -p 8088:8088 $(SERVICE_NAME):latest
```

## Migration Summary

### What Was Converted:

1. **CDP's Message Interface** → **Multi-Endpoint gRPC Streaming Interface**
   - `Process(context.Context, Message) error` → 4 specialized streaming endpoints:
     - `GetContractExecutions` (comprehensive analysis)
     - `GetContractInvocations` (function calls only)
     - `GetContractStateChanges` (state modifications only)
     - `GetContractEvents` (diagnostic events only)

2. **Pipeline Processing** → **Real-Time Analysis Service**
   - YAML pipeline configs → Service configuration with filtering
   - Multi-processor chains → Comprehensive single-service analysis
   - Batch processing → Real-time streaming analysis

3. **Generic Message Payloads** → **Strongly Typed Contract Analysis**
   - `Message{Payload: interface{}}` → Detailed contract execution structures:
     - `ContractExecutionAnalysis` with execution context
     - `ContractInvocation` with decoded arguments/returns
     - `ContractStateChange` with key-value modifications
     - `ContractDiagnosticEvent` with decoded event data

4. **Multi-Destination Output** → **Specialized Stream Endpoints**
   - Database/file outputs → Multiple gRPC streams for different data types
   - Pipeline forwarding → Direct streaming with consumer-driven filtering
   - Multiple subscribers → Specialized consumers for different analysis needs

### Key Benefits:

- **Enterprise-Grade Smart Contract Analytics**: Full execution context, not just events
- **Multiple Specialized Endpoints**: Different consumers can subscribe to exactly what they need
- **Deep XDR Analysis**: Raw and decoded representations for precise data reconstruction
- **Resource Usage Tracking**: CPU, memory, and ledger I/O monitoring for performance analysis
- **Cross-Contract Call Tracking**: Complete interaction mapping between contracts
- **Real-Time Streaming**: Direct consumer connection with microsecond latencies
- **Comprehensive Filtering**: Filter by contracts, functions, accounts, and success status
- **Backward Compatibility**: Raw JSON data included for legacy consumer support

### Enterprise Applications:

This comprehensive migration enables enterprise-grade use cases:

- **DeFi Protocol Analytics**: Track AMM swaps, liquidity operations, and yield farming
- **Compliance Monitoring**: Real-time transaction monitoring with state change tracking
- **Smart Contract Debugging**: Complete execution analysis with resource usage insights
- **Cross-Contract Security**: Monitor contract-to-contract interactions for security analysis
- **Performance Optimization**: Resource usage analysis for contract optimization
- **Audit Trail Generation**: Complete execution history with state modifications

### Migration Checklist:

- [ ] **Scope Analysis**: Understand your processor's full analytical scope (not just events)
- [ ] **Multi-Endpoint Design**: Create specialized protobuf schemas for different data views
- [ ] **Comprehensive Extraction**: Migrate all CDP analysis logic, not just basic processing
- [ ] **Resource Tracking**: Add CPU, memory, and I/O metrics for enterprise monitoring
- [ ] **Filtering Implementation**: Support consumer-driven filtering for performance
- [ ] **Backward Compatibility**: Include raw data for gradual consumer migration
- [ ] **Performance Testing**: Validate real-time streaming performance under load
- [ ] **Enterprise Deployment**: Set up monitoring, alerting, and scaling infrastructure

### Universal Migration Pattern:

This pattern transforms any CDP processor into a comprehensive real-time analysis service:

- **TransformToAppPayment** → **PaymentAnalysisService** (payment flows, compliance, volumes)
- **AccountYearAnalytics** → **AccountMetricsService** (activity patterns, growth analysis)
- **SoroswapProcessor** → **DEXAnalyticsService** (trading pairs, liquidity, arbitrage)
- **MarketMetricsProcessor** → **MarketDataService** (pricing, volume, volatility)

Each becomes a specialized, enterprise-grade real-time analytics engine that maintains all the sophisticated analysis logic from your CDP processors while providing modern gRPC streaming interfaces for real-time consumption.