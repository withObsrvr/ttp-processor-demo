# Stellar Processor Development Guide

A comprehensive guide for building custom Stellar processors based on official patterns and the working TTP processor implementation.

## Table of Contents

1. [Overview](#overview)
2. [Processor Types and Use Cases](#processor-types-and-use-cases)
3. [Architecture Patterns](#architecture-patterns)
4. [Protocol Buffer Conventions](#protocol-buffer-conventions)
5. [Building Your First Processor](#building-your-first-processor)
6. [Advanced Processor Patterns](#advanced-processor-patterns)
7. [Official Processor References](#official-processor-references)
8. [Protocol 23 Compatibility](#protocol-23-compatibility)
9. [Best Practices](#best-practices)
10. [Testing and Deployment](#testing-and-deployment)
11. [Examples and Templates](#examples-and-templates)

## Overview

Stellar processors are specialized components that transform raw blockchain ledger data into various output formats for different use cases. They range from simple event streaming services to complex data transformation and analytics pipelines.

### Processor Ecosystem

```
Raw Ledger Sources → Processors → Multiple Outputs
    (XDR data)     (Transform)   (Events, Analytics, Storage)
```

### Processing Paradigms

1. **Event Streaming** (gRPC): Real-time event delivery to consumers
2. **Data Transformation** (Pipeline): Convert blockchain data to application formats  
3. **Analytics & Aggregation**: Time-series analysis and metrics computation
4. **Storage & ETL**: Extract, transform, and load to databases
5. **DeFi Protocol Processing**: Domain-specific smart contract analysis

### Key Components

- **Input**: `xdr.LedgerCloseMeta` from ledger sources or storage
- **Processing**: Official Stellar libraries, custom transformation logic, or analytics engines
- **Output**: gRPC streams, database records, analytics dashboards, or file storage
- **Patterns**: Real-time streaming, batch processing, pipeline chaining, and configuration-driven workflows

## Processor Types and Use Cases

Based on analysis of production implementations, Stellar processors fall into distinct categories with specific patterns and interfaces.

### 1. Event Streaming Processors (gRPC)

**Purpose**: Real-time event delivery to consumer applications  
**Interface**: gRPC streaming services  
**Example**: TTP (Token Transfer Protocol) processor  

```go
// gRPC streaming interface
service EventService {
    rpc GetTTPEvents(GetEventsRequest) returns (stream TTPEvent);
}

// Implementation pattern
func (s *EventServer) GetTTPEvents(req *GetEventsRequest, stream EventService_GetTTPEventsServer) error {
    // Stream events in real-time to consumers
}
```

**Use Cases**: Gaming integrations, real-time notifications, live dashboards

### 2. Data Transformation Processors (Pipeline)

**Purpose**: Convert raw blockchain data into application-specific formats  
**Interface**: Message-based processing with chaining  
**Example**: TransformToAppPayment processor

```go
// Pipeline processor interface
type Processor interface {
    Process(context.Context, Message) error
    Subscribe(Processor) // Enable chaining
}

// Implementation pattern
type TransformToAppPayment struct {
    minAmount         *float64
    assetCode         *string
    addresses         []string
    networkPassphrase string
    processors        []Processor // Downstream chain
}

func (t *TransformToAppPayment) Process(ctx context.Context, msg Message) error {
    // Transform blockchain operations to AppPayment format
    payment := &AppPayment{
        SourceAccountId:      extractSource(operation),
        DestinationAccountId: extractDestination(operation),
        AssetCode:           extractAsset(operation),
        Amount:              extractAmount(operation),
        Type:                "payment",
    }
    
    // Forward to downstream processors
    return t.forwardToProcessors(ctx, payment)
}
```

**Configuration Pattern**:
```yaml
processors:
  - type: TransformToAppPayment
    config:
      network_passphrase: "Test SDF Network ; September 2015"
      min_amount: 100.0
      asset_code: "USDC"
```

**Use Cases**: ETL pipelines, data normalization, cross-platform integration

### 3. Analytics & Aggregation Processors

**Purpose**: Compute time-series analytics and aggregate metrics  
**Interface**: Stateful processors with persistent calculations  
**Example**: AccountYearAnalytics processor

```go
type AccountYearAnalytics struct {
    accountId    string
    year         int
    dailyCounts  map[string]int    // Date -> transaction count
    totalTxs     int
    highestDate  string
    highestCount int
    mutex        sync.RWMutex      // Thread-safe state management
}

func (a *AccountYearAnalytics) Process(ctx context.Context, msg Message) error {
    // Accumulate daily transaction metrics
    date := extractDate(msg)
    a.mutex.Lock()
    a.dailyCounts[date]++
    a.totalTxs++
    
    if a.dailyCounts[date] > a.highestCount {
        a.highestCount = a.dailyCounts[date]
        a.highestDate = date
    }
    a.mutex.Unlock()
    
    return nil
}

func (a *AccountYearAnalytics) GetMetrics() AccountYearMetrics {
    a.mutex.RLock()
    defer a.mutex.RUnlock()
    
    return AccountYearMetrics{
        AccountId:          a.accountId,
        TotalTransactions: a.totalTxs,
        HighestTxDate:     a.highestDate,
        HighestTxCount:    a.highestCount,
        DailyCounts:       a.dailyCounts,
    }
}
```

**Use Cases**: Financial analytics, account activity monitoring, trend analysis

### 4. DeFi Protocol Processors

**Purpose**: Extract and process smart contract events for specific DeFi protocols  
**Interface**: Contract-aware processors with protocol-specific logic  
**Example**: Soroswap AMM processor

```go
type SoroswapProcessor struct {
    pairs       map[string]PairInfo  // Cache of trading pairs
    stats       ProcessingStats
    mutex       sync.RWMutex
    processors  []Processor
}

func (s *SoroswapProcessor) Process(ctx context.Context, msg Message) error {
    event := msg.Payload.(Event)
    
    // Validate Soroswap contract
    if !s.isSoroswapContract(event.ContractID) {
        return nil
    }
    
    switch event.Type {
    case "new_pair":
        pair := s.extractPairInfo(event)
        s.cachePair(pair)
        
    case "sync":
        reserves := s.extractReserves(event)
        s.updateReserves(event.ContractID, reserves)
    }
    
    return s.forwardToProcessors(ctx, processedEvent)
}

func (s *SoroswapProcessor) extractPairInfo(event Event) PairInfo {
    // Decode Soroban contract event data
    var pairData struct {
        Token0  string `json:"token_0"`
        Token1  string `json:"token_1"`
        Pair    string `json:"pair"`
    }
    
    json.Unmarshal(event.Value, &pairData)
    
    return PairInfo{
        Token0Address: pairData.Token0,
        Token1Address: pairData.Token1,
        PairAddress:   pairData.Pair,
        Ledger:        event.Ledger,
    }
}
```

**Use Cases**: DEX analytics, liquidity tracking, arbitrage monitoring, DeFi dashboards

### 5. Storage & ETL Processors

**Purpose**: Persist processed data to databases or file systems  
**Interface**: Consumer endpoints in processing pipelines  
**Example**: Database storage processors

```yaml
# Configuration-driven ETL
pipelines:
  PaymentETLPipeline:
    source:
      type: S3BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledger-data"
        network: "mainnet"
    processors:
      - type: TransformToAppPayment
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    consumers:
      - type: SaveToMongoDB
        config:
          uri: "mongodb://localhost:27017/stellar"
          collection: "payments"
      - type: SaveToPostgreSQL
        config:
          host: "localhost"
          database: "stellar_analytics"
          table: "payment_events"
```

**Use Cases**: Data warehousing, business intelligence, compliance reporting

### Processor Interface Patterns

#### Core Interface
```go
type Processor interface {
    Process(context.Context, Message) error
    Subscribe(Processor)
}

type Message struct {
    Payload interface{}
}
```

#### Configuration Pattern
```go
type ProcessorConfig struct {
    Type   string
    Config map[string]interface{}
}

func NewProcessor(config ProcessorConfig) (Processor, error) {
    switch config.Type {
    case "TransformToAppPayment":
        return NewTransformToAppPayment(config.Config)
    case "AccountYearAnalytics":
        return NewAccountYearAnalytics(config.Config)
    case "SoroswapProcessor":
        return NewSoroswapProcessor(config.Config)
    default:
        return nil, fmt.Errorf("unknown processor type: %s", config.Type)
    }
}
```

#### Pipeline Chaining
```go
func (p *BaseProcessor) Subscribe(processor Processor) {
    p.downstreamProcessors = append(p.downstreamProcessors, processor)
}

func (p *BaseProcessor) forwardToProcessors(ctx context.Context, data interface{}) error {
    msg := Message{Payload: data}
    
    for _, processor := range p.downstreamProcessors {
        if err := processor.Process(ctx, msg); err != nil {
            return fmt.Errorf("downstream processor failed: %w", err)
        }
    }
    return nil
}
```

## Architecture Patterns

### 1. Data Flow Architecture

Based on the working TTP processor implementation:

```go
// Core data flow pattern
func (s *EventServer) GetTTPEvents(req *eventservice.GetEventsRequest, stream eventservice.EventService_GetTTPEventsServer) error {
    // 1. Connect to raw ledger source
    sourceReq := &rawledger.StreamLedgersRequest{StartLedger: req.StartLedger}
    rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
    
    // 2. Process each ledger
    for {
        rawLedgerMsg, err := rawLedgerStream.Recv()
        
        // 3. Unmarshal XDR data
        var lcm xdr.LedgerCloseMeta
        _, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
        
        // 4. Extract domain events using official processor
        events, err := s.processor.EventsFromLedger(lcm)
        
        // 5. Stream events to consumer
        for _, event := range events {
            stream.Send(event)
        }
    }
}
```

### 2. Service Structure Pattern

Every processor follows this structure:

```
processor-service/
├── go/
│   ├── main.go              # Service entry point
│   ├── server/
│   │   └── server.go        # gRPC service implementation
│   ├── gen/                 # Generated protobuf code
│   └── go.mod
├── protos/
│   ├── your_service/        # Your service definitions
│   └── ingest/              # Stellar data type definitions
├── Makefile                 # Build automation
└── api/
    └── openapi.yaml         # API documentation
```

### 3. Dependency Pattern

Processors depend on three key Stellar libraries:

```go
import (
    "github.com/stellar/go/processors/your_processor"  // Official processor logic
    "github.com/stellar/go/xdr"                        // XDR data structures
    "github.com/stellar/go/support/errors"             // Error handling
)
```

## Protocol Buffer Conventions

### 1. Service Definition Pattern

Based on `event_service.proto`:

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

// Service definition follows verb-based naming
service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

// Request follows standard pattern
message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;    // 0 for unbounded stream
}

// Event structure includes common fields
message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Domain-specific fields
    stellar.ingest.Asset asset = 5;
    string amount = 6;
    string from_account = 7;
    string to_account = 8;
}
```

### 2. Import Conventions

Standard imports for Stellar processors:

```protobuf
// Time handling
import "google/protobuf/timestamp.proto";

// Stellar data types
import "ingest/asset/asset.proto";

// For more complex processors, also import:
import "ingest/processors/other_processor/events.proto";
```

### 3. Go Package Naming

Follow Protocol 23 package structure:

```protobuf
// OLD (deprecated)
option go_package = "github.com/stellar/go/ingest/processors/token_transfer";

// NEW (Protocol 23+)
option go_package = "github.com/stellar/go/processors/token_transfer";
```

## Building Your First Processor

### Step 1: Define Your Domain Events

Create `protos/your_service/your_service.proto`:

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;
}

message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Your domain-specific fields here
    string your_custom_field = 5;
}
```

### Step 2: Create Processor Logic

If using an official Stellar processor:

```go
package server

import (
    "github.com/stellar/go/processors/your_processor" // Use official processor
    "github.com/stellar/go/xdr"
)

type EventServer struct {
    processor *your_processor.EventsProcessor
    // ... other fields
}

func NewEventServer(passphrase string) *EventServer {
    processor := your_processor.NewEventsProcessor(passphrase)
    return &EventServer{processor: processor}
}

func (s *EventServer) processLedger(lcm xdr.LedgerCloseMeta) error {
    events, err := s.processor.EventsFromLedger(lcm)
    if err != nil {
        return err
    }
    
    // Convert to your protobuf events and stream
    for _, event := range events {
        protoEvent := convertToProto(event)
        s.stream.Send(protoEvent)
    }
    return nil
}
```

### Step 3: Implement Custom Processing

For custom logic not covered by official processors:

```go
func (s *EventServer) extractCustomEvents(lcm xdr.LedgerCloseMeta) ([]*YourEvent, error) {
    var events []*YourEvent
    
    // Access transaction set
    switch lcm.V {
    case 0:
        return s.processV0(lcm.V0)
    case 1:
        return s.processV1(lcm.V1)
    case 2:
        return s.processV2(lcm.V2) // Protocol 23+
    }
    
    return events, nil
}

func (s *EventServer) processV1(v1 *xdr.LedgerCloseMetaV1) ([]*YourEvent, error) {
    var events []*YourEvent
    
    // Iterate through transactions
    for i, txResult := range v1.TxProcessing {
        tx := v1.TxSet.V1TxSet.Phases[0].V0Components[0].TxsMaybeDiscountedFee.Txs[i]
        
        // Process each operation
        for opIndex, op := range tx.Operations {
            if isYourTargetOperation(op) {
                event := &YourEvent{
                    LedgerSequence: uint32(v1.LedgerHeader.Header.LedgerSeq),
                    TransactionHash: txResult.TxResult.TransactionHash.HexString(),
                    OperationIndex: uint32(opIndex),
                    // Extract your custom fields
                }
                events = append(events, event)
            }
        }
    }
    
    return events, nil
}
```

### Step 4: Service Implementation

```go
func (s *EventServer) GetYourEvents(req *yourservice.GetEventsRequest, stream yourservice.YourEventService_GetYourEventsServer) error {
    // Follow the TTP processor pattern from server.go:154-324
    ctx := stream.Context()
    
    // Connect to raw ledger source
    sourceReq := &rawledger.StreamLedgersRequest{StartLedger: req.StartLedger}
    rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(ctx, sourceReq)
    
    for {
        // Get raw ledger
        rawLedgerMsg, err := rawLedgerStream.Recv()
        
        // Unmarshal XDR
        var lcm xdr.LedgerCloseMeta
        _, err = xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
        
        // Process with your logic
        events, err := s.extractCustomEvents(lcm)
        
        // Stream to consumer
        for _, event := range events {
            stream.Send(event)
        }
    }
}
```

## Advanced Processor Patterns

Beyond simple event streaming, advanced processor patterns enable complex data transformations, analytics, and pipeline architectures.

### 1. Configuration-Driven Pipeline Processing

**Pattern**: Use YAML configuration to build complex processing pipelines without code changes.

```yaml
# pipeline-config.yaml
pipelines:
  StellarAnalyticsPipeline:
    source:
      type: S3BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledger-data"
        network: "mainnet"
        ledger_range:
          start: 1000000
          end: 2000000
    
    processors:
      # Data transformation layer
      - type: TransformToAppPayment
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
          filters:
            min_amount: 1000.0
            asset_codes: ["USDC", "XLM"]
            
      # Analytics layer
      - type: AccountYearAnalytics
        config:
          year: 2024
          target_accounts: ["GABC123...", "GDEF456..."]
          
      # DeFi analysis layer  
      - type: SoroswapProcessor
        config:
          contracts: ["CSWAP123...", "CSWAP456..."]
          track_liquidity: true
    
    consumers:
      # Multiple output destinations
      - type: SaveToPostgreSQL
        config:
          connection: "postgresql://user:pass@localhost/stellar"
          tables:
            payments: "app_payments"
            analytics: "account_analytics"
            defi: "soroswap_events"
            
      - type: PublishToKafka
        config:
          brokers: ["kafka1:9092", "kafka2:9092"]
          topics:
            payments: "stellar.payments"
            analytics: "stellar.analytics"
```

**Implementation**:
```go
type PipelineConfig struct {
    Pipelines map[string]Pipeline `yaml:"pipelines"`
}

type Pipeline struct {
    Source     SourceConfig      `yaml:"source"`
    Processors []ProcessorConfig `yaml:"processors"`
    Consumers  []ConsumerConfig  `yaml:"consumers"`
}

func NewPipelineFromConfig(configPath string) (*Pipeline, error) {
    config, err := loadPipelineConfig(configPath)
    if err != nil {
        return nil, err
    }
    
    pipeline := &Pipeline{}
    
    // Build processor chain
    var processorChain []Processor
    for _, pConfig := range config.Processors {
        processor, err := NewProcessor(pConfig)
        if err != nil {
            return nil, err
        }
        processorChain = append(processorChain, processor)
    }
    
    // Link processors together
    for i := 0; i < len(processorChain)-1; i++ {
        processorChain[i].Subscribe(processorChain[i+1])
    }
    
    return pipeline, nil
}
```

### 2. Stateful Analytics Processors

**Pattern**: Maintain state across ledger processing for time-series analysis and aggregations.

```go
type MarketMetricsProcessor struct {
    assetPairs     map[string]*TradingPair
    volumeMetrics  map[string]*VolumeCalculator
    priceHistory   map[string]*PriceTracker
    timeWindows    []time.Duration // 1h, 24h, 7d windows
    mutex          sync.RWMutex
}

type TradingPair struct {
    BaseAsset   string
    QuoteAsset  string
    Volume24h   float64
    VolumeWeek  float64
    LastPrice   float64
    PriceChange PriceMetrics
}

func (m *MarketMetricsProcessor) Process(ctx context.Context, msg Message) error {
    lcm := msg.Payload.(xdr.LedgerCloseMeta)
    
    // Extract trading operations
    trades := m.extractTrades(lcm)
    
    m.mutex.Lock()
    defer m.mutex.Unlock()
    
    for _, trade := range trades {
        pairKey := m.getPairKey(trade.BaseAsset, trade.QuoteAsset)
        
        // Update volume metrics
        if pair, exists := m.assetPairs[pairKey]; exists {
            pair.Volume24h += trade.Volume
            pair.LastPrice = trade.Price
            
            // Update price change calculations
            m.updatePriceMetrics(pair, trade)
        } else {
            // Initialize new trading pair
            m.assetPairs[pairKey] = &TradingPair{
                BaseAsset:  trade.BaseAsset,
                QuoteAsset: trade.QuoteAsset,
                Volume24h:  trade.Volume,
                LastPrice:  trade.Price,
            }
        }
        
        // Update time-windowed metrics
        m.updateTimeWindowMetrics(pairKey, trade)
    }
    
    return nil
}

func (m *MarketMetricsProcessor) GetMarketSummary() map[string]TradingPair {
    m.mutex.RLock()
    defer m.mutex.RUnlock()
    
    // Return copy of current state
    summary := make(map[string]TradingPair)
    for key, pair := range m.assetPairs {
        summary[key] = *pair
    }
    return summary
}
```

### 3. Multi-Stage Data Transformation

**Pattern**: Chain processors for complex data transformations with intermediate validation.

```go
type DataValidationProcessor struct {
    validationRules map[string]ValidationRule
    processors      []Processor
    errorHandler    ErrorHandler
}

type ValidationRule func(interface{}) error

func (d *DataValidationProcessor) Process(ctx context.Context, msg Message) error {
    // Apply validation rules
    for name, rule := range d.validationRules {
        if err := rule(msg.Payload); err != nil {
            return d.errorHandler.HandleValidationError(name, err, msg)
        }
    }
    
    // Forward to next stage if validation passes
    return d.forwardToProcessors(ctx, msg.Payload)
}

// Example: Complex payment processing pipeline
func BuildPaymentProcessingPipeline() []Processor {
    return []Processor{
        // Stage 1: Extract raw payment operations
        &RawPaymentExtractor{
            operationTypes: []xdr.OperationType{
                xdr.OperationTypePayment,
                xdr.OperationTypePathPaymentStrictReceive,
                xdr.OperationTypePathPaymentStrictSend,
            },
        },
        
        // Stage 2: Validate payment data
        &DataValidationProcessor{
            validationRules: map[string]ValidationRule{
                "amount_validation":  validatePositiveAmount,
                "account_validation": validateAccountFormat,
                "asset_validation":   validateAssetCode,
            },
        },
        
        // Stage 3: Enrich with metadata
        &PaymentEnrichmentProcessor{
            accountService: NewAccountService(),
            assetService:   NewAssetService(),
            rateService:    NewExchangeRateService(),
        },
        
        // Stage 4: Apply business rules
        &BusinessRuleProcessor{
            rules: []BusinessRule{
                NewLargeTransactionRule(10000.0),
                NewComplianceRule(),
                NewSuspiciousActivityRule(),
            },
        },
        
        // Stage 5: Format for output
        &OutputFormatterProcessor{
            formats: map[string]Formatter{
                "json":     NewJSONFormatter(),
                "protobuf": NewProtobufFormatter(),
                "csv":      NewCSVFormatter(),
            },
        },
    }
}
```

### 4. Smart Contract Event Processing

**Pattern**: Protocol-specific processors for extracting and interpreting smart contract events.

```go
type SorobanEventProcessor struct {
    contractProcessors map[string]ContractProcessor
    eventFilters      []EventFilter
    outputStreams     map[string]chan ProcessedEvent
}

type ContractProcessor interface {
    ProcessEvent(event SorobanEvent) (ProcessedEvent, error)
    GetContractID() string
    GetEventTypes() []string
}

// Specific AMM processor implementation
type AMMProcessor struct {
    contractID    string
    pairRegistry  map[string]TradingPair
    liquidityData map[string]LiquidityPool
}

func (a *AMMProcessor) ProcessEvent(event SorobanEvent) (ProcessedEvent, error) {
    switch event.Type {
    case "swap":
        return a.processSwapEvent(event)
    case "add_liquidity":
        return a.processLiquidityEvent(event)
    case "remove_liquidity":
        return a.processLiquidityRemovalEvent(event)
    default:
        return ProcessedEvent{}, fmt.Errorf("unknown event type: %s", event.Type)
    }
}

func (a *AMMProcessor) processSwapEvent(event SorobanEvent) (ProcessedEvent, error) {
    // Decode swap event data
    var swapData struct {
        TokenIn     string `json:"token_in"`
        TokenOut    string `json:"token_out"`
        AmountIn    string `json:"amount_in"`
        AmountOut   string `json:"amount_out"`
        User        string `json:"user"`
        Reserves    []string `json:"reserves"`
    }
    
    if err := json.Unmarshal(event.Data, &swapData); err != nil {
        return ProcessedEvent{}, err
    }
    
    // Calculate price impact and slippage
    priceImpact := a.calculatePriceImpact(swapData)
    
    return ProcessedEvent{
        Type:        "amm_swap",
        ContractID:  a.contractID,
        Ledger:      event.Ledger,
        TxHash:      event.TxHash,
        User:        swapData.User,
        Data: SwapProcessedData{
            TokenIn:     swapData.TokenIn,
            TokenOut:    swapData.TokenOut,
            AmountIn:    swapData.AmountIn,
            AmountOut:   swapData.AmountOut,
            PriceImpact: priceImpact,
            Timestamp:   event.Timestamp,
        },
    }, nil
}
```

### 5. Parallel Processing with Worker Pools

**Pattern**: Process multiple ledgers concurrently while maintaining order and handling backpressure.

```go
type ParallelLedgerProcessor struct {
    workerCount    int
    inputQueue     chan LedgerJob
    outputQueue    chan ProcessedResult
    processors     []Processor
    orderManager   *OrderManager
    errorHandler   ErrorHandler
}

type LedgerJob struct {
    Sequence uint32
    LCM      xdr.LedgerCloseMeta
    Priority int
}

type ProcessedResult struct {
    Sequence uint32
    Events   []ProcessedEvent
    Metrics  ProcessingMetrics
    Error    error
}

func (p *ParallelLedgerProcessor) Start(ctx context.Context) error {
    // Start worker goroutines
    for i := 0; i < p.workerCount; i++ {
        go p.worker(ctx, i)
    }
    
    // Start order management
    go p.orderManager.Start(ctx)
    
    return nil
}

func (p *ParallelLedgerProcessor) worker(ctx context.Context, workerID int) {
    for {
        select {
        case <-ctx.Done():
            return
        case job := <-p.inputQueue:
            start := time.Now()
            
            // Process ledger through processor chain
            result := ProcessedResult{Sequence: job.Sequence}
            
            for _, processor := range p.processors {
                events, err := processor.ProcessLedger(job.LCM)
                if err != nil {
                    result.Error = err
                    p.errorHandler.HandleError(workerID, job, err)
                    break
                }
                result.Events = append(result.Events, events...)
            }
            
            result.Metrics = ProcessingMetrics{
                WorkerID:    workerID,
                Duration:    time.Since(start),
                EventCount:  len(result.Events),
                LedgerSize:  len(job.LCM.MustMarshalBinary()),
            }
            
            // Send to output queue with ordering
            p.orderManager.Submit(result)
        }
    }
}

type OrderManager struct {
    expectedSequence uint32
    pendingResults   map[uint32]ProcessedResult
    outputQueue      chan ProcessedResult
    mutex           sync.Mutex
}

func (o *OrderManager) Submit(result ProcessedResult) {
    o.mutex.Lock()
    defer o.mutex.Unlock()
    
    o.pendingResults[result.Sequence] = result
    
    // Deliver results in order
    for {
        if result, exists := o.pendingResults[o.expectedSequence]; exists {
            o.outputQueue <- result
            delete(o.pendingResults, o.expectedSequence)
            o.expectedSequence++
        } else {
            break
        }
    }
}
```

### 6. Migration Patterns for Legacy Processors

**Pattern**: Adapter pattern for integrating existing processors into new pipeline architectures.

```go
type LegacyProcessorAdapter struct {
    legacyProcessor LegacyProcessor
    inputAdapter    InputAdapter
    outputAdapter   OutputAdapter
}

type InputAdapter interface {
    AdaptInput(Message) (LegacyInput, error)
}

type OutputAdapter interface {
    AdaptOutput(LegacyOutput) (Message, error)
}

func (l *LegacyProcessorAdapter) Process(ctx context.Context, msg Message) error {
    // Adapt input format
    legacyInput, err := l.inputAdapter.AdaptInput(msg)
    if err != nil {
        return fmt.Errorf("input adaptation failed: %w", err)
    }
    
    // Process with legacy processor
    legacyOutput, err := l.legacyProcessor.Process(legacyInput)
    if err != nil {
        return fmt.Errorf("legacy processing failed: %w", err)
    }
    
    // Adapt output format
    adaptedOutput, err := l.outputAdapter.AdaptOutput(legacyOutput)
    if err != nil {
        return fmt.Errorf("output adaptation failed: %w", err)
    }
    
    // Forward to next processor
    return l.forwardToProcessors(ctx, adaptedOutput.Payload)
}

// Example migration from gRPC streaming to pipeline processing
type GRPCToPipelineAdapter struct {
    grpcService EventService
    converter   EventConverter
}

func (g *GRPCToPipelineAdapter) Process(ctx context.Context, msg Message) error {
    lcm := msg.Payload.(xdr.LedgerCloseMeta)
    
    // Convert LCM to gRPC request format
    req := &GetEventsRequest{
        StartLedger: lcm.LedgerSequence(),
        EndLedger:   lcm.LedgerSequence(),
    }
    
    // Create mock stream for collecting events
    mockStream := NewMockEventStream()
    
    // Process through existing gRPC service
    err := g.grpcService.GetEvents(req, mockStream)
    if err != nil {
        return err
    }
    
    // Convert collected events to pipeline messages
    for _, event := range mockStream.CollectedEvents() {
        pipelineMsg := g.converter.ConvertEvent(event)
        g.forwardToProcessors(ctx, pipelineMsg)
    }
    
    return nil
}
```

These advanced patterns enable building sophisticated data processing systems that can handle complex analytics, multiple data sources, parallel processing, and seamless integration with existing systems.

## Official Processor References

### Available Processors

From https://github.com/stellar/go/tree/master/processors:

1. **Token Transfer Processor** (`processors/token_transfer`)
   - Tracks asset transfers between accounts
   - Handles all payment operations
   - Used in the working TTP processor example

2. **Offer Processor** (if available)
   - Tracks DEX offer creation/updates
   - Manages orderbook changes

3. **Liquidity Pool Processor** (if available)
   - Tracks AMM pool operations
   - Handles pool creation and swaps

### Using Official Processors

```go
import "github.com/stellar/go/processors/token_transfer"

// Initialize with network passphrase
processor := token_transfer.NewEventsProcessor("Test SDF Network ; September 2015")

// Process ledger to get events
events, err := processor.EventsFromLedger(ledgerCloseMeta)
```

### Protocol 23 Processor Updates

With Protocol 23, processors moved locations:

```go
// OLD import path (deprecated)
import "github.com/stellar/go/ingest/processors/token_transfer"

// NEW import path (Protocol 23+)
import "github.com/stellar/go/processors/token_transfer"
```

## Protocol 23 Compatibility

### LedgerCloseMeta Format Changes

Protocol 23 introduced V2 format:

```go
func (s *EventServer) handleLedgerCloseMeta(lcm xdr.LedgerCloseMeta) error {
    switch lcm.V {
    case 0:
        return s.processV0(lcm.V0)
    case 1:
        return s.processV1(lcm.V1)
    case 2:
        // Protocol 23+ V2 format
        return s.processV2(lcm.V2)
    default:
        return fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
    }
}
```

### Dual BucketList Support (CAP-62)

Protocol 23 processors should validate dual bucket lists:

```go
func (s *EventServer) validateProtocol23Features(lcm xdr.LedgerCloseMeta) error {
    if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
        // Validate dual bucket list hash
        if lcm.V == 1 && lcm.V1 != nil {
            return s.validateDualBucketList(lcm.V1)
        }
    }
    return nil
}
```

### Update Dependencies

Use Protocol 23 branches:

```bash
go get github.com/stellar/go@protocol-23
```

## Best Practices

### 1. Error Handling

Follow robust error handling patterns:

```go
func (s *EventServer) processWithRetry(lcm xdr.LedgerCloseMeta) error {
    const maxRetries = 3
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        events, err := s.processor.EventsFromLedger(lcm)
        if err == nil {
            return s.streamEvents(events)
        }
        
        s.logger.Warn("processing failed, retrying",
            zap.Int("attempt", attempt+1),
            zap.Error(err))
            
        if attempt < maxRetries-1 {
            time.Sleep(time.Duration(attempt+1) * time.Second)
        }
    }
    
    return fmt.Errorf("failed after %d attempts", maxRetries)
}
```

### 2. Metrics and Monitoring

Include comprehensive metrics:

```go
type ProcessorMetrics struct {
    SuccessCount        int64
    ErrorCount          int64
    TotalEventsEmitted  int64
    ProcessingLatency   time.Duration
    LastProcessedLedger uint32
}

func (s *EventServer) recordMetrics(ledgerSeq uint32, eventCount int, duration time.Duration) {
    s.metrics.SuccessCount++
    s.metrics.TotalEventsEmitted += int64(eventCount)
    s.metrics.ProcessingLatency = duration
    s.metrics.LastProcessedLedger = ledgerSeq
}
```

### 3. Context Handling

Proper context cancellation:

```go
func (s *EventServer) GetEvents(req *GetEventsRequest, stream EventService_GetEventsServer) error {
    ctx := stream.Context()
    sourceCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    
    // Check for consumer cancellation
    select {
    case <-ctx.Done():
        cancel() // Cancel upstream
        return status.FromContextError(ctx.Err()).Err()
    default:
        // Continue processing
    }
}
```

### 4. Configuration Management

Environment-based configuration:

```go
type ProcessorConfig struct {
    NetworkPassphrase string
    SourceEndpoint    string
    LogLevel          string
    MetricsPort       int
}

func LoadConfig() *ProcessorConfig {
    return &ProcessorConfig{
        NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015"),
        SourceEndpoint:    getEnvOrDefault("SOURCE_ENDPOINT", "localhost:8080"),
        LogLevel:         getEnvOrDefault("LOG_LEVEL", "info"),
        MetricsPort:      getEnvAsInt("METRICS_PORT", 8088),
    }
}
```

## Testing and Deployment

### 1. Unit Testing

Test processor logic with known ledgers:

```go
func TestYourProcessor(t *testing.T) {
    processor := NewYourProcessor("Test SDF Network ; September 2015")
    
    // Load test ledger data
    lcm := loadTestLedgerCloseMeta(t, "testdata/ledger_123456.xdr")
    
    events, err := processor.ProcessLedger(lcm)
    require.NoError(t, err)
    
    assert.Len(t, events, expectedEventCount)
    assert.Equal(t, uint32(123456), events[0].LedgerSequence)
}
```

### 2. Integration Testing

Test with live ledger source:

```go
func TestIntegration(t *testing.T) {
    // Skip in short mode
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    server := NewEventServer("Test SDF Network ; September 2015", "localhost:8080")
    
    // Test bounded stream
    req := &GetEventsRequest{
        StartLedger: 100000,
        EndLedger:   100002,
    }
    
    events := collectEvents(t, server, req)
    assert.NotEmpty(t, events)
}
```

### 3. Performance Testing

Benchmark processing performance:

```go
func BenchmarkProcessLedger(b *testing.B) {
    processor := NewYourProcessor("Test SDF Network ; September 2015")
    lcm := loadTestLedgerCloseMeta(b, "testdata/busy_ledger.xdr")
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := processor.ProcessLedger(lcm)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### 4. Docker Deployment

Create production-ready Docker images:

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o processor ./cmd/processor

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/

COPY --from=builder /app/processor .

EXPOSE 8080 8088
CMD ["./processor"]
```

## Examples and Templates

### 1. Makefile Template

```makefile
.PHONY: build gen-proto run test clean

build:
	go build -o $(SERVICE_NAME) ./cmd/$(SERVICE_NAME)

gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		protos/*/*.proto

run: build
	./$(SERVICE_NAME)

test:
	go test -v ./...

test-integration:
	go test -v -tags=integration ./...

clean:
	rm -f $(SERVICE_NAME)
	rm -rf gen/

docker-build:
	docker build -t $(SERVICE_NAME):latest .

docker-run:
	docker run -p 8080:8080 -p 8088:8088 $(SERVICE_NAME):latest
```

### 2. Complete Processor Template

See the working TTP processor implementation in:
- `/ttp-processor/go/server/server.go` - Complete service implementation
- `/ttp-processor/protos/` - Proto definitions
- `/ttp-processor/go/main.go` - Service entry point

### 3. Proto File Template

```protobuf
syntax = "proto3";

package your_domain.v1;

option go_package = "github.com/your-org/your-processor/gen/your_service";

import "google/protobuf/timestamp.proto";
import "ingest/asset/asset.proto";

service YourEventService {
    rpc GetYourEvents(GetEventsRequest) returns (stream YourEvent);
}

message GetEventsRequest {
    uint32 start_ledger = 1;
    uint32 end_ledger = 2;
}

message YourEvent {
    uint32 ledger_sequence = 1;
    google.protobuf.Timestamp ledger_close_time = 2;
    string transaction_hash = 3;
    uint32 operation_index = 4;
    
    // Your domain-specific fields
    YourCustomData data = 5;
}

message YourCustomData {
    string field1 = 1;
    int64 field2 = 2;
    stellar.ingest.Asset asset = 3;
}
```

## Next Steps

1. **Study the Official Processors**: Review https://github.com/stellar/go/tree/master/processors
2. **Examine Proto Definitions**: Study https://github.com/stellar/go/tree/protocol-23/protos
3. **Use the Working Example**: Build on the TTP processor implementation
4. **Test with Protocol 23**: Ensure compatibility with latest Stellar protocol

## Resources

### Official Stellar Resources
- [Stellar Go SDK](https://github.com/stellar/go)
- [Official Processors](https://github.com/stellar/go/tree/master/processors)
- [Protocol 23 Protos](https://github.com/stellar/go/tree/protocol-23/protos)
- [Stellar Developer Documentation](https://developers.stellar.org)

### Implementation Examples
- [Working TTP Processor](../ttp-processor/) - gRPC streaming event processor
- [CDP Pipeline Workflow](https://github.com/withObsrvr/cdp-pipeline-workflow) - Production pipeline processors including:
  - Data transformation processors (TransformToAppPayment)
  - Analytics processors (AccountYearAnalytics)  
  - DeFi protocol processors (Soroswap, Phoenix AMM)
  - Smart contract execution analyzer (ContractInvocationProcessor)
  - Configuration-driven pipeline architecture
  - Multi-destination ETL patterns

### Migration Resources
- [Processor Migration Guide](./processor-migration-guide.md) - Complete guide for converting CDP pipeline processors to gRPC streaming architecture
  - Step-by-step ContractInvocationProcessor migration example
  - Comprehensive protobuf schema design
  - Multi-endpoint service implementation
  - Enterprise-grade metrics and monitoring

### Processor Categories Reference
- **Event Streaming**: Real-time gRPC services for consumer applications
- **Data Transformation**: Pipeline processors for ETL and format conversion
- **Analytics & Aggregation**: Stateful processors for metrics and time-series data
- **DeFi Protocol Processing**: Smart contract event extraction and interpretation
- **Storage & ETL**: Database persistence and data warehousing

This guide provides the foundation for building robust, production-ready Stellar processors following official patterns and proven production implementations across multiple processor paradigms.