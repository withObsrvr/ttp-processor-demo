# Contract Data Processor - Implementation Plan

## Overview
This document outlines the implementation plan for creating a hybrid contract data processor service that extracts Stellar smart contract data and streams it via Apache Arrow Flight for high-performance analytics.

## Architecture Overview

```
stellar-live-source-datalake → contract-data-processor → Arrow Flight → Consumers
      (gRPC/XDR)                  (Hybrid Service)         (Columnar)    (PostgreSQL)
```

### Key Components
1. **gRPC Client**: Connects to stellar-live-source-datalake for raw ledger data
2. **Contract Processor**: Uses stellar/go to extract contract data from XDR
3. **Arrow Flight Server**: Streams columnar contract data to consumers
4. **PostgreSQL Consumer**: Stores contract data for SQL analytics

## Implementation Phases

### Phase 1: Core Infrastructure (Day 1)
- [x] Create directory structure
- [x] Create Makefile
- [ ] Set up go.mod with dependencies
- [ ] Define protobuf interfaces
- [ ] Create basic logging infrastructure
- [ ] Set up configuration management

### Phase 2: Data Ingestion (Day 2)
- [ ] Implement gRPC client for stellar-live-source-datalake
- [ ] Create raw ledger stream handler
- [ ] Add connection management and reconnection logic
- [ ] Implement health checking for upstream service
- [ ] Add metrics for ingestion rate

### Phase 3: Contract Processing (Day 3-4)
- [ ] Port contract data processor from cdp-pipeline-workflow
- [ ] Integrate stellar/go contract processor
- [ ] Handle different contract data types:
  - Contract instance data
  - Contract code entries
  - Asset contracts
  - Balance data
- [ ] Implement filtering by contract ID
- [ ] Add error handling and logging

### Phase 4: Arrow Schema & Transformation (Day 5)
- [ ] Define Arrow schema for contract data
- [ ] Implement XDR to Arrow transformation
- [ ] Create batch builder for efficient processing
- [ ] Add memory management with allocators
- [ ] Implement schema versioning

### Phase 5: Hybrid Server Implementation (Day 6-7)
- [ ] Create Arrow Flight server on port 8816
- [ ] Implement DoGet for data streaming
- [ ] Add ticket parsing for filtering
- [ ] Create HTTP health server on port 8089
- [ ] Implement flowctl integration
- [ ] Add Prometheus metrics endpoint

### Phase 6: PostgreSQL Consumer (Day 8-9)
- [ ] Create Arrow Flight client
- [ ] Implement batch writer for PostgreSQL
- [ ] Define database schema
- [ ] Add connection pooling
- [ ] Implement retry logic
- [ ] Create indexes for performance

### Phase 7: Testing & Integration (Day 10)
- [ ] Unit tests for processor logic
- [ ] Integration tests with mock data
- [ ] Performance benchmarking
- [ ] Docker containerization
- [ ] Documentation

## Technical Specifications

### Dependencies
```go
// go.mod
module github.com/withObsrvr/ttp-processor-demo/contract-data-processor

require (
    github.com/stellar/go v0.0.0-20250718194041
    github.com/apache/arrow/go/v17 v17.0.0
    google.golang.org/grpc v1.64.0
    github.com/lib/pq v1.10.9
    github.com/rs/zerolog v1.33.0
)
```

### Arrow Schema
```go
type ContractDataSchema struct {
    ContractID           arrow.BinaryType    // Contract address
    ContractKeyType      arrow.StringType    // Key type
    ContractDurability   arrow.StringType    // Temporary/Persistent
    AssetCode           arrow.StringType    // Asset code (if applicable)
    AssetIssuer         arrow.StringType    // Asset issuer
    BalanceHolder       arrow.StringType    // Balance holder address
    Balance             arrow.Int64Type     // Balance amount
    LastModifiedLedger  arrow.Uint32Type    // Last modification
    Deleted             arrow.BooleanType   // Deletion flag
    LedgerSequence      arrow.Uint32Type    // Current ledger
    ClosedAt            arrow.TimestampType // Timestamp
}
```

### PostgreSQL Schema
```sql
CREATE TABLE contract_data (
    contract_id TEXT NOT NULL,
    contract_key_type TEXT,
    contract_durability TEXT,
    asset_code TEXT,
    asset_issuer TEXT,
    balance_holder TEXT,
    balance BIGINT,
    last_modified_ledger INTEGER,
    deleted BOOLEAN DEFAULT FALSE,
    ledger_sequence INTEGER NOT NULL,
    closed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (contract_id, ledger_sequence)
);

-- Indexes for common queries
CREATE INDEX idx_contract_data_contract_id ON contract_data(contract_id);
CREATE INDEX idx_contract_data_ledger_seq ON contract_data(ledger_sequence);
CREATE INDEX idx_contract_data_asset ON contract_data(asset_code, asset_issuer);
CREATE INDEX idx_contract_data_balance_holder ON contract_data(balance_holder);
```

### Configuration
```yaml
# Environment Variables
GRPC_PORT=50054                    # gRPC control port
ARROW_PORT=8816                     # Arrow Flight data port
HEALTH_PORT=8089                    # HTTP health check port
SOURCE_ENDPOINT=stellar-live-source-datalake:50053
NETWORK_PASSPHRASE=Test SDF Network ; September 2015
FLOWCTL_ENDPOINT=localhost:8080
FLOWCTL_ENABLED=true
BATCH_SIZE=1000
MAX_MEMORY_MB=1024

# PostgreSQL Consumer
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stellar_contracts
DB_USER=stellar
DB_PASSWORD=stellar
DB_BATCH_SIZE=1000
DB_FLUSH_INTERVAL=5s
```

## Performance Targets
- Ingestion rate: 10,000+ ledgers/second
- Processing latency: <100ms per batch
- Memory usage: <1GB under normal load
- PostgreSQL insert rate: 100,000+ rows/second

## Monitoring & Observability
- Structured JSON logging with zerolog
- Prometheus metrics:
  - Ledgers processed
  - Contract data extracted
  - Processing latency
  - Error rates
  - Memory usage
- Health endpoints:
  - /health - Overall health
  - /ready - Readiness probe
  - /metrics - Prometheus metrics

## Error Handling
- Exponential backoff for reconnections
- Circuit breaker for upstream failures
- Dead letter queue for failed records
- Graceful shutdown on SIGTERM

## Security Considerations
- No authentication on Arrow Flight (internal service)
- PostgreSQL connection uses SSL
- No PII in logs
- Contract data is public blockchain data

## Future Enhancements
1. Add Kafka producer for event streaming
2. Support for contract event filtering
3. Historical data backfill capability
4. Multi-network support (testnet/mainnet)
5. Contract code decompilation
6. Real-time contract analytics

## Success Criteria
- [ ] Successfully connects to stellar-live-source-datalake
- [ ] Extracts contract data from ledgers
- [ ] Streams data via Arrow Flight
- [ ] PostgreSQL consumer stores data correctly
- [ ] Handles 1M+ contracts without degradation
- [ ] Integrates with flowctl monitoring
- [ ] Passes all unit and integration tests

## References
- [Stellar Go SDK Contract Processor](https://github.com/stellar/go/tree/master/processors/contract)
- [Apache Arrow Flight Documentation](https://arrow.apache.org/docs/format/Flight.html)
- [CDP Pipeline Contract Processor](https://github.com/yourorg/cdp-pipeline-workflow)
- [TTP Processor Demo Architecture](../README.md)