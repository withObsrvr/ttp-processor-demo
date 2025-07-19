# Protocol 23 Upgrade Implementation Plan for stellar-live-source-datalake

## Executive Summary

This document outlines the implementation plan for updating the `stellar-live-source-datalake` service to support Stellar Protocol 23 and migrate from custom withObsrvr libraries to official Stellar Go SDK components. The upgrade introduces significant architectural changes to ledger state storage, XDR schema modifications, and new resource accounting mechanisms that require comprehensive updates to data ingestion pipelines.

**Critical Changes:**
1. **Library Migration**: Replace withObsrvr forks with official Stellar Go SDK components
2. **Protocol 23 Support**: Handle dual BucketList architecture and new XDR schemas
3. **Architecture Modernization**: Use official ingest framework patterns

**Timeline:**
- Protocol 23 Testnet: July 17, 2025 (completed)
- Protocol 23 Mainnet vote: August 26, 2025 at 17:00 UTC
- Implementation completion target: August 22, 2025

## Current State Analysis

### Current Implementation Architecture

The `stellar-live-source-datalake` currently implements:

**Core Components** (`stellar-live-source-datalake/go/server/server.go:1-426`):
- gRPC streaming service for raw ledger data
- Enterprise-grade metrics and circuit breaker patterns
- Storage backend support (GCS, S3, FS)
- Uses `github.com/stellar/go v0.0.0-20250409153303-3b29eb9ebb4c`
- **PROBLEM**: Leverages custom withObsrvr forks instead of official libraries:
  - `github.com/withObsrvr/stellar-cdp`
  - `github.com/withObsrvr/stellar-datastore`
  - `github.com/withObsrvr/stellar-ledgerbackend`

**Current Data Flow (Using Custom Libraries)**:
```
Storage Backend → withObsrvr/stellar-datastore → withObsrvr/stellar-cdp → LedgerCloseMeta → XDR bytes → gRPC stream
```

**Target Data Flow (Using Official Libraries)**:
```
History Archive/Storage → github.com/stellar/go/ingest/ledgerbackend → LedgerCloseMeta → XDR bytes → gRPC stream
```

**Protobuf Interface** (`stellar-live-source-datalake/protos/raw_ledger_service/raw_ledger_service.proto:1-23`):
```protobuf
message RawLedger {
    uint32 sequence = 1;
    bytes ledger_close_meta_xdr = 2; // Raw XDR bytes of LedgerCloseMeta
}
```

## Protocol 23 Breaking Changes Analysis

### 1. State Storage Architecture Changes (CAP-62)

**Impact:** Fundamental change to ledger storage
- Introduces dual BucketList architecture:
  - **Live State BucketList**: Contains live ledger state (classic entries + active Soroban)
  - **Hot Archive BucketList**: Stores evicted PERSISTENT entries
- New `HotArchiveBucketEntry` XDR type with three states:
  - `HOT_ARCHIVE_METAENTRY`: Bucket metadata
  - `HOT_ARCHIVE_ARCHIVED`: Evicted entries  
  - `HOT_ARCHIVE_LIVE`: Restored entries
- History Archive State now includes `hotArchiveBuckets`
- `bucketListHash` calculation changes to `SHA256(liveStateBucketListHash, hotArchiveHash)`

**Implications for datalake:**
- CDP publisher must handle dual BucketList structure
- Storage schema may need updates for hot archive data
- Ledger processing logic must accommodate new bucket types

### 2. Resource Accounting Changes (CAP-66)

**Impact:** Soroban resource model restructuring
- Separates read resources into "in-memory" and "disk" entries
- Introduces automatic restoration of archived entries
- New XDR structures:
  - `SorobanResourcesExtV0` with `archivedSorobanEntries` vector
  - Renamed configuration settings (`sorobanLiveStateTargetSizeBytes`)
- Ledger metadata changes:
  - `evictedTemporaryLedgerKeys` → `evictedKeys`
  - Removed `evictedPersistentLedgerEntries`

**Implications for datalake:**
- XDR parsing must handle new resource structures
- Fee calculation logic changes
- Contract size calculations differ between disk/memory

### 3. Parallel Transaction Execution (CAP-63)

**Impact:** Transaction set structure changes
- New transaction set structure for parallel execution
- Introduction of tx entry limits for efficient construction
- XDR modifications for conflict verification in transaction footprints

**Implications for datalake:**
- Transaction metadata structure changes
- Processing order considerations for parallel transactions

## Library Migration Analysis

### Current Custom Dependencies vs Official Stellar Libraries

| Current (withObsrvr) | Official Stellar Alternative | Migration Complexity |
|---------------------|------------------------------|---------------------|
| `withObsrvr/stellar-cdp` | `github.com/stellar/go/ingest/ledgerbackend` | **High** - Different API patterns |
| `withObsrvr/stellar-datastore` | `github.com/stellar/go/ingest/io` + `historyarchive` | **Medium** - Storage abstraction changes |
| `withObsrvr/stellar-ledgerbackend` | `github.com/stellar/go/ingest/ledgerbackend` | **Medium** - Direct replacement available |

### Official Stellar Go SDK Components for Data Ingestion

#### 1. **LedgerBackend Interface** (`github.com/stellar/go/ingest/ledgerbackend`)
```go
type LedgerBackend interface {
    GetLatestLedgerSequence(ctx context.Context) (sequence uint32, err error)
    GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error)
    PrepareRange(ctx context.Context, ledgerRange Range) error
    IsPrepared(ctx context.Context, ledgerRange Range) (bool, error)
    Close() error
}
```

**Available Implementations:**
- **CaptiveStellarCore**: Runs internal Stellar-Core subprocess
- **RPCLedgerBackend**: Fetches from Stellar RPC server
- **BufferedStorageBackend**: Buffered storage operations

#### 2. **History Archive Support** (`github.com/stellar/go/historyarchive`)
- Direct access to Stellar History Archives
- Support for S3, GCS, and other blob storage backends
- Built-in checkpoint and bucket list handling

#### 3. **Ingest Framework** (`github.com/stellar/go/ingest`)
- Higher-level readers for transactions and changes
- Built-in Protocol 23 support
- Processor framework for custom ingestion logic

## Implementation Strategy

### Phase 1: Library Migration Foundation (Week 1)

#### 1.1 Update Stellar Go Dependency
```bash
# Update to protocol-23 branch
cd stellar-live-source-datalake/go
go mod edit -require github.com/stellar/go@protocol-23
go mod tidy
```

#### 1.2 Remove Custom Dependencies and Add Official Libraries
```bash
# Remove withObsrvr dependencies
go mod edit -droprequire github.com/withObsrvr/stellar-cdp
go mod edit -droprequire github.com/withObsrvr/stellar-datastore  
go mod edit -droprequire github.com/withObsrvr/stellar-ledgerbackend

# Add official Stellar components (these are part of github.com/stellar/go)
# No additional dependencies needed - all in main stellar/go module
go mod tidy
```

#### 1.3 Update Import Statements
Replace custom imports with official ones:
```go
// Remove these imports
// cdp "github.com/withObsrvr/stellar-cdp"
// datastore "github.com/withObsrvr/stellar-datastore"
// ledgerbackend "github.com/withObsrvr/stellar-ledgerbackend"

// Add official imports
import (
    "github.com/stellar/go/ingest/ledgerbackend"
    "github.com/stellar/go/historyarchive"
    "github.com/stellar/go/ingest"
    "github.com/stellar/go/support/storage"
)
```

#### 1.4 Regenerate Protobuf Code
```bash
cd stellar-live-source-datalake
make gen-proto
```

### Phase 2: Core Architecture Replacement (Week 2)

#### 2.1 Replace Custom CDP with Official LedgerBackend
The current implementation uses the custom CDP (Continuous Data Pipeline) pattern. We need to replace this with the official Stellar ingest framework:

**Current Pattern (Custom CDP)**:
```go
// Current server.go implementation
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    // Setup storage backend (GCS, S3, FS)
    dsConfig := datastore.DataStoreConfig{...}
    publisherConfig := cdp.PublisherConfig{...}
    
    // Use CDP to process ledgers from storage
    err := cdp.ApplyLedgerMetadata(ledgerRange, publisherConfig, ctx, func(lcm xdr.LedgerCloseMeta) error {
        // Process each ledger
    })
}
```

**New Pattern (Official LedgerBackend)**:
```go
// New implementation using official Stellar Go SDK
func (s *RawLedgerServer) StreamRawLedgers(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    ctx := stream.Context()
    
    // Choose backend based on configuration
    backend, err := s.createLedgerBackend(ctx)
    if err != nil {
        return fmt.Errorf("failed to create ledger backend: %w", err)
    }
    defer backend.Close()
    
    // Prepare range for processing
    ledgerRange := ledgerbackend.UnboundedRange(req.StartLedger)
    if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
        return fmt.Errorf("failed to prepare range: %w", err)
    }
    
    // Stream ledgers
    return s.streamLedgersFromBackend(ctx, backend, req.StartLedger, stream)
}

func (s *RawLedgerServer) createLedgerBackend(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
    storageType := os.Getenv("STORAGE_TYPE")
    
    switch storageType {
    case "CAPTIVE_CORE":
        return s.createCaptiveCore()
    case "RPC":
        return s.createRPCBackend()
    case "ARCHIVE":
        return s.createArchiveBackend()
    default:
        return nil, fmt.Errorf("unsupported storage type: %s", storageType)
    }
}
```

#### 2.2 Implement Backend Creation Methods
```go
func (s *RawLedgerServer) createCaptiveCore() (ledgerbackend.LedgerBackend, error) {
    networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
    archiveURLs := strings.Split(os.Getenv("HISTORY_ARCHIVE_URLS"), ",")
    
    config := ledgerbackend.CaptiveCoreConfig{
        BinaryPath:         os.Getenv("STELLAR_CORE_BINARY_PATH"),
        NetworkPassphrase:  networkPassphrase,
        HistoryArchiveURLs: archiveURLs,
        CheckpointFrequency: 64,
        Log: s.logger,
    }
    
    return ledgerbackend.NewCaptive(config)
}

func (s *RawLedgerServer) createRPCBackend() (ledgerbackend.LedgerBackend, error) {
    rpcURL := os.Getenv("RPC_ENDPOINT")
    if rpcURL == "" {
        return nil, fmt.Errorf("RPC_ENDPOINT required for RPC backend")
    }
    
    return ledgerbackend.NewRPC(ledgerbackend.RPCConfig{
        RPCURL: rpcURL,
        Log:    s.logger,
    })
}

func (s *RawLedgerServer) createArchiveBackend() (ledgerbackend.LedgerBackend, error) {
    // For archive-based processing, we'll use BufferedStorageBackend
    // This replaces the custom stellar-datastore functionality
    archiveURLs := strings.Split(os.Getenv("HISTORY_ARCHIVE_URLS"), ",")
    
    archive, err := historyarchive.Connect(
        archiveURLs[0], // Use first archive URL
        historyarchive.ConnectOptions{
            Context: context.Background(),
        },
    )
    if err != nil {
        return nil, fmt.Errorf("failed to connect to history archive: %w", err)
    }
    
    return ledgerbackend.NewBufferedStorageBackend(archive, s.logger)
}
```

#### 2.3 Protocol 23 XDR Processing Updates
Update the streaming logic to handle Protocol 23 changes:

```go
func (s *RawLedgerServer) streamLedgersFromBackend(ctx context.Context, backend ledgerbackend.LedgerBackend, startSeq uint32, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    for seq := startSeq; ; seq++ {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        
        // Get ledger from backend
        lcm, err := backend.GetLedger(ctx, seq)
        if err != nil {
            s.handleLedgerError(err, seq)
            continue
        }
        
        // Process Protocol 23 specific changes
        if err := s.validateProtocol23Compatibility(lcm); err != nil {
            return fmt.Errorf("protocol 23 validation failed: %w", err)
        }
        
        // Convert to protobuf and stream
        rawLedger, err := s.convertLedgerToProto(lcm)
        if err != nil {
            return fmt.Errorf("failed to convert ledger: %w", err)
        }
        
        if err := stream.Send(rawLedger); err != nil {
            return fmt.Errorf("failed to send ledger: %w", err)
        }
        
        s.updateMetrics(lcm.LedgerSequence())
    }
}

func (s *RawLedgerServer) validateProtocol23Compatibility(lcm xdr.LedgerCloseMeta) error {
    // Check for Protocol 23 specific structures
    if lcm.LedgerSequence() >= PROTOCOL_23_ACTIVATION_LEDGER {
        // Validate dual BucketList hash structure (CAP-62)
        if lcm.V == 1 && lcm.V1 != nil {
            if err := s.validateDualBucketListHash(lcm.V1); err != nil {
                return fmt.Errorf("dual bucket list validation failed: %w", err)
            }
        }
        
        // Validate hot archive buckets (CAP-62)
        if err := s.validateHotArchiveBuckets(lcm); err != nil {
            return fmt.Errorf("hot archive validation failed: %w", err)
        }
    }
    return nil
}
```

#### 2.2 Hot Archive Support
Add support for hot archive bucket processing:

```go
// New function to handle hot archive buckets
func (s *RawLedgerServer) processHotArchiveBuckets(hotArchive *xdr.HotArchiveBucketEntry) error {
    switch hotArchive.Type {
    case xdr.HotArchiveBucketEntryTypeHotArchiveMetaentry:
        // Handle metadata entries
        return s.processHotArchiveMetadata(hotArchive.MetaEntry)
    case xdr.HotArchiveBucketEntryTypeHotArchiveArchived:
        // Handle archived entries
        return s.processArchivedEntry(hotArchive.Archived)
    case xdr.HotArchiveBucketEntryTypeHotArchiveLive:
        // Handle restored entries
        return s.processRestoredEntry(hotArchive.Live)
    default:
        return fmt.Errorf("unknown hot archive bucket entry type: %v", hotArchive.Type)
    }
}
```

### Phase 3: Configuration and Environment Updates (Week 3)

#### 3.1 Update Environment Variables
Replace storage-specific variables with backend-specific ones:

**Remove (Custom CDP/Datastore variables):**
```bash
# Old custom variables
STORAGE_TYPE=GCS|S3|FS
BUCKET_NAME=stellar-datastore
LEDGERS_PER_FILE=64
FILES_PER_PARTITION=10
```

**Add (Official Stellar SDK variables):**
```bash
# New official backend configuration
BACKEND_TYPE=CAPTIVE_CORE|RPC|ARCHIVE
NETWORK_PASSPHRASE="Public Network; September 2015"
HISTORY_ARCHIVE_URLS="https://history.stellar.org/prd/core-live/core_live_001,https://history.stellar.org/prd/core-live/core_live_002"

# For CAPTIVE_CORE backend
STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
STELLAR_CORE_CONFIG_PATH=/etc/stellar/stellar-core.cfg

# For RPC backend
RPC_ENDPOINT=https://stellar-rpc.example.com

# For ARCHIVE backend (replaces custom datastore)
ARCHIVE_STORAGE_TYPE=S3|GCS
ARCHIVE_BUCKET_NAME=stellar-archive-bucket
```

#### 3.2 Update Configuration Loading
Replace custom storage configuration with official backend configuration:

```go
// New configuration structure
type LedgerBackendConfig struct {
    BackendType       string
    NetworkPassphrase string
    HistoryArchiveURLs []string
    
    // Captive Core specific
    StellarCoreBinaryPath string
    StellarCoreConfigPath string
    
    // RPC specific
    RPCEndpoint string
    
    // Archive specific  
    ArchiveStorageType string
    ArchiveBucketName  string
}

func (s *RawLedgerServer) loadConfiguration() (*LedgerBackendConfig, error) {
    config := &LedgerBackendConfig{
        BackendType:       getEnvOrDefault("BACKEND_TYPE", "CAPTIVE_CORE"),
        NetworkPassphrase: getEnvOrDefault("NETWORK_PASSPHRASE", "Public Network; September 2015"),
        HistoryArchiveURLs: strings.Split(getEnvOrDefault("HISTORY_ARCHIVE_URLS", ""), ","),
    }
    
    switch config.BackendType {
    case "CAPTIVE_CORE":
        config.StellarCoreBinaryPath = os.Getenv("STELLAR_CORE_BINARY_PATH")
        config.StellarCoreConfigPath = os.Getenv("STELLAR_CORE_CONFIG_PATH")
        if config.StellarCoreBinaryPath == "" {
            return nil, fmt.Errorf("STELLAR_CORE_BINARY_PATH required for CAPTIVE_CORE backend")
        }
    case "RPC":
        config.RPCEndpoint = os.Getenv("RPC_ENDPOINT")
        if config.RPCEndpoint == "" {
            return nil, fmt.Errorf("RPC_ENDPOINT required for RPC backend")
        }
    case "ARCHIVE":
        config.ArchiveStorageType = os.Getenv("ARCHIVE_STORAGE_TYPE")
        config.ArchiveBucketName = os.Getenv("ARCHIVE_BUCKET_NAME")
        if config.ArchiveStorageType == "" || config.ArchiveBucketName == "" {
            return nil, fmt.Errorf("ARCHIVE_STORAGE_TYPE and ARCHIVE_BUCKET_NAME required for ARCHIVE backend")
        }
    }
    
    return config, nil
}
```

#### 3.3 Backward Compatibility Layer
Provide compatibility for existing environment variables during transition:

```go
func (s *RawLedgerServer) migrateOldConfiguration() {
    // Map old STORAGE_TYPE to new BACKEND_TYPE
    if oldStorageType := os.Getenv("STORAGE_TYPE"); oldStorageType != "" && os.Getenv("BACKEND_TYPE") == "" {
        switch oldStorageType {
        case "GCS", "S3", "FS":
            os.Setenv("BACKEND_TYPE", "ARCHIVE")
            os.Setenv("ARCHIVE_STORAGE_TYPE", oldStorageType)
            if bucketName := os.Getenv("BUCKET_NAME"); bucketName != "" {
                os.Setenv("ARCHIVE_BUCKET_NAME", bucketName)
            }
            s.logger.Warn("Using deprecated STORAGE_TYPE configuration. Please migrate to BACKEND_TYPE=ARCHIVE")
        }
    }
    
    // Warn about deprecated variables
    deprecatedVars := []string{"LEDGERS_PER_FILE", "FILES_PER_PARTITION"}
    for _, varName := range deprecatedVars {
        if os.Getenv(varName) != "" {
            s.logger.Warn("Environment variable deprecated and ignored", zap.String("variable", varName))
        }
    }
}

### Phase 4: Testing and Validation (Week 4)

#### 4.1 Unit Testing Updates
Update existing tests for Protocol 23 compatibility:

```go
// Add Protocol 23 specific test cases
func TestProtocol23LedgerProcessing(t *testing.T) {
    // Test dual BucketList processing
    // Test hot archive bucket handling
    // Test automatic restoration
    // Test parallel transaction processing
}

func TestXDRCompatibility(t *testing.T) {
    // Test XDR parsing for new structures
    // Test backward compatibility
    // Test resource accounting changes
}
```

#### 4.2 Integration Testing
- Test against Protocol 23 testnet data
- Validate hot archive bucket processing
- Verify dual BucketList hash calculations
- Test automatic archived entry restoration

#### 4.3 Performance Testing
- Benchmark dual BucketList processing
- Measure impact of hot archive on throughput
- Test parallel transaction handling
- Validate memory usage with in-memory state

### Phase 5: Deployment and Monitoring (Week 5)

#### 5.1 Configuration Updates
Add new environment variables for Protocol 23:

```bash
# New Protocol 23 configuration options
HOT_ARCHIVE_THRESHOLD=1000
ENABLE_DUAL_BUCKET_LIST=true
PROTOCOL_23_ACTIVATION_LEDGER=<activation_ledger>
AUTO_RESTORE_ARCHIVED=true
```

#### 5.2 Monitoring Enhancements
Update health check and metrics for Protocol 23:

```go
// Enhanced metrics for Protocol 23
type Protocol23Metrics struct {
    HotArchiveEntries     int64
    DualBucketListOps     int64
    AutoRestoredEntries   int64
    ParallelTxProcessed   int64
    Protocol23Errors      int64
}

// Update health check endpoint
func (s *RawLedgerServer) healthCheckProtocol23(w http.ResponseWriter, r *http.Request) {
    metrics := s.GetProtocol23Metrics()
    response := map[string]interface{}{
        "protocol_23_support": true,
        "hot_archive_enabled": s.isHotArchiveEnabled(),
        "dual_bucket_list":    s.isDualBucketListEnabled(),
        "metrics":            metrics,
    }
    json.NewEncoder(w).Encode(response)
}
```

## Best Practices and Recommendations

### 1. Backward Compatibility
- Maintain support for pre-Protocol 23 ledgers
- Implement feature detection based on ledger sequence
- Graceful degradation for missing Protocol 23 features

### 2. Error Handling
```go
// Robust error handling for Protocol 23 transitions
func (s *RawLedgerServer) handleProtocolTransition(lcm xdr.LedgerCloseMeta) error {
    if lcm.LedgerSequence() == PROTOCOL_23_ACTIVATION_LEDGER {
        log.Info("Transitioning to Protocol 23 processing")
        if err := s.enableProtocol23Features(); err != nil {
            return fmt.Errorf("failed to enable Protocol 23: %w", err)
        }
    }
    return nil
}
```

### 3. Performance Optimization
- Implement connection pooling for dual BucketList access
- Cache hot archive metadata for faster lookups
- Use worker pools for parallel transaction processing
- Monitor memory usage with in-memory state

### 4. Monitoring and Alerting
- Alert on Protocol 23 parsing errors
- Monitor hot archive bucket processing rates
- Track automatic restoration events
- Alert on dual BucketList hash mismatches

## Risk Assessment and Mitigation

### Critical Risk: Library Migration Breaking Changes
**Risk:** Switching from withObsrvr to official libraries breaks functionality
**Mitigation:** 
- Parallel implementation approach - keep old code during transition
- Comprehensive integration testing with both library sets
- Feature flags to switch between implementations
- Rollback plan to withObsrvr libraries if needed

### High Risk: XDR Parsing Failures  
**Risk:** New Protocol 23 XDR structures cause parsing failures
**Mitigation:** 
- Comprehensive unit tests for all new XDR types
- Gradual rollout with feature flags
- Fallback to raw bytes on parsing errors
- Version detection and backwards compatibility

### High Risk: Backend Performance Differences
**Risk:** Official backends perform differently than custom CDP solution
**Mitigation:**
- Extensive performance benchmarking across all backend types
- Load testing with real-world data volumes
- Configurable backend selection for optimization
- Monitoring and alerting for performance regressions

### Medium Risk: Configuration Migration Issues
**Risk:** Environment variable changes break existing deployments
**Mitigation:**
- Backward compatibility layer for old environment variables
- Migration guide and tooling
- Deprecation warnings with clear upgrade paths
- Phased rollout of new configuration

### Medium Risk: Dual BucketList Processing
**Risk:** Protocol 23 dual BucketList structure impacts throughput
**Mitigation:**
- Performance benchmarking before deployment
- Configurable processing modes
- Circuit breaker patterns for overload protection
- Memory usage monitoring for hot archive data

### Low Risk: Storage Backend Compatibility
**Risk:** Official storage backends don't support all current features
**Mitigation:**
- Test all supported backend types (Captive Core, RPC, Archive)
- Document feature parity matrix
- Custom storage layer if needed for specific requirements

## Success Criteria

1. **Library Migration Requirements:**
   - Complete removal of withObsrvr dependencies
   - All functionality working with official Stellar Go SDK
   - No degradation in feature set or performance
   - Clean, maintainable code using standard patterns

2. **Protocol 23 Functional Requirements:**
   - Successfully process Protocol 23 ledgers
   - Handle dual BucketList structure (CAP-62)
   - Support hot archive buckets and automatic restoration (CAP-66)
   - Process parallel transactions correctly (CAP-63)
   - Maintain backward compatibility with pre-Protocol 23 ledgers

3. **Performance Requirements:**
   - No degradation in processing throughput vs current implementation
   - Memory usage within acceptable limits despite in-memory state
   - 99.99% uptime during transition
   - Support for all backend types (Captive Core, RPC, Archive)

4. **Quality Requirements:**
   - Zero data loss during upgrade
   - All existing tests pass with new implementation
   - Comprehensive Protocol 23 test coverage
   - Performance benchmarks met across all backends
   - Complete documentation and migration guides

5. **Operational Requirements:**
   - Smooth migration path from current configuration
   - Clear rollback procedures
   - Comprehensive monitoring and alerting
   - Production-ready deployment process

## Migration Benefits

### Long-term Advantages of Using Official Libraries

1. **Maintenance and Support:**
   - Direct support from Stellar Development Foundation
   - Automatic updates for new protocol versions
   - Community support and documentation
   - No dependency on third-party forks

2. **Protocol Compatibility:**
   - Guaranteed Protocol 23 support and future protocols
   - Official XDR handling and validation
   - Standard patterns used by other Stellar applications
   - Reduced risk of compatibility issues

3. **Performance and Reliability:**
   - Optimized implementations from core Stellar team
   - Battle-tested in production environments (Horizon, RPC)
   - Multiple backend options for different use cases
   - Built-in monitoring and metrics

4. **Code Quality:**
   - Cleaner, more maintainable codebase
   - Standard Go patterns and practices
   - Better test coverage and CI/CD integration
   - Easier onboarding for new developers

## Conclusion

This Protocol 23 upgrade represents both a critical protocol update and an opportunity to modernize the `stellar-live-source-datalake` architecture. The migration from custom withObsrvr libraries to official Stellar Go SDK components will:

1. **Ensure Protocol 23 Compatibility:** Handle dual BucketList architecture, hot archive buckets, and new XDR schemas
2. **Improve Long-term Maintainability:** Use officially supported, community-backed libraries
3. **Enhance Reliability:** Leverage battle-tested implementations used by core Stellar infrastructure
4. **Future-proof the Implementation:** Automatic compatibility with future protocol upgrades

The success of this migration depends on:
- Careful planning and phased implementation
- Comprehensive testing with multiple backend types
- Thorough performance validation
- Robust rollback procedures
- Clear documentation and operational procedures

With the August 26, 2025 mainnet activation deadline, this implementation timeline provides adequate time for development, testing, and validation while ensuring the service is ready for Protocol 23 and positioned for long-term success.

## References

1. [Stellar Protocol 23 Announcement](https://stellar.org/blog/developers/announcing-protocol-23)
2. [CAP-62: Soroban Live State Prioritization](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0062.md)
3. [CAP-66: Soroban In-memory Read Resource](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0066.md)
4. [CAP-63: Parallelism-friendly Transaction Scheduling](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0063.md)
5. [Stellar ETL Best Practices](https://github.com/stellar/stellar-etl)
6. [Protocol 23 Upgrade Guide](https://stellar.org/blog/developers/protocol-23-upgrade-guide)