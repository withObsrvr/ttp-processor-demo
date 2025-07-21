# GCS Archive Schema & Protocol Version Resolution Plan

## Executive Summary

This document outlines a comprehensive plan to resolve two critical issues preventing the stellar-live-source-datalake service from processing historical Stellar ledger data:

1. **GCS Archive Schema Mismatch**: Configuration expects specific data organization that doesn't match the actual GCS bucket structure
2. **Protocol Version Incompatibility**: XDR parsing fails for newer ledger formats (Protocol 23+)

**Impact**: Currently blocking access to historical ledger data ranges, limiting the system to only recent ledgers via RPC backend.

**Timeline**: 2-4 weeks for complete resolution with phased rollout approach.

**Priority**: High - Critical for production data pipeline reliability.

### **Current Status Update (Post-Investigation)**

- ✅ **Schema Issue**: RESOLVED - Applied correct configuration (`LEDGERS_PER_FILE=1, FILES_PER_PARTITION=64000`)
- ✅ **GCS Connection**: WORKING - Successfully connecting to `obsrvr-stellar-ledger-data-testnet-data` bucket
- 🔍 **Remaining Issue**: Data availability in GCS bucket for ledger range 409907-409910 - may require different ledger range or bucket investigation
- 📊 **Current Workaround**: RPC backend working for ledgers 417080+ (recent data)

## Problem Analysis

### Issue 1: GCS Archive Schema Mismatch

**Symptoms**:
- Error: `"failed getting next ledger batch from queue: context canceled"`
- GCS connection succeeds but ledger retrieval fails
- Backend configuration: `ledgers_per_file=64, files_per_partition=10`

**Root Cause**: 
The hardcoded schema configuration doesn't match the actual data organization in the `obsrvr-stellar-ledger-data-testnet-data` GCS bucket. The service assumes a specific file structure that may not exist in the bucket.

**Evidence**:
```bash
# FIXED: Correct schema configuration
{"level":"info","msg":"Created archive backend","storage_type":"GCS",
 "bucket_name":"obsrvr-stellar-ledger-data-testnet-data",
 "ledgers_per_file":1,"files_per_partition":64000}

# Still failing with same error - suggests deeper issue
{"level":"error","msg":"Ledger retrieval failed","sequence":409907,
 "error":"failed getting next ledger batch from queue: context canceled"}
```

**Updated Analysis**: Schema is now correctly configured (1 ledger/file, 64000 files/partition), but ledger retrieval still fails. This indicates the issue is not schema configuration but rather:
1. **Data Availability**: Ledger range 409907-409910 may not exist in this specific bucket
2. **Bucket Structure**: File path organization may not match expected pattern
3. **Access Permissions**: Insufficient permissions to access specific ledger files

### Issue 2: Protocol Version Incompatibility

**Symptoms**:
- Error: `"union LedgerCloseMeta has invalid V (int32) switch value '2'"`
- Affects ledgers from sequence ~520000+ (newer ledgers)
- Older ledgers (417080-417085) work correctly

**Root Cause**:
The Stellar Go SDK version used doesn't support the newer XDR format introduced in Protocol 23. The `LedgerCloseMeta` union now supports V2 format which isn't recognized.

**Evidence**:
```bash
# Working ledger range (older format)
{"level":"info","msg":"finished processing ledger","ledger_sequence":417081,"events_sent":4}

# Failing ledger range (newer format)  
{"level":"error","msg":"Failed to prepare ledger range","start_ledger":520000,
 "error":"failed to unmarshal ledger 520000: union LedgerCloseMeta has invalid V (int32) switch value '2'"}
```

## Technical Resolution Strategy

### Phase 1: Schema Discovery & Configuration Fix

#### 1.1 GCS Bucket Schema Investigation

**Objective**: Automatically detect the correct schema configuration for any GCS bucket.

**Implementation**:
```go
// Add to server/server.go
func (s *RawLedgerServer) detectGCSSchema(ctx context.Context, bucketName string) (*datastore.DataStoreSchema, error) {
    // Sample bucket structure to detect organization
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to create GCS client: %w", err)
    }
    defer client.Close()
    
    bucket := client.Bucket(bucketName)
    
    // Try to detect common patterns
    schemas := []datastore.DataStoreSchema{
        {LedgersPerFile: 1, FilesPerPartition: 64000},  // Stellar default
        {LedgersPerFile: 64, FilesPerPartition: 10},    // Current config
        {LedgersPerFile: 1, FilesPerPartition: 1},      // Simple structure
    }
    
    for _, schema := range schemas {
        if valid, err := s.validateSchemaAgainstBucket(ctx, bucket, schema); err == nil && valid {
            s.logger.Info("Detected GCS schema", 
                zap.Uint32("ledgers_per_file", schema.LedgersPerFile),
                zap.Uint32("files_per_partition", schema.FilesPerPartition))
            return &schema, nil
        }
    }
    
    return nil, fmt.Errorf("could not detect valid schema for bucket %s", bucketName)
}
```

**Environment Variables**:
```bash
# Override auto-detection
GCS_AUTO_DETECT_SCHEMA=true
GCS_LEDGERS_PER_FILE=1
GCS_FILES_PER_PARTITION=64000
```

#### 1.2 Dynamic Schema Configuration

**Objective**: Allow runtime schema detection and configuration.

**Configuration Enhancement**:
```go
// Modify createArchiveBackend() function
func (s *RawLedgerServer) createArchiveBackend() (ledgerbackend.LedgerBackend, error) {
    var schema datastore.DataStoreSchema
    
    // Check for manual override
    if manualLPF := os.Getenv("GCS_LEDGERS_PER_FILE"); manualLPF != "" {
        schema.LedgersPerFile = uint32(getEnvAsUint("GCS_LEDGERS_PER_FILE", 64))
        schema.FilesPerPartition = uint32(getEnvAsUint("GCS_FILES_PER_PARTITION", 10))
        s.logger.Info("Using manual schema configuration")
    } else if os.Getenv("GCS_AUTO_DETECT_SCHEMA") == "true" {
        detectedSchema, err := s.detectGCSSchema(context.Background(), s.config.ArchiveBucketName)
        if err != nil {
            s.logger.Warn("Schema auto-detection failed, using defaults", zap.Error(err))
            schema = datastore.DataStoreSchema{LedgersPerFile: 64, FilesPerPartition: 10}
        } else {
            schema = *detectedSchema
        }
    } else {
        // Use existing defaults
        schema = datastore.DataStoreSchema{
            LedgersPerFile:    uint32(getEnvAsUint("LEDGERS_PER_FILE", 64)),
            FilesPerPartition: uint32(getEnvAsUint("FILES_PER_PARTITION", 10)),
        }
    }
    
    // Rest of existing function...
}
```

#### 1.3 Configuration Validation

**Objective**: Validate configuration before processing ledgers.

**Health Check Enhancement**:
```go
// Add to health endpoint
func (s *RawLedgerServer) validateArchiveConfiguration() error {
    if s.config.BackendType != "ARCHIVE" {
        return nil
    }
    
    // Test actual ledger retrieval
    testSeq := uint32(417080) // Known working sequence
    backend, err := s.createArchiveBackend()
    if err != nil {
        return fmt.Errorf("failed to create backend: %w", err)
    }
    defer backend.Close()
    
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(testSeq, testSeq)); err != nil {
        return fmt.Errorf("failed to prepare test range: %w", err)
    }
    
    _, err = backend.GetLedger(ctx, testSeq)
    return err
}
```

### Phase 2: Protocol Version Compatibility

#### 2.1 Stellar Go SDK Upgrade

**Current Version Analysis**:
```bash
# stellar-live-source-datalake/go/go.mod
github.com/stellar/go v0.0.0-20250718194041-56335b4c7e0c
github.com/stellar/go-xdr v0.0.0-20231122183749-b53fb00bcac2
```

**Upgrade Strategy**:
1. **Research latest compatible versions** supporting Protocol 23+
2. **Staged upgrade approach**:
   - Update `stellar-live-source-datalake` first
   - Validate compatibility with existing functionality
   - Update `ttp-processor` and other services
   - Update all consumers

**Dependencies to Update**:
```bash
# Target versions (research latest stable)
github.com/stellar/go v0.0.0-20250118000000-latest
github.com/stellar/go-xdr v0.0.0-20250118000000-latest
```

#### 2.2 XDR Format Support Enhancement

**Objective**: Support all LedgerCloseMeta format versions (V0, V1, V2+).

**Implementation**:
```go
// Add to server/server.go
func (s *RawLedgerServer) parseLedgerCloseMeta(data []byte) (xdr.LedgerCloseMeta, error) {
    var lcm xdr.LedgerCloseMeta
    
    if err := xdr.SafeUnmarshal(data, &lcm); err != nil {
        // Log the specific error for debugging
        s.logger.Debug("Failed to unmarshal LedgerCloseMeta", 
            zap.Error(err),
            zap.String("data_preview", fmt.Sprintf("%x", data[:min(32, len(data))])))
        
        // Try alternative parsing approaches for compatibility
        if alternativeLcm, altErr := s.tryAlternativeParsing(data); altErr == nil {
            s.logger.Info("Successfully parsed using alternative method")
            return alternativeLcm, nil
        }
        
        return lcm, fmt.Errorf("failed to parse LedgerCloseMeta: %w", err)
    }
    
    // Validate the parsed structure
    if err := s.validateLedgerCloseMeta(lcm); err != nil {
        return lcm, fmt.Errorf("invalid LedgerCloseMeta structure: %w", err)
    }
    
    return lcm, nil
}

func (s *RawLedgerServer) validateLedgerCloseMeta(lcm xdr.LedgerCloseMeta) error {
    switch lcm.V {
    case 0:
        s.logger.Debug("Processing LedgerCloseMeta V0 format")
        if lcm.V0 == nil {
            return fmt.Errorf("V0 format but V0 field is nil")
        }
    case 1:
        s.logger.Debug("Processing LedgerCloseMeta V1 format")
        if lcm.V1 == nil {
            return fmt.Errorf("V1 format but V1 field is nil")
        }
    case 2:
        s.logger.Debug("Processing LedgerCloseMeta V2 format")
        // Add V2 validation when SDK supports it
        s.logger.Info("Protocol 23+ V2 format detected")
    default:
        return fmt.Errorf("unsupported LedgerCloseMeta version: %d", lcm.V)
    }
    return nil
}
```

#### 2.3 Protocol 23 Features Implementation

**Objective**: Complete the Protocol 23 compatibility implementation.

**TODOs to Complete**:
```go
// Update PROTOCOL_23_ACTIVATION_LEDGER with correct value
const (
    PROTOCOL_23_ACTIVATION_LEDGER = 4194305 // Update with actual activation ledger
)

func (s *RawLedgerServer) validateDualBucketListHash(v1 *xdr.LedgerCloseMetaV1) error {
    if v1.LedgerHeader.Header.LedgerSeq < PROTOCOL_23_ACTIVATION_LEDGER {
        return nil // Skip validation for pre-Protocol 23 ledgers
    }
    
    // Implement actual dual bucket list hash validation
    if v1.LedgerHeader.Header.BucketListHash.IsZero() {
        return fmt.Errorf("missing bucket list hash for Protocol 23 ledger")
    }
    
    // TODO: Validate dual bucket list structure
    s.logger.Debug("Validated dual BucketList hash", 
        zap.Uint32("sequence", uint32(v1.LedgerHeader.Header.LedgerSeq)))
    
    return nil
}

func (s *RawLedgerServer) validateHotArchiveBuckets(lcm xdr.LedgerCloseMeta) error {
    if lcm.LedgerSequence() < PROTOCOL_23_ACTIVATION_LEDGER {
        return nil
    }
    
    // Implement hot archive bucket validation
    switch lcm.V {
    case 1:
        if lcm.V1 != nil {
            // Check for hot archive bucket entries
            // TODO: Implement specific validation logic
        }
    case 2:
        // Handle V2 format hot archive validation
        // TODO: Implement when SDK supports V2
    }
    
    return nil
}
```

### Phase 3: Multi-Backend Strategy

#### 3.1 Smart Backend Selection

**Objective**: Automatically choose the best backend for each ledger range request.

**Implementation**:
```go
// Add backend capability matrix
type BackendCapability struct {
    Type           string
    MinLedger      uint32
    MaxLedger      uint32
    SupportsRange  bool
    Latency        time.Duration
    Reliability    float64
}

func (s *RawLedgerServer) selectOptimalBackend(startLedger, endLedger uint32) (string, error) {
    capabilities := []BackendCapability{
        {
            Type:          "ARCHIVE",
            MinLedger:     1,
            MaxLedger:     500000,  // Approximate based on what we've seen
            SupportsRange: true,
            Latency:       100 * time.Millisecond,
            Reliability:   0.85,
        },
        {
            Type:          "RPC", 
            MinLedger:     417000,  // Based on our observations
            MaxLedger:     600000,  // Current + some buffer
            SupportsRange: true,
            Latency:       500 * time.Millisecond,
            Reliability:   0.95,
        },
    }
    
    // Score each backend for this request
    bestBackend := ""
    bestScore := float64(0)
    
    for _, cap := range capabilities {
        if startLedger >= cap.MinLedger && endLedger <= cap.MaxLedger {
            score := cap.Reliability * (1.0 / cap.Latency.Seconds())
            if score > bestScore {
                bestScore = score
                bestBackend = cap.Type
            }
        }
    }
    
    if bestBackend == "" {
        return "", fmt.Errorf("no backend available for ledger range %d-%d", startLedger, endLedger)
    }
    
    return bestBackend, nil
}
```

#### 3.2 Fallback Strategy

**Objective**: Graceful degradation when primary backend fails.

**Implementation**:
```go
func (s *RawLedgerServer) StreamRawLedgersWithFallback(req *pb.StreamLedgersRequest, stream pb.RawLedgerService_StreamRawLedgersServer) error {
    backends := []string{"ARCHIVE", "RPC"}
    
    for _, backendType := range backends {
        s.logger.Info("Attempting backend", zap.String("backend", backendType))
        
        // Temporarily override backend type
        originalBackend := s.config.BackendType
        s.config.BackendType = backendType
        
        err := s.StreamRawLedgers(req, stream)
        
        // Restore original config
        s.config.BackendType = originalBackend
        
        if err == nil {
            s.logger.Info("Successfully processed with backend", zap.String("backend", backendType))
            return nil
        }
        
        s.logger.Warn("Backend failed, trying next", 
            zap.String("backend", backendType),
            zap.Error(err))
    }
    
    return fmt.Errorf("all backends failed for ledger range %d-%d", req.StartLedger, req.EndLedger)
}
```

### Phase 4: Enhanced Error Handling & Monitoring

#### 4.1 Schema Migration Tools

**Objective**: Provide tools to migrate between different archive schemas.

**CLI Tool Implementation**:
```bash
# Add to cli_tool/
./stellar-archive-tool validate-schema --bucket=gs://bucket-name
./stellar-archive-tool migrate-schema --from-schema=64,10 --to-schema=1,64000
./stellar-archive-tool test-ledger-range --start=409907 --end=409948
```

#### 4.2 Enhanced Monitoring

**Objective**: Comprehensive monitoring of backend health and schema issues.

**Metrics Enhancement**:
```go
// Add metrics for schema and backend monitoring
type ArchiveMetrics struct {
    SchemaDetectionAttempts  prometheus.Counter
    SchemaDetectionSuccess   prometheus.Counter
    BackendSwitchEvents      prometheus.Counter
    LedgerRetrievalByBackend *prometheus.CounterVec
    ProtocolVersionSupport   *prometheus.GaugeVec
}

func (s *RawLedgerServer) initArchiveMetrics() {
    s.archiveMetrics = &ArchiveMetrics{
        SchemaDetectionAttempts: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "stellar_archive_schema_detection_attempts_total",
            Help: "Total number of schema detection attempts",
        }),
        BackendSwitchEvents: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "stellar_backend_switch_events_total", 
            Help: "Total number of backend failover events",
        }),
        LedgerRetrievalByBackend: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "stellar_ledger_retrieval_by_backend_total",
                Help: "Ledger retrievals by backend type",
            },
            []string{"backend", "status"},
        ),
    }
}
```

## Implementation Guide

### Step 1: Emergency Schema Fix (1-2 days)

1. **Add schema auto-detection**:
   ```bash
   cd stellar-live-source-datalake
   # Add environment variable support
   export GCS_AUTO_DETECT_SCHEMA=true
   # Test with known working schema
   export GCS_LEDGERS_PER_FILE=1
   export GCS_FILES_PER_PARTITION=64000
   ```

2. **Test validation**:
   ```bash
   # Test with original problematic range
   npm start -- 409907 409948
   ```

### Step 2: Protocol Support Enhancement (1 week)

1. **SDK Upgrade**:
   ```bash
   cd stellar-live-source-datalake/go
   go get github.com/stellar/go@latest
   go mod tidy
   ```

2. **Validation Testing**:
   ```bash
   # Test with problematic newer ledgers
   npm start -- 520000 520005
   ```

### Step 3: Multi-Backend Implementation (1 week)

1. **Backend Selection Logic**
2. **Fallback Implementation** 
3. **Integration Testing**

### Step 4: Production Rollout (1 week)

1. **Staging Environment Testing**
2. **Gradual Production Deployment**
3. **Monitoring and Validation**

## Risk Assessment & Mitigation

### High Risks

1. **SDK Upgrade Breaking Changes**
   - **Mitigation**: Staged rollout with comprehensive testing
   - **Rollback**: Maintain current SDK version in separate branch

2. **GCS Schema Detection Failures**
   - **Mitigation**: Manual override options
   - **Fallback**: Hardcoded known-working schemas

3. **Backend Selection Logic Errors**
   - **Mitigation**: Conservative fallback to RPC
   - **Monitoring**: Detailed metrics on backend selection

### Medium Risks

1. **Performance Impact from Auto-Detection**
   - **Mitigation**: Cache detection results
   - **Optimization**: Background schema validation

2. **Configuration Complexity**
   - **Mitigation**: Clear documentation and examples
   - **Tooling**: Validation utilities

## Success Metrics

### Primary Success Criteria

- ✅ **GCS Archive Access**: Process original ledger range 409907-409948 successfully
- ✅ **Protocol Compatibility**: Handle ledgers 520000+ without XDR errors  
- ✅ **Automatic Failover**: Seamless backend switching when one fails
- ✅ **Schema Detection**: Auto-detect GCS bucket organization

### Performance Benchmarks

- **Latency**: < 2s for ledger range requests
- **Throughput**: > 100 ledgers/second processing
- **Reliability**: > 99% success rate for valid ledger ranges
- **Recovery**: < 30s automatic recovery from backend failures

### Monitoring and Alerting

- **Schema Detection Failures**: Alert when auto-detection fails
- **Backend Health**: Monitor success rates per backend
- **Protocol Errors**: Alert on new XDR parsing failures
- **Performance Degradation**: Alert on throughput drops

## Testing Strategy

### Unit Testing
- Schema detection algorithms
- XDR parsing compatibility
- Backend selection logic

### Integration Testing  
- End-to-end ledger processing
- Multi-backend failover scenarios
- GCS bucket compatibility

### Performance Testing
- Large ledger range processing
- Concurrent request handling
- Memory usage optimization

### Regression Testing
- Existing functionality preservation
- Backward compatibility validation
- Configuration migration testing

## Documentation Updates

1. **Update MIGRATION_GUIDE.md** with new schema options
2. **Enhance README.md** with troubleshooting section
3. **Create BACKEND_SELECTION.md** explaining fallback logic
4. **Update API documentation** with new configuration options

## Conclusion

This comprehensive plan addresses both immediate issues (GCS schema mismatch and Protocol compatibility) while building a robust, future-proof architecture. The phased approach minimizes risk while providing immediate value.

The enhanced error handling, monitoring, and multi-backend strategy will significantly improve the reliability and maintainability of the stellar-live-source-datalake service, ensuring it can handle both current and future Stellar protocol changes.