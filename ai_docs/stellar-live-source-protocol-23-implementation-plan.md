# Stellar Live Source Protocol 23 Implementation Plan

## Executive Summary

This document outlines the implementation plan for updating `stellar-live-source` to support Protocol 23's new hybrid data access capabilities. The key innovation in Protocol 23 is that Stellar RPC endpoints can now transparently fetch historical ledgers from data lakes when the requested data is beyond their retention window, providing seamless access to both recent and historical data through a single API.

**Critical Enhancement:** Protocol 23 enables Stellar RPC to act as a unified gateway for both real-time and historical data, eliminating the need for separate data source configurations.

**Timeline:**
- Protocol 23 Testnet: Already active
- Protocol 23 Mainnet: August 26, 2025
- Implementation Target: August 15, 2025

## Current Architecture Analysis

### Current Implementation
The `stellar-live-source` service (`stellar-live-source/go/server/server.go`) implements:

**Core Components:**
- gRPC streaming service for raw ledger data
- Direct connection to Stellar RPC endpoints
- Enterprise-grade retry logic and circuit breakers
- Uses `github.com/stellar/stellar-rpc/client` for RPC communication
- Health check endpoints and comprehensive metrics

**Current Data Flow:**
```
Stellar RPC → stellar-live-source → gRPC Stream → Consumers
   (Recent ledgers only)
```

**Limitations:**
- Only accesses ledgers within RPC retention window (typically 7-30 days)
- No access to historical data beyond retention
- Requires separate `stellar-live-source-datalake` for historical data

## Protocol 23 Hybrid Architecture

### New RPC Capabilities
With Protocol 23, Stellar RPC introduces transparent data lake integration:

**Enhanced Data Flow:**
```
                    ┌─────────────────────┐
                    │  Stellar RPC Node   │
                    │  (Protocol 23)      │
                    └──────────┬──────────┘
                               │
            ┌──────────────────┴──────────────────┐
            │                                     │
   ┌────────▼─────────┐              ┌───────────▼──────────┐
   │ Recent Ledgers   │              │ Historical Ledgers   │
   │ (Local Storage)  │              │ (Data Lake Fetch)   │
   └──────────────────┘              └────────────────────┘
            │                                     │
            └──────────────────┬──────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │ stellar-live-source │
                    │ (Enhanced)          │
                    └─────────────────────┘
```

### Key Protocol 23 RPC Enhancements

1. **Transparent Historical Access**
   - RPC automatically detects when requested ledgers are beyond retention
   - Seamlessly fetches from configured data lakes (GCS, S3, etc.)
   - Returns unified response format regardless of data source

2. **Extended GetLedgers API**
   ```protobuf
   message GetLedgersRequest {
     uint32 start_ledger = 1;
     LedgerPaginationOptions pagination = 2;
     // New in Protocol 23
     HistoricalDataOptions historical = 3;
   }
   
   message HistoricalDataOptions {
     bool allow_data_lake_access = 1;
     uint32 max_historical_range = 2;
     repeated string preferred_archives = 3;
   }
   ```

3. **Response Metadata**
   ```protobuf
   message GetLedgersResponse {
     repeated LedgerInfo ledgers = 1;
     uint32 latest_ledger = 2;
     // New in Protocol 23
     DataSourceInfo source_info = 3;
   }
   
   message DataSourceInfo {
     enum DataSource {
       LOCAL = 0;
       DATA_LAKE = 1;
       HYBRID = 2;
     }
     DataSource source = 1;
     uint32 local_retention_start = 2;
     repeated string archives_used = 3;
   }
   ```

## Implementation Strategy

### Phase 1: Protocol 23 SDK Update (Week 1)

#### 1.1 Update Dependencies
```bash
cd stellar-live-source/go
# Update to Protocol 23 compatible RPC client
go get github.com/stellar/stellar-rpc@protocol-23
go get github.com/stellar/go@protocol-23
go mod tidy
```

#### 1.2 Update RPC Client Configuration
```go
// Enhanced configuration supporting historical data access
type Config struct {
    // Core configuration
    RPCEndpoint       string
    NetworkPassphrase string
    
    // Historical data access configuration
    EnableHistoricalAccess    bool
    MaxHistoricalRange        uint32
    PreferredArchives         []string
    HistoricalAccessTimeout   time.Duration
    CacheHistoricalResponses  bool
    HistoricalCacheDuration   time.Duration
}
```

### Phase 2: Enhanced RPC Client Implementation (Week 2)

#### 2.1 Update GetLedgers Request Builder
```go
func (s *RawLedgerServer) buildGetLedgersRequest(startLedger uint32) protocol.GetLedgersRequest {
    req := protocol.GetLedgersRequest{
        StartLedger: startLedger,
        Pagination: &protocol.LedgerPaginationOptions{
            Limit: 100,
        },
    }
    
    // Configure historical data access if enabled
    if s.config.EnableHistoricalAccess {
        req.Historical = &protocol.HistoricalDataOptions{
            AllowDataLakeAccess: true,
            MaxHistoricalRange:  s.config.MaxHistoricalRange,
            PreferredArchives:   s.config.PreferredArchives,
        }
    }
    
    return req
}
```

#### 2.2 Handle Response with Data Source Metadata
```go
func (s *RawLedgerServer) processLedgerResponse(resp *protocol.GetLedgersResponse) error {
    // Log data source information
    if resp.SourceInfo != nil {
        s.logger.Info("Ledger data source",
            zap.String("source", resp.SourceInfo.Source.String()),
            zap.Uint32("local_retention_start", resp.SourceInfo.LocalRetentionStart),
            zap.Strings("archives_used", resp.SourceInfo.ArchivesUsed),
        )
        
        // Update metrics based on data source
        s.updateDataSourceMetrics(resp.SourceInfo)
        
        // Handle hybrid responses (mix of local and historical)
        if resp.SourceInfo.Source == protocol.DataSourceHybrid {
            s.handleHybridResponse(resp)
        }
    }
    
    return nil
}
```

#### 2.3 Implement Adaptive Retry Logic
```go
func (s *RawLedgerServer) getLedgersWithRetry(ctx context.Context, req protocol.GetLedgersRequest) (*protocol.GetLedgersResponse, error) {
    var lastErr error
    
    // Different retry strategies for local vs historical data
    retryStrategy := s.getRetryStrategy(req)
    
    for attempt := 0; attempt < retryStrategy.MaxAttempts; attempt++ {
        resp, err := s.rpcClient.GetLedgers(ctx, req)
        if err == nil {
            return resp, nil
        }
        
        lastErr = err
        
        // Handle historical data access errors differently
        if isHistoricalAccessError(err) {
            // Try fallback archives if available
            if s.tryFallbackArchive(&req) {
                continue
            }
            
            // Log historical access failure
            s.logger.Warn("Historical data access failed",
                zap.Error(err),
                zap.Uint32("start_ledger", req.StartLedger),
                zap.Int("attempt", attempt+1),
            )
        }
        
        // Apply backoff based on error type
        backoff := retryStrategy.GetBackoff(attempt, err)
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(backoff):
            continue
        }
    }
    
    return nil, fmt.Errorf("failed after %d attempts: %w", retryStrategy.MaxAttempts, lastErr)
}
```

### Phase 3: Performance Optimization (Week 3)

#### 3.1 Implement Response Caching
```go
type HistoricalCache struct {
    mu      sync.RWMutex
    entries map[uint32]*CacheEntry
    ttl     time.Duration
}

type CacheEntry struct {
    ledger     *xdr.LedgerCloseMeta
    source     protocol.DataSource
    timestamp  time.Time
}

func (s *RawLedgerServer) getCachedLedger(sequence uint32) (*xdr.LedgerCloseMeta, bool) {
    if !s.config.CacheHistoricalResponses {
        return nil, false
    }
    
    s.cache.mu.RLock()
    defer s.cache.mu.RUnlock()
    
    entry, exists := s.cache.entries[sequence]
    if !exists {
        return nil, false
    }
    
    // Check TTL
    if time.Since(entry.timestamp) > s.cache.ttl {
        return nil, false
    }
    
    s.metrics.CacheHits++
    return entry.ledger, true
}
```

#### 3.2 Implement Predictive Prefetching
```go
type PrefetchManager struct {
    mu              sync.Mutex
    prefetchQueue   chan uint32
    activePrefetches map[uint32]bool
    maxConcurrent   int
}

func (s *RawLedgerServer) startPrefetchManager() {
    manager := &PrefetchManager{
        prefetchQueue:    make(chan uint32, 1000),
        activePrefetches: make(map[uint32]bool),
        maxConcurrent:    10,
    }
    
    // Start prefetch workers
    for i := 0; i < manager.maxConcurrent; i++ {
        go s.prefetchWorker(manager)
    }
    
    // Monitor stream patterns and predict future requests
    go s.predictivePrefetchLoop(manager)
}

func (s *RawLedgerServer) predictivePrefetchLoop(manager *PrefetchManager) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            // Analyze recent access patterns
            pattern := s.analyzeAccessPattern()
            
            // Prefetch predicted sequences
            for _, seq := range pattern.PredictNext(10) {
                select {
                case manager.prefetchQueue <- seq:
                default:
                    // Queue full, skip
                }
            }
        }
    }
}
```

### Phase 4: Monitoring and Observability (Week 4)

#### 4.1 Enhanced Metrics for Data Source Tracking
```go
type DataSourceMetrics struct {
    // Core metrics
    *EnterpriseMetrics
    
    // Data source specific metrics
    LocalLedgersFetched      int64
    HistoricalLedgersFetched int64
    HybridResponseCount      int64
    HistoricalAccessLatency  *LatencyTracker
    CacheHitRate            float64
    ArchiveFailures         map[string]int64
    DataLakeErrors          int64
    PrefetchHits            int64
    PrefetchMisses          int64
}

func (s *RawLedgerServer) UpdateDataSourceMetrics(source protocol.DataSource, latency time.Duration) {
    s.metrics.mu.Lock()
    defer s.metrics.mu.Unlock()
    
    switch source {
    case protocol.DataSourceLocal:
        s.metrics.LocalLedgersFetched++
    case protocol.DataSourceDataLake:
        s.metrics.HistoricalLedgersFetched++
        s.metrics.HistoricalAccessLatency.Record(latency)
    case protocol.DataSourceHybrid:
        s.metrics.HybridResponseCount++
    }
}
```

#### 4.2 Health Check Enhancements
```go
func (s *RawLedgerServer) HealthCheck() HealthStatus {
    status := HealthStatus{
        Timestamp: time.Now(),
        Service:   "stellar-live-source",
    }
    
    // Check RPC connectivity
    rpcHealth := s.checkRPCHealth()
    status.RPCStatus = rpcHealth
    
    // Check historical access capability
    if s.config.EnableHistoricalAccess {
        historicalHealth := s.checkHistoricalAccess()
        status.HistoricalAccessStatus = historicalHealth
        
        // Test archive connectivity
        for _, archive := range s.config.PreferredArchives {
            archiveStatus := s.testArchiveConnectivity(archive)
            status.ArchiveStatuses[archive] = archiveStatus
        }
    }
    
    // Performance metrics
    status.Metrics = map[string]interface{}{
        "cache_hit_rate":      s.metrics.CacheHitRate,
        "historical_latency_p99": s.metrics.HistoricalAccessLatency.P99(),
        "prefetch_efficiency": float64(s.metrics.PrefetchHits) / float64(s.metrics.PrefetchHits + s.metrics.PrefetchMisses),
        "local_vs_historical_ratio": float64(s.metrics.LocalLedgersFetched) / float64(s.metrics.LocalLedgersFetched + s.metrics.HistoricalLedgersFetched),
    }
    
    return status
}
```

### Phase 5: Configuration Migration (Week 5)

#### 5.1 Environment Variable Updates
```bash
# Core configuration
RPC_ENDPOINT=https://soroban-testnet.stellar.org
NETWORK_PASSPHRASE="Test SDF Network ; September 2015"

# Historical data access configuration
ENABLE_HISTORICAL_ACCESS=true
MAX_HISTORICAL_RANGE=10000000  # Maximum historical ledgers to fetch
PREFERRED_ARCHIVES=https://history.stellar.org/prd/core-live/core_live_001,https://history.stellar.org/prd/core-live/core_live_002
HISTORICAL_ACCESS_TIMEOUT=60s
CACHE_HISTORICAL_RESPONSES=true
HISTORICAL_CACHE_DURATION=1h
ENABLE_PREDICTIVE_PREFETCH=true
PREFETCH_CONCURRENCY=10
```

#### 5.2 Configuration Loading
```go
func (s *RawLedgerServer) loadConfiguration() error {
    // Load core configuration
    s.config.RPCEndpoint = os.Getenv("RPC_ENDPOINT")
    s.config.NetworkPassphrase = os.Getenv("NETWORK_PASSPHRASE")
    
    // Historical access configuration with defaults
    s.config.EnableHistoricalAccess = getBoolEnv("ENABLE_HISTORICAL_ACCESS", true) // Default to enabled
    
    // Load historical access configuration
    if s.config.EnableHistoricalAccess {
        s.config.MaxHistoricalRange = getUint32Env("MAX_HISTORICAL_RANGE", 1000000)
        s.config.PreferredArchives = getStringSliceEnv("PREFERRED_ARCHIVES", ",")
        s.config.HistoricalAccessTimeout = getDurationEnv("HISTORICAL_ACCESS_TIMEOUT", 30*time.Second)
        s.config.CacheHistoricalResponses = getBoolEnv("CACHE_HISTORICAL_RESPONSES", true)
        s.config.HistoricalCacheDuration = getDurationEnv("HISTORICAL_CACHE_DURATION", 1*time.Hour)
        
        s.logger.Info("Historical data access enabled",
            zap.Uint32("max_range", s.config.MaxHistoricalRange),
            zap.Strings("archives", s.config.PreferredArchives),
        )
    }
    
    return nil
}
```

## Testing Strategy

### Unit Tests
```go
func TestHistoricalDataAccess(t *testing.T) {
    tests := []struct {
        name           string
        requestLedger  uint32
        retentionStart uint32
        expectSource   protocol.DataSource
    }{
        {
            name:           "recent ledger from local",
            requestLedger:  1000,
            retentionStart: 900,
            expectSource:   protocol.DataSourceLocal,
        },
        {
            name:           "historical ledger from data lake",
            requestLedger:  500,
            retentionStart: 900,
            expectSource:   protocol.DataSourceDataLake,
        },
        {
            name:           "boundary case hybrid response",
            requestLedger:  890,
            retentionStart: 900,
            expectSource:   protocol.DataSourceHybrid,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

### Integration Tests
- Test against Protocol 23 testnet
- Verify seamless transition between local and historical data
- Validate caching behavior
- Test failover between archives
- Benchmark historical access performance

### Load Tests
- Simulate high-throughput historical queries
- Test cache efficiency under load
- Validate prefetch effectiveness
- Measure impact on RPC node performance

## Deployment Plan

### Phased Rollout

1. **Phase 1: Feature Flag Deployment**
   - Deploy with `ENABLE_HISTORICAL_ACCESS=false`
   - Validate existing functionality unchanged
   - Monitor baseline performance

2. **Phase 2: Limited Historical Access**
   - Enable for specific test streams
   - Monitor historical access patterns
   - Tune cache and prefetch parameters

3. **Phase 3: Full Production Rollout**
   - Enable historical access globally
   - Monitor all metrics
   - Optimize based on real usage patterns

### Rollback Strategy
```go
// Feature flag to disable historical access if needed
if s.config.DisableHistoricalAccess {
    // Fall back to local-only access
    req.Historical = nil
    return s.getLedgersLocalOnly(ctx, req)
}
```

## Benefits and Impact

### Immediate Benefits
1. **Unified Data Access**: Single service for all ledger data
2. **Simplified Architecture**: No need to manage separate historical services
3. **Improved Performance**: Caching and prefetching for frequently accessed historical data
4. **Better User Experience**: Seamless access to entire blockchain history

### Long-term Advantages
1. **Reduced Operational Complexity**: One service instead of two
2. **Cost Optimization**: Efficient caching reduces data lake access costs
3. **Enhanced Reliability**: Automatic failover between archives
4. **Future-proof**: Ready for future Protocol enhancements

## Migration Path for Consumers

### For Existing Consumers
No changes required! The service maintains backward compatibility while adding new capabilities.

### For New Features
Consumers can optionally use new metadata to optimize their processing:
```go
// Example consumer code
resp, err := client.StreamRawLedgers(ctx, &StreamLedgersRequest{
    StartLedger: 1000000,
})

// Optional: Check data source
if resp.SourceInfo != nil && resp.SourceInfo.Source == DataSourceDataLake {
    // Adjust processing for historical data if needed
    // e.g., different validation or caching strategies
}
```

## Success Criteria

1. **Functional Requirements**
   - ✓ Seamless access to historical ledgers via RPC
   - ✓ Transparent failover between data sources
   - ✓ Comprehensive monitoring and metrics
   - ✓ Backward compatibility maintained

2. **Performance Requirements**
   - Historical access latency < 1s (p99)
   - Cache hit rate > 80% for repeated queries
   - No degradation for local ledger access
   - Support for 1000+ concurrent historical queries

3. **Operational Requirements**
   - Zero downtime migration
   - Comprehensive health checks
   - Clear rollback procedures
   - Complete monitoring coverage

## Conclusion

Protocol 23's hybrid RPC capabilities represent a significant advancement in Stellar's data access architecture. By implementing these enhancements in `stellar-live-source`, we create a unified, high-performance service that provides seamless access to both recent and historical blockchain data. This eliminates the architectural complexity of managing separate data sources while improving performance through intelligent caching and prefetching.

The implementation plan ensures a smooth transition with zero downtime, comprehensive testing, and clear rollback procedures. The phased approach allows for careful validation at each step while maintaining full backward compatibility for existing consumers.

**Next Steps:**
1. Begin Phase 1 implementation with SDK updates
2. Set up Protocol 23 testnet environment
3. Start development of enhanced RPC client
4. Plan integration test scenarios

---

*Protocol 23 Implementation Plan for Stellar Live Source*  
*Enabling Unified Access to Real-time and Historical Blockchain Data*