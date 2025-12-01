# Bronze Resolver Library

The Bronze Resolver Library provides intent-based dataset resolution for DuckLake Bronze tables, enabling consumers to query historical Stellar ledger data through a simple, routing-aware API.

## Overview

The resolver sits between consumers and the DuckLake Bronze layer, handling:

- **Era Discovery**: Automatically routes requests to the correct era based on ledger ranges and protocol versions
- **Coverage Tracking**: Reports which ledger ranges are available and identifies gaps
- **Read Manifests**: Generates deterministic snapshots of dataset files for reproducible queries
- **Intent-Based Routing**: Supports multiple query strategies (latest, as-of-ledger, as-of-protocol, explicit)
- **Metadata Caching**: Reduces database load with TTL-based in-memory caching

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Consumer Application                  │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  Bronze Resolver Library                 │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────┐ │
│  │  Era Router    │  │  Coverage      │  │  Manifest │ │
│  │  (4 modes)     │  │  Tracker       │  │  Generator│ │
│  └────────────────┘  └────────────────┘  └───────────┘ │
│                    ┌──────────────┐                     │
│                    │    Cache     │                     │
│                    └──────────────┘                     │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│              DuckLake Bronze Metadata                    │
│  ┌────────────────┐  ┌────────────────┐                │
│  │  _meta_eras    │  │ _meta_lineage  │                │
│  │  (era bounds)  │  │ (partitions)   │                │
│  └────────────────┘  └────────────────┘                │
└─────────────────────────────────────────────────────────┘
```

## Installation

Add to your Go module:

```bash
go get github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/resolver
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/duckdb/duckdb-go/v2"
    "github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/resolver"
)

func main() {
    // 1. Connect to DuckDB with DuckLake catalog
    db, err := sql.Open("duckdb", "")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Load DuckLake extension and attach catalog
    _, _ = db.Exec("LOAD ducklake")
    _, _ = db.Exec(`
        ATTACH 'ducklake:postgres:postgresql:///ducklake_catalog?host=/path/to/sockets'
        AS mainnet
        (DATA_PATH '/path/to/data', METADATA_SCHEMA 'mainnet')
    `)

    // 2. Create resolver
    r, err := resolver.New(resolver.Config{
        DB:           db,
        CatalogName:  "mainnet",
        SchemaName:   "mainnet",
        CacheEnabled: true,
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. Resolve dataset with latest intent
    ctx := context.Background()
    result, err := r.ResolveDataset(ctx, "core.ledgers_row_v2", resolver.Intent{
        Mode:    resolver.IntentLatest,
        Network: "mainnet",
    })
    if err != nil {
        log.Fatal(err)
    }

    // 4. Use the resolved dataset
    log.Printf("Resolved to era: %s/%s", result.EraID, result.VersionLabel)
    log.Printf("Coverage: %d ranges, tail ledger: %d",
        len(result.Coverage.CommittedRanges), result.Coverage.TailLedger)
    log.Printf("PAS Verified: %v", result.PASVerified)
}
```

## Intent Modes

The resolver supports four intent modes for different query strategies:

### 1. Latest (Most Common)

Routes to the active era with the most recent data.

```go
result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
    Mode:    resolver.IntentLatest,
    Network: "mainnet",
})
```

**Use Case**: Real-time applications that always want the latest data.

### 2. As-of-Ledger (Historical Queries)

Routes to the era that contains a specific ledger number.

```go
ledger := uint32(50000000)
result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
    Mode:    resolver.IntentAsOfLedger,
    Network: "mainnet",
    Ledger:  &ledger,
})
```

**Use Case**: Historical analysis, backfilling, audit trails.

### 3. As-of-Protocol (Protocol-Specific)

Routes to the era for a specific Stellar protocol version.

```go
protocol := 21
result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
    Mode:     resolver.IntentAsOfProtocol,
    Network:  "mainnet",
    Protocol: &protocol,
})
```

**Use Case**: Testing against specific protocol versions, compatibility checks.

### 4. Explicit (Pinned Era)

Routes to a specific era by ID and version label.

```go
result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
    Mode:         resolver.IntentExplicit,
    Network:      "mainnet",
    EraID:        "p21_genesis",
    VersionLabel: "v1",
})
```

**Use Case**: Reproducible queries, testing, comparing era versions.

## Coverage Queries

Check data availability and identify gaps:

```go
// Get full coverage for a dataset/era
coverage, err := r.GetCoverage(ctx, "core.ledgers_row_v2", "p23_plus", "v1")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Committed Ranges: %d\n", len(coverage.CommittedRanges))
fmt.Printf("Tail Ledger: %d\n", coverage.TailLedger)
fmt.Printf("Total Rows: %d\n", coverage.TotalRows)

// Check for gaps
if len(coverage.Gaps) > 0 {
    fmt.Println("⚠️  Data gaps detected:")
    for _, gap := range coverage.Gaps {
        fmt.Printf("  Gap: ledgers %d - %d\n", gap.Start, gap.End)
    }
}

// Check if coverage is continuous
continuous, err := r.IsContinuous(ctx, "core.ledgers_row_v2", "p23_plus", "v1")
if !continuous {
    log.Println("Warning: Dataset has gaps in coverage")
}
```

## Read Manifests

Generate deterministic file lists for reproducible queries:

```go
// Request specific ledger range
result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
    Mode:    resolver.IntentLatest,
    Network: "mainnet",
    Range: &resolver.LedgerRange{
        Start: 50000000,
        End:   50001000,
    },
})

if result.Manifest != nil {
    fmt.Printf("Manifest files: %d\n", len(result.Manifest.Files))
    fmt.Printf("Total rows: %d\n", result.Manifest.TotalRows)
    fmt.Printf("Snapshot ID: %d\n", result.Manifest.SnapshotID)
    fmt.Printf("Checksum: %s\n", result.Manifest.Checksum)

    // Validate manifest integrity
    if r.ValidateManifest(result.Manifest) {
        fmt.Println("✓ Manifest checksum valid")
    }
}
```

## Era Management

List and inspect available eras:

```go
// Get all eras for a network
eras, err := r.GetEraMap(ctx, "mainnet")
if err != nil {
    log.Fatal(err)
}

for _, era := range eras {
    fmt.Printf("Era: %s/%s (%s)\n", era.EraID, era.VersionLabel, era.Status)
    fmt.Printf("  Ledgers: %d to %v\n", era.LedgerStart, era.LedgerEnd)
    fmt.Printf("  Protocol: %d\n", era.ProtocolVersion)
    fmt.Printf("  Created: %s\n", era.CreatedAt)
}

// Get active era
activeEra, err := r.GetActiveEra(ctx, "mainnet")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Active era: %s/%s\n", activeEra.EraID, activeEra.VersionLabel)
```

## Dataset Discovery

List available datasets:

```go
// List all datasets
datasets, err := r.ListDatasets(ctx, "mainnet", nil)
if err != nil {
    log.Fatal(err)
}

for _, ds := range datasets {
    fmt.Printf("Dataset: %s (v%d.%d) [%s]\n",
        ds.Name, ds.MajorVersion, ds.MinorVersion, ds.Tier)
}

// Filter by tier
bronzeOnly := "bronze"
datasets, err = r.ListDatasets(ctx, "mainnet", &bronzeOnly)
```

## Caching

The resolver includes built-in caching to reduce database load:

```go
// Create resolver with caching enabled (default TTL: 5 minutes)
r, err := resolver.New(resolver.Config{
    DB:           db,
    CatalogName:  "mainnet",
    SchemaName:   "mainnet",
    CacheEnabled: true,
    CacheTTL:     10 * time.Minute, // Optional: custom TTL
})

// Invalidate all cached data
r.InvalidateCache()

// Invalidate cache for specific network
r.InvalidateCacheForNetwork("mainnet")
```

**Cached Data**:
- Era maps (per network)
- Coverage information (per dataset/era/version)

**Cache Behavior**:
- Thread-safe with RWMutex
- Automatic expiration after TTL
- Transparent to callers (cache miss = database query)

## Error Handling

The resolver returns descriptive errors for common issues:

```go
result, err := r.ResolveDataset(ctx, "core.ledgers_row_v2", intent)
if err != nil {
    switch {
    case errors.Is(err, resolver.ErrNoActiveEra):
        log.Println("No active era found for network")
    case errors.Is(err, resolver.ErrLedgerNotFound):
        log.Println("Requested ledger not available in any era")
    case errors.Is(err, resolver.ErrDatasetNotFound):
        log.Println("Dataset does not exist in catalog")
    default:
        log.Printf("Resolver error: %v", err)
    }
}
```

## Performance Considerations

### Query Optimization

1. **Enable Caching**: Always enable caching for production deployments
   ```go
   CacheEnabled: true,
   CacheTTL:     5 * time.Minute,
   ```

2. **Reuse Resolvers**: Create one resolver per catalog and reuse it
   ```go
   // Good: Single resolver reused across requests
   resolver := createResolver()
   for request := range requests {
       result, _ := resolver.ResolveDataset(ctx, request.Dataset, request.Intent)
   }
   ```

3. **Batch Queries**: When possible, resolve multiple datasets in parallel
   ```go
   var wg sync.WaitGroup
   for _, dataset := range datasets {
       wg.Add(1)
       go func(ds string) {
           defer wg.Done()
           result, _ := resolver.ResolveDataset(ctx, ds, intent)
       }(dataset)
   }
   wg.Wait()
   ```

### Database Load

- Era queries are lightweight (small _meta_eras table)
- Coverage queries scan _meta_lineage (can be large for active eras)
- Caching reduces load by ~95% for read-heavy workloads

### Memory Usage

- Era maps: ~1KB per network
- Coverage data: ~100 bytes per partition
- Typical memory footprint: <10MB for large catalogs

## Integration Patterns

### Pattern 1: Real-Time Streaming

```go
// Initialize resolver once
resolver := createResolver()

// Continuously process latest data
for {
    result, err := resolver.ResolveDataset(ctx, "core.transactions_row_v2",
        resolver.Intent{
            Mode:    resolver.IntentLatest,
            Network: "mainnet",
        })

    // Query DuckDB using resolved era
    query := fmt.Sprintf(`
        SELECT * FROM %s.%s.transactions_row_v2
        WHERE ledger_sequence > ?
    `, result.CatalogName, result.Network)

    // Process results...
    time.Sleep(5 * time.Second)
}
```

### Pattern 2: Historical Backfill

```go
// Backfill data for specific ledger range
start := uint32(50000000)
end := uint32(51000000)

for ledger := start; ledger <= end; ledger += 1000 {
    result, err := resolver.ResolveDataset(ctx, "core.transactions_row_v2",
        resolver.Intent{
            Mode:    resolver.IntentAsOfLedger,
            Network: "mainnet",
            Ledger:  &ledger,
            Range: &resolver.LedgerRange{
                Start: ledger,
                End:   ledger + 999,
            },
        })

    // Use manifest to query specific files
    if result.Manifest != nil {
        for _, file := range result.Manifest.Files {
            // Process file...
        }
    }
}
```

### Pattern 3: Multi-Era Query

```go
// Query across multiple eras
eras, _ := resolver.GetEraMap(ctx, "mainnet")

var allResults []Result
for _, era := range eras {
    result, err := resolver.ResolveDataset(ctx, "core.ledgers_row_v2",
        resolver.Intent{
            Mode:         resolver.IntentExplicit,
            Network:      "mainnet",
            EraID:        era.EraID,
            VersionLabel: era.VersionLabel,
        })

    // Query this era's data
    // Aggregate results...
    allResults = append(allResults, processEra(result))
}
```

## Testing

The resolver includes a CLI tool for testing and debugging:

```bash
# Build the resolver-test tool
cd go
env GOWORK=off go build -o ../resolver-test ./cmd/resolver-test

# Test latest intent
./resolver-test --mode latest --dataset core.ledgers_row_v2

# Test as-of-ledger intent
./resolver-test --mode as_of_ledger --ledger 50000000 --dataset core.transactions_row_v2

# Test explicit era
./resolver-test --mode explicit --era p23_plus --dataset core.operations_row_v1

# Custom catalog path
./resolver-test \
  --catalog "ducklake:postgres:postgresql:///my_catalog?host=/tmp/sockets" \
  --data "/path/to/data" \
  --catalog-name "my_catalog" \
  --schema "testnet"
```

## API Reference

### Types

```go
// Config for creating a new resolver
type Config struct {
    DB           *sql.DB       // DuckDB connection
    CatalogName  string        // DuckLake catalog name
    SchemaName   string        // Metadata schema name
    CacheEnabled bool          // Enable in-memory caching
    CacheTTL     time.Duration // Cache TTL (default: 5min)
}

// Intent specifies how to resolve a dataset
type Intent struct {
    Mode         IntentMode     // Resolution strategy
    Network      string         // Network name (mainnet, testnet)
    Ledger       *uint32        // For IntentAsOfLedger
    Protocol     *int           // For IntentAsOfProtocol
    EraID        string         // For IntentExplicit
    VersionLabel string         // For IntentExplicit
    Range        *LedgerRange   // Optional: generate read manifest
    StrictPAS    bool           // Require PAS verification
}

// ResolvedDataset contains resolution results
type ResolvedDataset struct {
    Dataset       string        // Dataset name
    Network       string        // Network name
    EraID         string        // Resolved era ID
    VersionLabel  string        // Resolved version label
    SchemaHash    string        // Dataset schema hash
    Compatibility string        // Compatibility mode
    Coverage      Coverage      // Data coverage information
    Manifest      *ReadManifest // File manifest (if Range specified)
    PASVerified   bool          // PAS verification status
}

// Coverage describes data availability
type Coverage struct {
    CommittedRanges []LedgerRange // Available ledger ranges
    TailLedger      uint32        // Highest available ledger
    Gaps            []LedgerRange // Missing ledger ranges
    LastVerified    uint32        // Last PAS-verified ledger
    TotalRows       int64         // Total row count
}

// Era represents a time period in the catalog
type Era struct {
    EraID           string     // Era identifier
    VersionLabel    string     // Era version
    Network         string     // Network name
    LedgerStart     uint32     // First ledger in era
    LedgerEnd       *uint32    // Last ledger (nil if active)
    ProtocolVersion int        // Stellar protocol version
    Status          string     // active, frozen, deprecated
    CreatedAt       time.Time  // Era creation time
}
```

### Core Methods

```go
// Create new resolver
func New(config Config) (*Resolver, error)

// Resolve dataset to specific era
func (r *Resolver) ResolveDataset(ctx context.Context, dataset string, intent Intent) (*ResolvedDataset, error)

// Get coverage for dataset/era
func (r *Resolver) GetCoverage(ctx context.Context, dataset, eraID, versionLabel string) (*Coverage, error)

// Get coverage for specific range
func (r *Resolver) GetCoverageForRange(ctx context.Context, dataset, eraID, versionLabel string, ledgerRange LedgerRange) (*Coverage, error)

// List all eras for network
func (r *Resolver) GetEraMap(ctx context.Context, network string) ([]Era, error)

// Get active era for network
func (r *Resolver) GetActiveEra(ctx context.Context, network string) (*Era, error)

// List available datasets
func (r *Resolver) ListDatasets(ctx context.Context, network string, tier *string) ([]Dataset, error)

// Generate read manifest for range
func (r *Resolver) GetReadManifest(ctx context.Context, dataset, eraID, versionLabel string, ledgerRange LedgerRange) (*ReadManifest, error)

// Validate manifest checksum
func (r *Resolver) ValidateManifest(manifest *ReadManifest) bool

// Cache management
func (r *Resolver) InvalidateCache()
func (r *Resolver) InvalidateCacheForNetwork(network string)
```

## Dependencies

- **github.com/duckdb/duckdb-go/v2**: DuckDB driver for Go
- **DuckLake Extension**: DuckDB extension for PostgreSQL catalog integration

## License

Part of the TTP Processor Demo project.

## See Also

- [CYCLE5_BRONZE_RESOLVER_LIBRARY.md](../../CYCLE5_BRONZE_RESOLVER_LIBRARY.md) - Design specification
- [example_test.go](./example_test.go) - Usage examples
- [cmd/resolver-test](../cmd/resolver-test) - CLI testing tool
