# Structured Logging Style Guide
## Professional Go Application Logging Standards

**Version:** 1.0  
**Date:** August 3, 2025  
**Based on:** obsrvr-stellar-components logging implementation

## Overview

This guide defines a consistent, structured logging approach for Go applications that provides excellent observability, debugging capabilities, and operational insights. The style is based on the zerolog library and follows industry best practices for production systems.

## Library Choice: Zerolog

**Why Zerolog:**
- Zero allocation logging for high performance
- Structured JSON output for machine parsing
- Human-readable console output for development
- Excellent performance characteristics
- Rich context support with typed fields

**Installation:**
```bash
go get github.com/rs/zerolog
go get github.com/rs/zerolog/log
```

## Core Logging Principles

### 1. Structured Fields Over String Formatting

**❌ Bad - String interpolation:**
```go
log.Printf("Starting %s component with batch size %d on port %d", componentName, batchSize, port)
```

**✅ Good - Structured fields:**
```go
log.Info().
    Str("component", componentName).
    Int("batch_size", batchSize).
    Int("port", port).
    Msg("Starting component")
```

### 2. Consistent Field Naming

Use snake_case for all field names and maintain consistency across the application:

```go
// Standard field names
Str("component", "stellar-arrow-source")
Str("source_type", "rpc")
Str("backend_type", "rpc")
Int("batch_size", 1000)
Str("endpoint", "https://soroban-testnet.stellar.org")
Str("network", "Test SDF Network ; September 2015")
Bool("flowctl_enabled", false)
Str("version", "v1.0.0")
```

### 3. Meaningful Log Levels

**INFO (INF)** - Normal operational messages:
```go
log.Info().
    Str("component", "stellar-arrow-source").
    Bool("flowctl_enabled", false).
    Str("source_type", "rpc").
    Str("version", "v1.0.0").
    Msg("Starting stellar-arrow-source")
```

**WARN (WRN)** - Recoverable issues or important notices:
```go
log.Warn().
    Err(err).
    Msg("Backend health check failed, continuing anyway")
```

**ERROR (ERR)** - Errors that affect functionality:
```go
log.Error().
    Err(err).
    Uint32("sequence", sequence).
    Msg("Failed to get ledger record")
```

**DEBUG (DBG)** - Detailed diagnostic information:
```go
log.Debug().
    Str("operation", "ledger_processing").
    Uint32("sequence", sequence).
    Int("batch_size", batchSize).
    Msg("Processing ledger batch")
```

## Application Setup

### Basic Logger Configuration

```go
package main

import (
    "os"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

func init() {
    // Configure zerolog
    zerolog.TimeFieldFormat = time.RFC3339
    
    // Set log level from environment
    logLevel := os.Getenv("LOG_LEVEL")
    switch logLevel {
    case "debug":
        zerolog.SetGlobalLevel(zerolog.DebugLevel)
    case "info":
        zerolog.SetGlobalLevel(zerolog.InfoLevel)
    case "warn":
        zerolog.SetGlobalLevel(zerolog.WarnLevel)
    case "error":
        zerolog.SetGlobalLevel(zerolog.ErrorLevel)
    default:
        zerolog.SetGlobalLevel(zerolog.InfoLevel)
    }
    
    // Console output for development
    if os.Getenv("ENVIRONMENT") != "production" {
        log.Logger = log.Output(zerolog.ConsoleWriter{
            Out:        os.Stderr,
            TimeFormat: time.RFC3339,
            NoColor:    false,
        })
    }
}
```

### Component-Level Logger

Create a component-specific logger with consistent context:

```go
package main

import (
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

type ComponentLogger struct {
    logger zerolog.Logger
}

func NewComponentLogger(componentName, version string) *ComponentLogger {
    logger := log.With().
        Str("component", componentName).
        Str("version", version).
        Logger()
    
    return &ComponentLogger{
        logger: logger,
    }
}

func (cl *ComponentLogger) Info() *zerolog.Event {
    return cl.logger.Info()
}

func (cl *ComponentLogger) Error() *zerolog.Event {
    return cl.logger.Error()
}

func (cl *ComponentLogger) Warn() *zerolog.Event {
    return cl.logger.Warn()
}

func (cl *ComponentLogger) Debug() *zerolog.Event {
    return cl.logger.Debug()
}

// Usage
logger := NewComponentLogger("stellar-arrow-source", "v1.0.0")

logger.Info().
    Bool("flowctl_enabled", false).
    Str("source_type", "rpc").
    Msg("Starting stellar-arrow-source")
```

## Standard Logging Patterns

### 1. Application Startup

**Pattern:** Log component initialization with key configuration:

```go
func main() {
    logger := NewComponentLogger("stellar-arrow-source", "v1.0.0")
    
    logger.Info().
        Bool("flowctl_enabled", config.FlowCtlEnabled).
        Str("source_type", config.SourceType).
        Str("backend_type", config.BackendType).
        Str("network", config.NetworkPassphrase).
        Int("batch_size", config.BatchSize).
        Msg("Starting stellar-arrow-source")
}
```

### 2. Service Initialization

**Pattern:** Log each major service/component as it starts:

```go
func initializeBackend(config *Config) error {
    log.Info().Msg("Creating Stellar backend client")
    
    log.Info().
        Str("endpoint", config.RPCEndpoint).
        Msg("Initializing RPC backend")
    
    // ... initialization logic ...
    
    log.Info().Msg("RPC backend initialized successfully")
    
    log.Info().
        Str("backend_type", config.BackendType).
        Int("batch_size", config.BatchSize).
        Str("network", config.NetworkPassphrase).
        Msg("Stellar backend client created successfully")
    
    return nil
}
```

### 3. Processing Operations

**Pattern:** Log processing start, progress, and completion:

```go
func startProcessing(config *Config) {
    log.Info().
        Int("batch_size", config.BatchSize).
        Uint32("start_ledger", config.StartLedger).
        Uint32("end_ledger", config.EndLedger).
        Str("source_type", config.SourceType).
        Msg("Starting ledger processing")
    
    log.Info().
        Str("backend_type", config.BackendType).
        Msg("Starting backend stream processing")
    
    log.Info().
        Uint32("start_seq", config.StartLedger).
        Msg("Starting indefinite stream processing")
}
```

### 4. Server Startup

**Pattern:** Log server/listener startup with key details:

```go
func startHealthServer(port int, metricsEnabled bool) {
    log.Info().
        Bool("metrics_enabled", metricsEnabled).
        Int("port", port).
        Msg("Starting health server")
}

func startFlightServer(port int) {
    log.Info().
        Int("port", port).
        Msg("Starting Arrow Flight server")
}
```

### 5. Error Handling

**Pattern:** Include error details and context for debugging:

```go
func processLedger(sequence uint32) error {
    err := getLedgerRecord(sequence)
    if err != nil {
        log.Error().
            Err(err).
            Uint32("sequence", sequence).
            Msg("Failed to get ledger record")
        return err
    }
    return nil
}
```

### 6. Warnings with Context

**Pattern:** Include why the warning occurred and what action is being taken:

```go
func checkBackendHealth() {
    if err := backend.HealthCheck(); err != nil {
        log.Warn().
            Err(err).
            Msg("Backend health check failed, continuing anyway")
    }
}
```

### 7. Graceful Shutdown

**Pattern:** Log shutdown process with final statistics:

```go
func shutdown() {
    log.Info().Msg("Shutting down stellar-arrow-source")
    
    log.Info().Msg("Stopping stellar source service")
    
    log.Info().Msg("Closing Stellar backend client")
    
    log.Info().Msg("Closing Stellar backend manager")
    
    // Log final statistics
    log.Info().
        Uint64("total_ledgers", stats.TotalLedgers).
        Uint64("total_records", stats.TotalRecords).
        Uint64("total_bytes", stats.TotalBytes).
        Int("errors", stats.ErrorCount).
        Msg("Stellar backend client closed")
    
    log.Info().Msg("Stellar source service stopped")
    
    log.Info().Msg("Shutdown complete")
}
```

## Standard Field Names

Use these consistent field names across all applications:

### Core Application Fields
```go
Str("component", "service-name")     // Component/service name
Str("version", "v1.0.0")            // Application version
Str("environment", "production")     // Environment (dev/staging/prod)
```

### Configuration Fields
```go
Bool("flowctl_enabled", true)        // Feature flags
Str("source_type", "rpc")           // Type configurations
Str("backend_type", "rpc")          // Backend configurations
Int("batch_size", 1000)             // Size/limit configurations
Int("port", 8815)                   // Port numbers
Str("endpoint", "localhost:8080")   // Connection endpoints
```

### Processing Fields
```go
Uint32("sequence", 123456)          // Sequence numbers
Uint64("total_ledgers", 1000)       // Counts and totals
Uint64("total_bytes", 1024000)      // Size measurements
Int("errors", 5)                    // Error counts
Dur("duration", elapsed)            // Time durations
```

### Network/Connection Fields
```go
Str("network", "testnet")           // Network identifiers
Str("protocol", "arrow-flight")     // Protocol types
Str("address", "localhost:8080")    // Network addresses
Bool("tls_enabled", true)           // Security flags
```

## Performance Considerations

### 1. Zero Allocation Logging

Zerolog is designed for zero allocations. Use the fluent API correctly:

```go
// ✅ Good - Zero allocations
log.Info().
    Str("component", componentName).
    Int("port", port).
    Msg("Server started")

// ❌ Bad - Creates allocations
log.Info().Msgf("Server %s started on port %d", componentName, port)
```

### 2. Conditional Expensive Operations

Use lazy evaluation for expensive operations:

```go
// ✅ Good - Only computed if debug level is enabled
log.Debug().
    Interface("config", func() interface{} {
        if log.Debug().Enabled() {
            return config.ToMap() // Expensive operation
        }
        return nil
    }).
    Msg("Full configuration")

// Better - Use the enabled check
if log.Debug().Enabled() {
    log.Debug().
        Interface("config", config.ToMap()).
        Msg("Full configuration")
}
```

### 3. Reuse Logger Instances

Create component-specific loggers and reuse them:

```go
type Service struct {
    logger zerolog.Logger
}

func NewService(name string) *Service {
    return &Service{
        logger: log.With().Str("service", name).Logger(),
    }
}

func (s *Service) Process() {
    s.logger.Info().Msg("Processing started")
}
```

## Error Logging Best Practices

### 1. Include Error Context

Always include the original error and relevant context:

```go
func connectToDatabase(dsn string) error {
    conn, err := sql.Open("postgres", dsn)
    if err != nil {
        log.Error().
            Err(err).
            Str("dsn", dsn).
            Str("driver", "postgres").
            Msg("Failed to connect to database")
        return fmt.Errorf("database connection failed: %w", err)
    }
    return nil
}
```

### 2. Log at the Right Level

Don't log the same error at multiple levels:

```go
// ✅ Good - Log once at the appropriate level
func processRecord(record Record) error {
    if err := validate(record); err != nil {
        // Log at debug level for validation errors (common)
        log.Debug().
            Err(err).
            Str("record_id", record.ID).
            Msg("Record validation failed")
        return err
    }
    
    if err := save(record); err != nil {
        // Log at error level for save failures (serious)
        log.Error().
            Err(err).
            Str("record_id", record.ID).
            Msg("Failed to save record")
        return err
    }
    
    return nil
}
```

### 3. Include Recovery Actions

When logging errors, include what the system will do:

```go
func retryableOperation() error {
    err := riskyOperation()
    if err != nil {
        log.Warn().
            Err(err).
            Int("retry_attempt", attempt).
            Int("max_retries", maxRetries).
            Dur("retry_delay", retryDelay).
            Msg("Operation failed, will retry")
        return err
    }
    return nil
}
```

## Development vs Production Configuration

### Development Configuration

```go
func configureDevLogging() {
    // Human-readable console output
    log.Logger = log.Output(zerolog.ConsoleWriter{
        Out:        os.Stderr,
        TimeFormat: time.RFC3339,
        NoColor:    false,
    })
    
    // Enable debug logging
    zerolog.SetGlobalLevel(zerolog.DebugLevel)
}
```

### Production Configuration

```go
func configureProdLogging() {
    // JSON output for log aggregation
    zerolog.TimeFieldFormat = time.RFC3339
    
    // Info level by default
    zerolog.SetGlobalLevel(zerolog.InfoLevel)
    
    // Add hostname and service info
    log.Logger = log.With().
        Str("hostname", getHostname()).
        Str("service", serviceName).
        Str("version", version).
        Logger()
}
```

## Integration with Monitoring

### Structured Logs for Metrics

Use consistent fields that can be parsed by monitoring systems:

```go
// This log entry can be parsed to create metrics
log.Info().
    Str("operation", "ledger_processed").
    Uint32("ledger_sequence", sequence).
    Dur("processing_time", elapsed).
    Int("records_count", recordCount).
    Uint64("bytes_processed", byteCount).
    Msg("Ledger processing completed")
```

### Health Check Logging

```go
func healthCheck() bool {
    healthy := checkDependencies()
    
    log.Info().
        Bool("healthy", healthy).
        Str("check_type", "health").
        Dur("check_duration", elapsed).
        Interface("dependencies", depStatus).
        Msg("Health check completed")
    
    return healthy
}
```

## Common Patterns and Examples

### HTTP Request Logging

```go
func logHTTPRequest(r *http.Request, statusCode int, duration time.Duration) {
    log.Info().
        Str("method", r.Method).
        Str("path", r.URL.Path).
        Str("remote_addr", r.RemoteAddr).
        Str("user_agent", r.UserAgent()).
        Int("status_code", statusCode).
        Dur("duration", duration).
        Msg("HTTP request processed")
}
```

### Database Operation Logging

```go
func logDBOperation(operation string, table string, rowsAffected int64, duration time.Duration) {
    log.Debug().
        Str("operation", operation).
        Str("table", table).
        Int64("rows_affected", rowsAffected).
        Dur("duration", duration).
        Msg("Database operation completed")
}
```

### Background Job Logging

```go
func processJob(job Job) {
    start := time.Now()
    
    log.Info().
        Str("job_id", job.ID).
        Str("job_type", job.Type).
        Msg("Starting job processing")
    
    err := job.Execute()
    duration := time.Since(start)
    
    if err != nil {
        log.Error().
            Err(err).
            Str("job_id", job.ID).
            Str("job_type", job.Type).
            Dur("duration", duration).
            Msg("Job processing failed")
    } else {
        log.Info().
            Str("job_id", job.ID).
            Str("job_type", job.Type).
            Dur("duration", duration).
            Msg("Job processing completed")
    }
}
```

## Testing Considerations

### Log Testing

```go
func TestLogging(t *testing.T) {
    // Capture logs for testing
    var buf bytes.Buffer
    logger := zerolog.New(&buf)
    
    logger.Info().
        Str("component", "test").
        Msg("Test message")
    
    // Parse and verify log output
    var logEntry map[string]interface{}
    err := json.Unmarshal(buf.Bytes(), &logEntry)
    assert.NoError(t, err)
    assert.Equal(t, "test", logEntry["component"])
    assert.Equal(t, "Test message", logEntry["message"])
}
```

### Mock Logger for Tests

```go
type MockLogger struct {
    entries []LogEntry
}

func (m *MockLogger) Info() *zerolog.Event {
    // Implementation that captures log entries
    // for verification in tests
}
```

## Performance Benchmarks

The zerolog library provides excellent performance:

```go
// Benchmark results (typical)
BenchmarkZerolog-8         5000000    340 ns/op     0 B/op    0 allocs/op
BenchmarkStdLib-8          1000000   1520 ns/op   240 B/op    3 allocs/op
```

## Summary

This structured logging approach provides:

1. **Consistent Format:** All logs follow the same pattern
2. **Machine Readable:** Structured fields for monitoring/alerting
3. **Human Readable:** Clear messages with meaningful context
4. **High Performance:** Zero allocation logging
5. **Debuggable:** Rich context for troubleshooting
6. **Monitorable:** Easy integration with metrics and alerting

**Key Takeaways:**
- Use structured fields instead of string formatting
- Maintain consistent field naming across the application
- Include relevant context in every log entry
- Use appropriate log levels for different scenarios
- Configure differently for development vs production
- Design logs to support both debugging and monitoring

This approach will provide excellent observability and debugging capabilities for any Go application.