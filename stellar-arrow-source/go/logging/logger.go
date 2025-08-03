package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ComponentLogger provides structured logging for Arrow services
type ComponentLogger struct {
	logger zerolog.Logger
}

// NewComponentLogger creates a component-specific logger with consistent context
func NewComponentLogger(componentName, version string) *ComponentLogger {
	// Configure zerolog globally
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

	// Create component-specific logger
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

// LogStartup logs service startup with structured fields
func (cl *ComponentLogger) LogStartup(config StartupConfig) {
	cl.Info().
		Bool("flowctl_enabled", config.FlowCtlEnabled).
		Str("source_type", config.SourceType).
		Str("backend_type", config.BackendType).
		Str("network", config.NetworkPassphrase).
		Int("batch_size", config.BatchSize).
		Int("port", config.Port).
		Msg("Starting Arrow service")
}

// LogArrowProcessing logs Arrow-specific processing operations
func (cl *ComponentLogger) LogArrowProcessing(operation string, batchSize int, duration time.Duration) {
	cl.Info().
		Str("operation", operation).
		Int("batch_size", batchSize).
		Dur("processing_time", duration).
		Str("protocol", "arrow-flight").
		Msg("Arrow processing completed")
}

// LogArrowMemory logs Arrow memory allocator statistics
func (cl *ComponentLogger) LogArrowMemory(allocatedBytes, reservedBytes int64) {
	cl.Debug().
		Int64("allocated_bytes", allocatedBytes).
		Int64("reserved_bytes", reservedBytes).
		Str("allocator_type", "go_allocator").
		Msg("Arrow memory statistics")
}

// LogFlightConnection logs Arrow Flight connection events
func (cl *ComponentLogger) LogFlightConnection(operation string, clientAddr string, streamID string) {
	cl.Info().
		Str("operation", operation).
		Str("client_addr", clientAddr).
		Str("stream_id", streamID).
		Str("protocol", "arrow-flight").
		Msg("Arrow Flight connection event")
}

// LogSchemaValidation logs Arrow schema validation results
func (cl *ComponentLogger) LogSchemaValidation(schemaName string, compatible bool, version string) {
	cl.Info().
		Str("operation", "schema_validation").
		Str("schema_name", schemaName).
		Bool("compatible", compatible).
		Str("schema_version", version).
		Msg("Arrow schema validation completed")
}

// LogPerformanceMetrics logs performance metrics with structured fields
func (cl *ComponentLogger) LogPerformanceMetrics(metrics PerformanceMetrics) {
	cl.Info().
		Str("operation", "performance_metrics").
		Float64("records_per_second", metrics.RecordsPerSecond).
		Int64("total_records", metrics.TotalRecords).
		Dur("avg_processing_time", metrics.AvgProcessingTime).
		Int64("memory_usage_bytes", metrics.MemoryUsageBytes).
		Float64("memory_efficiency", metrics.MemoryEfficiency).
		Msg("Performance metrics collected")
}

// StartupConfig represents service startup configuration
type StartupConfig struct {
	FlowCtlEnabled    bool
	SourceType        string
	BackendType       string
	NetworkPassphrase string
	BatchSize         int
	Port              int
}

// PerformanceMetrics represents performance measurement data
type PerformanceMetrics struct {
	RecordsPerSecond   float64
	TotalRecords       int64
	AvgProcessingTime  time.Duration
	MemoryUsageBytes   int64
	MemoryEfficiency   float64
}

// NewProductionLogger creates a production-optimized logger
func NewProductionLogger(serviceName, version, hostname string) *ComponentLogger {
	// Production configuration
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Add service-level context
	logger := log.With().
		Str("service", serviceName).
		Str("version", version).
		Str("hostname", hostname).
		Logger()

	return &ComponentLogger{
		logger: logger,
	}
}