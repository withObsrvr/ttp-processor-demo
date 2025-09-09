package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ComponentLogger provides structured logging for the contract data processor
type ComponentLogger struct {
	logger    zerolog.Logger
	component string
	version   string
}

// NewComponentLogger creates a new component logger
func NewComponentLogger(component, version string) *ComponentLogger {
	// Set up pretty console logging for development
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	logger := zerolog.New(output).
		With().
		Timestamp().
		Str("component", component).
		Str("version", version).
		Logger()

	// Set global log level
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if os.Getenv("DEBUG") == "true" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	return &ComponentLogger{
		logger:    logger,
		component: component,
		version:   version,
	}
}

// Info returns an info level event
func (cl *ComponentLogger) Info() *zerolog.Event {
	return cl.logger.Info()
}

// Debug returns a debug level event
func (cl *ComponentLogger) Debug() *zerolog.Event {
	return cl.logger.Debug()
}

// Warn returns a warn level event
func (cl *ComponentLogger) Warn() *zerolog.Event {
	return cl.logger.Warn()
}

// Error returns an error level event
func (cl *ComponentLogger) Error() *zerolog.Event {
	return cl.logger.Error()
}

// Fatal returns a fatal level event
func (cl *ComponentLogger) Fatal() *zerolog.Event {
	return cl.logger.Fatal()
}

// With creates a child logger with additional context
func (cl *ComponentLogger) With() zerolog.Context {
	return cl.logger.With()
}

// StartupConfig holds configuration for startup logging
type StartupConfig struct {
	ServiceType       string
	NetworkPassphrase string
	SourceEndpoint    string
	GRPCPort          int
	ArrowPort         int
	HealthPort        int
	FlowctlEnabled    bool
	BatchSize         int
}

// LogStartup logs startup configuration
func (cl *ComponentLogger) LogStartup(config StartupConfig) {
	cl.Info().
		Str("service_type", config.ServiceType).
		Str("network", config.NetworkPassphrase).
		Str("source_endpoint", config.SourceEndpoint).
		Int("grpc_port", config.GRPCPort).
		Int("arrow_port", config.ArrowPort).
		Int("health_port", config.HealthPort).
		Bool("flowctl_enabled", config.FlowctlEnabled).
		Int("batch_size", config.BatchSize).
		Msg("Starting contract data processor")
}

// ProcessingMetrics holds metrics for processing operations
type ProcessingMetrics struct {
	LedgerSequence      uint32
	ContractsFound      int
	RecordsProcessed    int
	ProcessingDuration  time.Duration
	BytesProcessed      int64
}

// LogProcessing logs processing metrics
func (cl *ComponentLogger) LogProcessing(metrics ProcessingMetrics) {
	rate := float64(metrics.RecordsProcessed) / metrics.ProcessingDuration.Seconds()
	
	cl.Info().
		Uint32("ledger_sequence", metrics.LedgerSequence).
		Int("contracts_found", metrics.ContractsFound).
		Int("records_processed", metrics.RecordsProcessed).
		Dur("processing_duration", metrics.ProcessingDuration).
		Float64("records_per_second", rate).
		Int64("bytes_processed", metrics.BytesProcessed).
		Msg("Processed ledger")
}

// StreamingMetrics holds metrics for Arrow Flight streaming
type StreamingMetrics struct {
	RecordsStreamed   int64
	BytesStreamed     int64
	ClientsConnected  int
	StreamingDuration time.Duration
}

// LogStreaming logs streaming metrics
func (cl *ComponentLogger) LogStreaming(metrics StreamingMetrics) {
	rate := float64(metrics.RecordsStreamed) / metrics.StreamingDuration.Seconds()
	
	cl.Info().
		Int64("records_streamed", metrics.RecordsStreamed).
		Int64("bytes_streamed", metrics.BytesStreamed).
		Int("clients_connected", metrics.ClientsConnected).
		Dur("streaming_duration", metrics.StreamingDuration).
		Float64("records_per_second", rate).
		Msg("Streaming metrics")
}

// GetLogger returns the underlying zerolog logger
func (cl *ComponentLogger) GetLogger() zerolog.Logger {
	return cl.logger
}

// SetLevel sets the logging level
func SetLevel(level string) {
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		log.Warn().Str("level", level).Msg("Unknown log level, defaulting to info")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}