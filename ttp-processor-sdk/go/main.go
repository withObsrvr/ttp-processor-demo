package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/processor"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"github.com/stellar/go-stellar-sdk/xdr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	// Load configuration from environment
	config, err := LoadTTPConfig(logger)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := config.Validate(logger); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}

	// Log Go 1.25 runtime information
	logger.Info("Token Transfer Processor starting",
		zap.String("network", config.NetworkPassphrase),
		zap.String("network_name", config.NetworkName),
		zap.String("go_version", runtime.Version()),
		zap.Int("gomaxprocs", runtime.GOMAXPROCS(0)),
		zap.Int("num_cpu", runtime.NumCPU()))

	// Create processor
	proc, err := processor.New(config.ProcessorConfig)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Wrap flowctl-sdk metrics with TTP-specific helpers
	metrics := NewTTPMetrics(proc.Metrics())
	logger.Info("TTP metrics initialized",
		zap.String("network", config.NetworkName))

	// Enable Flight Recorder debug endpoint if requested (Go 1.25 feature)
	if parseBool(getEnv("ENABLE_FLIGHT_RECORDER", "false")) {
		// Note: We can't easily access the health server's mux from flowctl-sdk
		// This would require flowctl-sdk to expose a way to register additional handlers
		// For now, document that this feature requires flowctl-sdk enhancement
		logger.Info("Flight Recorder requested but requires flowctl-sdk enhancement to register handlers")
	}

	// Create stellar token transfer processor
	ttpProcessor := token_transfer.NewEventsProcessor(config.NetworkPassphrase)

	// Track filtering metrics
	var totalExtracted, totalFiltered, totalPassed int64

	// Track error metrics
	errorCollector := NewErrorCollector()

	// Create batch processor if enabled
	var batchProcessor *BatchProcessor
	if config.Batch.Enabled {
		batchProcessor = NewBatchProcessor(
			config.Batch,
			config.Filter,
			ttpProcessor,
			logger,
			config.ProcessorConfig.ID,
			config.ProcessorConfig.Version,
			metrics,
			config.NetworkName,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := batchProcessor.Stop(ctx); err != nil {
				logger.Error("Error stopping batch processor", zap.Error(err))
			}
		}()
	}

	// Register processor handler
	err = proc.OnProcess(
		func(ctx context.Context, event *flowctlv1.Event) (*flowctlv1.Event, error) {
			// Only process stellar ledger events
			if event.Type != "stellar.ledger.v1" {
				return nil, nil // Skip non-ledger events
			}

			// Parse the RawLedger from payload
			var rawLedger stellarv1.RawLedger
			if err := proto.Unmarshal(event.Payload, &rawLedger); err != nil {
				logger.Error("Failed to unmarshal RawLedger", zap.Error(err))
				return nil, fmt.Errorf("failed to unmarshal ledger: %w", err)
			}

			// If batch processing is enabled, add to batch
			if batchProcessor != nil {
				return batchProcessor.AddLedger(ctx, &rawLedger)
			}

			// Decode XDR
			var ledgerCloseMeta xdr.LedgerCloseMeta
			if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &ledgerCloseMeta); err != nil {
				errorCollector.AddError(
					SeverityFatal,
					"Failed to unmarshal XDR",
					err,
					map[string]string{
						"ledger_sequence": fmt.Sprintf("%d", rawLedger.Sequence),
						"stage":           "xdr_decode",
					},
				)
				logger.Error("Failed to unmarshal XDR", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
			}

			// Extract token transfer events using stellar's processor
			ttpEvents, err := ttpProcessor.EventsFromLedger(ledgerCloseMeta)
			if err != nil {
				errorCollector.AddError(
					SeverityFatal,
					"Failed to extract token transfer events",
					err,
					map[string]string{
						"ledger_sequence": fmt.Sprintf("%d", rawLedger.Sequence),
						"stage":           "event_extraction",
					},
				)
				logger.Error("Failed to extract token transfer events", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to extract events: %w", err)
			}

			// Record ledger processed
			metrics.RecordLedgerProcessed(config.NetworkName, false)

			// Track total extracted
			totalExtracted += int64(len(ttpEvents))

			// If no events found, skip
			if len(ttpEvents) == 0 {
				logger.Debug("No token transfer events in ledger", zap.Uint32("sequence", rawLedger.Sequence))
				return nil, nil
			}

			// Apply filtering and send individual events
			filteredCount := 0
			outputEvents := make([]*flowctlv1.Event, 0, len(ttpEvents))

			for eventIdx, ttpEvent := range ttpEvents {
				// Record event extracted
				eventType := getEventType(ttpEvent)
				metrics.RecordEventExtracted(eventType, config.NetworkName, false)

				// Apply filter
				if !config.Filter.ShouldIncludeEvent(ttpEvent, logger) {
					filteredCount++
					// Determine filter reason
					filterReason := "event_type"
					if config.Filter.MinAmount != nil && getEventAmount(ttpEvent) < *config.Filter.MinAmount {
						filterReason = "min_amount"
					}
					metrics.RecordEventFiltered(eventType, filterReason, config.NetworkName)
					continue
				}

				// Marshal Stellar proto directly into payload bytes
				payloadBytes, err := proto.Marshal(ttpEvent)
				if err != nil {
					logger.Error("Failed to marshal Stellar event", zap.Error(err))
					continue
				}

				// Create output event with Stellar proto in payload
				outputEvent := &flowctlv1.Event{
					Id:                fmt.Sprintf("token-transfer-%d-%d", rawLedger.Sequence, eventIdx),
					Type:              "stellar.token.transfer.v1",
					Payload:           payloadBytes, // Raw Stellar protobuf bytes
					Metadata:          make(map[string]string),
					Timestamp:         timestamppb.Now(),
					SourceComponentId: config.ProcessorConfig.ID,
					ContentType:       "application/protobuf; message=token_transfer.TokenTransferEvent",
					StellarCursor: &flowctlv1.StellarCursor{
						LedgerSequence: uint64(rawLedger.Sequence),
						IndexInLedger:  uint32(eventIdx),
					},
				}

				// Copy original metadata
				for k, v := range event.Metadata {
					outputEvent.Metadata[k] = v
				}
				outputEvent.Metadata["ledger_sequence"] = fmt.Sprintf("%d", rawLedger.Sequence)
				outputEvent.Metadata["event_type"] = eventType
				outputEvent.Metadata["processor_version"] = config.ProcessorConfig.Version
				outputEvent.Metadata["filter_enabled"] = fmt.Sprintf("%t", config.Filter.Enabled)

				outputEvents = append(outputEvents, outputEvent)
				metrics.RecordEventEmitted(eventType, config.NetworkName, false)
			}

			// Update metrics
			totalFiltered += int64(filteredCount)
			totalPassed += int64(len(outputEvents))

			// Log processing stats
			logger.Info("Processed token transfer events",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Int("extracted", len(ttpEvents)),
				zap.Int("filtered_out", filteredCount),
				zap.Int("passed", len(outputEvents)))

			// If all events filtered out, return nil
			if len(outputEvents) == 0 {
				return nil, nil
			}

			// Return first event (processor framework handles single event return)
			// TODO: Update processor framework to support multiple event return
			return outputEvents[0], nil
		},
		[]string{"stellar.ledger.v1"},           // Input types
		[]string{"stellar.token.transfer.v1"},   // Output types
	)
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the processor
	ctx := context.Background()
	if err := proc.Start(ctx); err != nil {
		log.Fatalf("Failed to start processor: %v", err)
	}

	logger.Info("Token Transfer Processor is running",
		zap.String("endpoint", config.ProcessorConfig.Endpoint),
		zap.Int("health_port", config.ProcessorConfig.HealthPort),
		zap.Bool("flowctl_enabled", config.ProcessorConfig.FlowctlConfig.Enabled))

	// Wait for interrupt signal (simplified - flowctl-sdk handles most lifecycle)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down processor...")
	proc.Stop()
	logger.Info("Processor stopped successfully")
}
