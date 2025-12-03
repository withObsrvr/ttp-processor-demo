package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/processor"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	"github.com/stellar/go-stellar-sdk/asset"
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
	config := processor.DefaultConfig()
	config.ID = getEnv("COMPONENT_ID", "ttp-processor")
	config.Name = "Token Transfer Processor"
	config.Description = "Extracts SEP-41 token transfer events from Stellar ledgers"
	config.Version = "2.0.0-sdk"
	config.Endpoint = getEnv("PORT", ":50051")
	config.HealthPort = parseInt(getEnv("HEALTH_PORT", "8088"))

	// Event types
	config.InputEventTypes = []string{"stellar.ledger.v1"}
	config.OutputEventTypes = []string{"stellar.token.transfer.v1"}

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		config.FlowctlConfig.Enabled = true
		config.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		config.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Get network passphrase
	networkPassphrase := os.Getenv("NETWORK_PASSPHRASE")
	if networkPassphrase == "" {
		log.Fatal("NETWORK_PASSPHRASE environment variable not set")
	}

	// Determine network name for metrics
	networkName := "unknown"
	if strings.Contains(networkPassphrase, "Public Global") {
		networkName = "mainnet"
	} else if strings.Contains(networkPassphrase, "Test SDF") {
		networkName = "testnet"
	}

	logger.Info("Token Transfer Processor starting",
		zap.String("network", networkPassphrase),
		zap.String("network_name", networkName))

	// Load filter configuration
	filterConfig := LoadFilterConfig(logger)

	// Load batch configuration
	batchConfig := LoadBatchConfig(logger)

	// Initialize metrics
	metricsEnabled := parseBool(getEnv("ENABLE_METRICS", "true"))
	metrics := NewProcessorMetrics(metricsEnabled)
	if metricsEnabled {
		logger.Info("Dimensional metrics enabled",
			zap.String("network", networkName))
	}

	// Validate configuration
	validator := NewConfigValidator(logger)
	if !validator.ValidateConfiguration(networkPassphrase, filterConfig, batchConfig) {
		log.Fatal("Configuration validation failed with fatal errors")
	}

	// Create processor
	proc, err := processor.New(config)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Create stellar token transfer processor
	ttpProcessor := token_transfer.NewEventsProcessor(networkPassphrase)

	// Track filtering metrics
	var totalExtracted, totalFiltered, totalPassed int64

	// Track error metrics
	errorCollector := NewErrorCollector()

	// Create batch processor if enabled
	var batchProcessor *BatchProcessor
	if batchConfig.Enabled {
		batchProcessor = NewBatchProcessor(
			batchConfig,
			filterConfig,
			ttpProcessor,
			logger,
			config.ID,
			config.Version,
			metrics,
			networkName,
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
			metrics.RecordLedgerProcessed(networkName, false)

			// Track total extracted
			totalExtracted += int64(len(ttpEvents))

			// If no events found, skip
			if len(ttpEvents) == 0 {
				logger.Debug("No token transfer events in ledger", zap.Uint32("sequence", rawLedger.Sequence))
				return nil, nil
			}

			// Apply filtering and convert to proto format
			convertedEvents := make([]*stellarv1.TokenTransferEvent, 0, len(ttpEvents))
			filteredCount := 0
			for _, ttpEvent := range ttpEvents {
				// Record event extracted
				eventType := getEventType(ttpEvent)
				metrics.RecordEventExtracted(eventType, networkName, false)

				// Apply filter
				if !filterConfig.ShouldIncludeEvent(ttpEvent, logger) {
					filteredCount++
					// Determine filter reason
					filterReason := "event_type"
					if filterConfig.MinAmount != nil && getEventAmount(ttpEvent) < *filterConfig.MinAmount {
						filterReason = "min_amount"
					}
					metrics.RecordEventFiltered(eventType, filterReason, networkName)
					continue
				}

				// Convert to proto
				converted := convertTokenTransferEvent(ttpEvent)
				if converted != nil {
					convertedEvents = append(convertedEvents, converted)
					metrics.RecordEventEmitted(eventType, networkName, false)
				}
			}

			// Update metrics
			totalFiltered += int64(filteredCount)
			totalPassed += int64(len(convertedEvents))

			// Log processing stats
			logger.Info("Processed token transfer events",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Int("extracted", len(ttpEvents)),
				zap.Int("filtered_out", filteredCount),
				zap.Int("passed", len(convertedEvents)))

			// If all events filtered out, return nil
			if len(convertedEvents) == 0 {
				return nil, nil
			}

			// Marshal token transfer events for output
			eventsData, err := proto.Marshal(&stellarv1.TokenTransferBatch{
				Events: convertedEvents,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal events: %w", err)
			}

			// Create output event
			outputEvent := &flowctlv1.Event{
				Id:                fmt.Sprintf("token-transfers-%d", rawLedger.Sequence),
				Type:              "stellar.token.transfer.v1",
				Payload:           eventsData,
				Metadata:          make(map[string]string),
				SourceComponentId: config.ID,
				ContentType:       "application/protobuf",
				StellarCursor: &flowctlv1.StellarCursor{
					LedgerSequence: uint64(rawLedger.Sequence),
				},
			}

			// Copy original metadata
			for k, v := range event.Metadata {
				outputEvent.Metadata[k] = v
			}
			outputEvent.Metadata["ledger_sequence"] = fmt.Sprintf("%d", rawLedger.Sequence)
			outputEvent.Metadata["events_extracted"] = fmt.Sprintf("%d", len(ttpEvents))
			outputEvent.Metadata["events_filtered"] = fmt.Sprintf("%d", filteredCount)
			outputEvent.Metadata["events_count"] = fmt.Sprintf("%d", len(convertedEvents))
			outputEvent.Metadata["processor_version"] = config.Version
			outputEvent.Metadata["filter_enabled"] = fmt.Sprintf("%t", filterConfig.Enabled)

			return outputEvent, nil
		},
		[]string{"stellar.ledger.v1"},           // Input types
		[]string{"stellar.token.transfer.v1"},   // Output types
	)
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the processor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := proc.Start(ctx); err != nil {
		log.Fatalf("Failed to start processor: %v", err)
	}

	logger.Info("Token Transfer Processor is running",
		zap.String("endpoint", config.Endpoint),
		zap.Int("health_port", config.HealthPort),
		zap.Bool("flowctl_enabled", config.FlowctlConfig.Enabled))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop the processor gracefully
	logger.Info("Shutting down processor...")
	if err := proc.Stop(); err != nil {
		log.Fatalf("Failed to stop processor: %v", err)
	}

	logger.Info("Processor stopped successfully")
}

// convertTokenTransferEvent converts stellar's token_transfer.TokenTransferEvent to our proto format
func convertTokenTransferEvent(ttpEvent *token_transfer.TokenTransferEvent) *stellarv1.TokenTransferEvent {
	if ttpEvent == nil || ttpEvent.Meta == nil {
		return nil
	}

	// Convert metadata
	meta := &stellarv1.TokenTransferEventMeta{
		LedgerSequence:   ttpEvent.Meta.LedgerSequence,
		ClosedAt:         timestamppb.New(ttpEvent.Meta.ClosedAt.AsTime()),
		TxHash:           ttpEvent.Meta.TxHash,
		TransactionIndex: ttpEvent.Meta.TransactionIndex,
		ContractAddress:  ttpEvent.Meta.ContractAddress,
	}

	// Operation index is optional
	if ttpEvent.Meta.OperationIndex != nil {
		opIdx := *ttpEvent.Meta.OperationIndex
		meta.OperationIndex = &opIdx
	}

	event := &stellarv1.TokenTransferEvent{
		Meta: meta,
	}

	// Convert the specific event type
	switch evt := ttpEvent.Event.(type) {
	case *token_transfer.TokenTransferEvent_Transfer:
		event.Event = &stellarv1.TokenTransferEvent_Transfer{
			Transfer: &stellarv1.Transfer{
				From:   evt.Transfer.From,
				To:     evt.Transfer.To,
				Asset:  convertAsset(evt.Transfer.Asset),
				Amount: evt.Transfer.Amount,
			},
		}

	case *token_transfer.TokenTransferEvent_Mint:
		event.Event = &stellarv1.TokenTransferEvent_Mint{
			Mint: &stellarv1.Mint{
				To:     evt.Mint.To,
				Asset:  convertAsset(evt.Mint.Asset),
				Amount: evt.Mint.Amount,
			},
		}

	case *token_transfer.TokenTransferEvent_Burn:
		event.Event = &stellarv1.TokenTransferEvent_Burn{
			Burn: &stellarv1.Burn{
				From:   evt.Burn.From,
				Asset:  convertAsset(evt.Burn.Asset),
				Amount: evt.Burn.Amount,
			},
		}

	case *token_transfer.TokenTransferEvent_Clawback:
		event.Event = &stellarv1.TokenTransferEvent_Clawback{
			Clawback: &stellarv1.Clawback{
				From:   evt.Clawback.From,
				Asset:  convertAsset(evt.Clawback.Asset),
				Amount: evt.Clawback.Amount,
			},
		}

	case *token_transfer.TokenTransferEvent_Fee:
		event.Event = &stellarv1.TokenTransferEvent_Fee{
			Fee: &stellarv1.Fee{
				From:   evt.Fee.From,
				Asset:  convertAsset(evt.Fee.Asset),
				Amount: evt.Fee.Amount,
			},
		}
	}

	return event
}

// convertAsset converts stellar's asset.Asset to our proto format
func convertAsset(stellarAsset *asset.Asset) *stellarv1.Asset {
	if stellarAsset == nil {
		return nil
	}

	result := &stellarv1.Asset{}

	switch a := stellarAsset.AssetType.(type) {
	case *asset.Asset_Native:
		result.Asset = &stellarv1.Asset_Native{
			Native: a.Native,
		}

	case *asset.Asset_IssuedAsset:
		result.Asset = &stellarv1.Asset_Issued{
			Issued: &stellarv1.IssuedAsset{
				AssetCode:   a.IssuedAsset.AssetCode,
				AssetIssuer: a.IssuedAsset.Issuer,
			},
		}
	}

	return result
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}
