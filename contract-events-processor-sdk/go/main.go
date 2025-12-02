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

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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
	config.ID = getEnv("COMPONENT_ID", "contract-events-processor")
	config.Name = "Contract Events Processor"
	config.Description = "Extracts Soroban contract events from Stellar ledgers"
	config.Version = "2.0.0-sdk"
	config.Endpoint = getEnv("PORT", ":50053")
	config.HealthPort = parseInt(getEnv("HEALTH_PORT", "8089"))

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		config.FlowctlConfig.Enabled = true
		config.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		config.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Create processor
	proc, err := processor.New(config)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Get network passphrase
	networkPassphrase := getEnv("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015")

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

			// Decode XDR
			var ledgerCloseMeta xdr.LedgerCloseMeta
			if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &ledgerCloseMeta); err != nil {
				logger.Error("Failed to unmarshal XDR", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
			}

			// Extract contract events
			contractEvents, err := extractContractEvents(ledgerCloseMeta, networkPassphrase, logger)
			if err != nil {
				logger.Error("Failed to extract contract events", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to extract events: %w", err)
			}

			// If no events found, skip
			if len(contractEvents) == 0 {
				logger.Debug("No contract events in ledger", zap.Uint32("sequence", rawLedger.Sequence))
				return nil, nil
			}

			logger.Info("Extracted contract events",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Int("count", len(contractEvents)))

			// Marshal contract events for output
			eventsData, err := proto.Marshal(&stellarv1.ContractEventBatch{
				Events: contractEvents,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal events: %w", err)
			}

			// Create output event
			outputEvent := &flowctlv1.Event{
				Id:                fmt.Sprintf("contract-events-%d", rawLedger.Sequence),
				Type:              "stellar.contract.events.v1",
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
			outputEvent.Metadata["events_count"] = fmt.Sprintf("%d", len(contractEvents))
			outputEvent.Metadata["processor_version"] = config.Version

			return outputEvent, nil
		},
		[]string{"stellar.ledger.v1"},              // Input types
		[]string{"stellar.contract.events.v1"},     // Output types
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

	logger.Info("Contract Events Processor is running",
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

// extractContractEvents extracts all contract events from a ledger
func extractContractEvents(ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string, logger *zap.Logger) ([]*stellarv1.ContractEvent, error) {
	sequence := ledgerCloseMeta.LedgerSequence()

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	var events []*stellarv1.ContractEvent

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err != nil {
			break // EOF or error
		}

		txSuccessful := tx.Result.Successful()
		txHash := tx.Result.TransactionHash.HexString()

		// Get transaction events
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			continue // Not a Soroban transaction
		}

		// Process operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIdx, event := range opEvents {
				if event.Type != xdr.ContractEventTypeContract {
					continue
				}

				contractEvent := convertToContractEvent(event, sequence, txHash, txSuccessful, uint32(tx.Index), int32(eventIdx), &opIndex)
				if contractEvent != nil {
					events = append(events, contractEvent)
				}
			}
		}

		// Process transaction-level events
		for eventIdx, txEvent := range txEvents.TransactionEvents {
			if txEvent.Event.Type == xdr.ContractEventTypeContract {
				contractEvent := convertToContractEvent(txEvent.Event, sequence, txHash, txSuccessful, uint32(tx.Index), int32(eventIdx), nil)
				if contractEvent != nil {
					events = append(events, contractEvent)
				}
			}
		}
	}

	return events, nil
}

// convertToContractEvent converts XDR contract event to proto format
func convertToContractEvent(event xdr.ContractEvent, ledgerSeq uint32, txHash string, txSuccess bool, txIndex uint32, eventIdx int32, opIndex *int) *stellarv1.ContractEvent {
	// Extract contract ID - convert to C... address format
	var contractID string
	if event.ContractId != nil {
		// Try to encode as StrKey
		contractIDBytes := make([]byte, len(*event.ContractId))
		copy(contractIDBytes, (*event.ContractId)[:])
		contractID = fmt.Sprintf("C%x", contractIDBytes) // Simplified - in production use strkey.Encode
	}

	// Marshal topics and data as XDR base64
	var topicsXDR []*stellarv1.ScValue
	for _, topic := range event.Body.V0.Topics {
		xdrBytes, _ := topic.MarshalBinary()
		topicsXDR = append(topicsXDR, &stellarv1.ScValue{
			XdrBase64: string(xdrBytes),
		})
	}

	dataXDR, _ := event.Body.V0.Data.MarshalBinary()

	contractEvent := &stellarv1.ContractEvent{
		Meta: &stellarv1.EventMeta{
			LedgerSequence: ledgerSeq,
			TxHash:         txHash,
			TxSuccessful:   txSuccess,
			TxIndex:        txIndex,
		},
		ContractId:      contractID,
		Topics:          topicsXDR,
		Data:            &stellarv1.ScValue{XdrBase64: string(dataXDR)},
		InSuccessfulTx:  txSuccess,
		EventIndex:      eventIdx,
	}

	if opIndex != nil {
		idx := int32(*opIndex)
		contractEvent.OperationIndex = &idx
	}

	return contractEvent
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
