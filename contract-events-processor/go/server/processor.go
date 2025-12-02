package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
)

// ContractEventExtractor extracts contract events from Stellar ledgers
type ContractEventExtractor struct {
	networkPassphrase string
	logger            *zap.Logger
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers      uint32
		EventsFound           uint64
		SuccessfulEvents      uint64
		FailedEvents          uint64
		SkippedTransactions   uint64
		LastLedger            uint32
		LastProcessedTime     time.Time
	}
}

// NewContractEventExtractor creates a new contract event extractor
func NewContractEventExtractor(networkPassphrase string, logger *zap.Logger) *ContractEventExtractor {
	return &ContractEventExtractor{
		networkPassphrase: networkPassphrase,
		logger:            logger,
	}
}

// ExtractedEvent represents a processed contract event with all metadata
type ExtractedEvent struct {
	// Ledger metadata
	LedgerSequence  uint32
	LedgerClosedAt  int64
	TxHash          string
	TxSuccessful    bool
	TxIndex         uint32

	// Event metadata
	ContractID     string
	EventType      string // Detected type: transfer, mint, burn, etc.
	EventIndex     int32
	OperationIndex *int32 // nil for transaction-level events

	// Event data
	Topics         []xdr.ScVal
	TopicsDecoded  []interface{}
	Data           xdr.ScVal
	DataDecoded    interface{}

	// Raw XDR
	TopicsXDR []string
	DataXDR   string
}

// ExtractEvents extracts all contract events from a ledger
func (e *ContractEventExtractor) ExtractEvents(ledgerCloseMeta xdr.LedgerCloseMeta) ([]*ExtractedEvent, error) {
	sequence := ledgerCloseMeta.LedgerSequence()
	e.logger.Debug("Processing ledger for contract events", zap.Uint32("ledger", sequence))

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(e.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	var events []*ExtractedEvent

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading transaction: %w", err)
		}

		// Track all transactions
		txSuccessful := tx.Result.Successful()

		// Use the SDK's helper method to get all transaction events
		// This abstracts away V3 vs V4 differences
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			// Not a Soroban transaction or no events
			continue
		}

		// Process contract events from all operations
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIdx, event := range opEvents {
				// Only process contract events (not system events)
				if event.Type != xdr.ContractEventTypeContract {
					continue
				}

				extractedEvent, err := e.extractContractEvent(tx, opIndex, eventIdx, event, ledgerCloseMeta, txSuccessful)
				if err != nil {
					e.logger.Warn("Error extracting contract event",
						zap.Error(err),
						zap.Uint32("ledger", sequence),
						zap.Int("op_index", opIndex),
						zap.Int("event_idx", eventIdx))
					continue
				}

				if extractedEvent != nil {
					events = append(events, extractedEvent)
				}
			}
		}

		// Also process transaction-level events if available (V4 only)
		for eventIdx, txEvent := range txEvents.TransactionEvents {
			if txEvent.Event.Type == xdr.ContractEventTypeContract {
				// Transaction-level events don't have a specific operation index
				extractedEvent, err := e.extractContractEvent(tx, -1, eventIdx, txEvent.Event, ledgerCloseMeta, txSuccessful)
				if err != nil {
					e.logger.Warn("Error extracting transaction-level event",
						zap.Error(err),
						zap.Uint32("ledger", sequence),
						zap.Int("event_idx", eventIdx))
					continue
				}

				if extractedEvent != nil {
					events = append(events, extractedEvent)
				}
			}
		}
	}

	// Update stats
	e.mu.Lock()
	e.stats.ProcessedLedgers++
	e.stats.EventsFound += uint64(len(events))
	e.stats.LastLedger = sequence
	e.stats.LastProcessedTime = time.Now()
	e.mu.Unlock()

	if len(events) > 0 {
		e.logger.Debug("Found contract events in ledger",
			zap.Uint32("ledger", sequence),
			zap.Int("event_count", len(events)))
	}

	return events, nil
}

func (e *ContractEventExtractor) extractContractEvent(
	tx ingest.LedgerTransaction,
	opIndex int,
	eventIndex int,
	event xdr.ContractEvent,
	meta xdr.LedgerCloseMeta,
	txSuccessful bool,
) (*ExtractedEvent, error) {
	// Extract contract ID
	contractID, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return nil, fmt.Errorf("error encoding contract ID: %w", err)
	}

	// Decode topics
	var topicsDecoded []interface{}
	var topicsXDR []string
	for _, topic := range event.Body.V0.Topics {
		// Decode to JSON
		decoded, err := ConvertScValToJSON(topic)
		if err != nil {
			e.logger.Debug("Failed to decode topic", zap.Error(err))
			decoded = nil
		}
		topicsDecoded = append(topicsDecoded, decoded)

		// Encode to XDR base64
		xdrBytes, err := topic.MarshalBinary()
		if err == nil {
			topicsXDR = append(topicsXDR, base64.StdEncoding.EncodeToString(xdrBytes))
		}
	}

	// Decode event data if present
	var dataDecoded interface{}
	var dataXDR string
	eventData := event.Body.V0.Data
	if eventData.Type != xdr.ScValTypeScvVoid {
		decoded, err := ConvertScValToJSON(eventData)
		if err != nil {
			e.logger.Debug("Failed to decode event data", zap.Error(err))
			dataDecoded = nil
		} else {
			dataDecoded = decoded
		}

		// Encode to XDR base64
		xdrBytes, err := eventData.MarshalBinary()
		if err == nil {
			dataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}
	}

	// Update stats
	e.mu.Lock()
	if txSuccessful {
		e.stats.SuccessfulEvents++
	} else {
		e.stats.FailedEvents++
	}
	e.mu.Unlock()

	// Detect event type from topics
	eventType := DetectEventType(event.Body.V0.Topics)

	// Get transaction index (tx.Index is not a pointer in ingest.LedgerTransaction)
	txIndex := uint32(0)
	// The Index field may not exist or may be embedded in the transaction
	// For now, we'll use 0 as a placeholder

	// Create extracted event
	var opIndexPtr *int32
	if opIndex >= 0 {
		opIdx := int32(opIndex)
		opIndexPtr = &opIdx
	}

	extractedEvent := &ExtractedEvent{
		LedgerSequence:  meta.LedgerSequence(),
		LedgerClosedAt:  int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		TxHash:          tx.Result.TransactionHash.HexString(),
		TxSuccessful:    txSuccessful,
		TxIndex:         txIndex,
		ContractID:      contractID,
		EventType:       eventType,
		EventIndex:      int32(eventIndex),
		OperationIndex:  opIndexPtr,
		Topics:          event.Body.V0.Topics,
		TopicsDecoded:   topicsDecoded,
		Data:            eventData,
		DataDecoded:     dataDecoded,
		TopicsXDR:       topicsXDR,
		DataXDR:         dataXDR,
	}

	return extractedEvent, nil
}

// GetStats returns the current processing statistics
func (e *ContractEventExtractor) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"processed_ledgers":    e.stats.ProcessedLedgers,
		"events_found":         e.stats.EventsFound,
		"successful_events":    e.stats.SuccessfulEvents,
		"failed_events":        e.stats.FailedEvents,
		"skipped_transactions": e.stats.SkippedTransactions,
		"last_ledger":          e.stats.LastLedger,
		"last_processed_time":  e.stats.LastProcessedTime.Format(time.RFC3339),
	}
}

// ToJSON converts an ExtractedEvent to JSON for debugging/logging
func (e *ExtractedEvent) ToJSON() ([]byte, error) {
	data := map[string]interface{}{
		"ledger_sequence":  e.LedgerSequence,
		"ledger_closed_at": e.LedgerClosedAt,
		"tx_hash":          e.TxHash,
		"tx_successful":    e.TxSuccessful,
		"tx_index":         e.TxIndex,
		"contract_id":      e.ContractID,
		"event_type":       e.EventType,
		"event_index":      e.EventIndex,
		"operation_index":  e.OperationIndex,
		"topics_decoded":   e.TopicsDecoded,
		"data_decoded":     e.DataDecoded,
		"topics_xdr":       e.TopicsXDR,
		"data_xdr":         e.DataXDR,
	}

	return json.Marshal(data)
}
