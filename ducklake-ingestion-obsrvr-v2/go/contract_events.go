package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractContractEvents extracts Soroban contract events from LedgerCloseMeta
// Captures three event types: CONTRACT, SYSTEM, DIAGNOSTIC (which includes invocation tree)
// Obsrvr playbook: core.contract_events_stream_v1
func (ing *Ingester) extractContractEvents(lcm *xdr.LedgerCloseMeta) []ContractEventData {
	var events []ContractEventData

	// Get ledger sequence and closed time
	var ledgerSeq uint32
	var closedAt time.Time
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	}

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract events: %v", err)
		return events
	}
	defer reader.Close()

	// Read all transactions
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract events: %v", err)
			continue
		}

		// Get transaction hash
		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Extract events from this transaction
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			// No events or error - continue to next transaction
			continue
		}

		// Extract diagnostic events (contains invocation tree)
		for diagIdx, diagEvent := range txEvents.DiagnosticEvents {
			eventData := extractDiagnosticEvent(diagEvent, txHash, ledgerSeq, closedAt, uint32(diagIdx))
			events = append(events, eventData)
		}

		// Extract operation-level contract events
		for opIdx, opEvents := range txEvents.OperationEvents {
			for eventIdx, contractEvent := range opEvents {
				eventData := extractContractEvent(contractEvent, txHash, ledgerSeq, closedAt, uint32(opIdx), uint32(eventIdx), false)
				events = append(events, eventData)
			}
		}
	}

	return events
}

// extractDiagnosticEvent extracts data from a diagnostic event
// Diagnostic events contain the contract invocation tree (what stellar.expert shows)
func extractDiagnosticEvent(diagEvent xdr.DiagnosticEvent, txHash string, ledgerSeq uint32, closedAt time.Time, diagIdx uint32) ContractEventData {
	// Diagnostic events wrap a ContractEvent with InSuccessfulContractCall flag
	eventData := extractContractEvent(
		diagEvent.Event,
		txHash,
		ledgerSeq,
		closedAt,
		diagIdx, // Use diagnostic index as operation index for diagnostic events
		0,       // Event index within operation (diagnostic events are transaction-level)
		diagEvent.InSuccessfulContractCall,
	)

	// Mark event type as diagnostic
	eventData.EventType = "diagnostic"

	return eventData
}

// extractContractEvent extracts data from a ContractEvent
// Handles SYSTEM, CONTRACT, and DIAGNOSTIC event types
func extractContractEvent(
	event xdr.ContractEvent,
	txHash string,
	ledgerSeq uint32,
	closedAt time.Time,
	opIndex uint32,
	eventIndex uint32,
	inSuccessfulCall bool,
) ContractEventData {
	// Generate unique event ID: {tx_hash}:{op_index}:{event_index}
	eventID := fmt.Sprintf("%s:%d:%d", txHash, opIndex, eventIndex)

	// Extract contract ID (nullable for system events)
	var contractID *string
	if event.ContractId != nil {
		id := hex.EncodeToString((*event.ContractId)[:])
		contractID = &id
	}

	// Determine event type
	eventType := eventTypeString(event.Type)

	// Extract topics and data from event body (with Hubble-compatible decoding)
	topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded := extractEventBody(event.Body)

	// Metadata
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	return ContractEventData{
		// Identity
		EventID:         eventID,
		ContractID:      contractID,
		LedgerSequence:  ledgerSeq,
		TransactionHash: txHash,
		ClosedAt:        closedAt,

		// Event Type
		EventType:                eventType,
		InSuccessfulContractCall: inSuccessfulCall,

		// Event Data (Hubble-compatible with decoded versions)
		TopicsJSON:    topicsJSON,
		TopicsDecoded: topicsDecoded,
		DataXDR:       dataXDR,
		DataDecoded:   dataDecoded,
		TopicCount:    topicCount,

		// Context
		OperationIndex: opIndex,
		EventIndex:     eventIndex,

		// Metadata
		CreatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// extractEventBody extracts topics and data from ContractEventBody
// Returns: (topicsJSON, topicsDecoded, topicCount, dataXDR, dataDecoded)
// Topics are serialized as JSON array of base64 XDR strings AND decoded to human-readable JSON
// Data is serialized as base64 XDR AND decoded to human-readable JSON
// This matches Hubble's schema with both raw and decoded versions
func extractEventBody(body xdr.ContractEventBody) (string, string, int32, string, string) {
	// ContractEventBody is a union, currently only V0 exists
	if body.V != 0 {
		log.Printf("Unknown ContractEventBody version: %d", body.V)
		return "[]", "[]", 0, "", "{}"
	}

	v0 := body.MustV0()

	// Extract topics as JSON array of base64 XDR strings
	topicsXDR := []string{}
	topicsDecodedArray := []interface{}{}
	for _, topic := range v0.Topics {
		// Store base64 XDR
		topicBytes, err := topic.MarshalBinary()
		if err != nil {
			log.Printf("Failed to marshal topic: %v", err)
			continue
		}
		topicsXDR = append(topicsXDR, base64.StdEncoding.EncodeToString(topicBytes))

		// Decode topic to human-readable JSON using Stellar SDK
		decodedTopic, err := ConvertScValToJSON(topic)
		if err != nil {
			log.Printf("Failed to decode topic: %v", err)
			topicsDecodedArray = append(topicsDecodedArray, map[string]interface{}{
				"error": err.Error(),
				"type":  topic.Type.String(),
			})
		} else {
			topicsDecodedArray = append(topicsDecodedArray, decodedTopic)
		}
	}

	// Marshal topics (raw XDR)
	topicsJSON, err := json.Marshal(topicsXDR)
	if err != nil {
		log.Printf("Failed to marshal topics JSON: %v", err)
		return "[]", "[]", 0, "", "{}"
	}

	// Marshal decoded topics
	topicsDecodedJSON, err := json.Marshal(topicsDecodedArray)
	if err != nil {
		log.Printf("Failed to marshal decoded topics: %v", err)
		topicsDecodedJSON = []byte("[]")
	}

	topicCount := int32(len(v0.Topics))

	// Extract data as base64 XDR
	dataBytes, err := v0.Data.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal data: %v", err)
		return string(topicsJSON), string(topicsDecodedJSON), topicCount, "", "{}"
	}

	dataXDR := base64.StdEncoding.EncodeToString(dataBytes)

	// Decode data to human-readable JSON using Stellar SDK
	decodedData, err := ConvertScValToJSON(v0.Data)
	if err != nil {
		log.Printf("Failed to decode data: %v", err)
		decodedData = map[string]interface{}{
			"error": err.Error(),
			"type":  v0.Data.Type.String(),
		}
	}

	// Marshal decoded data
	dataDecodedJSON, err := json.Marshal(decodedData)
	if err != nil {
		log.Printf("Failed to marshal decoded data: %v", err)
		dataDecodedJSON = []byte("{}")
	}

	return string(topicsJSON), string(topicsDecodedJSON), topicCount, dataXDR, string(dataDecodedJSON)
}

// eventTypeString converts ContractEventType to string
func eventTypeString(t xdr.ContractEventType) string {
	switch t {
	case xdr.ContractEventTypeSystem:
		return "system"
	case xdr.ContractEventTypeContract:
		return "contract"
	case xdr.ContractEventTypeDiagnostic:
		return "diagnostic"
	default:
		return "unknown"
	}
}
