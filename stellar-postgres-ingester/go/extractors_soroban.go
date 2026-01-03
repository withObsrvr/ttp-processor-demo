package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
	"github.com/withObsrvr/ttp-processor-demo/stellar-postgres-ingester/go/internal/processors/contract"
)

// extractContractEvents extracts Soroban contract events from raw ledger
// Captures three event types: CONTRACT, SYSTEM, DIAGNOSTIC (includes invocation tree)
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_events.go lines 19-82
func (w *Writer) extractContractEvents(rawLedger *pb.RawLedger) ([]ContractEventData, error) {
	var events []ContractEventData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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

	// Create transaction reader
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract events: %v", err)
		return events, nil
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

	return events, nil
}

// extractDiagnosticEvent extracts data from a diagnostic event
// Diagnostic events contain the contract invocation tree
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_events.go lines 85-102
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
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_events.go lines 105-162
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
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_events.go lines 165-246
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
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_events.go lines 249-260
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

// Wrapper functions to adapt sac package to contract processor signature
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_data.go lines 18-28
func sacAssetFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset {
	asset, ok := sac.AssetFromContractData(ledgerEntry, passphrase)
	if !ok {
		return nil
	}
	return &asset
}

func sacContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	return sac.ContractBalanceFromContractData(ledgerEntry, passphrase)
}

// extractContractData extracts contract data state from raw ledger
// Protocol 20+ Soroban smart contract storage with SAC detection
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_data.go lines 33-175
func (w *Writer) extractContractData(rawLedger *pb.RawLedger) ([]ContractDataData, error) {
	var contractDataList []ContractDataData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract data: %v", err)
		return contractDataList, nil
	}
	defer txReader.Close()

	// Get ledger header for stellar/go processor
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()

	// Create stellar/go contract processor with sac wrapper functions
	transformer := contract.NewTransformContractDataStruct(
		sacAssetFromContractData,
		sacContractBalanceFromContractData,
	)

	// Track unique contracts (to avoid duplicates within a ledger)
	contractMap := make(map[string]*ContractDataData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract data: %v", err)
			continue
		}

		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Failed to get transaction changes: %v", err)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Check if this is a contract data change
			if !isContractDataChange(change) {
				continue
			}

			// Transform using stellar/go processor
			contractOutput, err, shouldContinue := transformer.TransformContractData(
				change, w.config.Source.NetworkPassphrase, ledgerHeader)
			if err != nil {
				log.Printf("Failed to transform contract data: %v", err)
				continue
			}
			if !shouldContinue {
				continue
			}

			// Extract contract data XDR
			var contractDataXDR string
			if contractOutput.ContractDataXDR != "" {
				contractDataXDR = contractOutput.ContractDataXDR
			} else if change.Post != nil {
				// Fallback: encode XDR from change
				if cd, ok := change.Post.Data.GetContractData(); ok {
					if xdrBytes, err := cd.MarshalBinary(); err == nil {
						contractDataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
					}
				}
			}

			// Create contract data entry
			now := time.Now().UTC()
			ledgerRange := (ledgerSeq / 10000) * 10000

			// Create unique key for deduplication
			key := fmt.Sprintf("%s_%s_%s",
				contractOutput.ContractId,
				contractOutput.ContractKeyType,
				contractOutput.LedgerKeyHash)

			data := ContractDataData{
				// Identity (3 fields)
				ContractId:     contractOutput.ContractId,
				LedgerSequence: ledgerSeq,
				LedgerKeyHash:  contractOutput.LedgerKeyHash,

				// Contract metadata (2 fields)
				ContractKeyType:    contractOutput.ContractKeyType,
				ContractDurability: contractOutput.ContractDurability,

				// Asset information (3 fields, nullable)
				AssetCode:   nullableString(contractOutput.ContractDataAssetCode),
				AssetIssuer: nullableString(contractOutput.ContractDataAssetIssuer),
				AssetType:   nullableString(contractOutput.ContractDataAssetType),

				// Balance information (2 fields, nullable)
				BalanceHolder: nullableString(contractOutput.ContractDataBalanceHolder),
				Balance:       nullableString(contractOutput.ContractDataBalance),

				// Ledger metadata (4 fields)
				LastModifiedLedger: int32(contractOutput.LastModifiedLedger),
				LedgerEntryChange:  int32(contractOutput.LedgerEntryChange),
				Deleted:            contractOutput.Deleted,
				ClosedAt:           closedAt,

				// XDR data (1 field)
				ContractDataXDR: contractDataXDR,

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,
			}

			// Deduplicate by key
			contractMap[key] = &data
		}
	}

	// Convert map to slice
	for _, data := range contractMap {
		contractDataList = append(contractDataList, *data)
	}

	return contractDataList, nil
}

// isContractDataChange checks if a change involves contract data
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_data.go lines 178-186
func isContractDataChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// nullableString converts empty strings to nil pointers
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_data.go lines 196-205
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	str := s
	return &str
}

// extractContractCode extracts contract code (WASM) from raw ledger
// Protocol 20+ Soroban smart contract deployments
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 14-171
func (w *Writer) extractContractCode(rawLedger *pb.RawLedger) ([]ContractCodeData, error) {
	var contractCodeList []ContractCodeData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract code: %v", err)
		return contractCodeList, nil
	}
	defer txReader.Close()

	// Track unique contract code by hash (to avoid duplicates within a ledger)
	contractCodeMap := make(map[string]*ContractCodeData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for contract code: %v", err)
			continue
		}

		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			log.Printf("Failed to get transaction changes: %v", err)
			continue
		}

		// Process each change
		for _, change := range changes {
			// Check if this is a contract code change
			if !isContractCodeChange(change) {
				continue
			}

			// Extract contract code entry
			var contractCode *xdr.ContractCodeEntry
			var deleted bool
			var lastModifiedLedger uint32
			var ledgerKeyHash string

			if change.Post != nil {
				codeEntry, _ := change.Post.Data.GetContractCode()
				contractCode = &codeEntry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false

				// Get ledger key hash
				if key, err := change.Post.LedgerKey(); err == nil {
					if keyBytes, err := key.MarshalBinary(); err == nil {
						ledgerKeyHash = hex.EncodeToString(keyBytes)
					}
				}
			} else if change.Pre != nil {
				codeEntry, _ := change.Pre.Data.GetContractCode()
				contractCode = &codeEntry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true

				// Get ledger key hash
				if key, err := change.Pre.LedgerKey(); err == nil {
					if keyBytes, err := key.MarshalBinary(); err == nil {
						ledgerKeyHash = hex.EncodeToString(keyBytes)
					}
				}
			}

			if contractCode == nil {
				continue
			}

			// Create contract code entry
			now := time.Now().UTC()
			ledgerRange := (ledgerSeq / 10000) * 10000

			// Get contract code hash (hex encoded)
			codeHashHex := hex.EncodeToString(contractCode.Hash[:])

			// Get extension version
			var extV int32
			if contractCode.Ext.V == 1 && contractCode.Ext.V1 != nil {
				extV = 1
			} else {
				extV = 0
			}

			// Parse WASM metadata
			wasmMetadata := parseWASMMetadata(contractCode.Code)

			data := ContractCodeData{
				// Identity (2 fields)
				ContractCodeHash: codeHashHex,
				LedgerKeyHash:    ledgerKeyHash,

				// Extension (1 field)
				ContractCodeExtV: extV,

				// Ledger metadata (4 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				LedgerEntryChange:  int32(change.Type),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Ledger tracking (1 field)
				LedgerSequence: ledgerSeq,

				// WASM metadata (10 fields)
				NInstructions:     wasmMetadata.NInstructions,
				NFunctions:        wasmMetadata.NFunctions,
				NGlobals:          wasmMetadata.NGlobals,
				NTableEntries:     wasmMetadata.NTableEntries,
				NTypes:            wasmMetadata.NTypes,
				NDataSegments:     wasmMetadata.NDataSegments,
				NElemSegments:     wasmMetadata.NElemSegments,
				NImports:          wasmMetadata.NImports,
				NExports:          wasmMetadata.NExports,
				NDataSegmentBytes: wasmMetadata.NDataSegmentBytes,

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,
			}

			// Deduplicate by hash
			contractCodeMap[codeHashHex] = &data
		}
	}

	// Convert map to slice
	for _, data := range contractCodeMap {
		contractCodeList = append(contractCodeList, *data)
	}

	return contractCodeList, nil
}

// isContractCodeChange checks if a change involves contract code
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 173-182
func isContractCodeChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractCode {
		return true
	}
	return false
}

// WASMMetadata contains parsed WASM module metadata
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 184-196
type WASMMetadata struct {
	NInstructions     *int64
	NFunctions        *int64
	NGlobals          *int64
	NTableEntries     *int64
	NTypes            *int64
	NDataSegments     *int64
	NElemSegments     *int64
	NImports          *int64
	NExports          *int64
	NDataSegmentBytes *int64
}

// parseWASMMetadata parses WASM bytecode to extract metadata
// This is a simplified parser that counts sections
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 198-274
func parseWASMMetadata(wasmCode []byte) WASMMetadata {
	metadata := WASMMetadata{}

	// WASM magic number check
	if len(wasmCode) < 8 {
		return metadata
	}

	// Check magic number (0x00 0x61 0x73 0x6D) and version (0x01 0x00 0x00 0x00)
	if wasmCode[0] != 0x00 || wasmCode[1] != 0x61 || wasmCode[2] != 0x73 || wasmCode[3] != 0x6D {
		return metadata
	}

	// Parse WASM sections
	offset := 8 // Skip magic + version

	for offset < len(wasmCode) {
		if offset+1 >= len(wasmCode) {
			break
		}

		sectionType := wasmCode[offset]
		offset++

		// Read section size (LEB128)
		sectionSize, bytesRead := decodeLEB128(wasmCode[offset:])
		offset += bytesRead

		if sectionSize < 0 || offset+int(sectionSize) > len(wasmCode) {
			break
		}

		sectionData := wasmCode[offset : offset+int(sectionSize)]
		offset += int(sectionSize)

		// Count elements in each section
		switch sectionType {
		case 1: // Type section
			count := countWASMElements(sectionData)
			metadata.NTypes = &count
		case 2: // Import section
			count := countWASMElements(sectionData)
			metadata.NImports = &count
		case 3: // Function section
			count := countWASMElements(sectionData)
			metadata.NFunctions = &count
		case 4: // Table section
			count := countWASMElements(sectionData)
			metadata.NTableEntries = &count
		case 6: // Global section
			count := countWASMElements(sectionData)
			metadata.NGlobals = &count
		case 7: // Export section
			count := countWASMElements(sectionData)
			metadata.NExports = &count
		case 9: // Element section
			count := countWASMElements(sectionData)
			metadata.NElemSegments = &count
		case 10: // Code section (contains instructions)
			count := countWASMElements(sectionData)
			if count > 0 {
				// Approximate instruction count (very rough estimate)
				instrCount := int64(len(sectionData))
				metadata.NInstructions = &instrCount
			}
		case 11: // Data section
			count := countWASMElements(sectionData)
			metadata.NDataSegments = &count
			dataSize := int64(len(sectionData))
			metadata.NDataSegmentBytes = &dataSize
		}
	}

	return metadata
}

// countWASMElements counts the number of elements in a WASM section
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 276-285
func countWASMElements(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}

	// First value in section is count (LEB128)
	count, _ := decodeLEB128(data)
	return count
}

// decodeLEB128 decodes a signed LEB128 encoded integer
// Reference: ducklake-ingestion-obsrvr-v3/go/contract_code.go lines 287-309
func decodeLEB128(data []byte) (int64, int) {
	var result int64
	var shift uint
	var bytesRead int

	for bytesRead = 0; bytesRead < len(data) && bytesRead < 10; bytesRead++ {
		b := data[bytesRead]
		result |= int64(b&0x7F) << shift

		if b&0x80 == 0 {
			// Sign extend if negative
			if shift < 64 && (b&0x40) != 0 {
				result |= -(1 << (shift + 7))
			}
			return result, bytesRead + 1
		}

		shift += 7
	}

	return 0, bytesRead
}

// extractRestoredKeys extracts restored storage keys from raw ledger
// Protocol 20+ Soroban archival restoration tracking
// Reference: ducklake-ingestion-obsrvr-v3/go/restored_keys.go lines 14-123
func (w *Writer) extractRestoredKeys(rawLedger *pb.RawLedger) ([]RestoredKeyData, error) {
	var restoredKeysList []RestoredKeyData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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

	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for restored keys: %v", err)
		return restoredKeysList, nil
	}
	defer txReader.Close()

	// Track unique restored keys by hash (deduplicate within ledger)
	restoredKeysMap := make(map[string]*RestoredKeyData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for restored keys: %v", err)
			continue
		}

		// Check if transaction succeeded
		if !tx.Result.Successful() {
			continue // Only track successful restore operations
		}

		// Look for RestoreFootprint operations
		envelope := tx.Envelope
		var operations []xdr.Operation

		switch envelope.Type {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			operations = envelope.V1.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxV0:
			operations = envelope.V0.Tx.Operations
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			innerTx := envelope.FeeBump.Tx.InnerTx
			if innerTx.Type == xdr.EnvelopeTypeEnvelopeTypeTx {
				operations = innerTx.V1.Tx.Operations
			}
		default:
			continue
		}

		// Check each operation for RestoreFootprint
		for _, op := range operations {
			if op.Body.Type != xdr.OperationTypeRestoreFootprint {
				continue
			}

			// Get the transaction's Soroban resources footprint
			var footprint *xdr.LedgerFootprint

			switch envelope.Type {
			case xdr.EnvelopeTypeEnvelopeTypeTx:
				if envelope.V1.Tx.Ext.V == 1 && envelope.V1.Tx.Ext.SorobanData != nil {
					footprint = &envelope.V1.Tx.Ext.SorobanData.Resources.Footprint
				}
			}

			if footprint == nil {
				continue
			}

			// Process readWrite keys (these are being restored)
			restoredFromLedger := uint32(0) // Unknown without historical tracking

			for _, key := range footprint.ReadWrite {
				durability := "unknown"
				data := extractRestoredKeyData(key, ledgerSeq, durability, restoredFromLedger, closedAt)
				if data != nil {
					restoredKeysMap[data.KeyHash] = data
				}
			}
		}
	}

	// Convert map to slice
	for _, data := range restoredKeysMap {
		restoredKeysList = append(restoredKeysList, *data)
	}

	return restoredKeysList, nil
}

// extractRestoredKeyData extracts data from a single restored ledger key
// Reference: ducklake-ingestion-obsrvr-v3/go/restored_keys.go lines 125-206
func extractRestoredKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, restoredFromLedger uint32, closedAt time.Time) *RestoredKeyData {
	// Generate hash of the ledger key
	keyHashBytes, err := ledgerKey.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal restored ledger key: %v", err)
		return nil
	}

	hash := sha256.Sum256(keyHashBytes)
	keyHash := hex.EncodeToString(hash[:])

	// Extract contract ID and key type based on ledger entry type
	var contractID string
	var keyType string

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			// Safely extract contract ID
			if ledgerKey.ContractData.Contract.ContractId != nil {
				contractIDBytes, err := ledgerKey.ContractData.Contract.ContractId.MarshalBinary()
				if err == nil {
					contractID = hex.EncodeToString(contractIDBytes)
				}
			}

			// Safely extract key type from ScVal
			keyType = ledgerKey.ContractData.Key.Type.String()
		}

	case xdr.LedgerEntryTypeContractCode:
		if ledgerKey.ContractCode == nil {
			log.Printf("Warning: ContractCode is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractCode"
		} else {
			// Safely extract contract code hash
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil {
				contractID = hex.EncodeToString(codeHashBytes)
			}
			keyType = "ContractCode"
		}

	default:
		// Other ledger entry types (unlikely to be restored)
		keyType = ledgerKey.Type.String()
	}

	// Create restored key entry
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	data := RestoredKeyData{
		// Identity (2 fields)
		KeyHash:        keyHash,
		LedgerSequence: ledgerSeq,

		// Restoration details (4 fields)
		ContractID:         contractID,
		KeyType:            keyType,
		Durability:         durability,
		RestoredFromLedger: restoredFromLedger,

		// Metadata (3 fields)
		ClosedAt:    closedAt,
		LedgerRange: ledgerRange,
		CreatedAt:   now,
	}

	return &data
}
