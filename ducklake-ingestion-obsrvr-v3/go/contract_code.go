package main

import (
	"encoding/base64"
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractContractCode extracts contract code (WASM) from LedgerCloseMeta
// Protocol 20+ Soroban smart contract deployments
// Obsrvr playbook: core.contract_code_snapshot_v1
func (ing *Ingester) extractContractCode(lcm *xdr.LedgerCloseMeta) []ContractCodeData {
	var contractCodeList []ContractCodeData

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
		ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create transaction reader for contract code: %v", err)
		return contractCodeList
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

	return contractCodeList
}

// isContractCodeChange checks if a change involves contract code
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
func countWASMElements(data []byte) int64 {
	if len(data) == 0 {
		return 0
	}

	// First value in section is count (LEB128)
	count, _ := decodeLEB128(data)
	return count
}

// decodeLEB128 decodes a signed LEB128 encoded integer
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

// encodeContractCodeXDR encodes contract code entry to base64 XDR
func encodeContractCodeXDR(code *xdr.ContractCodeEntry) string {
	if code == nil {
		return ""
	}

	xdrBytes, err := code.MarshalBinary()
	if err != nil {
		log.Printf("Failed to encode contract code XDR: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(xdrBytes)
}
