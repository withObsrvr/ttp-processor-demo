package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"time"

	"github.com/stellar/go/xdr"
)

// extractEvictedKeys extracts evicted storage keys from LedgerCloseMeta
// Protocol 20+ Soroban archival tracking
// Obsrvr playbook: core.evicted_keys_state_v1
func (ing *Ingester) extractEvictedKeys(lcm *xdr.LedgerCloseMeta) []EvictedKeyData {
	var evictedKeysList []EvictedKeyData

	// Only LedgerCloseMetaV2 has evicted keys tracking
	if lcm.V != 2 {
		return evictedKeysList
	}

	v2Meta := lcm.MustV2()

	// Get ledger sequence and closed time
	ledgerSeq := uint32(v2Meta.LedgerHeader.Header.LedgerSeq)
	closedAt := time.Unix(int64(v2Meta.LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()

	// Track unique evicted keys by hash (deduplicate within ledger)
	evictedKeysMap := make(map[string]*EvictedKeyData)

	// Process evicted keys from V2 meta
	// NOTE: EvictedKeys are stored directly in LedgerCloseMetaV2
	// as "TTL and data/code keys that have been evicted at this ledger"
	for _, evictedKey := range v2Meta.EvictedKeys {
		// Determine durability based on key type
		// ContractData keys can be persistent or temporary
		// For now, we'll default to "unknown" unless we can determine it
		durability := "unknown"

		data := extractEvictedKeyData(evictedKey, ledgerSeq, durability, closedAt)
		if data != nil {
			evictedKeysMap[data.KeyHash] = data
		}
	}

	// Convert map to slice
	for _, data := range evictedKeysMap {
		evictedKeysList = append(evictedKeysList, *data)
	}

	return evictedKeysList
}

// extractEvictedKeyData extracts data from a single evicted ledger key
func extractEvictedKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, closedAt time.Time) *EvictedKeyData {
	// PERFORMANCE FIX: Replaced defer recover() with explicit nil checks
	// - Avoids expensive stack unwinding on every call
	// - Makes logic bugs visible instead of hiding them
	// - Improves throughput in high-volume ingestion

	// Generate hash of the ledger key
	keyHashBytes, err := ledgerKey.MarshalBinary()
	if err != nil {
		log.Printf("Failed to marshal evicted ledger key: %v", err)
		return nil
	}

	hash := sha256.Sum256(keyHashBytes)
	keyHash := hex.EncodeToString(hash[:])

	// Extract contract ID and key type based on ledger entry type
	var contractID string
	var keyType string

	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		// EXPLICIT NIL CHECKS: Prevent panics from malformed XDR
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			// Safely extract contract ID with nested nil checks
			// In early testnet data, these fields might not be properly populated
			contractBytes, err := ledgerKey.ContractData.Contract.MarshalBinary()
			if err == nil && len(contractBytes) > 0 {
				// Use hash of the full contract address as ID
				contractHash := sha256.Sum256(contractBytes)
				contractID = hex.EncodeToString(contractHash[:])
			}

			// Safely extract key type from ScVal
			keyType = "ContractData"
			if keyBytes, err := ledgerKey.ContractData.Key.MarshalBinary(); err == nil && len(keyBytes) > 0 {
				keyType = ledgerKey.ContractData.Key.Type.String()
			}
		}

	case xdr.LedgerEntryTypeContractCode:
		// EXPLICIT NIL CHECKS: Prevent panics from malformed XDR
		if ledgerKey.ContractCode == nil {
			log.Printf("Warning: ContractCode is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractCode"
		} else {
			// Safely extract contract code hash
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil && len(codeHashBytes) > 0 {
				contractID = hex.EncodeToString(codeHashBytes)
			}
			keyType = "ContractCode"
		}

	default:
		// Other ledger entry types (unlikely to be evicted)
		keyType = ledgerKey.Type.String()
	}

	// Create evicted key entry
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	data := EvictedKeyData{
		// Identity (2 fields)
		KeyHash:        keyHash,
		LedgerSequence: ledgerSeq,

		// Eviction details (3 fields)
		ContractID: contractID,
		KeyType:    keyType,
		Durability: durability,

		// Metadata (3 fields)
		ClosedAt:    closedAt,
		LedgerRange: ledgerRange,
		CreatedAt:   now,
	}

	return &data
}
