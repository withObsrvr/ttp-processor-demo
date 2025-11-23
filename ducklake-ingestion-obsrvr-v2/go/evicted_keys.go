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
func extractEvictedKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, closedAt time.Time) (result *EvictedKeyData) {
	// Recover from any panics due to nil pointer access in XDR structures
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in extractEvictedKeyData: %v (ledger %d, key type %v)", r, ledgerSeq, ledgerKey.Type)
			result = nil
		}
	}()

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
		if ledgerKey.ContractData != nil {
			// Extract contract ID - may panic if structure is unexpected
			contractIDBytes, err := ledgerKey.ContractData.Contract.ContractId.MarshalBinary()
			if err == nil {
				contractID = hex.EncodeToString(contractIDBytes)
			}

			// Extract key type from ScVal
			keyType = ledgerKey.ContractData.Key.Type.String()
		}

	case xdr.LedgerEntryTypeContractCode:
		if ledgerKey.ContractCode != nil {
			// Contract code hash
			codeHashBytes, err := ledgerKey.ContractCode.Hash.MarshalBinary()
			if err == nil {
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
