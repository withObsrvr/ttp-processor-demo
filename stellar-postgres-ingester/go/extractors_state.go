package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// extractConfigSettings extracts network configuration settings from raw ledger
// Protocol 20+ Soroban configuration parameters
// Reference: ducklake-ingestion-obsrvr-v3/go/config_settings.go lines 16-132
func (w *Writer) extractConfigSettings(rawLedger *pb.RawLedger) ([]ConfigSettingData, error) {
	var configSettingsList []ConfigSettingData

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
		log.Printf("Failed to create transaction reader for config settings: %v", err)
		return configSettingsList, nil
	}
	defer txReader.Close()

	// Track unique config settings by ID (deduplicate within ledger)
	configSettingsMap := make(map[int32]*ConfigSettingData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for config settings: %v", err)
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
			// Check if this is a config setting change
			if !isConfigSettingChange(change) {
				continue
			}

			// Extract config setting entry
			var configEntry *xdr.ConfigSettingEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetConfigSetting()
				configEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}

			if configEntry == nil {
				continue
			}

			// Extract config setting ID
			configSettingID := int32(configEntry.ConfigSettingId)

			// Create config setting entry
			now := time.Now().UTC()
			ledgerRange := (ledgerSeq / 10000) * 10000

			data := ConfigSettingData{
				// Identity (2 fields)
				ConfigSettingID: configSettingID,
				LedgerSequence:  ledgerSeq,

				// Ledger metadata (3 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Raw XDR for full fidelity
				ConfigSettingXDR: encodeConfigSettingXDR(configEntry),

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,
			}

			// Extract specific fields based on config setting type (if needed)
			// For now, XDR storage provides full fidelity
			parseConfigSettingFields(configEntry, &data)

			// Deduplicate by config setting ID
			configSettingsMap[configSettingID] = &data
		}
	}

	// Convert map to slice
	for _, data := range configSettingsMap {
		configSettingsList = append(configSettingsList, *data)
	}

	return configSettingsList, nil
}

// isConfigSettingChange checks if a change involves config settings
func isConfigSettingChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeConfigSetting {
		return true
	}
	return false
}

// parseConfigSettingFields extracts specific fields from config setting entry
// Based on ConfigSettingId, different fields are available
// NOTE: For MVP, we store the full XDR which provides complete fidelity
// Individual field parsing can be added later when needed for specific queries
func parseConfigSettingFields(entry *xdr.ConfigSettingEntry, data *ConfigSettingData) {
	// Store config setting ID for reference
	// Full config data is available in ConfigSettingXDR field
	// Individual fields can be parsed from XDR as needed

	// Future enhancement: Parse specific fields based on ConfigSettingId
	// For now, XDR storage provides full fidelity and forward compatibility
}

// encodeConfigSettingXDR encodes config setting entry to base64 XDR
func encodeConfigSettingXDR(entry *xdr.ConfigSettingEntry) string {
	if entry == nil {
		return ""
	}

	xdrBytes, err := entry.MarshalBinary()
	if err != nil {
		log.Printf("Failed to encode config setting XDR: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(xdrBytes)
}

// extractTTL extracts time-to-live (TTL) entries from raw ledger
// Protocol 20+ Soroban storage expiration tracking
// Reference: ducklake-ingestion-obsrvr-v3/go/ttl.go lines 16-140
func (w *Writer) extractTTL(rawLedger *pb.RawLedger) ([]TTLData, error) {
	var ttlList []TTLData

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
		log.Printf("Failed to create transaction reader for TTL: %v", err)
		return ttlList, nil
	}
	defer txReader.Close()

	// Track unique TTL entries by key hash (deduplicate within ledger)
	ttlMap := make(map[string]*TTLData)

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for TTL: %v", err)
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
			// Check if this is a TTL change
			if !isTTLChange(change) {
				continue
			}

			// Extract TTL entry
			var ttlEntry *xdr.TtlEntry
			var deleted bool
			var lastModifiedLedger uint32

			if change.Post != nil {
				entry, _ := change.Post.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			} else if change.Pre != nil {
				entry, _ := change.Pre.Data.GetTtl()
				ttlEntry = &entry
				lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}

			if ttlEntry == nil {
				continue
			}

			// Get key hash from TTL entry (hex encoded)
			keyHashBytes, err := ttlEntry.KeyHash.MarshalBinary()
			if err != nil {
				log.Printf("Failed to marshal key hash: %v", err)
				continue
			}
			keyHash := hex.EncodeToString(keyHashBytes)

			// Calculate TTL remaining and expired status
			liveUntilLedgerSeq := uint32(ttlEntry.LiveUntilLedgerSeq)
			ttlRemaining := int64(liveUntilLedgerSeq) - int64(ledgerSeq)
			expired := ttlRemaining <= 0

			// Create TTL entry
			now := time.Now().UTC()
			ledgerRange := (ledgerSeq / 10000) * 10000

			data := TTLData{
				// Identity (2 fields)
				KeyHash:        keyHash,
				LedgerSequence: ledgerSeq,

				// TTL tracking (3 fields)
				LiveUntilLedgerSeq: liveUntilLedgerSeq,
				TTLRemaining:       ttlRemaining,
				Expired:            expired,

				// Ledger metadata (3 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,
			}

			// Deduplicate by key hash
			ttlMap[keyHash] = &data
		}
	}

	// Convert map to slice
	for _, data := range ttlMap {
		ttlList = append(ttlList, *data)
	}

	return ttlList, nil
}

// isTTLChange checks if a change involves TTL entries
func isTTLChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeTtl {
		return true
	}
	return false
}

// extractEvictedKeys extracts evicted storage keys from raw ledger
// Protocol 20+ Soroban archival tracking (V2-only)
// Reference: ducklake-ingestion-obsrvr-v3/go/evicted_keys.go lines 15-53
func (w *Writer) extractEvictedKeys(rawLedger *pb.RawLedger) ([]EvictedKeyData, error) {
	var evictedKeysList []EvictedKeyData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Only LedgerCloseMetaV2 has evicted keys tracking
	if lcm.V != 2 {
		return evictedKeysList, nil
	}

	v2Meta := lcm.MustV2()

	// Get ledger sequence and closed time
	ledgerSeq := uint32(v2Meta.LedgerHeader.Header.LedgerSeq)
	closedAt := time.Unix(int64(v2Meta.LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()

	// Track unique evicted keys by hash (deduplicate within ledger)
	evictedKeysMap := make(map[string]*EvictedKeyData)

	// Process evicted keys from V2 meta
	// NOTE: EvictedKeys are stored directly in LedgerCloseMetaV2
	for _, evictedKey := range v2Meta.EvictedKeys {
		// Determine durability based on key type
		// For now, default to "unknown"
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

	return evictedKeysList, nil
}

// extractEvictedKeyData extracts data from a single evicted ledger key
// Reference: ducklake-ingestion-obsrvr-v3/go/evicted_keys.go lines 56-139
func extractEvictedKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, closedAt time.Time) *EvictedKeyData {
	// Generate SHA256 hash of the ledger key
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
		// Explicit nil checks to prevent panics
		if ledgerKey.ContractData == nil {
			log.Printf("Warning: ContractData is nil for ledger %d, key type %v", ledgerSeq, ledgerKey.Type)
			keyType = "ContractData"
		} else {
			// Safely extract contract ID
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
		// Explicit nil checks to prevent panics
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
