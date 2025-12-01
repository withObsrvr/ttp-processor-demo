package main

import (
	"encoding/base64"
	"io"
	"log"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// extractConfigSettings extracts network configuration settings from LedgerCloseMeta
// Protocol 20+ Soroban configuration parameters
// Obsrvr playbook: core.config_settings_snapshot_v1
func (ing *Ingester) extractConfigSettings(lcm *xdr.LedgerCloseMeta) []ConfigSettingData {
	var configSettingsList []ConfigSettingData

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
		log.Printf("Failed to create transaction reader for config settings: %v", err)
		return configSettingsList
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

			// Parse config setting values based on type
			data := ConfigSettingData{
				// Identity (2 fields)
				ConfigSettingID: configSettingID,
				LedgerSequence:  ledgerSeq,

				// Ledger metadata (3 fields)
				LastModifiedLedger: int32(lastModifiedLedger),
				Deleted:            deleted,
				ClosedAt:           closedAt,

				// Metadata (2 fields)
				CreatedAt:   now,
				LedgerRange: ledgerRange,

				// Raw XDR for full fidelity
				ConfigSettingXDR: encodeConfigSettingXDR(configEntry),
			}

			// Extract specific fields based on config setting type
			parseConfigSettingFields(configEntry, &data)

			// Deduplicate by config setting ID
			configSettingsMap[configSettingID] = &data
		}
	}

	// Convert map to slice
	for _, data := range configSettingsMap {
		configSettingsList = append(configSettingsList, *data)
	}

	return configSettingsList
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
// NOTE: For Cycle 16, we store the full XDR which provides complete fidelity
// Individual field parsing can be added later when needed for specific queries
func parseConfigSettingFields(entry *xdr.ConfigSettingEntry, data *ConfigSettingData) {
	// Store config setting ID for reference
	// Full config data is available in ConfigSettingXDR field
	// Individual fields can be parsed from XDR as needed

	// Future enhancement: Parse specific fields based on ConfigSettingId
	// For now, XDR storage provides full fidelity and forward compatibility
}

// Helper functions to create pointers for nullable fields
func ptrInt64(v int64) *int64 {
	return &v
}

func ptrUint32(v uint32) *uint32 {
	return &v
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
