package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// extractRestoredKeys extracts restored storage keys from LedgerCloseMeta
// Protocol 20+ Soroban archival restoration tracking
// Obsrvr playbook: core.restored_keys_state_v1
func (ing *Ingester) extractRestoredKeys(lcm *xdr.LedgerCloseMeta) []RestoredKeyData {
	var restoredKeysList []RestoredKeyData

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
		log.Printf("Failed to create transaction reader for restored keys: %v", err)
		return restoredKeysList
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
			// The footprint contains the keys being restored
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
			// Note: restored_from_ledger is unknown without historical tracking
			// We'll set it to 0 for now
			restoredFromLedger := uint32(0)

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

	return restoredKeysList
}

// extractRestoredKeyData extracts data from a single restored ledger key
func extractRestoredKeyData(ledgerKey xdr.LedgerKey, ledgerSeq uint32, durability string, restoredFromLedger uint32, closedAt time.Time) (result *RestoredKeyData) {
	// Recover from any panics due to nil pointer access in XDR structures
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in extractRestoredKeyData: %v (ledger %d, key type %v)", r, ledgerSeq, ledgerKey.Type)
			result = nil
		}
	}()

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
		ContractID:          contractID,
		KeyType:             keyType,
		Durability:          durability,
		RestoredFromLedger:  restoredFromLedger,

		// Metadata (3 fields)
		ClosedAt:    closedAt,
		LedgerRange: ledgerRange,
		CreatedAt:   now,
	}

	return &data
}
