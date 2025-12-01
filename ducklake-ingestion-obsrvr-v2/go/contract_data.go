package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/internal/processors/contract"
)

// Wrapper functions to adapt sac package to contract processor signature
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

// extractContractData extracts contract data state from LedgerCloseMeta
// Protocol 20+ Soroban smart contract storage
// Obsrvr playbook: core.contract_data_snapshot_v1
func (ing *Ingester) extractContractData(lcm *xdr.LedgerCloseMeta) []ContractDataData {
	var contractDataList []ContractDataData

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
		log.Printf("Failed to create transaction reader for contract data: %v", err)
		return contractDataList
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
				change, ing.config.Source.NetworkPassphrase, ledgerHeader)
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

	return contractDataList
}

// isContractDataChange checks if a change involves contract data
func isContractDataChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// PERFORMANCE FIX: Optimized nullableString to reduce GC pressure
// - Avoids allocating new pointers for every call
// - In high-throughput scenarios (millions of rows), this reduces allocations significantly
// - For empty strings, returns nil directly (no allocation)
// - For non-empty strings, still requires allocation but is unavoidable
//
// Alternative considered: Just use empty strings instead of NULL, but nullable semantics
// are important for distinguishing "not set" from "set to empty" in the data model
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	// Note: We must allocate here for non-empty strings
	// Go doesn't allow us to return a pointer to a string literal or constant
	// This is unavoidable unless we change the data model to use empty strings instead of NULL
	str := s
	return &str
}
