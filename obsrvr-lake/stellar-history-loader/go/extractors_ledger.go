package main

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractLedgers extracts ledger-level metadata from a single LedgerCloseMeta.
// Returns a slice with exactly one LedgerRowData element per ledger.
func extractLedgers(lcm xdr.LedgerCloseMeta, networkPassphrase string, ledgerSeq uint32, closedAt time.Time, ledgerRange uint32) ([]LedgerRowData, error) {
	// Get ledger header based on version
	var header xdr.LedgerHeaderHistoryEntry
	switch lcm.V {
	case 0:
		header = lcm.MustV0().LedgerHeader
	case 1:
		header = lcm.MustV1().LedgerHeader
	case 2:
		header = lcm.MustV2().LedgerHeader
	default:
		return nil, fmt.Errorf("unknown LedgerCloseMeta version: %d", lcm.V)
	}

	data := LedgerRowData{
		Sequence:           ledgerSeq,
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           closedAt,
		ProtocolVersion:    uint32(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            uint32(header.Header.BaseFee),
		BaseReserve:        uint32(header.Header.BaseReserve),
		MaxTxSetSize:       uint32(header.Header.MaxTxSetSize),
		IngestionTimestamp:  time.Now().UTC(),
		LedgerRange:        ledgerRange,
		PipelineVersion:    Version,
	}

	// Count transactions and operations based on LedgerCloseMeta version
	var txCount uint32
	var failedCount uint32
	var operationCount uint32
	var txSetOperationCount uint32

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = uint32(len(v0.TxSet.Txs))
		for _, tx := range v0.TxSet.Txs {
			opCount := uint32(len(tx.Operations()))
			txSetOperationCount += opCount
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	data.TransactionCount = int(txCount)
	data.SuccessfulTxCount = int(txCount - failedCount)
	data.FailedTxCount = int(failedCount)
	data.OperationCount = int(operationCount)
	data.TxSetOperationCount = int(txSetOperationCount)

	return []LedgerRowData{data}, nil
}
