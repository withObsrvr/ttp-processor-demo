package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/stellar/go/xdr"
)

// decodeTxMetaXDR decodes a base64-encoded TransactionMeta XDR and extracts
// balance changes and state changes.
func decodeTxMetaXDR(metaB64 string) ([]TxBalanceChange, []TxStateChange, error) {
	metaBytes, err := base64.StdEncoding.DecodeString(metaB64)
	if err != nil {
		return nil, nil, fmt.Errorf("base64 decode: %w", err)
	}

	var txMeta xdr.TransactionMeta
	if err := txMeta.UnmarshalBinary(metaBytes); err != nil {
		return nil, nil, fmt.Errorf("XDR unmarshal: %w", err)
	}

	var balanceChanges []TxBalanceChange
	var stateChanges []TxStateChange

	// Extract changes based on meta version
	var changeSets []xdr.LedgerEntryChanges

	switch txMeta.V {
	case 0:
		if txMeta.Operations != nil {
			for _, opMeta := range *txMeta.Operations {
				changeSets = append(changeSets, opMeta.Changes)
			}
		}
	case 1:
		if txMeta.V1 != nil {
			for _, opMeta := range txMeta.V1.Operations {
				changeSets = append(changeSets, opMeta.Changes)
			}
		}
	case 2:
		if txMeta.V2 != nil {
			for _, opMeta := range txMeta.V2.Operations {
				changeSets = append(changeSets, opMeta.Changes)
			}
		}
	case 3:
		if txMeta.V3 != nil {
			for _, opMeta := range txMeta.V3.Operations {
				changeSets = append(changeSets, opMeta.Changes)
			}
		}
	}

	// Process each change set
	for _, changes := range changeSets {
		bc, sc := processLedgerEntryChanges(changes)
		balanceChanges = append(balanceChanges, bc...)
		stateChanges = append(stateChanges, sc...)
	}

	// Deduplicate balance changes by aggregating same address+asset
	balanceChanges = deduplicateBalanceChanges(balanceChanges)

	return balanceChanges, stateChanges, nil
}

func processLedgerEntryChanges(changes xdr.LedgerEntryChanges) ([]TxBalanceChange, []TxStateChange) {
	var balanceChanges []TxBalanceChange
	var stateChanges []TxStateChange

	// Pair STATE entries with their subsequent UPDATED/REMOVED entries
	var prevState *xdr.LedgerEntry

	for _, change := range changes {
		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			if change.State != nil {
				entry := *change.State
				prevState = &entry
			}

		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			if change.Updated != nil {
				updated := *change.Updated
				if prevState != nil {
					bc, sc := extractChanges(prevState, &updated, "updated")
					balanceChanges = append(balanceChanges, bc...)
					stateChanges = append(stateChanges, sc...)
				}
				prevState = nil
			}

		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			if change.Created != nil {
				created := *change.Created
				bc, sc := extractChanges(nil, &created, "created")
				balanceChanges = append(balanceChanges, bc...)
				stateChanges = append(stateChanges, sc...)
			}
			prevState = nil

		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			if prevState != nil {
				bc, sc := extractChanges(prevState, nil, "removed")
				balanceChanges = append(balanceChanges, bc...)
				stateChanges = append(stateChanges, sc...)
			}
			prevState = nil
		}
	}

	return balanceChanges, stateChanges
}

func extractChanges(before, after *xdr.LedgerEntry, changeType string) ([]TxBalanceChange, []TxStateChange) {
	var balanceChanges []TxBalanceChange
	var stateChanges []TxStateChange

	// Determine which entry to inspect for type
	entry := after
	if entry == nil {
		entry = before
	}
	if entry == nil {
		return nil, nil
	}

	switch entry.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		bc := extractAccountBalanceChange(before, after, changeType)
		if bc != nil {
			balanceChanges = append(balanceChanges, *bc)
		}

	case xdr.LedgerEntryTypeTrustline:
		bc := extractTrustlineBalanceChange(before, after, changeType)
		if bc != nil {
			balanceChanges = append(balanceChanges, *bc)
		}

	case xdr.LedgerEntryTypeContractData:
		sc := extractContractDataChange(before, after, changeType)
		if sc != nil {
			stateChanges = append(stateChanges, *sc)
		}

	default:
		// Other entry types (offer, data, claimable balance, etc.)
		sc := extractGenericStateChange(before, after, changeType)
		if sc != nil {
			stateChanges = append(stateChanges, *sc)
		}
	}

	return balanceChanges, stateChanges
}

func extractAccountBalanceChange(before, after *xdr.LedgerEntry, changeType string) *TxBalanceChange {
	var beforeBalance int64
	var afterBalance int64
	var address string

	if before != nil {
		acc := before.Data.MustAccount()
		address = acc.AccountId.Address()
		beforeBalance = int64(acc.Balance)
	}
	if after != nil {
		acc := after.Data.MustAccount()
		address = acc.AccountId.Address()
		afterBalance = int64(acc.Balance)
	}

	if beforeBalance == afterBalance && changeType == "updated" {
		return nil
	}

	delta := afterBalance - beforeBalance
	return &TxBalanceChange{
		Address:   address,
		AssetCode: "XLM",
		AssetType: "native",
		Before:    formatStroopsToDecimal(beforeBalance),
		After:     formatStroopsToDecimal(afterBalance),
		Delta:     formatStroopsDelta(delta),
	}
}

func extractTrustlineBalanceChange(before, after *xdr.LedgerEntry, changeType string) *TxBalanceChange {
	var beforeBalance int64
	var afterBalance int64
	var address, assetCode, assetType string

	if before != nil {
		tl := before.Data.MustTrustLine()
		address = tl.AccountId.Address()
		beforeBalance = int64(tl.Balance)
		assetCode, assetType = trustlineAssetInfo(tl.Asset)
	}
	if after != nil {
		tl := after.Data.MustTrustLine()
		address = tl.AccountId.Address()
		afterBalance = int64(tl.Balance)
		assetCode, assetType = trustlineAssetInfo(tl.Asset)
	}

	if beforeBalance == afterBalance && changeType == "updated" {
		return nil
	}

	delta := afterBalance - beforeBalance
	return &TxBalanceChange{
		Address:   address,
		AssetCode: assetCode,
		AssetType: assetType,
		Before:    formatStroopsToDecimal(beforeBalance),
		After:     formatStroopsToDecimal(afterBalance),
		Delta:     formatStroopsDelta(delta),
	}
}

func extractContractDataChange(before, after *xdr.LedgerEntry, changeType string) *TxStateChange {
	entry := after
	if entry == nil {
		entry = before
	}
	cd := entry.Data.MustContractData()

	contractAddr := "unknown"
	if cd.Contract.ContractId != nil {
		contractAddr = hex.EncodeToString((*cd.Contract.ContractId)[:])
	}
	key := fmt.Sprintf("contract:%s", contractAddr)

	var beforeStr, afterStr *string
	if before != nil {
		s := summarizeScVal(before.Data.MustContractData().Val)
		beforeStr = &s
	}
	if after != nil {
		s := summarizeScVal(after.Data.MustContractData().Val)
		afterStr = &s
	}

	return &TxStateChange{
		Type:      changeType,
		EntryType: "contract_data",
		Key:       key,
		Before:    beforeStr,
		After:     afterStr,
	}
}

func extractGenericStateChange(before, after *xdr.LedgerEntry, changeType string) *TxStateChange {
	entry := after
	if entry == nil {
		entry = before
	}

	entryType := strings.ToLower(entry.Data.Type.String())
	key := fmt.Sprintf("%s_entry", entryType)

	return &TxStateChange{
		Type:      changeType,
		EntryType: entryType,
		Key:       key,
	}
}

func trustlineAssetInfo(asset xdr.TrustLineAsset) (string, string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if asset.AlphaNum4 != nil {
			code := strings.TrimRight(string(asset.AlphaNum4.AssetCode[:]), "\x00")
			return code, "credit_alphanum4"
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if asset.AlphaNum12 != nil {
			code := strings.TrimRight(string(asset.AlphaNum12.AssetCode[:]), "\x00")
			return code, "credit_alphanum12"
		}
	case xdr.AssetTypeAssetTypePoolShare:
		return "pool_share", "liquidity_pool"
	}
	return "unknown", "unknown"
}

func summarizeScVal(val xdr.ScVal) string {
	switch val.Type {
	case xdr.ScValTypeScvBool:
		if val.B != nil && *val.B {
			return "true"
		}
		return "false"
	case xdr.ScValTypeScvU32:
		if val.U32 != nil {
			return fmt.Sprintf("%d", *val.U32)
		}
	case xdr.ScValTypeScvI32:
		if val.I32 != nil {
			return fmt.Sprintf("%d", *val.I32)
		}
	case xdr.ScValTypeScvU64:
		if val.U64 != nil {
			return fmt.Sprintf("%d", *val.U64)
		}
	case xdr.ScValTypeScvI64:
		if val.I64 != nil {
			return fmt.Sprintf("%d", *val.I64)
		}
	case xdr.ScValTypeScvSymbol:
		if val.Sym != nil {
			return string(*val.Sym)
		}
	case xdr.ScValTypeScvString:
		if val.Str != nil {
			return string(*val.Str)
		}
	case xdr.ScValTypeScvAddress:
		if val.Address != nil {
			if val.Address.ContractId != nil {
				return fmt.Sprintf("contract:%s", hex.EncodeToString((*val.Address.ContractId)[:]))
			}
			if val.Address.AccountId != nil {
				return val.Address.AccountId.Address()
			}
		}
	default:
		return fmt.Sprintf("<%s>", val.Type.String())
	}
	return fmt.Sprintf("<%s>", val.Type.String())
}

func formatStroopsDelta(stroops int64) string {
	if stroops >= 0 {
		return "+" + formatStroopsToDecimal(stroops)
	}
	return "-" + formatStroopsToDecimal(-stroops)
}

func deduplicateBalanceChanges(changes []TxBalanceChange) []TxBalanceChange {
	type key struct {
		Address   string
		AssetCode string
	}
	seen := make(map[key]int)
	var result []TxBalanceChange

	for _, c := range changes {
		k := key{Address: c.Address, AssetCode: c.AssetCode}
		if idx, ok := seen[k]; ok {
			// Keep the earliest "before" and latest "after"
			result[idx].After = c.After
			result[idx].Delta = c.Delta
		} else {
			seen[k] = len(result)
			result = append(result, c)
		}
	}
	return result
}
