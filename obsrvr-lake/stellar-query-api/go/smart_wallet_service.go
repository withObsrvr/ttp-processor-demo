package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func (h *SmartWalletHandlers) GetSmartWalletInfo(ctx context.Context, contractID string) (*SmartWalletInfo, error) {
	if info, err := h.getSmartWalletFromSemantic(ctx, contractID); err == nil && info.IsSmartWallet {
		return info, nil
	}
	evidence, err := h.assembleWalletEvidence(ctx, contractID)
	if err != nil {
		return &SmartWalletInfo{ContractID: contractID}, nil
	}
	registry := NewWalletDetectorRegistry()
	result := registry.Detect(*evidence)
	return mapDetectionResult(contractID, evidence, result), nil
}

func (h *SmartWalletHandlers) getSmartWalletFromSemantic(ctx context.Context, contractID string) (*SmartWalletInfo, error) {
	if h.hotReader == nil {
		return nil, fmt.Errorf("no hot reader")
	}
	var walletType sql.NullString
	var walletSigners sql.NullString
	err := h.hotReader.db.QueryRowContext(ctx, `
		SELECT wallet_type, wallet_signers
		FROM semantic_entities_contracts
		WHERE contract_id = $1 AND wallet_type IS NOT NULL
	`, contractID).Scan(&walletType, &walletSigners)
	if err != nil || !walletType.Valid {
		return nil, fmt.Errorf("no materialized wallet data")
	}
	info := &SmartWalletInfo{
		ContractID:     contractID,
		IsSmartWallet:  true,
		WalletType:     walletType.String,
		Implementation: walletType.String,
		Confidence:     0.9,
	}
	if walletSigners.Valid && walletSigners.String != "" && walletSigners.String != "null" {
		var signers []WalletSignerInfo
		if err := json.Unmarshal([]byte(walletSigners.String), &signers); err == nil {
			info.Signers = signers
			info.SignerCount = len(signers)
		}
	}
	return info, nil
}

func (h *SmartWalletHandlers) assembleWalletEvidence(ctx context.Context, contractID string) (*WalletEvidence, error) {
	evidence := &WalletEvidence{ContractID: contractID}

	if h.hotReader != nil {
		var wasmHash sql.NullString
		if err := h.hotReader.db.QueryRowContext(ctx, `
			SELECT wasm_hash
			FROM contract_metadata
			WHERE contract_id = $1
		`, contractID).Scan(&wasmHash); err == nil && wasmHash.Valid {
			evidence.WasmHash = wasmHash.String
		}
	}
	if evidence.WasmHash == "" && h.coldReader != nil {
		var wasmHash sql.NullString
		query := fmt.Sprintf(`
			SELECT wasm_hash
			FROM %s.%s.contract_metadata
			WHERE contract_id = ?
		`, h.coldReader.catalogName, h.coldReader.schemaName)
		if err := h.coldReader.db.QueryRowContext(ctx, query, contractID).Scan(&wasmHash); err == nil && wasmHash.Valid {
			evidence.WasmHash = wasmHash.String
		}
	}
	if evidence.WasmHash == "" && walletRPCFallback != nil {
		if wasmHash, err := walletRPCFallback.LookupWasmHash(ctx, contractID); err == nil {
			evidence.WasmHash = wasmHash
		}
	}

	// Keep the on-demand path hot-only for now.
	// Smart-wallet detection is fundamentally a current-state/product lookup,
	// and cold scans over large historical tables can be too slow for request-time use.
	// We therefore prefer:
	//   1) materialized semantic_entities_contracts
	//   2) hot silver contract_invocations_raw / contract_data_current
	// and skip cold evidence gathering here.
	funcSeen := map[string]bool{}
	if h.hotReader != nil {
		rows, err := h.hotReader.db.QueryContext(ctx, `
			SELECT DISTINCT function_name
			FROM contract_invocations_raw
			WHERE contract_id = $1
			LIMIT 100
		`, contractID)
		if err == nil {
			for rows.Next() {
				var fn string
				if rows.Scan(&fn) == nil && !funcSeen[fn] {
					funcSeen[fn] = true
					evidence.ObservedFunctions = append(evidence.ObservedFunctions, fn)
				}
			}
			rows.Close()
		}
	}

	storageSeen := map[string]bool{}
	if h.hotReader != nil {
		rows, err := h.hotReader.db.QueryContext(ctx, `
			SELECT key_hash, data_value
			FROM contract_data_current
			WHERE contract_id = $1 AND durability = 'instance'
			LIMIT 200
		`, contractID)
		if err == nil {
			for rows.Next() {
				var entry StorageEntry
				var dataValue sql.NullString
				if rows.Scan(&entry.KeyHash, &dataValue) == nil && dataValue.Valid && !storageSeen[entry.KeyHash] {
					storageSeen[entry.KeyHash] = true
					entry.DataValue = dataValue.String
					evidence.InstanceStorage = append(evidence.InstanceStorage, entry)
				}
			}
			rows.Close()
		}
	}

	return evidence, nil
}
