package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// walletRPCFallback is initialized at startup when rpc_fallback config is present.
var walletRPCFallback *WalletRPCFallback

type WalletRPCFallback struct {
	url        string
	authHeader string
	client     *http.Client
}

type walletRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      int    `json:"id"`
}

type walletRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    any    `json:"data,omitempty"`
	} `json:"error,omitempty"`
	ID int `json:"id"`
}

type walletGetLedgerEntriesParams struct {
	Keys []string `json:"keys"`
}

type walletGetLedgerEntriesResult struct {
	LatestLedger int64 `json:"latestLedger"`
	Entries      []struct {
		DataXDR               string `json:"xdr"`
		LastModifiedLedgerSeq int64  `json:"lastModifiedLedgerSeq"`
		LiveUntilLedgerSeq    *int64 `json:"liveUntilLedgerSeq,omitempty"`
	} `json:"entries"`
}

func NewWalletRPCFallback(cfg RPCFallbackConfig) *WalletRPCFallback {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &WalletRPCFallback{
		url:        strings.TrimSpace(cfg.URL),
		authHeader: strings.TrimSpace(cfg.AuthHeader),
		client:     &http.Client{Timeout: timeout},
	}
}

func (r *WalletRPCFallback) LookupWasmHash(ctx context.Context, contractID string) (string, error) {
	executable, err := r.ResolveContractExecutable(ctx, contractID)
	if err != nil {
		return "", err
	}
	if executable.Type != "wasm" {
		return "", fmt.Errorf("contract executable is not wasm")
	}
	return executable.WasmHash, nil
}

func (r *WalletRPCFallback) ResolveContractExecutable(ctx context.Context, contractID string) (ContractExecutableReference, error) {
	if r == nil || r.url == "" {
		return ContractExecutableReference{}, fmt.Errorf("rpc fallback not configured")
	}

	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return ContractExecutableReference{}, fmt.Errorf("%w: %v", ErrInvalidContractID, err)
	}
	if len(raw) != 32 {
		return ContractExecutableReference{}, fmt.Errorf("%w: unexpected decoded length %d", ErrInvalidContractID, len(raw))
	}

	var contractIDHash xdr.ContractId
	copy(contractIDHash[:], raw)

	ledgerKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractIDHash,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvLedgerKeyContractInstance,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}

	keyXDR, err := xdr.MarshalBase64(ledgerKey)
	if err != nil {
		return ContractExecutableReference{}, fmt.Errorf("marshal contract instance ledger key: %w", err)
	}
	result, err := r.getLedgerEntries(ctx, []string{keyXDR})
	if err != nil {
		return ContractExecutableReference{}, err
	}
	if len(result.Entries) == 0 || result.Entries[0].DataXDR == "" {
		return ContractExecutableReference{}, ErrContractNotFound
	}

	entryResult := result.Entries[0]
	var entry xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(entryResult.DataXDR, &entry); err != nil {
		return ContractExecutableReference{}, fmt.Errorf("decode contract instance ledger entry xdr: %w", err)
	}
	if entry.Type != xdr.LedgerEntryTypeContractData || entry.ContractData == nil {
		return ContractExecutableReference{}, fmt.Errorf("unexpected contract instance ledger entry type: %s", entry.Type)
	}
	instance, ok := entry.ContractData.Val.GetInstance()
	if !ok {
		return ContractExecutableReference{}, errors.New("contract instance is missing its executable")
	}
	reference := ContractExecutableReference{
		InstanceLedger:   entryResult.LastModifiedLedgerSeq,
		LiveUntilLedger:  entryResult.LiveUntilLedgerSeq,
		ResolvedAtLedger: result.LatestLedger,
	}
	switch instance.Executable.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		wasmHash, ok := instance.Executable.GetWasmHash()
		if !ok {
			return ContractExecutableReference{}, errors.New("contract wasm executable is missing its hash")
		}
		reference.Type = "wasm"
		reference.WasmHash = hex.EncodeToString(wasmHash[:])
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		reference.Type = "stellar_asset"
	default:
		return ContractExecutableReference{}, fmt.Errorf("unsupported contract executable type: %s", instance.Executable.Type)
	}
	return reference, nil
}

func (r *WalletRPCFallback) FetchContractCode(ctx context.Context, wasmHash string) (ContractCodeReference, error) {
	if r == nil || r.url == "" {
		return ContractCodeReference{}, fmt.Errorf("rpc fallback not configured")
	}
	normalizedHash, err := normalizeContractWasmHash(wasmHash)
	if err != nil {
		return ContractCodeReference{}, err
	}
	rawHash, _ := hex.DecodeString(normalizedHash)
	var hash xdr.Hash
	copy(hash[:], rawHash)
	ledgerKey := xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	}
	keyXDR, err := xdr.MarshalBase64(ledgerKey)
	if err != nil {
		return ContractCodeReference{}, fmt.Errorf("marshal contract code ledger key: %w", err)
	}
	result, err := r.getLedgerEntries(ctx, []string{keyXDR})
	if err != nil {
		return ContractCodeReference{}, err
	}
	if len(result.Entries) == 0 || result.Entries[0].DataXDR == "" {
		return ContractCodeReference{}, ErrContractCodeAbsent
	}
	entryResult := result.Entries[0]
	var entry xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(entryResult.DataXDR, &entry); err != nil {
		return ContractCodeReference{}, fmt.Errorf("decode contract code ledger entry xdr: %w", err)
	}
	if entry.Type != xdr.LedgerEntryTypeContractCode || entry.ContractCode == nil {
		return ContractCodeReference{}, fmt.Errorf("unexpected contract code ledger entry type: %s", entry.Type)
	}
	entryHash := hex.EncodeToString(entry.ContractCode.Hash[:])
	if entryHash != normalizedHash {
		return ContractCodeReference{}, fmt.Errorf("contract code ledger entry hash %s does not match requested hash %s", entryHash, normalizedHash)
	}
	wasm := append([]byte(nil), entry.ContractCode.Code...)
	if err := validateWasmHash(normalizedHash, wasm); err != nil {
		return ContractCodeReference{}, err
	}
	return ContractCodeReference{
		WasmHash: normalizedHash, WASM: wasm, CodeLedger: entryResult.LastModifiedLedgerSeq,
		LiveUntilLedger: entryResult.LiveUntilLedgerSeq, ResolvedAtLedger: result.LatestLedger,
	}, nil
}

func (r *WalletRPCFallback) getLedgerEntries(ctx context.Context, keys []string) (walletGetLedgerEntriesResult, error) {
	var result walletGetLedgerEntriesResult

	payload := walletRPCRequest{
		JSONRPC: "2.0",
		Method:  "getLedgerEntries",
		Params: walletGetLedgerEntriesParams{
			Keys: keys,
		},
		ID: 1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return result, fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return result, fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if r.authHeader != "" {
		req.Header.Set("Authorization", r.authHeader)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return result, fmt.Errorf("rpc request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return result, fmt.Errorf("rpc returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}

	var rpcResp walletRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return result, fmt.Errorf("decode rpc response: %w", err)
	}
	if rpcResp.Error != nil {
		return result, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		return result, fmt.Errorf("decode getLedgerEntries result: %w", err)
	}
	return result, nil
}
