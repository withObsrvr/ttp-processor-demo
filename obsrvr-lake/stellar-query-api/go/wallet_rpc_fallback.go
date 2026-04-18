package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
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
	Entries []struct {
		DataXDR string `json:"xdr"`
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
	if r == nil || r.url == "" {
		return "", fmt.Errorf("rpc fallback not configured")
	}

	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return "", fmt.Errorf("decode contract id: %w", err)
	}
	if len(raw) != 32 {
		return "", fmt.Errorf("unexpected contract id length: %d", len(raw))
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
		return "", fmt.Errorf("marshal ledger key: %w", err)
	}

	payload := walletRPCRequest{
		JSONRPC: "2.0",
		Method:  "getLedgerEntries",
		Params: walletGetLedgerEntriesParams{
			Keys: []string{keyXDR},
		},
		ID: 1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if r.authHeader != "" {
		req.Header.Set("Authorization", r.authHeader)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("rpc request failed: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp walletRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", fmt.Errorf("decode rpc response: %w", err)
	}
	if rpcResp.Error != nil {
		return "", fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var result walletGetLedgerEntriesResult
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		return "", fmt.Errorf("decode getLedgerEntries result: %w", err)
	}
	if len(result.Entries) == 0 || result.Entries[0].DataXDR == "" {
		return "", fmt.Errorf("contract instance not found")
	}

	var entry xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &entry); err != nil {
		return "", fmt.Errorf("decode ledger entry xdr: %w", err)
	}
	if entry.Type != xdr.LedgerEntryTypeContractData || entry.ContractData == nil {
		return "", fmt.Errorf("unexpected ledger entry type: %s", entry.Type)
	}
	instance, ok := entry.ContractData.Val.GetInstance()
	if !ok {
		return "", fmt.Errorf("contract instance missing executable")
	}
	wasmHash, ok := instance.Executable.GetWasmHash()
	if !ok {
		return "", fmt.Errorf("contract executable is not wasm")
	}
	return fmt.Sprintf("%x", wasmHash[:]), nil
}
