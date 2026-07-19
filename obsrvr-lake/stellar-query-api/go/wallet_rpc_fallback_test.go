package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestWalletRPCFallbackResolvesCurrentExecutableAndFetchesValidatedCode(t *testing.T) {
	wasm := testContractWASM(t)
	hashBytes := sha256Bytes(wasm)
	var hash xdr.Hash
	copy(hash[:], hashBytes)
	contractRaw := make([]byte, 32)
	contractRaw[31] = 7
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractRaw)
	if err != nil {
		t.Fatal(err)
	}
	var xdrContractID xdr.ContractId
	copy(xdrContractID[:], contractRaw)
	executable, err := xdr.NewContractExecutable(xdr.ContractExecutableTypeContractExecutableWasm, hash)
	if err != nil {
		t.Fatal(err)
	}
	instanceValue, err := xdr.NewScVal(xdr.ScValTypeScvContractInstance, xdr.ScContractInstance{Executable: executable})
	if err != nil {
		t.Fatal(err)
	}
	instanceXDR, err := xdr.MarshalBase64(xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &xdrContractID},
			Key:      xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, Durability: xdr.ContractDataDurabilityPersistent, Val: instanceValue,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	codeXDR, err := xdr.MarshalBase64(xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.ContractCodeEntry{Ext: xdr.ContractCodeEntryExt{V: 0}, Hash: hash, Code: wasm},
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()
		var request walletRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Method != "getLedgerEntries" {
			t.Errorf("method: %s", request.Method)
		}
		entry := instanceXDR
		lastModified := 90
		if call == 2 {
			entry = codeXDR
			lastModified = 80
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": 1,
			"result": map[string]any{"latestLedger": 100, "entries": []map[string]any{{"xdr": entry, "lastModifiedLedgerSeq": lastModified, "liveUntilLedgerSeq": 200}}},
		})
	}))
	defer server.Close()

	client := NewWalletRPCFallback(RPCFallbackConfig{Enabled: true, URL: server.URL, TimeoutSeconds: 2})
	reference, err := client.ResolveContractExecutable(t.Context(), contractID)
	if err != nil {
		t.Fatalf("ResolveContractExecutable: %v", err)
	}
	if reference.Type != "wasm" || reference.WasmHash != hexBytes(hashBytes) || reference.ResolvedAtLedger != 100 || reference.InstanceLedger != 90 {
		t.Fatalf("unexpected executable: %#v", reference)
	}
	code, err := client.FetchContractCode(t.Context(), reference.WasmHash)
	if err != nil {
		t.Fatalf("FetchContractCode: %v", err)
	}
	if string(code.WASM) != string(wasm) || code.CodeLedger != 80 {
		t.Fatalf("unexpected code: %#v", code)
	}
}

func TestWalletRPCFallbackRecognizesStellarAssetExecutable(t *testing.T) {
	contractRaw := make([]byte, 32)
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractRaw)
	if err != nil {
		t.Fatal(err)
	}
	var xdrContractID xdr.ContractId
	copy(xdrContractID[:], contractRaw)
	executable, err := xdr.NewContractExecutable(xdr.ContractExecutableTypeContractExecutableStellarAsset, nil)
	if err != nil {
		t.Fatal(err)
	}
	instanceValue, err := xdr.NewScVal(xdr.ScValTypeScvContractInstance, xdr.ScContractInstance{Executable: executable})
	if err != nil {
		t.Fatal(err)
	}
	instanceXDR, err := xdr.MarshalBase64(xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &xdrContractID},
			Key:      xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, Durability: xdr.ContractDataDurabilityPersistent, Val: instanceValue,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"latestLedger": 100, "entries": []map[string]any{{"xdr": instanceXDR}}}})
	}))
	defer server.Close()
	client := NewWalletRPCFallback(RPCFallbackConfig{Enabled: true, URL: server.URL})
	reference, err := client.ResolveContractExecutable(t.Context(), contractID)
	if err != nil {
		t.Fatal(err)
	}
	if reference.Type != "stellar_asset" || reference.WasmHash != "" {
		t.Fatalf("unexpected SAC executable: %#v", reference)
	}
}

func TestWalletRPCFallbackDistinguishesInvalidAndMissingContracts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": 1,
			"result": map[string]any{"latestLedger": 100, "entries": []map[string]any{}},
		})
	}))
	defer server.Close()
	client := NewWalletRPCFallback(RPCFallbackConfig{Enabled: true, URL: server.URL})

	if _, err := client.ResolveContractExecutable(t.Context(), "not-a-contract"); !errors.Is(err, ErrInvalidContractID) {
		t.Fatalf("expected ErrInvalidContractID, got %v", err)
	}
	contractID, err := strkey.Encode(strkey.VersionByteContract, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ResolveContractExecutable(t.Context(), contractID); !errors.Is(err, ErrContractNotFound) {
		t.Fatalf("expected ErrContractNotFound, got %v", err)
	}
}

func sha256Bytes(value []byte) []byte {
	hash := sha256.Sum256(value)
	return hash[:]
}

func hexBytes(value []byte) string {
	return hex.EncodeToString(value)
}
