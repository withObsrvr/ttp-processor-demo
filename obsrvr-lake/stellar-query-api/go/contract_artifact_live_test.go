package main

import (
	"os"
	"slices"
	"testing"
)

const referenceOracleContractID = "CAYIP67UDVX5UPXGN3XDAWVIEFBAVG6G7LUESEOU3NUQKTWN55W34YBG"

// TestContractArtifactLiveReference is opt-in so the ordinary unit suite is
// deterministic. Operators can run it against a network RPC before rollout:
//
//	CONTRACT_INTERFACE_LIVE_ID=C... CONTRACT_INTERFACE_LIVE_RPC=https://... \
//	  go test -run TestContractArtifactLiveReference
func TestContractArtifactLiveReference(t *testing.T) {
	contractID := os.Getenv("CONTRACT_INTERFACE_LIVE_ID")
	rpcURL := os.Getenv("CONTRACT_INTERFACE_LIVE_RPC")
	if contractID == "" || rpcURL == "" {
		t.Skip("set CONTRACT_INTERFACE_LIVE_ID and CONTRACT_INTERFACE_LIVE_RPC")
	}
	rpc := NewWalletRPCFallback(RPCFallbackConfig{
		Enabled: true, URL: rpcURL, AuthHeader: os.Getenv("CONTRACT_INTERFACE_LIVE_AUTH_HEADER"), TimeoutSeconds: 15,
	})
	store, err := NewFileContractArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service := NewContractArtifactService("testnet", rpc, store, 0)
	response, err := service.Resolve(t.Context(), contractID)
	if err != nil {
		t.Fatalf("resolve live contract interface: %v", err)
	}
	if response.Executable.Type != "wasm" || response.Executable.WasmHash == "" {
		t.Fatalf("unexpected executable: %#v", response.Executable)
	}
	if len(response.Interface.Functions) == 0 {
		t.Fatal("authoritative interface contains no functions")
	}
	if contractID == referenceOracleContractID {
		functionNames := make([]string, 0, len(response.Interface.Functions))
		initHasDocumentation := false
		for _, function := range response.Interface.Functions {
			functionNames = append(functionNames, function.Name)
			if function.Name == "init" && function.Doc != "" {
				initHasDocumentation = true
			}
		}
		for _, expected := range []string{"init", "get_price", "get_price_pers", "set_publishers", "update_bls_agg", "update_via_auth", "update_secp256k1", "update_ed25519_args", "update_ed25519_stored", "update_batch_ed25519_args", "update_ed25519_persistent"} {
			if !slices.Contains(functionNames, expected) {
				t.Fatalf("reference interface missing %s; got %v", expected, functionNames)
			}
		}
		if !hasContractStruct(response.Interface, "PriceEntry") || !hasContractUnion(response.Interface, "DataKey") || !hasContractError(response.Interface, "Error") {
			t.Fatalf("reference interface missing PriceEntry/DataKey/Error: %#v", response.Interface)
		}
		if !initHasDocumentation {
			t.Fatal("reference init function is missing its authoritative documentation")
		}
	}
	wasm, err := service.ResolveWASM(t.Context(), contractID)
	if err != nil {
		t.Fatalf("resolve live contract wasm: %v", err)
	}
	if wasm.WasmHash != response.Executable.WasmHash || len(wasm.WASM) == 0 {
		t.Fatalf("interface/WASM hash mismatch: interface=%s wasm=%s bytes=%d", response.Executable.WasmHash, wasm.WasmHash, len(wasm.WASM))
	}
	if response.Executable.WasmSizeBytes != len(wasm.WASM) {
		t.Fatalf("interface/WASM size mismatch: interface=%d wasm=%d", response.Executable.WasmSizeBytes, len(wasm.WASM))
	}
}

func hasContractStruct(spec ContractSpec, name string) bool {
	return slices.ContainsFunc(spec.Structs, func(item ContractSpecStruct) bool { return item.Name == name })
}

func hasContractUnion(spec ContractSpec, name string) bool {
	return slices.ContainsFunc(spec.Unions, func(item ContractSpecUnion) bool { return item.Name == name })
}

func hasContractError(spec ContractSpec, name string) bool {
	return slices.ContainsFunc(spec.Errors, func(item ContractSpecEnum) bool { return item.Name == name })
}
