package main

import "testing"

func TestApplyRuntimeEnvOverridesEnablesRPCAndArtifactCache(t *testing.T) {
	t.Setenv("RPC_FALLBACK_URL", "https://rpc.example")
	t.Setenv("RPC_FALLBACK_AUTH_HEADER", "Api-Key test")
	t.Setenv("RPC_FALLBACK_TIMEOUT", "12")
	t.Setenv("CONTRACT_ARTIFACT_CACHE_DIR", "/data/contracts")
	t.Setenv("CONTRACT_ARTIFACT_MAX_WASM_BYTES", "123456")
	config := &Config{}

	if err := applyRuntimeEnvOverrides(config); err != nil {
		t.Fatal(err)
	}
	if config.RPCFallback == nil || !config.RPCFallback.Enabled || config.RPCFallback.URL != "https://rpc.example" || config.RPCFallback.TimeoutSeconds != 12 {
		t.Fatalf("unexpected rpc config: %#v", config.RPCFallback)
	}
	if config.ContractArtifacts == nil || config.ContractArtifacts.CacheDirectory != "/data/contracts" || config.ContractArtifacts.MaxWASMBytes != 123456 {
		t.Fatalf("unexpected artifact config: %#v", config.ContractArtifacts)
	}
}

func TestApplyRuntimeEnvOverridesRejectsInvalidLimits(t *testing.T) {
	t.Setenv("CONTRACT_ARTIFACT_MAX_WASM_BYTES", "not-a-number")
	if err := applyRuntimeEnvOverrides(&Config{}); err == nil {
		t.Fatal("expected invalid max wasm bytes error")
	}
}
