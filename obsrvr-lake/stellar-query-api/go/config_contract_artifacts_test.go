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

func TestApplyRuntimeEnvOverridesPreservesUnspecifiedRPCValues(t *testing.T) {
	t.Setenv("RPC_FALLBACK_URL", "https://rpc.env.example")
	t.Setenv("RPC_FALLBACK_AUTH_HEADER", "")
	t.Setenv("RPC_FALLBACK_TIMEOUT", "")
	config := &Config{RPCFallback: &RPCFallbackConfig{
		Enabled: true, URL: "https://rpc.yaml.example", AuthHeader: "Api-Key yaml", TimeoutSeconds: 5,
	}}

	if err := applyRuntimeEnvOverrides(config); err != nil {
		t.Fatal(err)
	}
	if config.RPCFallback.URL != "https://rpc.env.example" || config.RPCFallback.AuthHeader != "Api-Key yaml" || config.RPCFallback.TimeoutSeconds != 5 {
		t.Fatalf("unexpected rpc config: %#v", config.RPCFallback)
	}
}

func TestApplyRuntimeEnvOverridesAppliesRPCSecretsIndependently(t *testing.T) {
	t.Setenv("RPC_FALLBACK_URL", "")
	t.Setenv("RPC_FALLBACK_AUTH_HEADER", "Api-Key injected")
	t.Setenv("RPC_FALLBACK_TIMEOUT", "12")
	config := &Config{RPCFallback: &RPCFallbackConfig{
		Enabled: true, URL: "https://rpc.yaml.example", AuthHeader: "Api-Key yaml", TimeoutSeconds: 5,
	}}

	if err := applyRuntimeEnvOverrides(config); err != nil {
		t.Fatal(err)
	}
	if config.RPCFallback.URL != "https://rpc.yaml.example" || config.RPCFallback.AuthHeader != "Api-Key injected" || config.RPCFallback.TimeoutSeconds != 12 {
		t.Fatalf("unexpected rpc config: %#v", config.RPCFallback)
	}
}

func TestApplyRuntimeEnvOverridesRejectsInvalidRPCTimeoutWithoutURLOverride(t *testing.T) {
	t.Setenv("RPC_FALLBACK_URL", "")
	t.Setenv("RPC_FALLBACK_TIMEOUT", "not-a-number")
	config := &Config{RPCFallback: &RPCFallbackConfig{Enabled: true, URL: "https://rpc.yaml.example"}}

	if err := applyRuntimeEnvOverrides(config); err == nil {
		t.Fatal("expected invalid RPC timeout error")
	}
}
