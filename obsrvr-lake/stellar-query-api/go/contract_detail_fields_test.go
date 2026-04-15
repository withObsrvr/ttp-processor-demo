package main

import "testing"

func TestNormalizeContractStorageType(t *testing.T) {
	tests := []struct {
		name       string
		durability string
		want       string
	}{
		{name: "persistent canonical", durability: "persistent", want: "persistent"},
		{name: "temporary canonical", durability: "temporary", want: "temporary"},
		{name: "instance canonical", durability: "instance", want: "instance"},
		{name: "persistent enum", durability: "ContractDataDurabilityPersistent", want: "persistent"},
		{name: "temporary enum", durability: "ContractDataDurabilityTemporary", want: "temporary"},
		{name: "instance enum", durability: "ContractDataDurabilityInstance", want: "instance"},
		{name: "unknown passthrough", durability: "weird_value", want: "weird_value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeContractStorageType(tt.durability)
			if got != tt.want {
				t.Fatalf("normalizeContractStorageType(%q) = %q, want %q", tt.durability, got, tt.want)
			}
		})
	}
}

func TestContractMetadataHasSufficientDetail(t *testing.T) {
	creator := "GABC"
	wasmHash := "abc123"
	createdAt := "2026-01-01T00:00:00Z"
	createdLedger := int64(123)

	tests := []struct {
		name string
		resp *ContractMetadataResponse
		want bool
	}{
		{name: "nil response", resp: nil, want: false},
		{name: "empty response", resp: &ContractMetadataResponse{}, want: false},
		{name: "creator only", resp: &ContractMetadataResponse{CreatorAddress: &creator}, want: true},
		{name: "wasm only", resp: &ContractMetadataResponse{WasmHash: &wasmHash}, want: true},
		{name: "created ledger only", resp: &ContractMetadataResponse{CreatedLedger: &createdLedger}, want: true},
		{name: "created at only", resp: &ContractMetadataResponse{CreatedAt: &createdAt}, want: true},
		{name: "persistent entries only", resp: &ContractMetadataResponse{PersistentEntries: 1}, want: true},
		{name: "total entries only", resp: &ContractMetadataResponse{TotalEntries: 1}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contractMetadataHasSufficientDetail(tt.resp)
			if got != tt.want {
				t.Fatalf("contractMetadataHasSufficientDetail() = %v, want %v", got, tt.want)
			}
		})
	}
}
