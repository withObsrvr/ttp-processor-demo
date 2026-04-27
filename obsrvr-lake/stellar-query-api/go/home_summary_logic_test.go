package main

import (
	"testing"
)

func TestDefaultSorobanConfig(t *testing.T) {
	cfg := defaultSorobanConfig()
	if cfg == nil {
		t.Fatal("expected default config")
	}
	if cfg.Instructions.LedgerMax != 100000000 {
		t.Fatalf("unexpected ledger max: %d", cfg.Instructions.LedgerMax)
	}
	if cfg.LedgerLimits.MaxReadBytes != 200000 {
		t.Fatalf("unexpected max read bytes: %d", cfg.LedgerLimits.MaxReadBytes)
	}
	if cfg.LedgerLimits.MaxWriteBytes != 66560 {
		t.Fatalf("unexpected max write bytes: %d", cfg.LedgerLimits.MaxWriteBytes)
	}
}

func TestSplitProtocolAndContractFallback(t *testing.T) {
	protocol, contract := splitProtocolAndContract("USDC", "C123")
	if protocol != "USDC" || contract != "" {
		t.Fatalf("unexpected fallback split: %q / %q", protocol, contract)
	}
}
