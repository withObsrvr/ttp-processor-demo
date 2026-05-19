package main

import "testing"

func TestIsSQLIdentifier(t *testing.T) {
	valid := []string{"testnet_catalog", "mainnet1", "_catalog"}
	for _, name := range valid {
		if !isSQLIdentifier(name) {
			t.Fatalf("isSQLIdentifier(%q) = false, want true", name)
		}
	}

	invalid := []string{"", "1catalog", "testnet-catalog", "testnet catalog", "cat';DROP"}
	for _, name := range invalid {
		if isSQLIdentifier(name) {
			t.Fatalf("isSQLIdentifier(%q) = true, want false", name)
		}
	}
}
