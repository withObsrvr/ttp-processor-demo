package main

import (
	"fmt"
	"sort"
	"strings"
)

var transformerKnownWalletImplementations = map[string]transformerWalletType{
	// OpenZeppelin stellar-contracts multisig account example/account implementation,
	// built locally from /home/tillman/Documents/stellar-contracts @ v0.7.1.
	"28e1e11f3f75b9385ff026d13f9d422592dde73c4f130d168465916349acbbbc": twOpenZeppelin,

	// OpenZeppelin smart-account implementation observed on testnet.
	"8537b8166c0078440a5324c12f6db48d6340d157c306a54c5ea81405abcc2611": twOpenZeppelin,

	// Crossmint stellar-smart-account implementation built locally and fetched
	// directly from testnet wallets.
	"76d2ba826c1b5a7b6cc0aaebe058cc3ffc373c2171f90d63ebb7481a28f577bd": twCrossmint,
}

func normalizeTransformerWasmHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

func transformerKnownWalletTypeByWasmHash(hash string) (transformerWalletType, bool) {
	wt, ok := transformerKnownWalletImplementations[normalizeTransformerWasmHash(hash)]
	return wt, ok
}

func transformerKnownWalletHashesSQLList() string {
	if len(transformerKnownWalletImplementations) == 0 {
		return "''"
	}
	hashes := make([]string, 0, len(transformerKnownWalletImplementations))
	for hash := range transformerKnownWalletImplementations {
		hashes = append(hashes, fmt.Sprintf("'%s'", hash))
	}
	sort.Strings(hashes)
	return strings.Join(hashes, ",")
}
