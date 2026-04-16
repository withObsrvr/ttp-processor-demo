package main

import "strings"

// knownWalletImplementations maps normalized wasm hashes to wallet implementations.
//
// Add entries here as implementations are verified from a trusted build or
// deployment source. This classifies an implementation once, then automatically
// classifies every contract deployed with the same code hash.
var knownWalletImplementations = map[string]WalletType{
	// OpenZeppelin stellar-contracts multisig account example/account implementation.
	// Built locally from:
	//   repo: /home/tillman/Documents/stellar-contracts
	//   tag:  v0.7.1
	//   package: multisig-account-example
	//   command: stellar contract build --package multisig-account-example
	"28e1e11f3f75b9385ff026d13f9d422592dde73c4f130d168465916349acbbbc": WalletTypeOpenZeppelin,

	// OpenZeppelin smart-account implementation observed on testnet for contract:
	// CBA4GX3ON5AO6NLMFU23AAT76ZX4CI5MD3RZ27NKGCAZRWHUIOBJJ27S
	"8537b8166c0078440a5324c12f6db48d6340d157c306a54c5ea81405abcc2611": WalletTypeOpenZeppelin,

	// Crossmint stellar-smart-account implementation.
	// Built locally from:
	//   repo: /home/tillman/Documents/stellar-smart-account
	//   package: smart-account
	//   command: stellar contract build --package smart-account
	// Also fetched directly from testnet contracts:
	//   CDZZX66G2VXJVMYJRO7RKMXHRNVVZ3WAGGHNDRH2TWOQNZYDYHF5CHJC
	//   CDZZYIVRHAPHPHNOZFAGTU25NTL5G2MXNWYW4Y42IIQIQH56TPNEJF2N
	"76d2ba826c1b5a7b6cc0aaebe058cc3ffc373c2171f90d63ebb7481a28f577bd": WalletTypeCrossmint,
}

func normalizeWasmHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

func knownWalletTypeByWasmHash(hash string) (WalletType, bool) {
	wt, ok := knownWalletImplementations[normalizeWasmHash(hash)]
	return wt, ok
}

func knownWalletHashes() []string {
	hashes := make([]string, 0, len(knownWalletImplementations))
	for hash := range knownWalletImplementations {
		hashes = append(hashes, hash)
	}
	return hashes
}

// WasmHashDetector classifies contracts by known implementation hash.
// This is the highest-confidence path because it identifies the code itself,
// not just behavior that happened to be observed on-chain.
type WasmHashDetector struct{}

func (d *WasmHashDetector) Name() string     { return "wasm_hash" }
func (d *WasmHashDetector) Type() WalletType { return "" }

func (d *WasmHashDetector) Match(evidence WalletEvidence) bool {
	_, ok := knownWalletTypeByWasmHash(evidence.WasmHash)
	return ok
}

func (d *WasmHashDetector) Extract(evidence WalletEvidence) *WalletDetectionResult {
	walletType, ok := knownWalletTypeByWasmHash(evidence.WasmHash)
	if !ok {
		return nil
	}

	confidence := 0.98
	if evidence.HasCheckAuth {
		confidence = 0.99
	}

	return &WalletDetectionResult{
		WalletType: walletType,
		Confidence: confidence,
	}
}
