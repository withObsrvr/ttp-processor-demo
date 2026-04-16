package main

import "strings"

// OpenZeppelinDetector identifies OpenZeppelin stellar-contracts AccountContract wallets.
//
// OpenZeppelin smart accounts use structured signer management with functions like
// add_signer, remove_signer, and guardians-based recovery. Instance storage may
// contain "owner", "guardian", "signer" keys in a different format than Crossmint.
//
// Detection signals (any one is sufficient):
//   - Observed functions include signer management (add_signer, remove_signer, etc.)
//   - Instance storage contains owner/guardian patterns without Crossmint type tags
//   - __check_auth events present (strongest signal, boosts confidence)
//
// Rationale: __check_auth is a host-dispatched callback — it never appears as a
// top-level InvokeHostFunction, so contract_invocations_raw cannot prove its
// existence. Requiring HasCheckAuth therefore produced false negatives on any
// wallet whose auth events weren't captured upstream. We accept the admin-
// function surface itself as sufficient evidence.
//
// Reference: https://github.com/OpenZeppelin/stellar-contracts/
type OpenZeppelinDetector struct{}

func (d *OpenZeppelinDetector) Name() string     { return "openzeppelin" }
func (d *OpenZeppelinDetector) Type() WalletType { return WalletTypeOpenZeppelin }

// ozSignerFunctions are function names that indicate OpenZeppelin-style signer management
var ozSignerFunctions = map[string]bool{
	"add_signer":     true,
	"remove_signer":  true,
	"set_signer":     true,
	"get_signers":    true,
	"add_guardian":   true,
	"remove_guardian": true,
	"recover":        true,
	"set_threshold":  true,
}

// ozStoragePatterns are instance storage patterns specific to OpenZeppelin
var ozStoragePatterns = []string{
	"owner",
	"guardian",
	"threshold",
	"recovery",
}

func (d *OpenZeppelinDetector) Match(evidence WalletEvidence) bool {
	// Primary signal: OZ-specific signer-management function in observed_functions.
	for _, fn := range evidence.ObservedFunctions {
		if ozSignerFunctions[fn] {
			return true
		}
	}

	// Secondary signal: OZ-specific storage patterns without Crossmint type tags.
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		for _, pattern := range ozStoragePatterns {
			if strings.Contains(lower, pattern) && !hasCrossmintTypeTag(lower) {
				return true
			}
		}
	}

	return false
}

func (d *OpenZeppelinDetector) Extract(evidence WalletEvidence) *WalletDetectionResult {
	result := &WalletDetectionResult{
		WalletType: WalletTypeOpenZeppelin,
		Confidence: 0.75, // admin-function signature alone
	}

	// Boost confidence when admin functions are present
	for _, fn := range evidence.ObservedFunctions {
		if ozSignerFunctions[fn] {
			result.Confidence = 0.85
			break
		}
	}
	// __check_auth evidence, when available, is strongest
	if evidence.HasCheckAuth {
		result.Confidence = 0.9
	}

	// Extract signer info from storage entries
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		if strings.Contains(lower, "signer") || strings.Contains(lower, "owner") || strings.Contains(lower, "guardian") {
			signerType := "unknown"
			if strings.Contains(lower, "owner") {
				signerType = "owner"
			} else if strings.Contains(lower, "guardian") {
				signerType = "guardian"
			}
			result.Signers = append(result.Signers, WalletSignerInfo{
				ID:       entry.KeyHash,
				KeyType:  signerType,
				RawValue: truncate(entry.DataValue, 200),
			})
		}
	}

	return result
}

// hasCrossmintTypeTag checks if a storage value contains Crossmint-specific signer types
func hasCrossmintTypeTag(lower string) bool {
	return strings.Contains(lower, "ed25519") ||
		strings.Contains(lower, "secp256k1") ||
		strings.Contains(lower, "secp256r1") ||
		strings.Contains(lower, "webauthn") ||
		strings.Contains(lower, "passkey")
}
