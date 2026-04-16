package main

import "strings"

// SEP50FallbackDetector is the generic fallback for any contract that looks
// like a smart wallet but doesn't match a specific implementation.
//
// Match signals (any one is sufficient):
//   - __check_auth events present (strongest)
//   - Instance storage contains signer/policy entries
//   - observed_functions contains a narrow admin-function surface
//     (e.g. allow_signing_key, add_passkey, set_webauthn_verifier)
//
// __check_auth is no longer a hard requirement — it's host-dispatched and
// not recorded in contract_invocations_raw, so gating on it produced false
// negatives for wallets whose auth events weren't captured upstream.
//
// This detector has the lowest priority in the registry — it only fires if
// Crossmint and OpenZeppelin detectors both decline.
type SEP50FallbackDetector struct{}

func (d *SEP50FallbackDetector) Name() string     { return "sep50_fallback" }
func (d *SEP50FallbackDetector) Type() WalletType { return WalletTypeSEP50Generic }

// sep50GenericAdminFunctions are admin-function names we accept as evidence of
// a custom-account / smart-wallet implementation even without __check_auth.
var sep50GenericAdminFunctions = map[string]bool{
	"allow_signing_key":     true,
	"add_passkey":           true,
	"set_webauthn_verifier": true,
	"__check_auth":          true, // host-dispatched, but if it somehow appears
}

func (d *SEP50FallbackDetector) Match(evidence WalletEvidence) bool {
	if evidence.HasCheckAuth {
		return true
	}

	// Admin-function surface signals a custom-account implementation.
	for _, fn := range evidence.ObservedFunctions {
		if sep50GenericAdminFunctions[fn] {
			return true
		}
	}

	// Signer-like or policy-like instance storage (original heuristic).
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		if strings.Contains(lower, "signer") || strings.Contains(lower, "policy") {
			return true
		}
	}

	return false
}

func (d *SEP50FallbackDetector) Extract(evidence WalletEvidence) *WalletDetectionResult {
	result := &WalletDetectionResult{
		WalletType: WalletTypeSEP50Generic,
		Confidence: 0.7,
	}

	if evidence.HasCheckAuth {
		result.Confidence = 0.8
	}

	// Extract any signer-like entries from storage (broad matching)
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		if strings.Contains(lower, "signer") || strings.Contains(lower, "policy") ||
			strings.Contains(lower, "auth") {
			result.Signers = append(result.Signers, WalletSignerInfo{
				ID:       entry.KeyHash,
				KeyType:  "unknown",
				RawValue: truncate(entry.DataValue, 200),
			})
		}
	}

	// Extract policies
	for _, entry := range evidence.InstanceStorage {
		if strings.Contains(strings.ToLower(entry.DataValue), "policy") {
			result.Policies = append(result.Policies, truncate(entry.DataValue, 100))
		}
	}

	return result
}
