package main

import "strings"

// SEP50FallbackDetector is the generic fallback for any contract with __check_auth
// that doesn't match a specific wallet implementation.
//
// This preserves the behavior of the original monolithic detection: any contract
// that emits __check_auth events OR has signer-like instance storage is classified
// as a smart wallet.
//
// This detector has the lowest priority in the registry — it only fires if
// Crossmint and OpenZeppelin detectors both decline.
type SEP50FallbackDetector struct{}

func (d *SEP50FallbackDetector) Name() string     { return "sep50_fallback" }
func (d *SEP50FallbackDetector) Type() WalletType { return WalletTypeSEP50Generic }

func (d *SEP50FallbackDetector) Match(evidence WalletEvidence) bool {
	// Any contract with __check_auth is a smart wallet
	if evidence.HasCheckAuth {
		return true
	}

	// Also match if instance storage has signer-like entries (original heuristic)
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
