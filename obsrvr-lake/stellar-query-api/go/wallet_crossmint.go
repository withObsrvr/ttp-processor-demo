package main

import "strings"

// CrossmintDetector identifies Crossmint stellar-smart-account contracts.
//
// Crossmint smart accounts store signers in instance storage with type tags
// like "Ed25519", "Secp256k1", "WebAuthn"/"Passkey". The contract implements
// __check_auth for custom authentication and supports multi-signer policies.
//
// Detection signals:
//   - __check_auth events present (required)
//   - Instance storage contains signer entries with key type identifiers
//
// Reference: https://github.com/Crossmint/stellar-smart-account
type CrossmintDetector struct{}

func (d *CrossmintDetector) Name() string        { return "crossmint" }
func (d *CrossmintDetector) Type() WalletType    { return WalletTypeCrossmint }

// crossmintSignerTypes are the key type identifiers used in Crossmint signer storage
var crossmintSignerTypes = []string{
	"Ed25519",
	"Secp256k1",
	"Secp256r1",
	"WebAuthn",
	"Passkey",
}

func (d *CrossmintDetector) Match(evidence WalletEvidence) bool {
	if !evidence.HasCheckAuth {
		return false
	}
	// Look for Crossmint-specific signer type tags in instance storage
	for _, entry := range evidence.InstanceStorage {
		for _, sigType := range crossmintSignerTypes {
			if strings.Contains(entry.DataValue, sigType) {
				return true
			}
		}
	}
	return false
}

func (d *CrossmintDetector) Extract(evidence WalletEvidence) *WalletDetectionResult {
	result := &WalletDetectionResult{
		WalletType: WalletTypeCrossmint,
		Confidence: 0.9,
	}

	for _, entry := range evidence.InstanceStorage {
		for _, sigType := range crossmintSignerTypes {
			if strings.Contains(entry.DataValue, sigType) {
				signer := WalletSignerInfo{
					ID:       entry.KeyHash,
					KeyType:  classifyCrossmintSignerType(entry.DataValue),
					RawValue: truncate(entry.DataValue, 200),
				}
				result.Signers = append(result.Signers, signer)
				break // one signer per storage entry
			}
		}
	}

	if len(result.Signers) > 0 {
		result.Confidence = 0.95
	}

	return result
}

func classifyCrossmintSignerType(dataValue string) string {
	lower := strings.ToLower(dataValue)
	switch {
	case strings.Contains(lower, "webauthn") || strings.Contains(lower, "passkey"):
		return "webauthn"
	case strings.Contains(lower, "secp256k1"):
		return "secp256k1"
	case strings.Contains(lower, "secp256r1"):
		return "secp256r1"
	case strings.Contains(lower, "ed25519"):
		return "ed25519"
	default:
		return "unknown"
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
