package main

import "strings"

type OpenZeppelinDetector struct{}

func (d *OpenZeppelinDetector) Name() string     { return "openzeppelin" }
func (d *OpenZeppelinDetector) Type() WalletType { return WalletTypeOpenZeppelin }

var ozSignerFunctions = map[string]bool{
	"add_signer": true, "remove_signer": true, "set_signer": true, "get_signers": true,
	"add_guardian": true, "remove_guardian": true, "recover": true, "set_threshold": true,
}

var ozStoragePatterns = []string{"owner", "guardian", "threshold", "recovery"}

func (d *OpenZeppelinDetector) Match(evidence WalletEvidence) bool {
	if !evidence.HasCheckAuth {
		return false
	}
	for _, fn := range evidence.ObservedFunctions {
		if ozSignerFunctions[fn] {
			return true
		}
	}
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
	result := &WalletDetectionResult{WalletType: WalletTypeOpenZeppelin, Confidence: 0.85}
	for _, fn := range evidence.ObservedFunctions {
		if ozSignerFunctions[fn] {
			result.Confidence = 0.9
		}
	}
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
				ID: entry.KeyHash, KeyType: signerType, RawValue: walletTruncate(entry.DataValue, 200),
			})
		}
	}
	return result
}

func hasCrossmintTypeTag(lower string) bool {
	return strings.Contains(lower, "ed25519") || strings.Contains(lower, "secp256k1") ||
		strings.Contains(lower, "secp256r1") || strings.Contains(lower, "webauthn") ||
		strings.Contains(lower, "passkey")
}
