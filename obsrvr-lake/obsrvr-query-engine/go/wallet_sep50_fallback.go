package main

import "strings"

type SEP50FallbackDetector struct{}

func (d *SEP50FallbackDetector) Name() string     { return "sep50_fallback" }
func (d *SEP50FallbackDetector) Type() WalletType { return WalletTypeSEP50Generic }

func (d *SEP50FallbackDetector) Match(evidence WalletEvidence) bool {
	if evidence.HasCheckAuth {
		return true
	}
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		if strings.Contains(lower, "signer") || strings.Contains(lower, "policy") {
			return true
		}
	}
	return false
}

func (d *SEP50FallbackDetector) Extract(evidence WalletEvidence) *WalletDetectionResult {
	result := &WalletDetectionResult{WalletType: WalletTypeSEP50Generic, Confidence: 0.7}
	if evidence.HasCheckAuth {
		result.Confidence = 0.8
	}
	for _, entry := range evidence.InstanceStorage {
		lower := strings.ToLower(entry.DataValue)
		if strings.Contains(lower, "signer") || strings.Contains(lower, "policy") || strings.Contains(lower, "auth") {
			result.Signers = append(result.Signers, WalletSignerInfo{
				ID: entry.KeyHash, KeyType: "unknown", RawValue: walletTruncate(entry.DataValue, 200),
			})
		}
	}
	for _, entry := range evidence.InstanceStorage {
		if strings.Contains(strings.ToLower(entry.DataValue), "policy") {
			result.Policies = append(result.Policies, walletTruncate(entry.DataValue, 100))
		}
	}
	return result
}
