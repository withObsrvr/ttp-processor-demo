package main

// WalletDetectorRegistry holds detectors in priority order.
// More specific detectors (Crossmint, OpenZeppelin) come first;
// the generic SEP-50 fallback comes last.
type WalletDetectorRegistry struct {
	detectors []WalletDetector
}

// NewWalletDetectorRegistry creates a registry with all known detectors.
// Order matters — first match wins.
func NewWalletDetectorRegistry() *WalletDetectorRegistry {
	return &WalletDetectorRegistry{
		detectors: []WalletDetector{
			&CrossmintDetector{},
			&OpenZeppelinDetector{},
			&SEP50FallbackDetector{},
		},
	}
}

// Detect runs detectors in priority order and returns the first match.
// Returns nil if no detector matches (contract is not a smart wallet).
func (reg *WalletDetectorRegistry) Detect(evidence WalletEvidence) *WalletDetectionResult {
	for _, d := range reg.detectors {
		if d.Match(evidence) {
			if result := d.Extract(evidence); result != nil {
				return result
			}
		}
	}
	return nil
}
