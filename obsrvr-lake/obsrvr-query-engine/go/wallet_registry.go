package main

// WalletDetectorRegistry holds detectors in priority order.
type WalletDetectorRegistry struct {
	detectors []WalletDetector
}

func NewWalletDetectorRegistry() *WalletDetectorRegistry {
	return &WalletDetectorRegistry{
		detectors: []WalletDetector{
			&CrossmintDetector{},
			&OpenZeppelinDetector{},
			&SEP50FallbackDetector{},
		},
	}
}

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
