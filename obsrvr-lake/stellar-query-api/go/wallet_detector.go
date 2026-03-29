package main

// WalletType represents a specific smart wallet implementation
type WalletType string

const (
	WalletTypeCrossmint    WalletType = "crossmint"
	WalletTypeOpenZeppelin WalletType = "openzeppelin"
	WalletTypeSEP50Generic WalletType = "sep50_generic"
)

// WalletSignerInfo describes a signer extracted from on-chain data
type WalletSignerInfo struct {
	ID       string `json:"id"`
	KeyType  string `json:"key_type,omitempty"`  // "ed25519", "secp256k1", "webauthn", "unknown"
	Weight   *int   `json:"weight,omitempty"`
	RawValue string `json:"raw_value,omitempty"`
}

// WalletDetectionResult is what a detector returns when it matches
type WalletDetectionResult struct {
	WalletType WalletType       `json:"wallet_type"`
	Confidence float64          `json:"confidence"` // 0.0-1.0
	Signers    []WalletSignerInfo `json:"signers,omitempty"`
	Policies   []string         `json:"policies,omitempty"`
}

// StorageEntry represents a single contract instance storage entry
type StorageEntry struct {
	KeyHash   string
	DataValue string
}

// WalletEvidence is the bundle of on-chain data passed to each detector.
// All data gathering happens in the caller — detectors are pure functions.
type WalletEvidence struct {
	ContractID        string
	ObservedFunctions []string       // from contract_invocations_raw
	HasCheckAuth      bool           // from bronze contract_events topics
	InstanceStorage   []StorageEntry // from contract_data_current WHERE durability='instance'
}

// WalletDetector is the interface each wallet implementation detector must satisfy.
// Adding a new wallet type = implementing this interface in a new file.
type WalletDetector interface {
	// Name returns a human-readable identifier (e.g., "crossmint")
	Name() string

	// Type returns the WalletType constant this detector identifies
	Type() WalletType

	// Match returns true if this detector should attempt extraction.
	// Called with lightweight evidence; should be fast.
	Match(evidence WalletEvidence) bool

	// Extract performs full analysis and returns the detection result.
	// Only called if Match returned true. Returns nil if analysis fails.
	Extract(evidence WalletEvidence) *WalletDetectionResult
}
