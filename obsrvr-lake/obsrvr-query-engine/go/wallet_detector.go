package main

// WalletType represents a specific smart wallet implementation.
type WalletType string

const (
	WalletTypeCrossmint    WalletType = "crossmint"
	WalletTypeOpenZeppelin WalletType = "openzeppelin"
	WalletTypeSEP50Generic WalletType = "sep50_generic"
)

// WalletSignerInfo describes a signer extracted from on-chain data.
type WalletSignerInfo struct {
	ID       string `json:"id"`
	KeyType  string `json:"key_type,omitempty"`
	Weight   *int   `json:"weight,omitempty"`
	RawValue string `json:"raw_value,omitempty"`
}

// WalletDetectionResult is what a detector returns when it matches.
type WalletDetectionResult struct {
	WalletType WalletType        `json:"wallet_type"`
	Confidence float64           `json:"confidence"`
	Signers    []WalletSignerInfo `json:"signers,omitempty"`
	Policies   []string          `json:"policies,omitempty"`
}

// StorageEntry represents a single contract instance storage entry.
type StorageEntry struct {
	KeyHash   string
	DataValue string
}

// WalletEvidence is the bundle of on-chain data passed to each detector.
type WalletEvidence struct {
	ContractID        string
	ObservedFunctions []string
	HasCheckAuth      bool
	InstanceStorage   []StorageEntry
}

// WalletDetector is the interface each wallet implementation detector must satisfy.
type WalletDetector interface {
	Name() string
	Type() WalletType
	Match(evidence WalletEvidence) bool
	Extract(evidence WalletEvidence) *WalletDetectionResult
}
