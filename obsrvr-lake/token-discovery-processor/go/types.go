package main

import (
	"time"

	"github.com/lib/pq"
)

// DiscoveredToken represents a token discovered from contract activity
type DiscoveredToken struct {
	// Primary key
	ContractID string `json:"contract_id"`

	// Classification
	TokenType       string `json:"token_type"`       // "sep41", "lp", "sac", "unknown"
	DetectionMethod string `json:"detection_method"` // "function_calls", "sac_deploy", "lp_pattern"

	// SEP-41 metadata
	Name     *string `json:"name,omitempty"`
	Symbol   *string `json:"symbol,omitempty"`
	Decimals *int    `json:"decimals,omitempty"`

	// SAC (Stellar Asset Contract) fields
	IsSAC              bool    `json:"is_sac"`
	ClassicAssetCode   *string `json:"classic_asset_code,omitempty"`
	ClassicAssetIssuer *string `json:"classic_asset_issuer,omitempty"`

	// LP token fields
	LPPoolType *string `json:"lp_pool_type,omitempty"` // "constant_product", "stable"
	LPAssetA   *string `json:"lp_asset_a,omitempty"`
	LPAssetB   *string `json:"lp_asset_b,omitempty"`
	LPFeeBPS   *int    `json:"lp_fee_bps,omitempty"`

	// Discovery tracking
	FirstSeenLedger    int64     `json:"first_seen_ledger"`
	LastActivityLedger int64     `json:"last_activity_ledger"`
	DiscoveredAt       time.Time `json:"discovered_at"`
	UpdatedAt          time.Time `json:"updated_at"`

	// Cached stats
	HolderCount   int64   `json:"holder_count"`
	TransferCount int64   `json:"transfer_count"`
	TotalSupply   *string `json:"total_supply,omitempty"`

	// SEP-41 detection
	ObservedFunctions pq.StringArray `json:"observed_functions"`
	SEP41Score        int            `json:"sep41_score"`
}

// ContractActivity represents aggregated contract activity from silver layer
type ContractActivity struct {
	ContractID    string   `json:"contract_id"`
	Functions     []string `json:"functions"`
	FirstSeen     int64    `json:"first_seen"`
	LastSeen      int64    `json:"last_seen"`
	CallCount     int64    `json:"call_count"`
	TransferCount int64    `json:"transfer_count"`
}

// ProcessorStats tracks processor statistics
type ProcessorStats struct {
	LastLedgerProcessed     int64         `json:"last_ledger_processed"`
	LastProcessedAt         time.Time     `json:"last_processed_at"`
	TokensDiscovered        int64         `json:"tokens_discovered"`
	TokensUpdated           int64         `json:"tokens_updated"`
	CyclesCompleted         int64         `json:"cycles_completed"`
	LastCycleDuration       time.Duration `json:"last_cycle_duration"`
	TotalTokensInRegistry   int64         `json:"total_tokens_in_registry"`
	SEP41TokenCount         int64         `json:"sep41_token_count"`
	LPTokenCount            int64         `json:"lp_token_count"`
	SACTokenCount           int64         `json:"sac_token_count"`
	UnknownTokenCount       int64         `json:"unknown_token_count"`
	LastError               string        `json:"last_error,omitempty"`
	ConsecutiveErrorCount   int           `json:"consecutive_error_count"`
}

// DetectionResult holds the result of token type detection
type DetectionResult struct {
	TokenType       string
	DetectionMethod string
	Score           int
	IsSAC           bool
	LPPoolType      *string
}
