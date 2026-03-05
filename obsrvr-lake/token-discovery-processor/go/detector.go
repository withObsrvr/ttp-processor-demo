package main

import (
	"strings"
)

// SEP-41 core functions - tokens must implement these
var sep41CoreFunctions = map[string]bool{
	"transfer": true,
	"balance":  true,
	"decimals": true,
	"name":     true,
	"symbol":   true,
}

// SEP-41 optional functions
var sep41OptionalFunctions = map[string]bool{
	"approve":       true,
	"allowance":     true,
	"burn":          true,
	"burn_from":     true,
	"transfer_from": true,
	"mint":          true,
	"spendable_balance": true,
	"authorized":    true,
	"set_admin":     true,
	"admin":         true,
}

// AMM/LP token functions
var ammFunctions = map[string]bool{
	"swap":           true,
	"deposit":        true,
	"withdraw":       true,
	"get_reserves":   true,
	"get_pools":      true,
	"pool_id":        true,
	"add_liquidity":  true,
	"remove_liquidity": true,
}

// SAC (Stellar Asset Contract) initialization functions
var sacFunctions = map[string]bool{
	"init":            true,
	"set_authorized":  true,
	"clawback":        true,
	"set_admin":       true,
}

// Detector handles token type detection
type Detector struct {
	config *DiscoveryConfig
}

// NewDetector creates a new token detector
func NewDetector(config *DiscoveryConfig) *Detector {
	return &Detector{config: config}
}

// DetectTokenType analyzes observed functions and determines token type
func (d *Detector) DetectTokenType(observedFunctions []string) DetectionResult {
	sep41Core := 0
	sep41Optional := 0
	ammCount := 0
	sacCount := 0

	// Normalize function names to lowercase
	normalizedFunctions := make([]string, len(observedFunctions))
	for i, fn := range observedFunctions {
		normalizedFunctions[i] = strings.ToLower(fn)
	}

	// Count function matches
	for _, fn := range normalizedFunctions {
		if sep41CoreFunctions[fn] {
			sep41Core++
		}
		if sep41OptionalFunctions[fn] {
			sep41Optional++
		}
		if ammFunctions[fn] {
			ammCount++
		}
		if sacFunctions[fn] {
			sacCount++
		}
	}

	sep41Total := sep41Core + sep41Optional

	// Detection logic with priority:
	// 1. SAC tokens (if has clawback + SEP-41 functions)
	// 2. LP tokens (if has AMM functions)
	// 3. SEP-41 tokens (if has minimum core functions)
	// 4. Unknown

	// Check for SAC pattern: has clawback or set_authorized + SEP-41 functions
	if sacCount > 0 && sep41Total >= d.config.SEP41MinFunctions {
		return DetectionResult{
			TokenType:       "sac",
			DetectionMethod: "sac_pattern",
			Score:           sep41Total,
			IsSAC:           true,
		}
	}

	// Check for LP/AMM pattern
	if ammCount >= 2 {
		return DetectionResult{
			TokenType:       "lp",
			DetectionMethod: "amm_pattern",
			Score:           ammCount,
			LPPoolType:      strPtr("constant_product"),
		}
	}

	// Check for SEP-41 pattern: needs minimum core functions
	if sep41Core >= d.config.SEP41MinFunctions || sep41Total >= d.config.SEP41MinFunctions {
		return DetectionResult{
			TokenType:       "sep41",
			DetectionMethod: "function_calls",
			Score:           sep41Total,
		}
	}

	// Unknown token type
	return DetectionResult{
		TokenType:       "unknown",
		DetectionMethod: "insufficient_data",
		Score:           0,
	}
}

// IsSEP41Function returns true if the function is a SEP-41 function
func (d *Detector) IsSEP41Function(fn string) bool {
	normalized := strings.ToLower(fn)
	return sep41CoreFunctions[normalized] || sep41OptionalFunctions[normalized]
}

// IsAMMFunction returns true if the function is an AMM function
func (d *Detector) IsAMMFunction(fn string) bool {
	normalized := strings.ToLower(fn)
	return ammFunctions[normalized]
}

// CalculateSEP41Score returns the SEP-41 compliance score
func (d *Detector) CalculateSEP41Score(observedFunctions []string) int {
	score := 0
	for _, fn := range observedFunctions {
		normalized := strings.ToLower(fn)
		if sep41CoreFunctions[normalized] {
			score += 2 // Core functions count more
		} else if sep41OptionalFunctions[normalized] {
			score += 1
		}
	}
	return score
}

// Helper function to create string pointer
func strPtr(s string) *string {
	return &s
}
