package main

import (
	"fmt"
	"strings"
)

// TxSummary represents a human-readable summary of a transaction
type TxSummary struct {
	Description       string   `json:"description"`
	Type              string   `json:"type"` // transfer, mint, burn, swap, contract_call, classic
	InvolvedContracts []string `json:"involved_contracts,omitempty"`
}

// GenerateTxSummary creates a human-readable summary from decoded operations and events.
// Pattern matches known operation types to produce summaries like:
// - "Transferred 100 USDC to GXYZ..."
// - "Minted 100 tokens to GXYZ..."
// - "Called swap on contract CABCD..."
func GenerateTxSummary(ops []DecodedOperation, events []UnifiedEvent) TxSummary {
	if len(ops) == 0 && len(events) == 0 {
		return TxSummary{Description: "Empty transaction", Type: "unknown"}
	}

	var contracts []string
	for _, op := range ops {
		if op.ContractID != nil {
			contracts = appendUnique(contracts, *op.ContractID)
		}
	}

	// Single event transactions — straightforward summaries
	if len(events) == 1 {
		e := events[0]
		return summarizeSingleEvent(e, contracts)
	}

	// Multiple events — check for swap pattern
	if len(events) == 2 {
		if summary := detectSwapPattern(events, contracts); summary != nil {
			return *summary
		}
	}

	// Single operation with function name
	if len(ops) == 1 && ops[0].FunctionName != nil {
		op := ops[0]
		contractDisplay := "contract"
		if op.ContractID != nil {
			contractDisplay = abbreviateAddr(*op.ContractID)
		}
		return TxSummary{
			Description:       fmt.Sprintf("Called %s on %s", *op.FunctionName, contractDisplay),
			Type:              "contract_call",
			InvolvedContracts: contracts,
		}
	}

	// Multiple operations — summarize count
	if len(ops) > 1 {
		sorobanCount := 0
		for _, op := range ops {
			if op.IsSorobanOp {
				sorobanCount++
			}
		}
		if sorobanCount > 0 {
			return TxSummary{
				Description:       fmt.Sprintf("Transaction with %d operations (%d Soroban)", len(ops), sorobanCount),
				Type:              "multi_op",
				InvolvedContracts: contracts,
			}
		}
		return TxSummary{
			Description: fmt.Sprintf("Transaction with %d operations", len(ops)),
			Type:        "multi_op",
		}
	}

	// Fallback: summarize from events
	if len(events) > 0 {
		transferCount := 0
		for _, e := range events {
			if e.EventType == "transfer" {
				transferCount++
			}
		}
		if transferCount > 0 {
			return TxSummary{
				Description:       fmt.Sprintf("Transaction with %d transfers", transferCount),
				Type:              "multi_transfer",
				InvolvedContracts: contracts,
			}
		}
	}

	return TxSummary{Description: "Transaction", Type: "unknown"}
}

func summarizeSingleEvent(e UnifiedEvent, contracts []string) TxSummary {
	amountStr := ""
	if e.Amount != nil {
		amountStr = *e.Amount
	}
	assetStr := ""
	if e.AssetCode != nil {
		assetStr = " " + *e.AssetCode
	}

	switch e.EventType {
	case "transfer":
		toAddr := "unknown"
		if e.To != nil {
			toAddr = abbreviateAddr(*e.To)
		}
		return TxSummary{
			Description:       fmt.Sprintf("Transferred %s%s to %s", amountStr, assetStr, toAddr),
			Type:              "transfer",
			InvolvedContracts: contracts,
		}
	case "mint":
		toAddr := "unknown"
		if e.To != nil {
			toAddr = abbreviateAddr(*e.To)
		}
		return TxSummary{
			Description:       fmt.Sprintf("Minted %s%s to %s", amountStr, assetStr, toAddr),
			Type:              "mint",
			InvolvedContracts: contracts,
		}
	case "burn":
		fromAddr := "unknown"
		if e.From != nil {
			fromAddr = abbreviateAddr(*e.From)
		}
		return TxSummary{
			Description:       fmt.Sprintf("Burned %s%s from %s", amountStr, assetStr, fromAddr),
			Type:              "burn",
			InvolvedContracts: contracts,
		}
	default:
		return TxSummary{
			Description:       fmt.Sprintf("Token event: %s", e.EventType),
			Type:              e.EventType,
			InvolvedContracts: contracts,
		}
	}
}

func detectSwapPattern(events []UnifiedEvent, contracts []string) *TxSummary {
	// Swap pattern: one transfer in, one transfer out, same address involved
	e0, e1 := events[0], events[1]
	if e0.EventType != "transfer" || e1.EventType != "transfer" {
		return nil
	}

	// Check if same address is sender of one and receiver of another
	if e0.From != nil && e1.To != nil && *e0.From == *e1.To {
		return buildSwapSummary(e0, e1, contracts)
	}
	if e1.From != nil && e0.To != nil && *e1.From == *e0.To {
		return buildSwapSummary(e1, e0, contracts)
	}
	return nil
}

func buildSwapSummary(outgoing, incoming UnifiedEvent, contracts []string) *TxSummary {
	outAmt, inAmt := "", ""
	if outgoing.Amount != nil {
		outAmt = *outgoing.Amount
	}
	if incoming.Amount != nil {
		inAmt = *incoming.Amount
	}
	outAsset, inAsset := "tokens", "tokens"
	if outgoing.AssetCode != nil {
		outAsset = *outgoing.AssetCode
	}
	if incoming.AssetCode != nil {
		inAsset = *incoming.AssetCode
	}

	return &TxSummary{
		Description:       fmt.Sprintf("Swapped %s %s for %s %s", outAmt, outAsset, inAmt, inAsset),
		Type:              "swap",
		InvolvedContracts: contracts,
	}
}

func abbreviateAddr(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:4] + "..." + addr[len(addr)-4:]
}

func appendUnique(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

// operationTypeNameDecode maps operation type numbers to human-readable names
func operationTypeNameDecode(opType int32) string {
	names := map[int32]string{
		0: "create_account", 1: "payment", 2: "path_payment_strict_receive",
		3: "manage_sell_offer", 4: "create_passive_sell_offer", 5: "set_options",
		6: "change_trust", 7: "allow_trust", 8: "account_merge", 9: "inflation",
		10: "manage_data", 11: "bump_sequence", 12: "manage_buy_offer",
		13: "path_payment_strict_send", 14: "create_claimable_balance",
		15: "claim_claimable_balance", 16: "begin_sponsoring_future_reserves",
		17: "end_sponsoring_future_reserves", 18: "revoke_sponsorship",
		19: "clawback", 20: "clawback_claimable_balance", 21: "set_trust_line_flags",
		22: "liquidity_pool_deposit", 23: "liquidity_pool_withdraw",
		24: "invoke_host_function", 25: "extend_footprint_ttl", 26: "restore_footprint",
	}
	if name, ok := names[opType]; ok {
		return name
	}
	return fmt.Sprintf("op_type_%d", opType)
}

// FormatFunctionCall formats a function name for display, handling common patterns
func FormatFunctionCall(functionName string) string {
	// Replace underscores with spaces for display
	return strings.ReplaceAll(functionName, "_", " ")
}
