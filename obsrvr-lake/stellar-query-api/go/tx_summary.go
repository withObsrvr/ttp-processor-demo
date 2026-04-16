package main

import (
	"fmt"
	"strconv"
	"strings"
)

// SwapDetail contains structured data about a detected swap
type SwapDetail struct {
	SoldAsset    string `json:"sold_asset"`
	SoldAmount   string `json:"sold_amount"`
	BoughtAsset  string `json:"bought_asset"`
	BoughtAmount string `json:"bought_amount"`
	Router       string `json:"router,omitempty"`
	Trader       string `json:"trader"`
}

// TransferDetail contains structured data about a transfer, mint, or burn
type TransferDetail struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

// TxSummary represents a human-readable summary of a transaction
type TxSummary struct {
	Description       string          `json:"description"`
	Type              string          `json:"type"` // transfer, mint, burn, swap, contract_call, create_account, payment, etc.
	InvolvedContracts []string        `json:"involved_contracts,omitempty"`
	Swap              *SwapDetail     `json:"swap,omitempty"`
	Transfer          *TransferDetail `json:"transfer,omitempty"`
	Mint              *TransferDetail `json:"mint,omitempty"`
	Burn              *TransferDetail `json:"burn,omitempty"`
}

// GenerateTxSummary creates a human-readable summary from decoded operations and events.
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

	// Single classic operation — use human-readable templates
	if len(ops) == 1 && !ops[0].IsSorobanOp {
		if summary := summarizeClassicOp(ops[0]); summary != nil {
			summary.InvolvedContracts = contracts
			return *summary
		}
	}

	// Multiple operations should take precedence over event-only fallback.
	// Otherwise a tx with multiple classic ops plus one derived token event can
	// be mislabeled as a single transfer/mint/burn.
	if len(ops) > 1 {
		return summarizeMultiOp(ops, events, contracts)
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

	// Single Soroban operation with function name
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

// summarizeClassicOp generates a human-readable description for a single classic Stellar operation
func summarizeClassicOp(op DecodedOperation) *TxSummary {
	amount := formatAmountForDisplay(op.Amount)
	asset := assetForDisplay(op.AssetCode)
	dest := abbreviateAddrSafe(op.Destination)
	source := abbreviateAddr(op.SourceAccount)

	switch op.TypeName {
	case "create_account":
		return &TxSummary{
			Description: fmt.Sprintf("Created account %s with %s XLM", dest, amount),
			Type:        "create_account",
			Transfer: &TransferDetail{
				Asset:  "XLM",
				Amount: derefStr(op.Amount),
				From:   op.SourceAccount,
				To:     derefStr(op.Destination),
			},
		}

	case "payment":
		return &TxSummary{
			Description: fmt.Sprintf("Sent %s %s to %s", amount, asset, dest),
			Type:        "payment",
			Transfer: &TransferDetail{
				Asset:  asset,
				Amount: derefStr(op.Amount),
				From:   op.SourceAccount,
				To:     derefStr(op.Destination),
			},
		}

	case "path_payment_strict_receive", "path_payment_strict_send":
		return &TxSummary{
			Description: fmt.Sprintf("Path payment of %s %s to %s", amount, asset, dest),
			Type:        "path_payment",
			Transfer: &TransferDetail{
				Asset:  asset,
				Amount: derefStr(op.Amount),
				From:   op.SourceAccount,
				To:     derefStr(op.Destination),
			},
		}

	case "change_trust":
		if asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Added trustline for %s", asset),
				Type:        "change_trust",
			}
		}
		return &TxSummary{
			Description: "Updated trustline",
			Type:        "change_trust",
		}

	case "account_merge":
		return &TxSummary{
			Description: fmt.Sprintf("Merged %s into %s", source, dest),
			Type:        "account_merge",
		}

	case "manage_sell_offer":
		if amount != "" && asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Sell offer: %s %s", amount, asset),
				Type:        "manage_sell_offer",
			}
		}
		return &TxSummary{
			Description: "Created or updated sell offer",
			Type:        "manage_sell_offer",
		}

	case "manage_buy_offer":
		if amount != "" && asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Buy offer: %s %s", amount, asset),
				Type:        "manage_buy_offer",
			}
		}
		return &TxSummary{
			Description: "Created or updated buy offer",
			Type:        "manage_buy_offer",
		}

	case "create_passive_sell_offer":
		if amount != "" && asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Passive sell offer: %s %s", amount, asset),
				Type:        "create_passive_sell_offer",
			}
		}
		return &TxSummary{
			Description: "Created passive sell offer",
			Type:        "create_passive_sell_offer",
		}

	case "set_options":
		return &TxSummary{
			Description: "Updated account settings",
			Type:        "set_options",
		}

	case "manage_data":
		return &TxSummary{
			Description: "Updated account data entry",
			Type:        "manage_data",
		}

	case "bump_sequence":
		return &TxSummary{
			Description: "Bumped account sequence number",
			Type:        "bump_sequence",
		}

	case "allow_trust":
		return &TxSummary{
			Description: fmt.Sprintf("Updated trust authorization for %s", dest),
			Type:        "allow_trust",
		}

	case "set_trust_line_flags":
		return &TxSummary{
			Description: "Updated trustline flags",
			Type:        "set_trust_line_flags",
		}

	case "create_claimable_balance":
		if amount != "" && asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Created claimable balance of %s %s", amount, asset),
				Type:        "create_claimable_balance",
			}
		}
		return &TxSummary{
			Description: "Created claimable balance",
			Type:        "create_claimable_balance",
		}

	case "claim_claimable_balance":
		return &TxSummary{
			Description: "Claimed a claimable balance",
			Type:        "claim_claimable_balance",
		}

	case "clawback":
		if amount != "" && asset != "" {
			return &TxSummary{
				Description: fmt.Sprintf("Clawed back %s %s from %s", amount, asset, dest),
				Type:        "clawback",
			}
		}
		return &TxSummary{
			Description: "Clawed back assets",
			Type:        "clawback",
		}

	case "begin_sponsoring_future_reserves":
		return &TxSummary{
			Description: fmt.Sprintf("Began sponsoring reserves for %s", dest),
			Type:        "begin_sponsoring",
		}

	case "end_sponsoring_future_reserves":
		return &TxSummary{
			Description: "Ended reserve sponsorship",
			Type:        "end_sponsoring",
		}

	case "revoke_sponsorship":
		return &TxSummary{
			Description: "Revoked sponsorship",
			Type:        "revoke_sponsorship",
		}

	case "inflation":
		return &TxSummary{
			Description: "Triggered inflation payout",
			Type:        "inflation",
		}

	case "liquidity_pool_deposit":
		return &TxSummary{
			Description: "Deposited into liquidity pool",
			Type:        "liquidity_pool_deposit",
		}

	case "liquidity_pool_withdraw":
		return &TxSummary{
			Description: "Withdrew from liquidity pool",
			Type:        "liquidity_pool_withdraw",
		}

	case "extend_footprint_ttl":
		return &TxSummary{
			Description: "Extended contract storage TTL",
			Type:        "extend_footprint_ttl",
		}

	case "restore_footprint":
		return &TxSummary{
			Description: "Restored expired contract storage",
			Type:        "restore_footprint",
		}
	}

	return nil
}

func summarizeMultiOp(ops []DecodedOperation, events []UnifiedEvent, contracts []string) TxSummary {
	sorobanCount := 0
	classicTypes := make(map[string]int)
	for _, op := range ops {
		if op.IsSorobanOp {
			sorobanCount++
		} else {
			classicTypes[op.TypeName]++
		}
	}

	// All same classic type
	if sorobanCount == 0 && len(classicTypes) == 1 {
		for typeName, count := range classicTypes {
			label := humanizeOpType(typeName)
			if count > 1 {
				return TxSummary{
					Description: fmt.Sprintf("%d %s operations", count, label),
					Type:        typeName,
				}
			}
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

func summarizeSingleEvent(e UnifiedEvent, contracts []string) TxSummary {
	amountStr := formatEventAmountForDisplay(e)
	assetStr := ""
	if e.AssetCode != nil {
		assetStr = " " + *e.AssetCode
	}

	asset := strings.TrimSpace(assetStr)
	if asset == "" {
		asset = "tokens"
	}

	switch e.EventType {
	case "transfer":
		toAddr := "unknown"
		if e.To != nil {
			toAddr = abbreviateAddr(*e.To)
		}
		detail := &TransferDetail{Asset: asset, Amount: amountStr}
		if e.From != nil {
			detail.From = *e.From
		}
		if e.To != nil {
			detail.To = *e.To
		}
		return TxSummary{
			Description:       fmt.Sprintf("Sent %s %s to %s", amountStr, asset, toAddr),
			Type:              "transfer",
			InvolvedContracts: contracts,
			Transfer:          detail,
		}
	case "mint":
		toAddr := "unknown"
		if e.To != nil {
			toAddr = abbreviateAddr(*e.To)
		}
		detail := &TransferDetail{Asset: asset, Amount: amountStr}
		if e.To != nil {
			detail.To = *e.To
		}
		return TxSummary{
			Description:       fmt.Sprintf("Minted %s %s to %s", amountStr, asset, toAddr),
			Type:              "mint",
			InvolvedContracts: contracts,
			Mint:              detail,
		}
	case "burn":
		fromAddr := "unknown"
		if e.From != nil {
			fromAddr = abbreviateAddr(*e.From)
		}
		detail := &TransferDetail{Asset: asset, Amount: amountStr}
		if e.From != nil {
			detail.From = *e.From
		}
		return TxSummary{
			Description:       fmt.Sprintf("Burned %s %s from %s", amountStr, asset, fromAddr),
			Type:              "burn",
			InvolvedContracts: contracts,
			Burn:              detail,
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
	e0, e1 := events[0], events[1]
	if e0.EventType != "transfer" || e1.EventType != "transfer" {
		return nil
	}

	if e0.From != nil && e1.To != nil && *e0.From == *e1.To {
		return buildSwapSummary(e0, e1, contracts)
	}
	if e1.From != nil && e0.To != nil && *e1.From == *e0.To {
		return buildSwapSummary(e1, e0, contracts)
	}
	return nil
}

func buildSwapSummary(outgoing, incoming UnifiedEvent, contracts []string) *TxSummary {
	outAmt := formatEventAmountForDisplay(outgoing)
	inAmt := formatEventAmountForDisplay(incoming)
	outAsset, inAsset := "tokens", "tokens"
	if outgoing.AssetCode != nil {
		outAsset = *outgoing.AssetCode
	}
	if incoming.AssetCode != nil {
		inAsset = *incoming.AssetCode
	}

	detail := &SwapDetail{
		SoldAsset:    outAsset,
		SoldAmount:   outAmt,
		BoughtAsset:  inAsset,
		BoughtAmount: inAmt,
	}
	if outgoing.From != nil {
		detail.Trader = *outgoing.From
	}
	if len(contracts) > 0 {
		detail.Router = contracts[0]
	}

	return &TxSummary{
		Description:       fmt.Sprintf("Swapped %s %s for %s %s", outAmt, outAsset, inAmt, inAsset),
		Type:              "swap",
		InvolvedContracts: contracts,
		Swap:              detail,
	}
}

// formatAmountForDisplay converts a stroops string to human-readable format (e.g. "10,000")
func formatAmountForDisplay(amount *string) string {
	if amount == nil || *amount == "" {
		return ""
	}
	return formatIntegerAmountWithDecimals(*amount, 7)
}

func formatEventAmountForDisplay(e UnifiedEvent) string {
	if e.Amount == nil || *e.Amount == "" {
		return ""
	}
	if e.TokenDecimals != nil {
		return formatIntegerAmountWithDecimals(*e.Amount, *e.TokenDecimals)
	}
	return *e.Amount
}

func formatIntegerAmountWithDecimals(raw string, decimals int) string {
	if raw == "" {
		return ""
	}
	if decimals < 0 {
		return raw
	}

	negative := strings.HasPrefix(raw, "-")
	if negative {
		raw = strings.TrimPrefix(raw, "-")
	}
	if raw == "" {
		return ""
	}

	for len(raw) > 1 && raw[0] == '0' {
		raw = raw[1:]
	}

	if _, err := strconv.ParseInt(raw, 10, 64); err == nil && decimals == 0 {
		if negative {
			return "-" + formatWithCommasMust(raw)
		}
		return formatWithCommasMust(raw)
	}

	if decimals == 0 {
		if negative {
			return "-" + formatWithCommasMust(raw)
		}
		return formatWithCommasMust(raw)
	}

	if len(raw) <= decimals {
		raw = strings.Repeat("0", decimals-len(raw)+1) + raw
	}
	split := len(raw) - decimals
	whole := raw[:split]
	frac := raw[split:]
	whole = formatWithCommasMust(whole)
	frac = strings.TrimRight(frac, "0")

	formatted := whole
	if frac != "" {
		formatted = whole + "." + frac
	}
	if negative {
		formatted = "-" + formatted
	}
	return formatted
}

// formatWithCommas adds thousand separators to an integer
func formatWithCommas(n int64) string {
	return formatWithCommasMust(strconv.FormatInt(n, 10))
}

func formatWithCommasMust(s string) string {
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func assetForDisplay(assetCode *string) string {
	if assetCode == nil || *assetCode == "" {
		return ""
	}
	return *assetCode
}

func abbreviateAddrSafe(addr *string) string {
	if addr == nil || *addr == "" {
		return "unknown"
	}
	return abbreviateAddr(*addr)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func humanizeOpType(typeName string) string {
	switch typeName {
	case "create_account":
		return "account creation"
	case "payment":
		return "payment"
	case "path_payment_strict_receive", "path_payment_strict_send":
		return "path payment"
	case "change_trust":
		return "trustline"
	case "manage_sell_offer":
		return "sell offer"
	case "manage_buy_offer":
		return "buy offer"
	case "set_options":
		return "settings"
	default:
		return strings.ReplaceAll(typeName, "_", " ")
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
	return strings.ReplaceAll(functionName, "_", " ")
}
