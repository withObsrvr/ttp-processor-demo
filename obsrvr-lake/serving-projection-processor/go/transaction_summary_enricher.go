package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type projectorDecodedOperation struct {
	Index         int
	Type          int32
	TypeName      string
	SourceAccount string
	ContractID    *string
	FunctionName  *string
	Destination   *string
	AssetCode     *string
	Amount        *string
	IsSorobanOp   bool
}

type projectorUnifiedEvent struct {
	ContractID *string
	EventType  string
	From       *string
	To         *string
	Amount     *string
	AssetCode  *string
	EventIndex *int
}

type summaryEnricher struct {
	silverPool *pgxpool.Pool
}

func newSummaryEnricher(silverPool *pgxpool.Pool) *summaryEnricher {
	return &summaryEnricher{silverPool: silverPool}
}

func (e *summaryEnricher) BuildSummaries(ctx context.Context, rows []bronzeTransactionRow) (map[string]TxSummary, error) {
	out := make(map[string]TxSummary, len(rows))
	if len(rows) == 0 || e == nil || e.silverPool == nil {
		for _, row := range rows {
			out[row.TransactionHash] = fallbackSummaryFromBronze(row)
		}
		return out, nil
	}

	hashes := make([]string, 0, len(rows))
	bronzeByHash := make(map[string]bronzeTransactionRow, len(rows))
	for _, row := range rows {
		hashes = append(hashes, row.TransactionHash)
		bronzeByHash[row.TransactionHash] = row
	}

	opsByHash, err := e.loadOperations(ctx, hashes)
	if err != nil {
		return nil, err
	}
	eventsByHash, err := e.loadEvents(ctx, hashes)
	if err != nil {
		return nil, err
	}
	fnByKey, err := e.loadInvocationFunctions(ctx, hashes)
	if err != nil {
		return nil, err
	}

	for txHash, ops := range opsByHash {
		for i := range ops {
			if ops[i].FunctionName == nil || *ops[i].FunctionName == "" {
				if fn, ok := fnByKey[fmt.Sprintf("%s:%d", txHash, ops[i].Index)]; ok && fn != "" {
					fnCopy := fn
					ops[i].FunctionName = &fnCopy
				}
			}
		}
		opsByHash[txHash] = ops
	}

	for _, row := range rows {
		ops := opsByHash[row.TransactionHash]
		events := eventsByHash[row.TransactionHash]
		summary := generateProjectorTxSummary(ops, events)
		if summary.Type == "unknown" || summary.Description == "Transaction" || summary.Description == "Empty transaction" {
			fallback := fallbackSummaryFromBronze(row)
			if summary.Type == "unknown" {
				summary.Type = fallback.Type
			}
			if summary.Description == "Transaction" || summary.Description == "Empty transaction" {
				summary.Description = fallback.Description
			}
			if len(summary.InvolvedContracts) == 0 {
				summary.InvolvedContracts = fallback.InvolvedContracts
			}
		}
		out[row.TransactionHash] = summary
	}

	return out, nil
}

func (e *summaryEnricher) loadOperations(ctx context.Context, hashes []string) (map[string][]projectorDecodedOperation, error) {
	rows, err := e.silverPool.Query(ctx, `
		SELECT transaction_hash, operation_index, type, source_account, contract_id, function_name,
		       destination, asset_code, amount::text, COALESCE(is_soroban_op, false)
		FROM enriched_history_operations
		WHERE transaction_hash = ANY($1)
		ORDER BY transaction_hash ASC, operation_index ASC
	`, hashes)
	if err != nil {
		return nil, fmt.Errorf("query enriched operations: %w", err)
	}
	defer rows.Close()

	out := map[string][]projectorDecodedOperation{}
	for rows.Next() {
		var txHash string
		var op projectorDecodedOperation
		if err := rows.Scan(&txHash, &op.Index, &op.Type, &op.SourceAccount, &op.ContractID, &op.FunctionName, &op.Destination, &op.AssetCode, &op.Amount, &op.IsSorobanOp); err != nil {
			return nil, fmt.Errorf("scan enriched operation: %w", err)
		}
		op.TypeName = operationTypeName(op.Type)
		out[txHash] = append(out[txHash], op)
	}
	return out, rows.Err()
}

func (e *summaryEnricher) loadEvents(ctx context.Context, hashes []string) (map[string][]projectorUnifiedEvent, error) {
	rows, err := e.silverPool.Query(ctx, `
		SELECT transaction_hash, token_contract_id, from_account, to_account, amount::text, asset_code, event_index
		FROM token_transfers_raw
		WHERE transaction_successful = true
		  AND transaction_hash = ANY($1)
		ORDER BY transaction_hash ASC, timestamp ASC, COALESCE(event_index, -1) ASC
	`, hashes)
	if err != nil {
		return nil, fmt.Errorf("query token transfers: %w", err)
	}
	defer rows.Close()

	out := map[string][]projectorUnifiedEvent{}
	for rows.Next() {
		var txHash string
		var ev projectorUnifiedEvent
		if err := rows.Scan(&txHash, &ev.ContractID, &ev.From, &ev.To, &ev.Amount, &ev.AssetCode, &ev.EventIndex); err != nil {
			return nil, fmt.Errorf("scan token transfer: %w", err)
		}
		switch {
		case ev.From == nil && ev.To != nil:
			ev.EventType = "mint"
		case ev.To == nil && ev.From != nil:
			ev.EventType = "burn"
		default:
			ev.EventType = "transfer"
		}
		out[txHash] = append(out[txHash], ev)
	}
	return out, rows.Err()
}

func (e *summaryEnricher) loadInvocationFunctions(ctx context.Context, hashes []string) (map[string]string, error) {
	rows, err := e.silverPool.Query(ctx, `
		SELECT transaction_hash, operation_index, function_name
		FROM contract_invocations_raw
		WHERE transaction_hash = ANY($1)
	`, hashes)
	if err != nil {
		return nil, fmt.Errorf("query contract invocations: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var txHash string
		var opIndex int
		var functionName *string
		if err := rows.Scan(&txHash, &opIndex, &functionName); err != nil {
			return nil, fmt.Errorf("scan contract invocation: %w", err)
		}
		if functionName != nil && *functionName != "" {
			out[fmt.Sprintf("%s:%d", txHash, opIndex)] = *functionName
		}
	}
	return out, rows.Err()
}

func fallbackSummaryFromBronze(r bronzeTransactionRow) TxSummary {
	if r.SorobanContractID != nil && *r.SorobanContractID != "" {
		return TxSummary{
			Description:       fmt.Sprintf("Invoked contract %s", *r.SorobanContractID),
			Type:              "contract_call",
			InvolvedContracts: []string{*r.SorobanContractID},
		}
	}
	if r.OperationCount != nil && *r.OperationCount > 1 {
		return TxSummary{Description: "Transaction with multiple operations", Type: "multi_op", InvolvedContracts: []string{}}
	}
	if r.SourceAccount != nil && *r.SourceAccount != "" {
		return TxSummary{Description: fmt.Sprintf("Transaction from %s", *r.SourceAccount), Type: "classic", InvolvedContracts: []string{}}
	}
	return TxSummary{Description: "Transaction", Type: "classic", InvolvedContracts: []string{}}
}

func generateProjectorTxSummary(ops []projectorDecodedOperation, events []projectorUnifiedEvent) TxSummary {
	contracts := collectContracts(ops, events)
	if len(ops) == 0 && len(events) == 0 {
		return TxSummary{Description: "Empty transaction", Type: "unknown", InvolvedContracts: contracts}
	}
	if len(events) == 1 {
		return summarizeSingleProjectorEvent(events[0], contracts)
	}
	if len(events) == 2 {
		if summary := detectProjectorSwap(events, contracts); summary != nil {
			return *summary
		}
	}
	if len(ops) == 1 {
		if summary := summarizeSingleProjectorOp(ops[0], contracts); summary != nil {
			return *summary
		}
	}
	if len(events) > 0 {
		transfers := 0
		for _, event := range events {
			if event.EventType == "transfer" {
				transfers++
			}
		}
		if transfers > 0 {
			return TxSummary{Description: fmt.Sprintf("Transaction with %d transfers", transfers), Type: "multi_transfer", InvolvedContracts: contracts}
		}
	}
	if len(ops) > 1 {
		return TxSummary{Description: fmt.Sprintf("Transaction with %d operations", len(ops)), Type: "multi_op", InvolvedContracts: contracts}
	}
	return TxSummary{Description: "Transaction", Type: "unknown", InvolvedContracts: contracts}
}

func summarizeSingleProjectorEvent(event projectorUnifiedEvent, contracts []string) TxSummary {
	asset := projectorAssetForDisplay(event)
	amount := stringValue(event.Amount)
	switch event.EventType {
	case "mint":
		to := abbreviateValue(event.To)
		return TxSummary{
			Description:       fmt.Sprintf("Minted %s %s to %s", amount, asset, to),
			Type:              "mint",
			InvolvedContracts: contracts,
			Mint:              &TransferDetail{Asset: asset, Amount: amount, To: stringValue(event.To)},
		}
	case "burn":
		from := abbreviateValue(event.From)
		return TxSummary{
			Description:       fmt.Sprintf("Burned %s %s from %s", amount, asset, from),
			Type:              "burn",
			InvolvedContracts: contracts,
			Burn:              &TransferDetail{Asset: asset, Amount: amount, From: stringValue(event.From)},
		}
	default:
		to := abbreviateValue(event.To)
		return TxSummary{
			Description:       fmt.Sprintf("Sent %s %s to %s", amount, asset, to),
			Type:              "transfer",
			InvolvedContracts: contracts,
			Transfer:          &TransferDetail{Asset: asset, Amount: amount, From: stringValue(event.From), To: stringValue(event.To)},
		}
	}
}

func summarizeSingleProjectorOp(op projectorDecodedOperation, contracts []string) *TxSummary {
	amount := stringValue(op.Amount)
	asset := stringValue(op.AssetCode)
	if asset == "" {
		asset = "XLM"
	}
	dest := abbreviateValue(op.Destination)
	contractDisplay := abbreviateValue(op.ContractID)

	switch op.TypeName {
	case "create_account":
		return &TxSummary{
			Description:       fmt.Sprintf("Created account %s with %s XLM", dest, amount),
			Type:              "create_account",
			InvolvedContracts: contracts,
			Transfer:          &TransferDetail{Asset: "XLM", Amount: amount, From: op.SourceAccount, To: stringValue(op.Destination)},
		}
	case "payment":
		return &TxSummary{
			Description:       fmt.Sprintf("Sent %s %s to %s", amount, asset, dest),
			Type:              "payment",
			InvolvedContracts: contracts,
			Transfer:          &TransferDetail{Asset: asset, Amount: amount, From: op.SourceAccount, To: stringValue(op.Destination)},
		}
	case "path_payment_strict_receive", "path_payment_strict_send":
		return &TxSummary{
			Description:       fmt.Sprintf("Path payment of %s %s to %s", amount, asset, dest),
			Type:              "path_payment",
			InvolvedContracts: contracts,
			Transfer:          &TransferDetail{Asset: asset, Amount: amount, From: op.SourceAccount, To: stringValue(op.Destination)},
		}
	}

	if op.IsSorobanOp && op.FunctionName != nil && *op.FunctionName != "" {
		return &TxSummary{
			Description:       fmt.Sprintf("Called %s on %s", *op.FunctionName, contractDisplay),
			Type:              "contract_call",
			InvolvedContracts: contracts,
		}
	}

	if op.IsSorobanOp && op.ContractID != nil {
		return &TxSummary{
			Description:       fmt.Sprintf("Invoked contract %s", contractDisplay),
			Type:              "contract_call",
			InvolvedContracts: contracts,
		}
	}

	return nil
}

func detectProjectorSwap(events []projectorUnifiedEvent, contracts []string) *TxSummary {
	first, second := events[0], events[1]
	if first.EventType != "transfer" || second.EventType != "transfer" {
		return nil
	}
	var trader string
	switch {
	case first.From != nil && second.To != nil && *first.From == *second.To:
		trader = *first.From
	case first.To != nil && second.From != nil && *first.To == *second.From:
		trader = *first.To
	default:
		return nil
	}
	sold := first
	bought := second
	if first.To != nil && *first.To == trader {
		sold = second
		bought = first
	}
	return &TxSummary{
		Description:       fmt.Sprintf("Swapped %s %s for %s %s", stringValue(sold.Amount), projectorAssetForDisplay(sold), stringValue(bought.Amount), projectorAssetForDisplay(bought)),
		Type:              "swap",
		InvolvedContracts: contracts,
		Swap: &SwapDetail{
			SoldAsset:    projectorAssetForDisplay(sold),
			SoldAmount:   stringValue(sold.Amount),
			BoughtAsset:  projectorAssetForDisplay(bought),
			BoughtAmount: stringValue(bought.Amount),
			Trader:       trader,
		},
	}
}

func collectContracts(ops []projectorDecodedOperation, events []projectorUnifiedEvent) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, op := range ops {
		if op.ContractID != nil && *op.ContractID != "" {
			if _, ok := seen[*op.ContractID]; !ok {
				seen[*op.ContractID] = struct{}{}
				out = append(out, *op.ContractID)
			}
		}
	}
	for _, event := range events {
		if event.ContractID != nil && *event.ContractID != "" {
			if _, ok := seen[*event.ContractID]; !ok {
				seen[*event.ContractID] = struct{}{}
				out = append(out, *event.ContractID)
			}
		}
	}
	sort.Strings(out)
	return out
}

func projectorAssetForDisplay(event projectorUnifiedEvent) string {
	if event.AssetCode != nil && *event.AssetCode != "" {
		return *event.AssetCode
	}
	if event.ContractID != nil && *event.ContractID != "" {
		return abbreviateValue(event.ContractID)
	}
	return "asset"
}

func abbreviateValue(v *string) string {
	if v == nil || *v == "" {
		return ""
	}
	if len(*v) <= 12 {
		return *v
	}
	return (*v)[:6] + "..." + (*v)[len(*v)-4:]
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func operationTypeName(opType int32) string {
	names := map[int32]string{
		0:  "create_account",
		1:  "payment",
		2:  "path_payment_strict_receive",
		3:  "manage_sell_offer",
		4:  "create_passive_sell_offer",
		5:  "set_options",
		6:  "change_trust",
		7:  "allow_trust",
		8:  "account_merge",
		9:  "inflation",
		10: "manage_data",
		11: "bump_sequence",
		12: "manage_buy_offer",
		13: "path_payment_strict_send",
		14: "create_claimable_balance",
		15: "claim_claimable_balance",
		16: "begin_sponsoring_future_reserves",
		17: "end_sponsoring_future_reserves",
		18: "revoke_sponsorship",
		19: "clawback",
		20: "clawback_claimable_balance",
		21: "set_trust_line_flags",
		22: "liquidity_pool_deposit",
		23: "liquidity_pool_withdraw",
		24: "invoke_host_function",
		25: "extend_footprint_ttl",
		26: "restore_footprint",
	}
	if name, ok := names[opType]; ok {
		return name
	}
	return strings.ToLower(fmt.Sprintf("op_%d", opType))
}
