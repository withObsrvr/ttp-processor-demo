package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	pb "github.com/withObsrvr/obsrvr-lake-unified-processor/gen/bronze_ledger_service"
)

var SilverEraID = "unified-silver-processor"
var SilverVersionLabel = "v1.0.0-dev"

// =============================================================================
// Helper functions
// =============================================================================

// optStr safely dereferences an optional *string, returning nil for SQL NULL.
func optStr(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}

// optInt64 safely dereferences an optional *int64.
func optInt64(v *int64) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

// optInt32 safely dereferences an optional *int32.
func optInt32(v *int32) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

// =============================================================================
// PHASE A: Bronze -> Silver transforms (receive proto data, write to silver)
// =============================================================================

// transformEnrichedOperations joins operations + transactions + ledger to create
// a denormalized enriched view with all available columns from proto data.
func (w *DuckLakeWriter) transformEnrichedOperations(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, data *pb.BronzeLedgerData) error {
	if len(data.Operations) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.enriched_history_operations", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		id, transaction_hash, source_account, type, type_string,
		operation_index, ledger_sequence, closed_at, created_at,
		transaction_successful, operation_result_code, operation_trace_code,
		ledger_range, source_account_muxed,
		asset_type, asset_code, asset_issuer,
		source_asset_type, source_asset_code, source_asset_issuer,
		destination, amount, source_amount, starting_balance,
		offer_id, selling_asset_code, selling_asset_issuer,
		buying_asset_code, buying_asset_issuer, price,
		soroban_operation, soroban_contract_id, soroban_function,
		tx_successful, tx_fee_charged, tx_memo_type, tx_memo,
		fee_charged, memo_type, memo,
		ledger_closed_at,
		is_payment_op, is_soroban_op,
		era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45)`, table)

	// Build tx lookup for enrichment
	txMap := map[string]*pb.TransactionRow{}
	for _, t := range data.Transactions {
		txMap[t.TransactionHash] = t
	}

	for _, op := range data.Operations {
		txRow := txMap[op.TransactionHash]
		var feeCharged int64
		var memoType, memo *string
		var txSuccessful bool
		if txRow != nil {
			feeCharged = txRow.FeeCharged
			memoType = txRow.MemoType
			memo = txRow.Memo
			txSuccessful = txRow.Successful
		}

		id := fmt.Sprintf("%d-%d-%d", data.LedgerSequence, op.TransactionIndex, op.OperationIndex)

		// Derive is_payment_op and is_soroban_op
		isPaymentOp := op.Type == 1 || op.Type == 2 || op.Type == 13
		isSorobanOp := op.Type == 24

		_, err := tx.ExecContext(ctx, query,
			id, op.TransactionHash, op.SourceAccount, op.Type, op.TypeString,
			op.OperationIndex, data.LedgerSequence, data.ClosedAt, op.CreatedAt,
			op.TransactionSuccessful, optStr(op.OperationResultCode), optStr(op.OperationTraceCode),
			data.LedgerRange, optStr(op.SourceAccountMuxed),
			optStr(op.AssetType), optStr(op.AssetCode), optStr(op.AssetIssuer),
			optStr(op.SourceAssetType), optStr(op.SourceAssetCode), optStr(op.SourceAssetIssuer),
			optStr(op.Destination), optInt64(op.Amount), optInt64(op.SourceAmount), optInt64(op.StartingBalance),
			optInt64(op.OfferId), optStr(op.SellingAssetCode), optStr(op.SellingAssetIssuer),
			optStr(op.BuyingAssetCode), optStr(op.BuyingAssetIssuer), optStr(op.Price),
			optStr(op.SorobanOperation), optStr(op.SorobanContractId), optStr(op.SorobanFunction),
			txSuccessful, feeCharged, optStr(memoType), optStr(memo),
			feeCharged, optStr(memoType), optStr(memo),
			data.ClosedAt,
			isPaymentOp, isSorobanOp,
			SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert enriched op %s: %w", id, err)
		}
	}

	return nil
}

// transformContractInvocations extracts Soroban invocations from operations.
func (w *DuckLakeWriter) transformContractInvocations(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	table := fmt.Sprintf("%s.%s.contract_invocations_raw", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		ledger_sequence, transaction_index, operation_index, transaction_hash,
		source_account, contract_id, function_name, arguments_json, successful,
		closed_at, ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, table)

	for _, op := range data.Operations {
		if op.Type != 24 { // InvokeHostFunction
			continue
		}
		if op.SorobanContractId == nil {
			continue
		}

		_, err := tx.ExecContext(ctx, query,
			data.LedgerSequence, op.TransactionIndex, op.OperationIndex, op.TransactionHash,
			op.SourceAccount, *op.SorobanContractId,
			optStr(op.SorobanFunction), optStr(op.SorobanArgumentsJson), op.TransactionSuccessful,
			data.ClosedAt, data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert contract invocation: %w", err)
		}
	}

	return nil
}

// transformTokenTransfers extracts classic payments and Soroban SEP-41 transfers.
func (w *DuckLakeWriter) transformTokenTransfers(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, data *pb.BronzeLedgerData) error {
	table := fmt.Sprintf("%s.%s.token_transfers_raw", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		timestamp, transaction_hash, ledger_sequence, source_type,
		from_account, to_account, asset_code, asset_issuer, amount,
		token_contract_id, operation_type, transaction_successful, event_index,
		ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, table)

	// --- Classic payments (types 1=Payment, 2=PathPaymentStrictReceive, 13=PathPaymentStrictSend) ---
	for _, op := range data.Operations {
		if op.Type != 1 && op.Type != 2 && op.Type != 13 {
			continue
		}
		if !op.TransactionSuccessful {
			continue
		}

		var amount float64
		if op.Amount != nil {
			// Stellar amounts are stored as stroops (1/10^7)
			amount = float64(*op.Amount)
		}

		_, err := tx.ExecContext(ctx, query,
			data.ClosedAt, op.TransactionHash, data.LedgerSequence, "classic",
			op.SourceAccount, optStr(op.Destination),
			optStr(op.AssetCode), optStr(op.AssetIssuer), amount,
			nil, // token_contract_id (classic has none)
			op.Type, op.TransactionSuccessful, nil, // event_index (classic has none)
			data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert classic token transfer: %w", err)
		}
	}

	// --- Soroban SEP-41 events ---
	// Parse contract events for transfer/mint/burn topics.
	// Contract events have topics_decoded as JSON with topic0 indicating the event type.
	for _, evt := range data.ContractEvents {
		if evt.EventType != "contract" || evt.TopicCount < 2 {
			continue
		}
		if !evt.InSuccessfulContractCall {
			continue
		}

		// Check topic0 for SEP-41 event types
		topic0 := ""
		if evt.Topic0Decoded != nil {
			topic0 = *evt.Topic0Decoded
		}

		// Normalize: topics_decoded may contain quoted strings like "\"transfer\""
		topic0Clean := strings.Trim(topic0, "\"")

		switch topic0Clean {
		case "transfer", "mint", "burn", "clawback":
			// Valid SEP-41 event
		default:
			continue
		}

		// For Soroban events, from/to are encoded in topics.
		// topic1 = from (for transfer/burn/clawback), topic2 = to (for transfer/mint)
		var fromAccount, toAccount *string

		if evt.Topic1Decoded != nil {
			s := strings.Trim(*evt.Topic1Decoded, "\"")
			if s != "" {
				fromAccount = &s
			}
		}
		if evt.Topic2Decoded != nil {
			s := strings.Trim(*evt.Topic2Decoded, "\"")
			if s != "" {
				toAccount = &s
			}
		}

		// For mint: from is nil, to is in topic1 or topic2
		// For burn: to is nil, from is in topic1
		// For transfer: from=topic1, to=topic2
		switch topic0Clean {
		case "mint":
			// Mint: no sender, recipient in topic1
			toAccount = fromAccount
			fromAccount = nil
		case "burn", "clawback":
			// Burn/clawback: sender in topic1, no recipient
			toAccount = nil
		}

		// Amount is in data_decoded
		// For now, store 0 if we can't parse — proper JSON parsing would be more robust
		var amount float64 // default 0

		// Look up transaction success from the txMap
		txHash := evt.TransactionHash

		eventIndex := evt.EventIndex

		_, err := tx.ExecContext(ctx, query,
			data.ClosedAt, txHash, data.LedgerSequence, "soroban",
			optStr(fromAccount), optStr(toAccount),
			nil, nil, // asset_code, asset_issuer (Soroban tokens use contract_id)
			amount, evt.ContractId,
			24, // operation_type = InvokeHostFunction
			true, eventIndex,
			data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert soroban token transfer: %w", err)
		}
	}

	return nil
}

// transformTrades copies trade data to silver.
func (w *DuckLakeWriter) transformTrades(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	if len(data.Trades) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.trades", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		ledger_sequence, transaction_hash, operation_index, trade_index,
		trade_type, trade_timestamp, seller_account,
		selling_asset_code, selling_asset_issuer, selling_amount,
		buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
		price, ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, table)

	for _, t := range data.Trades {
		_, err := tx.ExecContext(ctx, query,
			data.LedgerSequence, t.TransactionHash, t.OperationIndex, t.TradeIndex,
			t.TradeType, t.TradeTimestamp, t.SellerAccount,
			optStr(t.SellingAssetCode), optStr(t.SellingAssetIssuer), t.SellingAmount,
			t.BuyerAccount, optStr(t.BuyingAssetCode), optStr(t.BuyingAssetIssuer), t.BuyingAmount,
			t.Price, data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert trade: %w", err)
		}
	}
	return nil
}

// transformTokenRegistry upserts token metadata from contract data rows.
func (w *DuckLakeWriter) transformTokenRegistry(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	table := fmt.Sprintf("%s.%s.token_registry", cat, silver)

	for _, cd := range data.ContractData {
		if cd.TokenName == nil {
			continue
		}

		// DuckLake has no ON CONFLICT — use DELETE then INSERT
		delQuery := fmt.Sprintf("DELETE FROM %s WHERE contract_id = $1", table)
		if _, err := tx.ExecContext(ctx, delQuery, cd.ContractId); err != nil {
			return fmt.Errorf("delete token registry for %s: %w", cd.ContractId, err)
		}

		// Determine token type
		tokenType := "soroban_token"
		if cd.AssetCode != nil && *cd.AssetCode != "" {
			tokenType = "wrapped_classic"
		}

		insQuery := fmt.Sprintf(`INSERT INTO %s (
			contract_id, token_name, token_symbol, token_decimals,
			asset_code, asset_issuer, token_type,
			first_seen_ledger, last_updated_ledger,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$9)`, table)

		_, err := tx.ExecContext(ctx, insQuery,
			cd.ContractId, *cd.TokenName, optStr(cd.TokenSymbol), optInt32(cd.TokenDecimals),
			optStr(cd.AssetCode), optStr(cd.AssetIssuer), tokenType,
			data.LedgerSequence, data.ClosedAt,
		)
		if err != nil {
			return fmt.Errorf("insert token registry for %s: %w", cd.ContractId, err)
		}
	}
	return nil
}

// transformContractMetadata maps contract creations.
func (w *DuckLakeWriter) transformContractMetadata(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	if len(data.ContractCreations) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.contract_metadata", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		contract_id, creator_address, wasm_hash, created_ledger, created_at,
		ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, table)

	for _, c := range data.ContractCreations {
		_, err := tx.ExecContext(ctx, query,
			c.ContractId, c.CreatorAddress, optStr(c.WasmHash), c.CreatedLedger, c.CreatedAt,
			data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert contract metadata: %w", err)
		}
	}
	return nil
}

// transformCurrentState updates all current-state tables using DELETE+INSERT pattern.
func (w *DuckLakeWriter) transformCurrentState(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	// --- accounts_current ---
	if len(data.Accounts) > 0 {
		table := fmt.Sprintf("%s.%s.accounts_current", cat, silver)
		for _, a := range data.Accounts {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE account_id = $1", table), a.AccountId); err != nil {
				return fmt.Errorf("delete accounts_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				account_id, balance, sequence_number, flags, home_domain,
				last_modified_ledger, ledger_sequence, closed_at, sponsor_account,
				ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, table),
				a.AccountId, a.Balance, a.SequenceNumber, a.Flags,
				optStr(a.HomeDomain),
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt,
				optStr(a.SponsorAccount),
				data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert accounts_current: %w", err)
			}
		}
	}

	// --- trustlines_current ---
	if len(data.Trustlines) > 0 {
		table := fmt.Sprintf("%s.%s.trustlines_current", cat, silver)
		for _, t := range data.Trustlines {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				"DELETE FROM %s WHERE account_id = $1 AND asset_code = $2 AND asset_issuer = $3", table),
				t.AccountId, t.AssetCode, t.AssetIssuer); err != nil {
				return fmt.Errorf("delete trustlines_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				account_id, asset_type, asset_issuer, asset_code,
				balance, flags,
				last_modified_ledger, ledger_sequence, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, table),
				t.AccountId, t.AssetType, t.AssetIssuer, t.AssetCode,
				t.Balance, 0, // flags not in proto TrustlineRow; authorized maps to flags
				data.LedgerSequence, data.LedgerSequence, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert trustlines_current: %w", err)
			}
		}
	}

	// --- offers_current ---
	if len(data.Offers) > 0 {
		table := fmt.Sprintf("%s.%s.offers_current", cat, silver)
		for _, o := range data.Offers {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE offer_id = $1", table), o.OfferId); err != nil {
				return fmt.Errorf("delete offers_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
				buying_asset_type, buying_asset_code, buying_asset_issuer,
				amount, flags,
				last_modified_ledger, ledger_sequence, created_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, table),
				o.OfferId, o.SellerAccount,
				o.SellingAssetType, optStr(o.SellingAssetCode), optStr(o.SellingAssetIssuer),
				o.BuyingAssetType, optStr(o.BuyingAssetCode), optStr(o.BuyingAssetIssuer),
				o.Amount, o.Flags,
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert offers_current: %w", err)
			}
		}
	}

	// --- claimable_balances_current ---
	if len(data.ClaimableBalances) > 0 {
		table := fmt.Sprintf("%s.%s.claimable_balances_current", cat, silver)
		for _, cb := range data.ClaimableBalances {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE balance_id = $1", table), cb.BalanceId); err != nil {
				return fmt.Errorf("delete claimable_balances_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				balance_id, sponsor, asset_type, asset_code, asset_issuer,
				amount, claimants_count,
				last_modified_ledger, ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, table),
				cb.BalanceId, cb.Sponsor, cb.AssetType,
				optStr(cb.AssetCode), optStr(cb.AssetIssuer),
				cb.Amount, cb.ClaimantsCount,
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert claimable_balances_current: %w", err)
			}
		}
	}

	// --- liquidity_pools_current ---
	if len(data.LiquidityPools) > 0 {
		table := fmt.Sprintf("%s.%s.liquidity_pools_current", cat, silver)
		for _, lp := range data.LiquidityPools {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE liquidity_pool_id = $1", table), lp.LiquidityPoolId); err != nil {
				return fmt.Errorf("delete liquidity_pools_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				liquidity_pool_id, pool_type, fee, total_pool_shares,
				asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
				asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
				last_modified_ledger, ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`, table),
				lp.LiquidityPoolId, lp.PoolType, lp.Fee, lp.TotalPoolShares,
				lp.AssetAType, optStr(lp.AssetACode), optStr(lp.AssetAIssuer), lp.AssetAAmount,
				lp.AssetBType, optStr(lp.AssetBCode), optStr(lp.AssetBIssuer), lp.AssetBAmount,
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert liquidity_pools_current: %w", err)
			}
		}
	}

	// --- native_balances_current ---
	if len(data.NativeBalances) > 0 {
		table := fmt.Sprintf("%s.%s.native_balances_current", cat, silver)
		for _, nb := range data.NativeBalances {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE account_id = $1", table), nb.AccountId); err != nil {
				return fmt.Errorf("delete native_balances_current: %w", err)
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				account_id, balance, buying_liabilities, selling_liabilities,
				sequence_number, last_modified_ledger, ledger_sequence, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, table),
				nb.AccountId, nb.Balance, nb.BuyingLiabilities, nb.SellingLiabilities,
				nb.SequenceNumber, nb.LastModifiedLedger, data.LedgerSequence, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert native_balances_current: %w", err)
			}
		}
	}

	// --- contract_data_current ---
	if len(data.ContractData) > 0 {
		table := fmt.Sprintf("%s.%s.contract_data_current", cat, silver)
		for _, cd := range data.ContractData {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				"DELETE FROM %s WHERE contract_id = $1 AND key_hash = $2", table),
				cd.ContractId, cd.LedgerKeyHash); err != nil {
				return fmt.Errorf("delete contract_data_current: %w", err)
			}
			if cd.Deleted {
				continue // Don't reinsert deleted entries
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				contract_id, key_hash, durability,
				asset_type, asset_code, asset_issuer,
				last_modified_ledger, ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, table),
				cd.ContractId, cd.LedgerKeyHash, cd.ContractDurability,
				optStr(cd.AssetType), optStr(cd.AssetCode), optStr(cd.AssetIssuer),
				cd.LastModifiedLedger, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert contract_data_current: %w", err)
			}
		}
	}

	// --- contract_code_current ---
	if len(data.ContractCode) > 0 {
		table := fmt.Sprintf("%s.%s.contract_code_current", cat, silver)
		for _, cc := range data.ContractCode {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				"DELETE FROM %s WHERE contract_code_hash = $1", table),
				cc.ContractCodeHash); err != nil {
				return fmt.Errorf("delete contract_code_current: %w", err)
			}
			if cc.Deleted {
				continue
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				contract_code_hash, contract_code_ext_v,
				n_instructions, n_functions,
				last_modified_ledger, ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, table),
				cc.ContractCodeHash, cc.ContractCodeExtV,
				cc.NInstructions, cc.NFunctions,
				cc.LastModifiedLedger, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert contract_code_current: %w", err)
			}
		}
	}

	// --- ttl_current ---
	if len(data.TtlEntries) > 0 {
		table := fmt.Sprintf("%s.%s.ttl_current", cat, silver)
		for _, ttl := range data.TtlEntries {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE key_hash = $1", table), ttl.KeyHash); err != nil {
				return fmt.Errorf("delete ttl_current: %w", err)
			}
			if ttl.Deleted {
				continue
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				key_hash, live_until_ledger_seq, ttl_remaining,
				expired, last_modified_ledger, ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, table),
				ttl.KeyHash, ttl.LiveUntilLedgerSeq, ttl.TtlRemaining,
				ttl.Expired, ttl.LastModifiedLedger, data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert ttl_current: %w", err)
			}
		}
	}

	// --- config_settings_current ---
	if len(data.ConfigSettings) > 0 {
		table := fmt.Sprintf("%s.%s.config_settings_current", cat, silver)
		for _, cs := range data.ConfigSettings {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE config_setting_id = $1", table), cs.ConfigSettingId); err != nil {
				return fmt.Errorf("delete config_settings_current: %w", err)
			}
			if cs.Deleted {
				continue
			}
			_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (
				config_setting_id, config_setting_xdr, last_modified_ledger,
				ledger_sequence, closed_at, ledger_range, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7)`, table),
				cs.ConfigSettingId, cs.ConfigSettingXdr, cs.LastModifiedLedger,
				data.LedgerSequence, data.ClosedAt, data.LedgerRange, data.ClosedAt,
			)
			if err != nil {
				return fmt.Errorf("insert config_settings_current: %w", err)
			}
		}
	}

	return nil
}

// transformEffects passes through effect data from bronze to silver.
func (w *DuckLakeWriter) transformEffects(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	if len(data.Effects) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.effects", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		ledger_sequence, transaction_hash, operation_index, effect_index,
		effect_type, effect_type_string, account_id,
		amount, asset_code, asset_issuer, asset_type,
		created_at, ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, table)

	for _, e := range data.Effects {
		_, err := tx.ExecContext(ctx, query,
			e.LedgerSequence, e.TransactionHash, e.OperationIndex, e.EffectIndex,
			e.EffectType, e.EffectTypeString, optStr(e.AccountId),
			optStr(e.Amount), optStr(e.AssetCode), optStr(e.AssetIssuer), optStr(e.AssetType),
			e.CreatedAt, e.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert effect: %w", err)
		}
	}

	return nil
}

// transformEvictedKeys passes through evicted key data from bronze to silver.
func (w *DuckLakeWriter) transformEvictedKeys(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	if len(data.EvictedKeys) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.evicted_keys", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		contract_id, key_hash, ledger_sequence,
		closed_at, ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7)`, table)

	for _, ek := range data.EvictedKeys {
		_, err := tx.ExecContext(ctx, query,
			ek.ContractId, ek.KeyHash, data.LedgerSequence,
			ek.ClosedAt, data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert evicted key: %w", err)
		}
	}

	return nil
}

// transformRestoredKeys passes through restored key data from bronze to silver.
func (w *DuckLakeWriter) transformRestoredKeys(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	if len(data.RestoredKeys) == 0 {
		return nil
	}

	table := fmt.Sprintf("%s.%s.restored_keys", cat, silver)
	query := fmt.Sprintf(`INSERT INTO %s (
		contract_id, key_hash, ledger_sequence,
		closed_at, ledger_range, era_id, version_label
	) VALUES ($1,$2,$3,$4,$5,$6,$7)`, table)

	for _, rk := range data.RestoredKeys {
		_, err := tx.ExecContext(ctx, query,
			rk.ContractId, rk.KeyHash, data.LedgerSequence,
			rk.ClosedAt, data.LedgerRange, SilverEraID, SilverVersionLabel,
		)
		if err != nil {
			return fmt.Errorf("insert restored key: %w", err)
		}
	}

	return nil
}

// transformSnapshots creates append-only snapshot inserts for accounts, trustlines, offers, and account_signers.
func (w *DuckLakeWriter) transformSnapshots(ctx context.Context, tx *sql.Tx, cat, silver string, data *pb.BronzeLedgerData) error {
	// --- accounts_snapshot ---
	if len(data.Accounts) > 0 {
		table := fmt.Sprintf("%s.%s.accounts_snapshot", cat, silver)
		query := fmt.Sprintf(`INSERT INTO %s (
			account_id, balance, sequence_number, flags, home_domain,
			last_modified_ledger, ledger_sequence, closed_at, sponsor_account,
			ledger_range, era_id, version_label
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, table)

		for _, a := range data.Accounts {
			_, err := tx.ExecContext(ctx, query,
				a.AccountId, a.Balance, a.SequenceNumber, a.Flags,
				optStr(a.HomeDomain),
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt,
				optStr(a.SponsorAccount),
				data.LedgerRange, SilverEraID, SilverVersionLabel,
			)
			if err != nil {
				return fmt.Errorf("insert accounts_snapshot: %w", err)
			}
		}
	}

	// --- trustlines_snapshot ---
	if len(data.Trustlines) > 0 {
		table := fmt.Sprintf("%s.%s.trustlines_snapshot", cat, silver)
		query := fmt.Sprintf(`INSERT INTO %s (
			account_id, asset_type, asset_issuer, asset_code,
			balance, last_modified_ledger, ledger_sequence, closed_at,
			ledger_range, era_id, version_label
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, table)

		for _, t := range data.Trustlines {
			_, err := tx.ExecContext(ctx, query,
				t.AccountId, t.AssetType, t.AssetIssuer, t.AssetCode,
				t.Balance, data.LedgerSequence, data.LedgerSequence, data.ClosedAt,
				data.LedgerRange, SilverEraID, SilverVersionLabel,
			)
			if err != nil {
				return fmt.Errorf("insert trustlines_snapshot: %w", err)
			}
		}
	}

	// --- offers_snapshot ---
	if len(data.Offers) > 0 {
		table := fmt.Sprintf("%s.%s.offers_snapshot", cat, silver)
		query := fmt.Sprintf(`INSERT INTO %s (
			offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, flags,
			last_modified_ledger, ledger_sequence, closed_at,
			ledger_range, era_id, version_label
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, table)

		for _, o := range data.Offers {
			_, err := tx.ExecContext(ctx, query,
				o.OfferId, o.SellerAccount,
				o.SellingAssetType, optStr(o.SellingAssetCode), optStr(o.SellingAssetIssuer),
				o.BuyingAssetType, optStr(o.BuyingAssetCode), optStr(o.BuyingAssetIssuer),
				o.Amount, o.Flags,
				data.LedgerSequence, data.LedgerSequence, data.ClosedAt,
				data.LedgerRange, SilverEraID, SilverVersionLabel,
			)
			if err != nil {
				return fmt.Errorf("insert offers_snapshot: %w", err)
			}
		}
	}

	// --- account_signers_snapshot ---
	if len(data.AccountSigners) > 0 {
		table := fmt.Sprintf("%s.%s.account_signers_snapshot", cat, silver)
		query := fmt.Sprintf(`INSERT INTO %s (
			account_id, signer, weight, sponsor, deleted,
			last_modified_ledger, ledger_sequence, closed_at,
			ledger_range, era_id, version_label
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, table)

		for _, as := range data.AccountSigners {
			_, err := tx.ExecContext(ctx, query,
				as.AccountId, as.Signer, as.Weight,
				as.Sponsor, as.Deleted,
				data.LedgerSequence, data.LedgerSequence, as.ClosedAt,
				data.LedgerRange, SilverEraID, SilverVersionLabel,
			)
			if err != nil {
				return fmt.Errorf("insert account_signers_snapshot: %w", err)
			}
		}
	}

	return nil
}

// =============================================================================
// PHASE B: Silver -> Silver semantic transforms (INSERT-SELECT within same tx)
// =============================================================================
// These read from silver tables written in Phase A and write to semantic tables.
// They use SQL INSERT-SELECT, not proto data.
// Note: DuckLake has no ON CONFLICT — semantic aggregation tables use DELETE+INSERT.

// transformSemanticActivities builds activity summaries from enriched operations.
func (w *DuckLakeWriter) transformSemanticActivities(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.enriched_history_operations", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_activities", cat, silver)

	query := fmt.Sprintf(`INSERT INTO %s (
		id, ledger_sequence, timestamp, activity_type, description,
		source_account, destination_account, contract_id,
		asset_code, asset_issuer, amount,
		is_soroban, soroban_function_name,
		transaction_hash, operation_index, successful, fee_charged,
		created_at
	)
	SELECT
		e.id,
		e.ledger_sequence,
		COALESCE(e.ledger_closed_at, e.closed_at),
		CASE
			WHEN e.type = 0 THEN 'account_created'
			WHEN e.type = 1 THEN 'payment'
			WHEN e.type = 2 THEN 'path_payment'
			WHEN e.type = 6 THEN 'path_payment'
			WHEN e.type = 13 THEN 'path_payment'
			WHEN e.type = 3 THEN 'manage_offer'
			WHEN e.type = 4 THEN 'create_passive_offer'
			WHEN e.type = 5 THEN 'set_options'
			WHEN e.type = 7 THEN 'account_merge'
			WHEN e.type = 8 THEN 'inflation'
			WHEN e.type = 9 THEN 'manage_data'
			WHEN e.type = 10 THEN 'bump_sequence'
			WHEN e.type = 12 THEN 'manage_offer'
			WHEN e.type = 24 AND e.soroban_function IS NOT NULL THEN 'contract_call'
			WHEN e.type = 24 THEN 'invoke_host_function'
			ELSE 'other'
		END,
		CASE
			WHEN e.type = 0 THEN 'Created account ' || COALESCE(e.destination, '')
			WHEN e.type = 1 THEN 'Sent ' || COALESCE(CAST(e.amount AS TEXT), '?') || ' ' || COALESCE(e.asset_code, 'XLM') || ' to ' || COALESCE(e.destination, '')
			WHEN e.type IN (2, 6, 13) THEN 'Path payment ' || COALESCE(CAST(e.amount AS TEXT), '?') || ' ' || COALESCE(e.asset_code, 'XLM')
			WHEN e.type = 7 THEN 'Account merge'
			WHEN e.type = 24 AND e.soroban_function IS NOT NULL THEN 'Called ' || e.soroban_function || ' on ' || COALESCE(e.soroban_contract_id, '')
			WHEN e.type = 24 THEN 'Invoked host function'
			ELSE NULL
		END,
		e.source_account,
		e.destination,
		e.soroban_contract_id,
		e.asset_code,
		e.asset_issuer,
		CAST(e.amount AS TEXT),
		COALESCE(e.is_soroban_op, false),
		e.soroban_function,
		e.transaction_hash,
		e.operation_index,
		COALESCE(e.transaction_successful, false),
		e.tx_fee_charged,
		COALESCE(e.ledger_closed_at, e.closed_at)
	FROM %s e
	WHERE e.ledger_sequence = $1`, dst, src)

	_, err := tx.ExecContext(ctx, query, seq)
	if err != nil {
		return fmt.Errorf("insert semantic activities: %w", err)
	}

	return nil
}

// transformSemanticEntitiesContracts aggregates contract entity data from invocations + registry + metadata.
func (w *DuckLakeWriter) transformSemanticEntitiesContracts(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	invocations := fmt.Sprintf("%s.%s.contract_invocations_raw", cat, silver)
	registry := fmt.Sprintf("%s.%s.token_registry", cat, silver)
	metadata := fmt.Sprintf("%s.%s.contract_metadata", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_entities_contracts", cat, silver)

	// Delete existing entries for contracts active in this ledger
	delQuery := fmt.Sprintf(`DELETE FROM %s WHERE contract_id IN (
		SELECT DISTINCT contract_id FROM %s WHERE ledger_sequence = $1
	)`, dst, invocations)
	if _, err := tx.ExecContext(ctx, delQuery, seq); err != nil {
		return fmt.Errorf("delete semantic entities contracts: %w", err)
	}

	insQuery := fmt.Sprintf(`INSERT INTO %s (
		contract_id, contract_type,
		token_name, token_symbol, token_decimals,
		deployer_account, deployed_at, deployed_ledger,
		total_invocations, last_activity, unique_callers,
		updated_at
	)
	SELECT
		ci.contract_id,
		CASE WHEN tr.token_name IS NOT NULL THEN 'token' ELSE 'contract' END,
		tr.token_name,
		tr.token_symbol,
		tr.token_decimals,
		cm.creator_address,
		cm.created_at,
		cm.created_ledger,
		COUNT(*),
		MAX(ci.closed_at),
		COUNT(DISTINCT ci.source_account),
		CURRENT_TIMESTAMP
	FROM %s ci
	LEFT JOIN %s tr ON tr.contract_id = ci.contract_id
	LEFT JOIN %s cm ON cm.contract_id = ci.contract_id
	WHERE ci.ledger_sequence = $1
	GROUP BY ci.contract_id, tr.token_name, tr.token_symbol, tr.token_decimals,
		cm.creator_address, cm.created_at, cm.created_ledger`,
		dst, invocations, registry, metadata)

	_, err := tx.ExecContext(ctx, insQuery, seq)
	if err != nil {
		return fmt.Errorf("insert semantic entities contracts: %w", err)
	}

	return nil
}

// transformSemanticFlows builds value flow analysis from token transfers.
func (w *DuckLakeWriter) transformSemanticFlows(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.token_transfers_raw", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_flows_value", cat, silver)

	query := fmt.Sprintf(`INSERT INTO %s (
		id, ledger_sequence, timestamp, flow_type,
		from_account, to_account, contract_id,
		asset_code, asset_issuer, asset_type, amount,
		transaction_hash, operation_type, successful,
		created_at
	)
	SELECT
		t.transaction_hash || ':' || ROW_NUMBER() OVER (PARTITION BY t.transaction_hash ORDER BY COALESCE(t.from_account, ''), COALESCE(t.to_account, '')),
		t.ledger_sequence,
		t.timestamp,
		CASE
			WHEN t.from_account IS NULL OR t.from_account = '' THEN 'mint'
			WHEN t.to_account IS NULL OR t.to_account = '' THEN 'burn'
			ELSE 'transfer'
		END,
		t.from_account,
		t.to_account,
		t.token_contract_id,
		t.asset_code,
		t.asset_issuer,
		CASE
			WHEN t.source_type = 'soroban' THEN 'soroban_token'
			WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
			WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
			ELSE 'credit_alphanum12'
		END,
		CAST(COALESCE(t.amount, 0) AS TEXT),
		t.transaction_hash,
		t.operation_type,
		COALESCE(t.transaction_successful, false),
		t.timestamp
	FROM %s t
	WHERE t.ledger_sequence = $1
	  AND t.amount IS NOT NULL`, dst, src)

	_, err := tx.ExecContext(ctx, query, seq)
	if err != nil {
		return fmt.Errorf("insert semantic flows: %w", err)
	}

	return nil
}

// transformSemanticContractFunctions aggregates per-function call stats from contract_invocations_raw.
func (w *DuckLakeWriter) transformSemanticContractFunctions(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.contract_invocations_raw", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_contract_functions", cat, silver)

	// Delete existing entries for functions active in this ledger
	delQuery := fmt.Sprintf(`DELETE FROM %s WHERE (contract_id, function_name) IN (
		SELECT DISTINCT contract_id, function_name FROM %s
		WHERE ledger_sequence = $1 AND function_name IS NOT NULL AND function_name != ''
	)`, dst, src)
	if _, err := tx.ExecContext(ctx, delQuery, seq); err != nil {
		return fmt.Errorf("delete semantic contract functions: %w", err)
	}

	insQuery := fmt.Sprintf(`INSERT INTO %s (
		contract_id, function_name,
		total_calls, successful_calls, failed_calls, unique_callers,
		first_called, last_called, updated_at
	)
	SELECT
		contract_id, function_name,
		COUNT(*),
		COUNT(*) FILTER (WHERE successful),
		COUNT(*) FILTER (WHERE NOT successful),
		COUNT(DISTINCT source_account),
		MIN(closed_at), MAX(closed_at), CURRENT_TIMESTAMP
	FROM %s
	WHERE ledger_sequence = $1
	  AND function_name IS NOT NULL AND function_name != ''
	GROUP BY contract_id, function_name`, dst, src)

	_, err := tx.ExecContext(ctx, insQuery, seq)
	if err != nil {
		return fmt.Errorf("insert semantic contract functions: %w", err)
	}

	return nil
}

// transformSemanticAssetStats aggregates asset transfer stats from token_transfers_raw.
func (w *DuckLakeWriter) transformSemanticAssetStats(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.token_transfers_raw", cat, silver)
	registry := fmt.Sprintf("%s.%s.token_registry", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_asset_stats", cat, silver)

	// Delete existing entries for assets active in this ledger
	delQuery := fmt.Sprintf(`DELETE FROM %s WHERE asset_key IN (
		SELECT DISTINCT
			CASE
				WHEN t.source_type = 'soroban' THEN t.token_contract_id
				WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
				ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
			END
		FROM %s t
		WHERE t.ledger_sequence = $1
	)`, dst, src)
	if _, err := tx.ExecContext(ctx, delQuery, seq); err != nil {
		return fmt.Errorf("delete semantic asset stats: %w", err)
	}

	insQuery := fmt.Sprintf(`INSERT INTO %s (
		asset_key, asset_code, asset_issuer, asset_type,
		token_name, token_symbol, token_decimals, contract_id,
		transfer_count_24h, transfer_volume_24h,
		first_seen, last_transfer, updated_at
	)
	SELECT
		CASE
			WHEN t.source_type = 'soroban' THEN t.token_contract_id
			WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
			ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
		END,
		t.asset_code,
		t.asset_issuer,
		CASE
			WHEN t.source_type = 'soroban' THEN 'soroban_token'
			WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
			WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
			ELSE 'credit_alphanum12'
		END,
		tr.token_name,
		tr.token_symbol,
		tr.token_decimals,
		t.token_contract_id,
		COUNT(*),
		COALESCE(SUM(t.amount), 0),
		MIN(t.timestamp),
		MAX(t.timestamp),
		CURRENT_TIMESTAMP
	FROM %s t
	LEFT JOIN %s tr ON tr.contract_id = t.token_contract_id
	WHERE t.ledger_sequence = $1
	  AND NOT (t.source_type = 'soroban' AND t.token_contract_id IS NULL)
	GROUP BY
		CASE
			WHEN t.source_type = 'soroban' THEN t.token_contract_id
			WHEN t.asset_code IS NULL OR t.asset_code = '' THEN 'native'
			ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
		END,
		t.asset_code, t.asset_issuer, t.source_type, t.token_contract_id,
		tr.token_name, tr.token_symbol, tr.token_decimals`, dst, src, registry)

	_, err := tx.ExecContext(ctx, insQuery, seq)
	if err != nil {
		return fmt.Errorf("insert semantic asset stats: %w", err)
	}

	return nil
}

// transformSemanticDexPairs aggregates DEX trading pair stats from trades.
func (w *DuckLakeWriter) transformSemanticDexPairs(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.trades", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_dex_pairs", cat, silver)

	// Delete existing entries for pairs active in this ledger
	delQuery := fmt.Sprintf(`DELETE FROM %s WHERE pair_key IN (
		SELECT DISTINCT
			COALESCE(selling_asset_code, 'XLM') || ':' || COALESCE(selling_asset_issuer, 'native')
			|| '/' || COALESCE(buying_asset_code, 'XLM') || ':' || COALESCE(buying_asset_issuer, 'native')
		FROM %s WHERE ledger_sequence = $1
	)`, dst, src)
	if _, err := tx.ExecContext(ctx, delQuery, seq); err != nil {
		return fmt.Errorf("delete semantic dex pairs: %w", err)
	}

	insQuery := fmt.Sprintf(`INSERT INTO %s (
		pair_key, selling_asset_code, selling_asset_issuer,
		buying_asset_code, buying_asset_issuer,
		trade_count, selling_volume, buying_volume,
		last_price, unique_sellers, unique_buyers,
		first_trade, last_trade, updated_at
	)
	SELECT
		COALESCE(selling_asset_code, 'XLM') || ':' || COALESCE(selling_asset_issuer, 'native')
		  || '/' || COALESCE(buying_asset_code, 'XLM') || ':' || COALESCE(buying_asset_issuer, 'native'),
		selling_asset_code, selling_asset_issuer,
		buying_asset_code, buying_asset_issuer,
		COUNT(*),
		SUM(CAST(selling_amount AS DOUBLE)),
		SUM(CAST(buying_amount AS DOUBLE)),
		(ARRAY_AGG(price ORDER BY trade_timestamp DESC))[1],
		COUNT(DISTINCT seller_account),
		COUNT(DISTINCT buyer_account),
		MIN(trade_timestamp), MAX(trade_timestamp), CURRENT_TIMESTAMP
	FROM %s
	WHERE ledger_sequence = $1
	GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer`,
		dst, src)

	_, err := tx.ExecContext(ctx, insQuery, seq)
	if err != nil {
		return fmt.Errorf("insert semantic dex pairs: %w", err)
	}

	return nil
}

// transformSemanticAccountSummary aggregates account activity stats from enriched_history_operations.
func (w *DuckLakeWriter) transformSemanticAccountSummary(ctx context.Context, tx *sql.Tx, cat, silver string, seq int64) error {
	src := fmt.Sprintf("%s.%s.enriched_history_operations", cat, silver)
	dst := fmt.Sprintf("%s.%s.semantic_account_summary", cat, silver)

	// Delete existing entries for accounts active in this ledger
	delQuery := fmt.Sprintf(`DELETE FROM %s WHERE account_id IN (
		SELECT DISTINCT source_account FROM %s
		WHERE ledger_sequence = $1 AND source_account IS NOT NULL AND source_account != ''
	)`, dst, src)
	if _, err := tx.ExecContext(ctx, delQuery, seq); err != nil {
		return fmt.Errorf("delete semantic account summary: %w", err)
	}

	insQuery := fmt.Sprintf(`INSERT INTO %s (
		account_id, total_operations,
		total_payments_sent, total_payments_received,
		total_contract_calls, unique_contracts_called,
		first_activity, last_activity, updated_at
	)
	SELECT
		e.source_account,
		COUNT(*),
		COUNT(*) FILTER (WHERE e.type IN (1, 2, 13)),
		0,
		COUNT(*) FILTER (WHERE e.type = 24),
		COUNT(DISTINCT e.soroban_contract_id) FILTER (WHERE e.type = 24),
		MIN(e.closed_at),
		MAX(e.closed_at),
		CURRENT_TIMESTAMP
	FROM %s e
	WHERE e.ledger_sequence = $1
	  AND e.source_account IS NOT NULL AND e.source_account != ''
	GROUP BY e.source_account`, dst, src)

	_, err := tx.ExecContext(ctx, insQuery, seq)
	if err != nil {
		// Log but don't fail — DuckDB may not support all aggregate FILTER syntax
		log.Printf("warning: semantic account summary insert failed (non-fatal): %v", err)
		return nil
	}

	return nil
}
