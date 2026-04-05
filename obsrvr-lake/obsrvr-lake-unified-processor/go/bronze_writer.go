package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// writeBronzeLedger inserts all bronze data for a single ledger into DuckLake tables.
func (w *DuckLakeWriter) writeBronzeLedger(ctx context.Context, tx *sql.Tx, cat, schema string, data *BronzeData) error {
	if err := writeLedgerRow(ctx, tx, cat, schema, data.Ledger); err != nil {
		return fmt.Errorf("write ledger: %w", err)
	}
	if err := writeTransactionRows(ctx, tx, cat, schema, data.Transactions); err != nil {
		return fmt.Errorf("write transactions: %w", err)
	}
	if err := writeOperationRows(ctx, tx, cat, schema, data.Operations); err != nil {
		return fmt.Errorf("write operations: %w", err)
	}
	if err := writeEffectRows(ctx, tx, cat, schema, data.Effects); err != nil {
		return fmt.Errorf("write effects: %w", err)
	}
	if err := writeTradeRows(ctx, tx, cat, schema, data.Trades); err != nil {
		return fmt.Errorf("write trades: %w", err)
	}
	if err := writeAccountRows(ctx, tx, cat, schema, data.Accounts); err != nil {
		return fmt.Errorf("write accounts: %w", err)
	}
	if err := writeTrustlineRows(ctx, tx, cat, schema, data.Trustlines); err != nil {
		return fmt.Errorf("write trustlines: %w", err)
	}
	if err := writeOfferRows(ctx, tx, cat, schema, data.Offers); err != nil {
		return fmt.Errorf("write offers: %w", err)
	}
	if err := writeAccountSignerRows(ctx, tx, cat, schema, data.AccountSigners); err != nil {
		return fmt.Errorf("write account signers: %w", err)
	}
	if err := writeClaimableBalanceRows(ctx, tx, cat, schema, data.ClaimableBalances); err != nil {
		return fmt.Errorf("write claimable balances: %w", err)
	}
	if err := writeLiquidityPoolRows(ctx, tx, cat, schema, data.LiquidityPools); err != nil {
		return fmt.Errorf("write liquidity pools: %w", err)
	}
	if err := writeConfigSettingRows(ctx, tx, cat, schema, data.ConfigSettings); err != nil {
		return fmt.Errorf("write config settings: %w", err)
	}
	if err := writeTTLRows(ctx, tx, cat, schema, data.TTLEntries); err != nil {
		return fmt.Errorf("write TTL: %w", err)
	}
	if err := writeEvictedKeyRows(ctx, tx, cat, schema, data.EvictedKeys); err != nil {
		return fmt.Errorf("write evicted keys: %w", err)
	}
	if err := writeRestoredKeyRows(ctx, tx, cat, schema, data.RestoredKeys); err != nil {
		return fmt.Errorf("write restored keys: %w", err)
	}
	if err := writeContractEventRows(ctx, tx, cat, schema, data.ContractEvents); err != nil {
		return fmt.Errorf("write contract events: %w", err)
	}
	if err := writeContractDataRows(ctx, tx, cat, schema, data.ContractData); err != nil {
		return fmt.Errorf("write contract data: %w", err)
	}
	if err := writeContractCodeRows(ctx, tx, cat, schema, data.ContractCode); err != nil {
		return fmt.Errorf("write contract code: %w", err)
	}
	if err := writeNativeBalanceRows(ctx, tx, cat, schema, data.NativeBalances); err != nil {
		return fmt.Errorf("write native balances: %w", err)
	}
	if err := writeContractCreationRows(ctx, tx, cat, schema, data.ContractCreations); err != nil {
		return fmt.Errorf("write contract creations: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Ledger writer
// ---------------------------------------------------------------------------

func writeLedgerRow(ctx context.Context, tx *sql.Tx, cat, schema string, row *LedgerRow) error {
	if row == nil {
		return nil
	}
	table := fmt.Sprintf("%s.%s.ledgers_row_v2", cat, schema)
	cols := []string{
		"sequence", "ledger_hash", "previous_ledger_hash", "closed_at", "protocol_version",
		"total_coins", "fee_pool", "base_fee", "base_reserve", "max_tx_set_size",
		"successful_tx_count", "failed_tx_count", "ingestion_timestamp", "ledger_range",
		"transaction_count", "operation_count", "tx_set_operation_count",
		"soroban_fee_write1kb", "node_id", "signature", "ledger_header",
		"bucket_list_size", "live_soroban_state_size", "evicted_keys_count",
		"soroban_op_count", "total_fee_charged", "contract_events_count",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	_, err := tx.ExecContext(ctx, query,
		row.Sequence, row.LedgerHash, row.PreviousLedgerHash, row.ClosedAt, row.ProtocolVersion,
		row.TotalCoins, row.FeePool, row.BaseFee, row.BaseReserve, row.MaxTxSetSize,
		row.SuccessfulTxCount, row.FailedTxCount, row.IngestionTimestamp, row.LedgerRange,
		row.TransactionCount, row.OperationCount, row.TxSetOperationCount,
		row.SorobanFeeWrite1KB, row.NodeID, row.Signature, row.LedgerHeader,
		row.BucketListSize, row.LiveSorobanStateSize, row.EvictedKeysCount,
		row.SorobanOpCount, row.TotalFeeCharged, row.ContractEventsCount,
		row.EraID, row.VersionLabel,
	)
	return err
}

// ---------------------------------------------------------------------------
// Transaction writer
// ---------------------------------------------------------------------------

func writeTransactionRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []TransactionRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.transactions_row_v2", cat, schema)
	cols := []string{
		"ledger_sequence", "transaction_hash", "source_account", "source_account_muxed",
		"fee_charged", "max_fee", "successful", "transaction_result_code",
		"operation_count", "memo_type", "memo", "created_at",
		"account_sequence", "ledger_range", "signatures_count", "new_account",
		"fee_account_muxed", "inner_transaction_hash", "fee_bump_fee", "max_fee_bid",
		"inner_source_account",
		"timebounds_min_time", "timebounds_max_time",
		"ledgerbounds_min", "ledgerbounds_max",
		"min_sequence_number", "min_sequence_age",
		"soroban_host_function_type", "soroban_contract_id",
		"rent_fee_charged", "soroban_resources_instructions",
		"soroban_resources_read_bytes", "soroban_resources_write_bytes",
		"soroban_data_size_bytes", "soroban_data_resources",
		"soroban_fee_base", "soroban_fee_resources", "soroban_fee_refund",
		"soroban_fee_charged", "soroban_fee_wasted",
		"soroban_contract_events_count",
		"tx_envelope", "tx_result", "tx_meta", "tx_fee_meta",
		"tx_signers", "extra_signers",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.LedgerSequence, r.TransactionHash, r.SourceAccount, r.SourceAccountMuxed,
			r.FeeCharged, r.MaxFee, r.Successful, r.TransactionResultCode,
			r.OperationCount, r.MemoType, r.Memo, r.CreatedAt,
			r.AccountSequence, r.LedgerRange, r.SignaturesCount, r.NewAccount,
			r.FeeAccountMuxed, r.InnerTransactionHash, r.FeeBumpFee, r.MaxFeeBid,
			r.InnerSourceAccount,
			r.TimeboundsMinTime, r.TimeboundsMaxTime,
			r.LedgerboundsMin, r.LedgerboundsMax,
			r.MinSequenceNumber, r.MinSequenceAge,
			r.SorobanHostFunctionType, r.SorobanContractID,
			r.RentFeeCharged, r.SorobanResourcesInstructions,
			r.SorobanResourcesReadBytes, r.SorobanResourcesWriteBytes,
			r.SorobanDataSizeBytes, r.SorobanDataResources,
			r.SorobanFeeBase, r.SorobanFeeResources, r.SorobanFeeRefund,
			r.SorobanFeeCharged, r.SorobanFeeWasted,
			r.SorobanContractEventsCount,
			r.TxEnvelope, r.TxResult, r.TxMeta, r.TxFeeMeta,
			r.TxSigners, r.ExtraSigners,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert transaction %s: %w", r.TransactionHash, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Operation writer
// ---------------------------------------------------------------------------

func writeOperationRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []OperationRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.operations_row_v2", cat, schema)
	cols := []string{
		"transaction_hash", "transaction_index", "operation_index",
		"ledger_sequence", "source_account", "source_account_muxed",
		"type", "type_string", "created_at",
		"transaction_successful", "operation_result_code", "operation_trace_code", "ledger_range",
		"amount", "asset", "asset_type", "asset_code", "asset_issuer",
		"destination",
		"source_asset", "source_asset_type", "source_asset_code", "source_asset_issuer",
		"source_amount", "destination_min", "starting_balance",
		"trustline_limit", "trustor", "authorize",
		"authorize_to_maintain_liabilities", "trust_line_flags",
		"offer_id", "price", "price_r",
		"buying_asset", "buying_asset_type", "buying_asset_code", "buying_asset_issuer",
		"selling_asset", "selling_asset_type", "selling_asset_code", "selling_asset_issuer",
		"set_flags", "clear_flags",
		"home_domain", "master_weight",
		"low_threshold", "medium_threshold", "high_threshold",
		"data_name", "data_value",
		"balance_id", "claimants_count", "sponsored_id", "bump_to",
		"soroban_auth_required",
		"soroban_operation", "soroban_contract_id",
		"soroban_function", "soroban_arguments_json",
		"contract_calls_json", "contracts_involved", "max_call_depth",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.TransactionHash, r.TransactionIndex, r.OperationIndex,
			r.LedgerSequence, r.SourceAccount, r.SourceAccountMuxed,
			r.OpType, r.TypeString, r.CreatedAt,
			r.TransactionSuccessful, r.OperationResultCode, r.OperationTraceCode, r.LedgerRange,
			r.Amount, r.Asset, r.AssetType, r.AssetCode, r.AssetIssuer,
			r.Destination,
			r.SourceAsset, r.SourceAssetType, r.SourceAssetCode, r.SourceAssetIssuer,
			r.SourceAmount, r.DestinationMin, r.StartingBalance,
			r.TrustlineLimit, r.Trustor, r.Authorize,
			r.AuthorizeToMaintainLiabilities, r.TrustLineFlags,
			r.OfferID, r.Price, r.PriceR,
			r.BuyingAsset, r.BuyingAssetType, r.BuyingAssetCode, r.BuyingAssetIssuer,
			r.SellingAsset, r.SellingAssetType, r.SellingAssetCode, r.SellingAssetIssuer,
			r.SetFlags, r.ClearFlags,
			r.HomeDomain, r.MasterWeight,
			r.LowThreshold, r.MediumThreshold, r.HighThreshold,
			r.DataName, r.DataValue,
			r.BalanceID, r.ClaimantsCount, r.SponsoredID, r.BumpTo,
			r.SorobanAuthRequired,
			r.SorobanOperation, r.SorobanContractID,
			r.SorobanFunction, r.SorobanArgumentsJSON,
			r.ContractCallsJSON, r.ContractsInvolved, r.MaxCallDepth,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert operation %s:%d: %w", r.TransactionHash, r.OperationIndex, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Effect writer
// ---------------------------------------------------------------------------

func writeEffectRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []EffectRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.effects_row_v1", cat, schema)
	cols := []string{
		"ledger_sequence", "transaction_hash", "operation_index",
		"effect_index", "effect_type", "effect_type_string",
		"account_id", "amount", "asset_code", "asset_issuer",
		"asset_type",
		"trustline_limit", "authorize_flag", "clawback_flag",
		"signer_account", "signer_weight",
		"offer_id", "seller_account",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.LedgerSequence, r.TransactionHash, r.OperationIndex,
			r.EffectIndex, r.EffectType, r.EffectTypeString,
			r.AccountID, r.Amount, r.AssetCode, r.AssetIssuer,
			r.AssetType,
			r.TrustlineLimit, r.AuthorizeFlag, r.ClawbackFlag,
			r.SignerAccount, r.SignerWeight,
			r.OfferID, r.SellerAccount,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert effect: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Trade writer
// ---------------------------------------------------------------------------

func writeTradeRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []TradeRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.trades_row_v1", cat, schema)
	cols := []string{
		"ledger_sequence", "transaction_hash", "operation_index",
		"trade_index", "trade_type", "trade_timestamp",
		"seller_account", "selling_asset_code", "selling_asset_issuer",
		"selling_amount", "buyer_account", "buying_asset_code",
		"buying_asset_issuer", "buying_amount", "price",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.LedgerSequence, r.TransactionHash, r.OperationIndex,
			r.TradeIndex, r.TradeType, r.TradeTimestamp,
			r.SellerAccount, r.SellingAssetCode, r.SellingAssetIssuer,
			r.SellingAmount, r.BuyerAccount, r.BuyingAssetCode,
			r.BuyingAssetIssuer, r.BuyingAmount, r.Price,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert trade: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Account writer
// ---------------------------------------------------------------------------

func writeAccountRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []AccountRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.accounts_snapshot_v1", cat, schema)
	cols := []string{
		"account_id", "ledger_sequence", "closed_at",
		"balance", "sequence_number", "num_subentries",
		"num_sponsoring", "num_sponsored", "home_domain",
		"master_weight", "low_threshold", "med_threshold",
		"high_threshold", "flags", "auth_required",
		"auth_revocable", "auth_immutable",
		"auth_clawback_enabled", "signers", "sponsor_account",
		"created_at", "updated_at",
		"ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.AccountID, r.LedgerSequence, r.ClosedAt,
			r.Balance, r.SequenceNumber, r.NumSubentries,
			r.NumSponsoring, r.NumSponsored, r.HomeDomain,
			r.MasterWeight, r.LowThreshold, r.MedThreshold,
			r.HighThreshold, r.Flags, r.AuthRequired,
			r.AuthRevocable, r.AuthImmutable,
			r.AuthClawbackEnabled, r.Signers, r.SponsorAccount,
			r.CreatedAt, r.UpdatedAt,
			r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert account %s: %w", r.AccountID, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Trustline writer
// ---------------------------------------------------------------------------

func writeTrustlineRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []TrustlineRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.trustlines_snapshot_v1", cat, schema)
	cols := []string{
		"account_id", "asset_code", "asset_issuer", "asset_type",
		"balance", "trust_limit", "buying_liabilities",
		"selling_liabilities", "authorized",
		"authorized_to_maintain_liabilities", "clawback_enabled",
		"ledger_sequence", "created_at",
		"ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.AccountID, r.AssetCode, r.AssetIssuer, r.AssetType,
			r.Balance, r.TrustLimit, r.BuyingLiabilities,
			r.SellingLiabilities, r.Authorized,
			r.AuthorizedToMaintainLiabilities, r.ClawbackEnabled,
			r.LedgerSequence, r.CreatedAt,
			r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert trustline: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Offer writer
// ---------------------------------------------------------------------------

func writeOfferRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []OfferRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.offers_snapshot_v1", cat, schema)
	cols := []string{
		"offer_id", "seller_account", "ledger_sequence",
		"closed_at", "selling_asset_type", "selling_asset_code",
		"selling_asset_issuer", "buying_asset_type", "buying_asset_code",
		"buying_asset_issuer", "amount", "price", "flags",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.OfferID, r.SellerAccount, r.LedgerSequence,
			r.ClosedAt, r.SellingAssetType, r.SellingAssetCode,
			r.SellingAssetIssuer, r.BuyingAssetType, r.BuyingAssetCode,
			r.BuyingAssetIssuer, r.Amount, r.Price, r.Flags,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert offer %d: %w", r.OfferID, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Account Signer writer
// ---------------------------------------------------------------------------

func writeAccountSignerRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []AccountSignerRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.account_signers_snapshot_v1", cat, schema)
	cols := []string{
		"account_id", "signer", "ledger_sequence",
		"weight", "sponsor", "deleted",
		"closed_at", "ledger_range", "created_at",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.AccountID, r.Signer, r.LedgerSequence,
			r.Weight, r.Sponsor, r.Deleted,
			r.ClosedAt, r.LedgerRange, r.CreatedAt,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert account signer: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Claimable Balance writer
// ---------------------------------------------------------------------------

func writeClaimableBalanceRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ClaimableBalanceRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.claimable_balances_snapshot_v1", cat, schema)
	cols := []string{
		"balance_id", "sponsor", "ledger_sequence",
		"closed_at", "asset_type", "asset_code",
		"asset_issuer", "amount", "claimants_count",
		"flags", "created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.BalanceID, r.Sponsor, r.LedgerSequence,
			r.ClosedAt, r.AssetType, r.AssetCode,
			r.AssetIssuer, r.Amount, r.ClaimantsCount,
			r.Flags, r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert claimable balance: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Liquidity Pool writer
// ---------------------------------------------------------------------------

func writeLiquidityPoolRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []LiquidityPoolRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.liquidity_pools_snapshot_v1", cat, schema)
	cols := []string{
		"liquidity_pool_id", "ledger_sequence", "closed_at",
		"pool_type", "fee", "trustline_count",
		"total_pool_shares", "asset_a_type", "asset_a_code",
		"asset_a_issuer", "asset_a_amount", "asset_b_type",
		"asset_b_code", "asset_b_issuer", "asset_b_amount",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.LiquidityPoolID, r.LedgerSequence, r.ClosedAt,
			r.PoolType, r.Fee, r.TrustlineCount,
			r.TotalPoolShares, r.AssetAType, r.AssetACode,
			r.AssetAIssuer, r.AssetAAmount, r.AssetBType,
			r.AssetBCode, r.AssetBIssuer, r.AssetBAmount,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert liquidity pool: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Config Setting writer
// ---------------------------------------------------------------------------

func writeConfigSettingRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ConfigSettingRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.config_settings_snapshot_v1", cat, schema)
	cols := []string{
		"config_setting_id", "ledger_sequence",
		"last_modified_ledger", "deleted",
		"closed_at",
		"ledger_max_instructions", "tx_max_instructions",
		"fee_rate_per_instructions_increment", "tx_memory_limit",
		"ledger_max_read_ledger_entries", "ledger_max_read_bytes",
		"ledger_max_write_ledger_entries", "ledger_max_write_bytes",
		"tx_max_read_ledger_entries", "tx_max_read_bytes",
		"tx_max_write_ledger_entries", "tx_max_write_bytes",
		"contract_max_size_bytes",
		"config_setting_xdr", "created_at",
		"ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.ConfigSettingID, r.LedgerSequence,
			r.LastModifiedLedger, r.Deleted,
			r.ClosedAt,
			r.LedgerMaxInstructions, r.TxMaxInstructions,
			r.FeeRatePerInstructionsIncrement, r.TxMemoryLimit,
			r.LedgerMaxReadLedgerEntries, r.LedgerMaxReadBytes,
			r.LedgerMaxWriteLedgerEntries, r.LedgerMaxWriteBytes,
			r.TxMaxReadLedgerEntries, r.TxMaxReadBytes,
			r.TxMaxWriteLedgerEntries, r.TxMaxWriteBytes,
			r.ContractMaxSizeBytes,
			r.ConfigSettingXDR, r.CreatedAt,
			r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert config setting: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// TTL writer
// ---------------------------------------------------------------------------

func writeTTLRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []TTLRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.ttl_snapshot_v1", cat, schema)
	cols := []string{
		"key_hash", "ledger_sequence", "live_until_ledger_seq",
		"ttl_remaining", "expired", "last_modified_ledger",
		"deleted", "closed_at", "created_at",
		"ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.KeyHash, r.LedgerSequence, r.LiveUntilLedgerSeq,
			r.TTLRemaining, r.Expired, r.LastModifiedLedger,
			r.Deleted, r.ClosedAt, r.CreatedAt,
			r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert TTL: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Evicted Key writer
// ---------------------------------------------------------------------------

func writeEvictedKeyRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []EvictedKeyRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.evicted_keys_state_v1", cat, schema)
	cols := []string{
		"key_hash", "ledger_sequence", "contract_id",
		"key_type", "durability", "closed_at",
		"ledger_range", "created_at",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.KeyHash, r.LedgerSequence, r.ContractID,
			r.KeyType, r.Durability, r.ClosedAt,
			r.LedgerRange, r.CreatedAt,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert evicted key: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Restored Key writer
// ---------------------------------------------------------------------------

func writeRestoredKeyRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []RestoredKeyRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.restored_keys_state_v1", cat, schema)
	cols := []string{
		"key_hash", "ledger_sequence", "contract_id",
		"key_type", "durability", "restored_from_ledger",
		"closed_at", "ledger_range", "created_at",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.KeyHash, r.LedgerSequence, r.ContractID,
			r.KeyType, r.Durability, r.RestoredFromLedger,
			r.ClosedAt, r.LedgerRange, r.CreatedAt,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert restored key: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Contract Event writer
// ---------------------------------------------------------------------------

func writeContractEventRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ContractEventRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.contract_events_stream_v1", cat, schema)
	cols := []string{
		"event_id", "contract_id", "ledger_sequence",
		"transaction_hash", "closed_at", "event_type",
		"in_successful_contract_call", "topics_json",
		"topics_decoded", "data_xdr", "data_decoded",
		"topic_count", "topic0_decoded", "topic1_decoded",
		"topic2_decoded", "topic3_decoded",
		"operation_index", "event_index",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.EventID, r.ContractID, r.LedgerSequence,
			r.TransactionHash, r.ClosedAt, r.EventType,
			r.InSuccessfulContractCall, r.TopicsJSON,
			r.TopicsDecoded, r.DataXDR, r.DataDecoded,
			r.TopicCount, r.Topic0Decoded, r.Topic1Decoded,
			r.Topic2Decoded, r.Topic3Decoded,
			r.OperationIndex, r.EventIndex,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert contract event: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Contract Data writer
// ---------------------------------------------------------------------------

func writeContractDataRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ContractDataRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.contract_data_snapshot_v1", cat, schema)
	cols := []string{
		"contract_id", "ledger_sequence", "ledger_key_hash",
		"contract_key_type", "contract_durability",
		"asset_code", "asset_issuer", "asset_type",
		"balance_holder", "balance",
		"last_modified_ledger", "ledger_entry_change",
		"deleted", "closed_at", "contract_data_xdr",
		"token_name", "token_symbol", "token_decimals",
		"created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.ContractID, r.LedgerSequence, r.LedgerKeyHash,
			r.ContractKeyType, r.ContractDurability,
			r.AssetCode, r.AssetIssuer, r.AssetType,
			r.BalanceHolder, r.Balance,
			r.LastModifiedLedger, r.LedgerEntryChange,
			r.Deleted, r.ClosedAt, r.ContractDataXDR,
			r.TokenName, r.TokenSymbol, r.TokenDecimals,
			r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert contract data: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Contract Code writer
// ---------------------------------------------------------------------------

func writeContractCodeRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ContractCodeRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.contract_code_snapshot_v1", cat, schema)
	cols := []string{
		"contract_code_hash", "ledger_key_hash",
		"contract_code_ext_v", "last_modified_ledger",
		"ledger_entry_change", "deleted",
		"closed_at", "ledger_sequence",
		"n_instructions", "n_functions", "n_globals",
		"n_table_entries", "n_types", "n_data_segments",
		"n_elem_segments", "n_imports", "n_exports",
		"n_data_segment_bytes", "created_at", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.ContractCodeHash, r.LedgerKeyHash,
			r.ContractCodeExtV, r.LastModifiedLedger,
			r.LedgerEntryChange, r.Deleted,
			r.ClosedAt, r.LedgerSequence,
			r.NInstructions, r.NFunctions, r.NGlobals,
			r.NTableEntries, r.NTypes, r.NDataSegments,
			r.NElemSegments, r.NImports, r.NExports,
			r.NDataSegmentBytes, r.CreatedAt, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert contract code: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Native Balance writer
// ---------------------------------------------------------------------------

func writeNativeBalanceRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []NativeBalanceRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.native_balances_snapshot_v1", cat, schema)
	cols := []string{
		"account_id", "balance", "buying_liabilities",
		"selling_liabilities", "num_subentries",
		"num_sponsoring", "num_sponsored",
		"sequence_number", "last_modified_ledger",
		"ledger_sequence", "ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.AccountID, r.Balance, r.BuyingLiabilities,
			r.SellingLiabilities, r.NumSubentries,
			r.NumSponsoring, r.NumSponsored,
			r.SequenceNumber, r.LastModifiedLedger,
			r.LedgerSequence, r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert native balance: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Contract Creation writer
// ---------------------------------------------------------------------------

func writeContractCreationRows(ctx context.Context, tx *sql.Tx, cat, schema string, rows []ContractCreationRow) error {
	if len(rows) == 0 {
		return nil
	}
	table := fmt.Sprintf("%s.%s.contract_creations_v1", cat, schema)
	cols := []string{
		"contract_id", "creator_address", "wasm_hash",
		"created_ledger", "created_at",
		"ledger_range",
		"era_id", "version_label",
	}
	placeholders := makePlaceholders(len(cols))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), placeholders)

	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, query,
			r.ContractID, r.CreatorAddress, r.WasmHash,
			r.CreatedLedger, r.CreatedAt,
			r.LedgerRange,
			r.EraID, r.VersionLabel,
		); err != nil {
			return fmt.Errorf("insert contract creation: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makePlaceholders generates "$1,$2,...,$n" for parameterized queries.
func makePlaceholders(n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(parts, ",")
}
