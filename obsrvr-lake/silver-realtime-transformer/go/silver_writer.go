package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

// SilverWriter writes transformed data to silver hot buffer (silver_hot PostgreSQL)
type SilverWriter struct {
	db *sql.DB
}

// NewSilverWriter creates a new silver writer
func NewSilverWriter(db *sql.DB) *SilverWriter {
	return &SilverWriter{db: db}
}

// WriteEnrichedOperation inserts an enriched operation row
func (sw *SilverWriter) WriteEnrichedOperation(ctx context.Context, tx *sql.Tx, row *EnrichedOperationRow) error {
	query := `
		INSERT INTO enriched_history_operations (
			transaction_hash, operation_index, ledger_sequence, source_account,
			type, type_string, created_at, transaction_successful,
			operation_result_code, operation_trace_code, ledger_range,
			source_account_muxed, asset, asset_type, asset_code, asset_issuer,
			source_asset, source_asset_type, source_asset_code, source_asset_issuer,
			destination, destination_muxed, amount, source_amount,
			from_account, from_muxed, to_address, to_muxed,
			limit_amount, offer_id,
			selling_asset, selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset, buying_asset_type, buying_asset_code, buying_asset_issuer,
			price_n, price_d, price,
			starting_balance, home_domain, inflation_dest,
			set_flags, set_flags_s, clear_flags, clear_flags_s,
			master_key_weight, low_threshold, med_threshold, high_threshold,
			signer_account_id, signer_key, signer_weight,
			data_name, data_value,
			host_function_type, parameters, address, contract_id, function_name,
			balance_id, claimant, claimant_muxed, predicate,
			liquidity_pool_id, reserve_a_asset, reserve_a_amount,
			reserve_b_asset, reserve_b_amount, shares, shares_received,
			into_account, into_muxed,
			sponsor, sponsored_id, begin_sponsor,
			tx_successful, tx_fee_charged, tx_max_fee, tx_operation_count,
			tx_memo_type, tx_memo,
			ledger_closed_at, ledger_total_coins, ledger_fee_pool,
			ledger_base_fee, ledger_base_reserve,
			ledger_transaction_count, ledger_operation_count,
			ledger_successful_tx_count, ledger_failed_tx_count,
			is_payment_op, is_soroban_op
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
			$31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44,
			$45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58,
			$59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72,
			$73, $74, $75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86,
			$87, $88, $89, $90, $91, $92, $93, $94, $95
		)
		ON CONFLICT (transaction_hash, operation_index) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.TransactionHash, row.OperationIndex, row.LedgerSequence, row.SourceAccount,
		row.Type, row.TypeString, row.CreatedAt, row.TransactionSuccessful,
		row.OperationResultCode, row.OperationTraceCode, row.LedgerRange,
		row.SourceAccountMuxed, row.Asset, row.AssetType, row.AssetCode, row.AssetIssuer,
		row.SourceAsset, row.SourceAssetType, row.SourceAssetCode, row.SourceAssetIssuer,
		row.Destination, row.DestinationMuxed, row.Amount, row.SourceAmount,
		row.FromAccount, row.FromMuxed, row.To, row.ToMuxed,
		row.LimitAmount, row.OfferID,
		row.SellingAsset, row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAsset, row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.PriceN, row.PriceD, row.Price,
		row.StartingBalance, row.HomeDomain, row.InflationDest,
		row.SetFlags, row.SetFlagsS, row.ClearFlags, row.ClearFlagsS,
		row.MasterKeyWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.SignerAccountID, row.SignerKey, row.SignerWeight,
		row.DataName, row.DataValue,
		row.HostFunctionType, row.Parameters, row.Address, row.ContractID, row.FunctionName,
		row.BalanceID, row.Claimant, row.ClaimantMuxed, row.Predicate,
		row.LiquidityPoolID, row.ReserveAAsset, row.ReserveAAmount,
		row.ReserveBAsset, row.ReserveBAmount, row.Shares, row.SharesReceived,
		row.Into, row.IntoMuxed,
		row.Sponsor, row.SponsoredID, row.BeginSponsor,
		row.TxSuccessful, row.TxFeeCharged, row.TxMaxFee, row.TxOperationCount,
		row.TxMemoType, row.TxMemo,
		row.LedgerClosedAt, row.LedgerTotalCoins, row.LedgerFeePool,
		row.LedgerBaseFee, row.LedgerBaseReserve,
		row.LedgerTransactionCount, row.LedgerOperationCount,
		row.LedgerSuccessfulTxCount, row.LedgerFailedTxCount,
		row.IsPaymentOp, row.IsSorobanOp,
	)

	if err != nil {
		return fmt.Errorf("failed to write enriched operation: %w", err)
	}

	// Also write to Soroban table if it's a Soroban operation
	if row.IsSorobanOp != nil && *row.IsSorobanOp {
		return sw.WriteEnrichedOperationSoroban(ctx, tx, row)
	}

	return nil
}

// WriteEnrichedOperationSoroban inserts into the soroban-specific table
func (sw *SilverWriter) WriteEnrichedOperationSoroban(ctx context.Context, tx *sql.Tx, row *EnrichedOperationRow) error {
	query := `
		INSERT INTO enriched_history_operations_soroban (
			transaction_hash, operation_index, ledger_sequence, source_account,
			type, type_string, created_at, transaction_successful,
			operation_result_code, operation_trace_code, ledger_range,
			source_account_muxed, asset, asset_type, asset_code, asset_issuer,
			source_asset, source_asset_type, source_asset_code, source_asset_issuer,
			destination, destination_muxed, amount, source_amount,
			from_account, from_muxed, to_address, to_muxed,
			limit_amount, offer_id,
			selling_asset, selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset, buying_asset_type, buying_asset_code, buying_asset_issuer,
			price_n, price_d, price,
			starting_balance, home_domain, inflation_dest,
			set_flags, set_flags_s, clear_flags, clear_flags_s,
			master_key_weight, low_threshold, med_threshold, high_threshold,
			signer_account_id, signer_key, signer_weight,
			data_name, data_value,
			host_function_type, parameters, address, contract_id, function_name,
			balance_id, claimant, claimant_muxed, predicate,
			liquidity_pool_id, reserve_a_asset, reserve_a_amount,
			reserve_b_asset, reserve_b_amount, shares, shares_received,
			into_account, into_muxed,
			sponsor, sponsored_id, begin_sponsor,
			tx_successful, tx_fee_charged, tx_max_fee, tx_operation_count,
			tx_memo_type, tx_memo,
			ledger_closed_at, ledger_total_coins, ledger_fee_pool,
			ledger_base_fee, ledger_base_reserve,
			ledger_transaction_count, ledger_operation_count,
			ledger_successful_tx_count, ledger_failed_tx_count,
			is_payment_op, is_soroban_op
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
			$31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44,
			$45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58,
			$59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72,
			$73, $74, $75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86,
			$87, $88, $89, $90, $91, $92, $93, $94, $95
		)
		ON CONFLICT (transaction_hash, operation_index) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.TransactionHash, row.OperationIndex, row.LedgerSequence, row.SourceAccount,
		row.Type, row.TypeString, row.CreatedAt, row.TransactionSuccessful,
		row.OperationResultCode, row.OperationTraceCode, row.LedgerRange,
		row.SourceAccountMuxed, row.Asset, row.AssetType, row.AssetCode, row.AssetIssuer,
		row.SourceAsset, row.SourceAssetType, row.SourceAssetCode, row.SourceAssetIssuer,
		row.Destination, row.DestinationMuxed, row.Amount, row.SourceAmount,
		row.FromAccount, row.FromMuxed, row.To, row.ToMuxed,
		row.LimitAmount, row.OfferID,
		row.SellingAsset, row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAsset, row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.PriceN, row.PriceD, row.Price,
		row.StartingBalance, row.HomeDomain, row.InflationDest,
		row.SetFlags, row.SetFlagsS, row.ClearFlags, row.ClearFlagsS,
		row.MasterKeyWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.SignerAccountID, row.SignerKey, row.SignerWeight,
		row.DataName, row.DataValue,
		row.HostFunctionType, row.Parameters, row.Address, row.ContractID, row.FunctionName,
		row.BalanceID, row.Claimant, row.ClaimantMuxed, row.Predicate,
		row.LiquidityPoolID, row.ReserveAAsset, row.ReserveAAmount,
		row.ReserveBAsset, row.ReserveBAmount, row.Shares, row.SharesReceived,
		row.Into, row.IntoMuxed,
		row.Sponsor, row.SponsoredID, row.BeginSponsor,
		row.TxSuccessful, row.TxFeeCharged, row.TxMaxFee, row.TxOperationCount,
		row.TxMemoType, row.TxMemo,
		row.LedgerClosedAt, row.LedgerTotalCoins, row.LedgerFeePool,
		row.LedgerBaseFee, row.LedgerBaseReserve,
		row.LedgerTransactionCount, row.LedgerOperationCount,
		row.LedgerSuccessfulTxCount, row.LedgerFailedTxCount,
		row.IsPaymentOp, row.IsSorobanOp,
	)

	return err
}

// WriteTokenTransfer inserts a token transfer row
func (sw *SilverWriter) WriteTokenTransfer(ctx context.Context, tx *sql.Tx, row *TokenTransferRow) error {
	query := `
		INSERT INTO token_transfers_raw (
			timestamp, transaction_hash, ledger_sequence, source_type,
			from_account, to_account, asset_code, asset_issuer, amount,
			token_contract_id, operation_type, transaction_successful
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
		ON CONFLICT DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.Timestamp, row.TransactionHash, row.LedgerSequence, row.SourceType,
		row.FromAccount, row.ToAccount, row.AssetCode, row.AssetIssuer, row.Amount,
		row.TokenContractID, row.OperationType, row.TransactionSuccessful,
	)

	if err != nil {
		return fmt.Errorf("failed to write token transfer: %w", err)
	}

	return nil
}

// WriteAccountCurrent upserts an account current state row
func (sw *SilverWriter) WriteAccountCurrent(ctx context.Context, tx *sql.Tx, row *AccountCurrentRow) error {
	query := `
		INSERT INTO accounts_current (
			account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, created_at, updated_at,
			last_modified_ledger, ledger_range, era_id, version_label
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24
		)
		ON CONFLICT (account_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			sequence_number = EXCLUDED.sequence_number,
			num_subentries = EXCLUDED.num_subentries,
			num_sponsoring = EXCLUDED.num_sponsoring,
			num_sponsored = EXCLUDED.num_sponsored,
			home_domain = EXCLUDED.home_domain,
			master_weight = EXCLUDED.master_weight,
			low_threshold = EXCLUDED.low_threshold,
			med_threshold = EXCLUDED.med_threshold,
			high_threshold = EXCLUDED.high_threshold,
			flags = EXCLUDED.flags,
			auth_required = EXCLUDED.auth_required,
			auth_revocable = EXCLUDED.auth_revocable,
			auth_immutable = EXCLUDED.auth_immutable,
			auth_clawback_enabled = EXCLUDED.auth_clawback_enabled,
			signers = EXCLUDED.signers,
			sponsor_account = EXCLUDED.sponsor_account,
			updated_at = EXCLUDED.updated_at,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_range = EXCLUDED.ledger_range
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.Balance, row.SequenceNumber, row.NumSubentries,
		row.NumSponsoring, row.NumSponsored, row.HomeDomain,
		row.MasterWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.Flags, row.AuthRequired, row.AuthRevocable, row.AuthImmutable, row.AuthClawbackEnabled,
		row.Signers, row.SponsorAccount, row.CreatedAt, row.UpdatedAt,
		row.LastModifiedLedger, row.LedgerRange, row.EraID, row.VersionLabel,
	)

	if err != nil {
		return fmt.Errorf("failed to write account current: %w", err)
	}

	return nil
}

// WriteTrustlineCurrent upserts a trustline current state row
func (sw *SilverWriter) WriteTrustlineCurrent(ctx context.Context, tx *sql.Tx, row *TrustlineCurrentRow) error {
	query := `
		INSERT INTO trustlines_current (
			account_id, asset_type, asset_issuer, asset_code, liquidity_pool_id,
			balance, trust_line_limit, buying_liabilities, selling_liabilities,
			flags, last_modified_ledger, ledger_sequence, created_at, sponsor, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5,
			ROUND($6::NUMERIC * 10000000)::BIGINT,
			ROUND($7::NUMERIC * 10000000)::BIGINT,
			ROUND($8::NUMERIC * 10000000)::BIGINT,
			ROUND($9::NUMERIC * 10000000)::BIGINT,
			$10, $11, $12, $13, $14, $15
		)
		ON CONFLICT (account_id, asset_type, COALESCE(asset_code, ''), COALESCE(asset_issuer, ''), COALESCE(liquidity_pool_id, '')) DO UPDATE SET
			balance = EXCLUDED.balance,
			trust_line_limit = EXCLUDED.trust_line_limit,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			flags = EXCLUDED.flags,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			sponsor = EXCLUDED.sponsor,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.AssetType, row.AssetIssuer, row.AssetCode, row.LiquidityPoolID,
		row.Balance, row.TrustLineLimit, row.BuyingLiabilities, row.SellingLiabilities,
		row.Flags, row.LastModifiedLedger, row.LedgerSequence, row.CreatedAt, row.Sponsor, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write trustline current: %w", err)
	}

	return nil
}

// WriteOfferCurrent upserts an offer current state row
func (sw *SilverWriter) WriteOfferCurrent(ctx context.Context, tx *sql.Tx, row *OfferCurrentRow) error {
	query := `
		INSERT INTO offers_current (
			offer_id, seller_id, selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price_n, price_d, price, flags,
			last_modified_ledger, ledger_sequence, created_at, sponsor, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9::BIGINT, $10, $11, $12::DECIMAL, $13,
			$14, $15, $16, $17, $18
		)
		ON CONFLICT (offer_id) DO UPDATE SET
			seller_id = EXCLUDED.seller_id,
			selling_asset_type = EXCLUDED.selling_asset_type,
			selling_asset_code = EXCLUDED.selling_asset_code,
			selling_asset_issuer = EXCLUDED.selling_asset_issuer,
			buying_asset_type = EXCLUDED.buying_asset_type,
			buying_asset_code = EXCLUDED.buying_asset_code,
			buying_asset_issuer = EXCLUDED.buying_asset_issuer,
			amount = EXCLUDED.amount,
			price_n = EXCLUDED.price_n,
			price_d = EXCLUDED.price_d,
			price = EXCLUDED.price,
			flags = EXCLUDED.flags,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			sponsor = EXCLUDED.sponsor,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.OfferID, row.SellerID, row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.Amount, row.PriceN, row.PriceD, row.Price, row.Flags,
		row.LastModifiedLedger, row.LedgerSequence, row.CreatedAt, row.Sponsor, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write offer current: %w", err)
	}

	return nil
}

// WriteAccountSnapshot appends an account snapshot row (SCD Type 2 - Step 1: INSERT)
func (sw *SilverWriter) WriteAccountSnapshot(ctx context.Context, tx *sql.Tx, row *AccountSnapshotRow) error {
	query := `
		INSERT INTO accounts_snapshot (
			account_id, ledger_sequence, closed_at, balance, sequence_number,
			num_subentries, num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, created_at, updated_at,
			ledger_range, era_id, version_label, valid_to
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24, $25, NULL
		)
		ON CONFLICT (account_id, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.LedgerSequence, row.ClosedAt, row.Balance, row.SequenceNumber,
		row.NumSubentries, row.NumSponsoring, row.NumSponsored, row.HomeDomain,
		row.MasterWeight, row.LowThreshold, row.MedThreshold, row.HighThreshold,
		row.Flags, row.AuthRequired, row.AuthRevocable, row.AuthImmutable, row.AuthClawbackEnabled,
		row.Signers, row.SponsorAccount, row.CreatedAt, row.UpdatedAt,
		row.LedgerRange, row.EraID, row.VersionLabel,
	)

	if err != nil {
		return fmt.Errorf("failed to write account snapshot: %w", err)
	}

	return nil
}

// UpdateAccountSnapshotValidTo updates valid_to for previous versions (SCD Type 2 - Step 2: UPDATE)
// Only updates accounts that have new snapshots in the given ledger range
func (sw *SilverWriter) UpdateAccountSnapshotValidTo(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) error {
	// This implements the incremental LEAD() pattern from the shape up doc:
	// 1. Find affected accounts (those with new snapshots)
	// 2. Calculate LEAD(closed_at) ONLY for those accounts in boundary partitions
	// 3. Update valid_to for previous versions

	query := `
		WITH affected_accounts AS (
			-- Accounts that have new snapshots in this batch
			SELECT DISTINCT account_id
			FROM accounts_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		current_and_prev_range AS (
			-- Get current and previous ledger_range partitions
			SELECT DISTINCT ledger_range
			FROM accounts_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
			UNION
			SELECT DISTINCT ledger_range - 1 as ledger_range
			FROM accounts_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		next_closed_at AS (
			-- Calculate LEAD(closed_at) but ONLY for affected accounts in boundary partitions
			SELECT
				account_id,
				ledger_sequence,
				LEAD(closed_at) OVER (PARTITION BY account_id ORDER BY ledger_sequence) as valid_to
			FROM accounts_snapshot
			WHERE account_id IN (SELECT account_id FROM affected_accounts)
			  AND ledger_range IN (SELECT ledger_range FROM current_and_prev_range)
		)
		UPDATE accounts_snapshot s
		SET valid_to = nca.valid_to
		FROM next_closed_at nca
		WHERE s.account_id = nca.account_id
		  AND s.ledger_sequence = nca.ledger_sequence
		  AND s.valid_to IS NULL  -- Only update if not already set
		  AND nca.valid_to IS NOT NULL  -- Don't set NULL (means current version)
	`

	_, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to update account snapshot valid_to: %w", err)
	}

	return nil
}

// WriteTrustlineSnapshot appends a trustline snapshot row (SCD Type 2 - Step 1: INSERT)
func (sw *SilverWriter) WriteTrustlineSnapshot(ctx context.Context, tx *sql.Tx, row *TrustlineSnapshotRow) error {
	query := `
		INSERT INTO trustlines_snapshot (
			account_id, asset_code, asset_issuer, asset_type, balance, trust_limit,
			buying_liabilities, selling_liabilities, authorized,
			authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence, created_at, ledger_range, era_id, version_label, valid_to
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NULL
		)
		ON CONFLICT (account_id, asset_code, asset_issuer, asset_type, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.AssetCode, row.AssetIssuer, row.AssetType, row.Balance, row.TrustLimit,
		row.BuyingLiabilities, row.SellingLiabilities, row.Authorized,
		row.AuthorizedToMaintainLiabilities, row.ClawbackEnabled,
		row.LedgerSequence, row.CreatedAt, row.LedgerRange, row.EraID, row.VersionLabel,
	)

	if err != nil {
		return fmt.Errorf("failed to write trustline snapshot: %w", err)
	}

	return nil
}

// UpdateTrustlineSnapshotValidTo updates valid_to for previous versions (SCD Type 2 - Step 2: UPDATE)
func (sw *SilverWriter) UpdateTrustlineSnapshotValidTo(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) error {
	query := `
		WITH affected_trustlines AS (
			SELECT DISTINCT account_id, asset_code, asset_issuer, asset_type
			FROM trustlines_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		current_and_prev_range AS (
			SELECT DISTINCT ledger_range
			FROM trustlines_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
			UNION
			SELECT DISTINCT ledger_range - 1 as ledger_range
			FROM trustlines_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		next_closed_at AS (
			SELECT
				account_id, asset_code, asset_issuer, asset_type, ledger_sequence,
				LEAD(created_at) OVER (PARTITION BY account_id, asset_code, asset_issuer, asset_type ORDER BY ledger_sequence) as valid_to
			FROM trustlines_snapshot
			WHERE (account_id, asset_code, asset_issuer, asset_type) IN
				(SELECT account_id, asset_code, asset_issuer, asset_type FROM affected_trustlines)
			  AND ledger_range IN (SELECT ledger_range FROM current_and_prev_range)
		)
		UPDATE trustlines_snapshot s
		SET valid_to = nca.valid_to
		FROM next_closed_at nca
		WHERE s.account_id = nca.account_id
		  AND s.asset_code = nca.asset_code
		  AND s.asset_issuer = nca.asset_issuer
		  AND s.asset_type = nca.asset_type
		  AND s.ledger_sequence = nca.ledger_sequence
		  AND s.valid_to IS NULL
		  AND nca.valid_to IS NOT NULL
	`

	_, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to update trustline snapshot valid_to: %w", err)
	}

	return nil
}

// WriteOfferSnapshot appends an offer snapshot row (SCD Type 2 - Step 1: INSERT)
func (sw *SilverWriter) WriteOfferSnapshot(ctx context.Context, tx *sql.Tx, row *OfferSnapshotRow) error {
	query := `
		INSERT INTO offers_snapshot (
			offer_id, seller_account, ledger_sequence, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, created_at, ledger_range, era_id, version_label, valid_to
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NULL
		)
		ON CONFLICT (offer_id, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.OfferID, row.SellerAccount, row.LedgerSequence, row.ClosedAt,
		row.SellingAssetType, row.SellingAssetCode, row.SellingAssetIssuer,
		row.BuyingAssetType, row.BuyingAssetCode, row.BuyingAssetIssuer,
		row.Amount, row.Price, row.Flags, row.CreatedAt, row.LedgerRange, row.EraID, row.VersionLabel,
	)

	if err != nil {
		return fmt.Errorf("failed to write offer snapshot: %w", err)
	}

	return nil
}

// UpdateOfferSnapshotValidTo updates valid_to for previous versions (SCD Type 2 - Step 2: UPDATE)
func (sw *SilverWriter) UpdateOfferSnapshotValidTo(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) error {
	query := `
		WITH affected_offers AS (
			SELECT DISTINCT offer_id
			FROM offers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		current_and_prev_range AS (
			SELECT DISTINCT ledger_range
			FROM offers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
			UNION
			SELECT DISTINCT ledger_range - 1 as ledger_range
			FROM offers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		next_closed_at AS (
			SELECT
				offer_id, ledger_sequence,
				LEAD(closed_at) OVER (PARTITION BY offer_id ORDER BY ledger_sequence) as valid_to
			FROM offers_snapshot
			WHERE offer_id IN (SELECT offer_id FROM affected_offers)
			  AND ledger_range IN (SELECT ledger_range FROM current_and_prev_range)
		)
		UPDATE offers_snapshot s
		SET valid_to = nca.valid_to
		FROM next_closed_at nca
		WHERE s.offer_id = nca.offer_id
		  AND s.ledger_sequence = nca.ledger_sequence
		  AND s.valid_to IS NULL
		  AND nca.valid_to IS NOT NULL
	`

	_, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to update offer snapshot valid_to: %w", err)
	}

	return nil
}

// WriteAccountSignerSnapshot appends an account signer snapshot row (SCD Type 2 - Step 1: INSERT)
func (sw *SilverWriter) WriteAccountSignerSnapshot(ctx context.Context, tx *sql.Tx, row *AccountSignerSnapshotRow) error {
	query := `
		INSERT INTO account_signers_snapshot (
			account_id, signer, ledger_sequence, closed_at, weight, sponsor,
			ledger_range, era_id, version_label, valid_to
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, NULL
		)
		ON CONFLICT (account_id, signer, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.Signer, row.LedgerSequence, row.ClosedAt, row.Weight, row.Sponsor,
		row.LedgerRange, row.EraID, row.VersionLabel,
	)

	if err != nil {
		return fmt.Errorf("failed to write account signer snapshot: %w", err)
	}

	return nil
}

// UpdateAccountSignerSnapshotValidTo updates valid_to for previous versions (SCD Type 2 - Step 2: UPDATE)
func (sw *SilverWriter) UpdateAccountSignerSnapshotValidTo(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) error {
	query := `
		WITH affected_signers AS (
			SELECT DISTINCT account_id, signer
			FROM account_signers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		current_and_prev_range AS (
			SELECT DISTINCT ledger_range
			FROM account_signers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
			UNION
			SELECT DISTINCT ledger_range - 1 as ledger_range
			FROM account_signers_snapshot
			WHERE ledger_sequence BETWEEN $1 AND $2
		),
		next_closed_at AS (
			SELECT
				account_id, signer, ledger_sequence,
				LEAD(closed_at) OVER (PARTITION BY account_id, signer ORDER BY ledger_sequence) as valid_to
			FROM account_signers_snapshot
			WHERE (account_id, signer) IN (SELECT account_id, signer FROM affected_signers)
			  AND ledger_range IN (SELECT ledger_range FROM current_and_prev_range)
		)
		UPDATE account_signers_snapshot s
		SET valid_to = nca.valid_to
		FROM next_closed_at nca
		WHERE s.account_id = nca.account_id
		  AND s.signer = nca.signer
		  AND s.ledger_sequence = nca.ledger_sequence
		  AND s.valid_to IS NULL
		  AND nca.valid_to IS NOT NULL
	`

	_, err := tx.ExecContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return fmt.Errorf("failed to update account signer snapshot valid_to: %w", err)
	}

	return nil
}

// WriteContractInvocation inserts a contract invocation row
func (sw *SilverWriter) WriteContractInvocation(ctx context.Context, tx *sql.Tx, row *ContractInvocationRow) error {
	query := `
		INSERT INTO contract_invocations_raw (
			ledger_sequence,
			transaction_index,
			operation_index,
			transaction_hash,
			source_account,
			contract_id,
			function_name,
			arguments_json,
			successful,
			closed_at,
			ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT (ledger_sequence, transaction_index, operation_index) DO UPDATE SET
			contract_id = EXCLUDED.contract_id,
			function_name = EXCLUDED.function_name,
			arguments_json = EXCLUDED.arguments_json,
			successful = EXCLUDED.successful
	`

	_, err := tx.ExecContext(ctx, query,
		row.LedgerSequence,
		row.TransactionIndex,
		row.OperationIndex,
		row.TransactionHash,
		row.SourceAccount,
		row.ContractID,
		row.FunctionName,
		row.ArgumentsJSON,
		row.Successful,
		row.ClosedAt,
		row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write contract invocation: %w", err)
	}

	return nil
}

// WriteContractCall inserts a contract call row (flattened from call graph)
func (sw *SilverWriter) WriteContractCall(ctx context.Context, tx *sql.Tx, row *ContractCallRow) error {
	query := `
		INSERT INTO contract_invocation_calls (
			ledger_sequence,
			transaction_index,
			operation_index,
			transaction_hash,
			from_contract,
			to_contract,
			function_name,
			call_depth,
			execution_order,
			successful,
			closed_at,
			ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index, execution_order) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.LedgerSequence,
		row.TransactionIndex,
		row.OperationIndex,
		row.TransactionHash,
		row.FromContract,
		row.ToContract,
		row.FunctionName,
		row.CallDepth,
		row.ExecutionOrder,
		row.Successful,
		row.ClosedAt,
		row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write contract call: %w", err)
	}

	return nil
}

// WriteContractHierarchy inserts a contract hierarchy row (ancestry chain)
func (sw *SilverWriter) WriteContractHierarchy(ctx context.Context, tx *sql.Tx, row *ContractHierarchyRow) error {
	query := `
		INSERT INTO contract_invocation_hierarchy (
			transaction_hash,
			root_contract,
			child_contract,
			path_depth,
			full_path,
			ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
		ON CONFLICT (transaction_hash, root_contract, child_contract) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.TransactionHash,
		row.RootContract,
		row.ChildContract,
		row.PathDepth,
		pq.Array(row.FullPath),
		row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write contract hierarchy: %w", err)
	}

	return nil
}

// WriteLiquidityPoolCurrent upserts a liquidity pool current state row
func (sw *SilverWriter) WriteLiquidityPoolCurrent(ctx context.Context, tx *sql.Tx, row *LiquidityPoolCurrentRow) error {
	query := `
		INSERT INTO liquidity_pools_current (
			liquidity_pool_id, pool_type, fee, trustline_count, total_pool_shares,
			asset_a_type, asset_a_code, asset_a_issuer, asset_a_amount,
			asset_b_type, asset_b_code, asset_b_issuer, asset_b_amount,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
		)
		ON CONFLICT (liquidity_pool_id) DO UPDATE SET
			pool_type = EXCLUDED.pool_type,
			fee = EXCLUDED.fee,
			trustline_count = EXCLUDED.trustline_count,
			total_pool_shares = EXCLUDED.total_pool_shares,
			asset_a_type = EXCLUDED.asset_a_type,
			asset_a_code = EXCLUDED.asset_a_code,
			asset_a_issuer = EXCLUDED.asset_a_issuer,
			asset_a_amount = EXCLUDED.asset_a_amount,
			asset_b_type = EXCLUDED.asset_b_type,
			asset_b_code = EXCLUDED.asset_b_code,
			asset_b_issuer = EXCLUDED.asset_b_issuer,
			asset_b_amount = EXCLUDED.asset_b_amount,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.LiquidityPoolID, row.PoolType, row.Fee, row.TrustlineCount, row.TotalPoolShares,
		row.AssetAType, row.AssetACode, row.AssetAIssuer, row.AssetAAmount,
		row.AssetBType, row.AssetBCode, row.AssetBIssuer, row.AssetBAmount,
		row.LedgerSequence, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write liquidity pool current: %w", err)
	}

	return nil
}

// WriteClaimableBalanceCurrent upserts a claimable balance current state row
func (sw *SilverWriter) WriteClaimableBalanceCurrent(ctx context.Context, tx *sql.Tx, row *ClaimableBalanceCurrentRow) error {
	query := `
		INSERT INTO claimable_balances_current (
			balance_id, sponsor, asset_type, asset_code, asset_issuer, amount,
			claimants_count, flags, last_modified_ledger, ledger_sequence,
			closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)
		ON CONFLICT (balance_id) DO UPDATE SET
			sponsor = EXCLUDED.sponsor,
			asset_type = EXCLUDED.asset_type,
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			amount = EXCLUDED.amount,
			claimants_count = EXCLUDED.claimants_count,
			flags = EXCLUDED.flags,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.BalanceID, row.Sponsor, row.AssetType, row.AssetCode, row.AssetIssuer, row.Amount,
		row.ClaimantsCount, row.Flags, row.LedgerSequence, row.LedgerSequence,
		row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write claimable balance current: %w", err)
	}

	return nil
}

// WriteNativeBalanceCurrent upserts a native balance current state row
func (sw *SilverWriter) WriteNativeBalanceCurrent(ctx context.Context, tx *sql.Tx, row *NativeBalanceCurrentRow) error {
	query := `
		INSERT INTO native_balances_current (
			account_id, balance, buying_liabilities, selling_liabilities,
			num_subentries, num_sponsoring, num_sponsored, sequence_number,
			last_modified_ledger, ledger_sequence, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT (account_id) DO UPDATE SET
			balance = EXCLUDED.balance,
			buying_liabilities = EXCLUDED.buying_liabilities,
			selling_liabilities = EXCLUDED.selling_liabilities,
			num_subentries = EXCLUDED.num_subentries,
			num_sponsoring = EXCLUDED.num_sponsoring,
			num_sponsored = EXCLUDED.num_sponsored,
			sequence_number = EXCLUDED.sequence_number,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.AccountID, row.Balance, row.BuyingLiabilities, row.SellingLiabilities,
		row.NumSubentries, row.NumSponsoring, row.NumSponsored, row.SequenceNumber,
		row.LastModifiedLedger, row.LedgerSequence, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write native balance current: %w", err)
	}

	return nil
}

// WriteTrade inserts a trade event row (append-only event stream)
func (sw *SilverWriter) WriteTrade(ctx context.Context, tx *sql.Tx, row *TradeRow) error {
	query := `
		INSERT INTO trades (
			ledger_sequence, transaction_hash, operation_index, trade_index,
			trade_type, trade_timestamp, seller_account,
			selling_asset_code, selling_asset_issuer, selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			price, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10::BIGINT, $11, $12, $13, $14::BIGINT, $15::DECIMAL, $16, $17
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index, trade_index) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.LedgerSequence, row.TransactionHash, row.OperationIndex, row.TradeIndex,
		row.TradeType, row.TradeTimestamp, row.SellerAccount,
		row.SellingAssetCode, row.SellingAssetIssuer, row.SellingAmount,
		row.BuyerAccount, row.BuyingAssetCode, row.BuyingAssetIssuer, row.BuyingAmount,
		row.Price, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write trade: %w", err)
	}

	return nil
}

// WriteEffect inserts an effect event row (append-only event stream)
func (sw *SilverWriter) WriteEffect(ctx context.Context, tx *sql.Tx, row *EffectRow) error {
	query := `
		INSERT INTO effects (
			ledger_sequence, transaction_hash, operation_index, effect_index,
			effect_type, effect_type_string, account_id,
			amount, asset_code, asset_issuer, asset_type,
			trustline_limit, authorize_flag, clawback_flag,
			signer_account, signer_weight, offer_id, seller_account,
			created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (ledger_sequence, transaction_hash, operation_index, effect_index) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.LedgerSequence, row.TransactionHash, row.OperationIndex, row.EffectIndex,
		row.EffectType, row.EffectTypeString, row.AccountID,
		row.Amount, row.AssetCode, row.AssetIssuer, row.AssetType,
		row.TrustlineLimit, row.AuthorizeFlag, row.ClawbackFlag,
		row.SignerAccount, row.SignerWeight, row.OfferID, row.SellerAccount,
		row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write effect: %w", err)
	}

	return nil
}

// =============================================================================
// Phase 3: Soroban Tables
// =============================================================================

// WriteContractDataCurrent upserts a contract data current state row
func (sw *SilverWriter) WriteContractDataCurrent(ctx context.Context, tx *sql.Tx, row *ContractDataCurrentRow) error {
	query := `
		INSERT INTO contract_data_current (
			contract_id, key_hash, durability, asset_type, asset_code, asset_issuer,
			data_value, last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
		ON CONFLICT (contract_id, key_hash) DO UPDATE SET
			durability = EXCLUDED.durability,
			asset_type = EXCLUDED.asset_type,
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			data_value = EXCLUDED.data_value,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.ContractID, row.KeyHash, row.Durability, row.AssetType, row.AssetCode, row.AssetIssuer,
		row.DataValue, row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write contract data current: %w", err)
	}

	return nil
}

// WriteContractCodeCurrent upserts a contract code current state row
func (sw *SilverWriter) WriteContractCodeCurrent(ctx context.Context, tx *sql.Tx, row *ContractCodeCurrentRow) error {
	query := `
		INSERT INTO contract_code_current (
			contract_code_hash, contract_code_ext_v,
			n_data_segment_bytes, n_data_segments, n_elem_segments, n_exports,
			n_functions, n_globals, n_imports, n_instructions, n_table_entries, n_types,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (contract_code_hash) DO UPDATE SET
			contract_code_ext_v = EXCLUDED.contract_code_ext_v,
			n_data_segment_bytes = EXCLUDED.n_data_segment_bytes,
			n_data_segments = EXCLUDED.n_data_segments,
			n_elem_segments = EXCLUDED.n_elem_segments,
			n_exports = EXCLUDED.n_exports,
			n_functions = EXCLUDED.n_functions,
			n_globals = EXCLUDED.n_globals,
			n_imports = EXCLUDED.n_imports,
			n_instructions = EXCLUDED.n_instructions,
			n_table_entries = EXCLUDED.n_table_entries,
			n_types = EXCLUDED.n_types,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.ContractCodeHash, row.ContractCodeExtV,
		row.NDataSegmentBytes, row.NDataSegments, row.NElemSegments, row.NExports,
		row.NFunctions, row.NGlobals, row.NImports, row.NInstructions, row.NTableEntries, row.NTypes,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write contract code current: %w", err)
	}

	return nil
}

// WriteTTLCurrent upserts a TTL current state row
func (sw *SilverWriter) WriteTTLCurrent(ctx context.Context, tx *sql.Tx, row *TTLCurrentRow) error {
	query := `
		INSERT INTO ttl_current (
			key_hash, live_until_ledger_seq, ttl_remaining, expired,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		ON CONFLICT (key_hash) DO UPDATE SET
			live_until_ledger_seq = EXCLUDED.live_until_ledger_seq,
			ttl_remaining = EXCLUDED.ttl_remaining,
			expired = EXCLUDED.expired,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.KeyHash, row.LiveUntilLedgerSeq, row.TTLRemaining, row.Expired,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write TTL current: %w", err)
	}

	return nil
}

// WriteEvictedKey inserts an evicted key event row (append-only event stream)
func (sw *SilverWriter) WriteEvictedKey(ctx context.Context, tx *sql.Tx, row *EvictedKeyRow) error {
	query := `
		INSERT INTO evicted_keys (
			contract_id, key_hash, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
		ON CONFLICT (contract_id, key_hash, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.ContractID, row.KeyHash, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write evicted key: %w", err)
	}

	return nil
}

// WriteRestoredKey inserts a restored key event row (append-only event stream)
func (sw *SilverWriter) WriteRestoredKey(ctx context.Context, tx *sql.Tx, row *RestoredKeyRow) error {
	query := `
		INSERT INTO restored_keys (
			contract_id, key_hash, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
		ON CONFLICT (contract_id, key_hash, ledger_sequence) DO NOTHING
	`

	_, err := tx.ExecContext(ctx, query,
		row.ContractID, row.KeyHash, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write restored key: %w", err)
	}

	return nil
}

// =============================================================================
// Phase 4: Config Settings
// =============================================================================

// WriteConfigSettingsCurrent upserts a config settings current state row
func (sw *SilverWriter) WriteConfigSettingsCurrent(ctx context.Context, tx *sql.Tx, row *ConfigSettingsCurrentRow) error {
	query := `
		INSERT INTO config_settings_current (
			config_setting_id,
			ledger_max_instructions, tx_max_instructions,
			fee_rate_per_instructions_increment, tx_memory_limit,
			ledger_max_read_ledger_entries, ledger_max_read_bytes,
			ledger_max_write_ledger_entries, ledger_max_write_bytes,
			tx_max_read_ledger_entries, tx_max_read_bytes,
			tx_max_write_ledger_entries, tx_max_write_bytes,
			contract_max_size_bytes, config_setting_xdr,
			last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (config_setting_id) DO UPDATE SET
			ledger_max_instructions = EXCLUDED.ledger_max_instructions,
			tx_max_instructions = EXCLUDED.tx_max_instructions,
			fee_rate_per_instructions_increment = EXCLUDED.fee_rate_per_instructions_increment,
			tx_memory_limit = EXCLUDED.tx_memory_limit,
			ledger_max_read_ledger_entries = EXCLUDED.ledger_max_read_ledger_entries,
			ledger_max_read_bytes = EXCLUDED.ledger_max_read_bytes,
			ledger_max_write_ledger_entries = EXCLUDED.ledger_max_write_ledger_entries,
			ledger_max_write_bytes = EXCLUDED.ledger_max_write_bytes,
			tx_max_read_ledger_entries = EXCLUDED.tx_max_read_ledger_entries,
			tx_max_read_bytes = EXCLUDED.tx_max_read_bytes,
			tx_max_write_ledger_entries = EXCLUDED.tx_max_write_ledger_entries,
			tx_max_write_bytes = EXCLUDED.tx_max_write_bytes,
			contract_max_size_bytes = EXCLUDED.contract_max_size_bytes,
			config_setting_xdr = EXCLUDED.config_setting_xdr,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			ledger_sequence = EXCLUDED.ledger_sequence,
			closed_at = EXCLUDED.closed_at,
			ledger_range = EXCLUDED.ledger_range,
			updated_at = NOW()
	`

	_, err := tx.ExecContext(ctx, query,
		row.ConfigSettingID,
		row.LedgerMaxInstructions, row.TxMaxInstructions,
		row.FeeRatePerInstructionsIncrement, row.TxMemoryLimit,
		row.LedgerMaxReadLedgerEntries, row.LedgerMaxReadBytes,
		row.LedgerMaxWriteLedgerEntries, row.LedgerMaxWriteBytes,
		row.TxMaxReadLedgerEntries, row.TxMaxReadBytes,
		row.TxMaxWriteLedgerEntries, row.TxMaxWriteBytes,
		row.ContractMaxSizeBytes, row.ConfigSettingXDR,
		row.LastModifiedLedger, row.LedgerSequence, row.ClosedAt, row.CreatedAt, row.LedgerRange,
	)

	if err != nil {
		return fmt.Errorf("failed to write config settings current: %w", err)
	}

	return nil
}
