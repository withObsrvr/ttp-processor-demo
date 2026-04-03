package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// transformCurrentState materializes "current" silver tables from bronze snapshots.
// Uses DELETE+INSERT since DuckLake does not support ON CONFLICT/UPSERT.
func transformCurrentState(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	if err := transformAccountsCurrent(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("accounts_current: %w", err)
	}
	if err := transformTrustlinesCurrent(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("trustlines_current: %w", err)
	}
	if err := transformOffersCurrent(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("offers_current: %w", err)
	}
	if err := transformSilverTrades(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("trades: %w", err)
	}
	return nil
}

// transformAccountsCurrent updates the latest account state.
// For each account seen in this ledger range, delete old row and insert new.
func transformAccountsCurrent(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	// Delete accounts that have new snapshots in this range
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s.%s.accounts_current
		WHERE account_id IN (
			SELECT DISTINCT account_id FROM %s.%s.accounts_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
	`, cat, silver, cat, bronze)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	// Insert the latest snapshot per account from this range
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.accounts_current (
			account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, last_modified_ledger, updated_at
		)
		SELECT
			account_id, balance, sequence_number, num_subentries,
			num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, ledger_sequence, closed_at
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.%s.accounts_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		) sub WHERE rn = 1
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  accounts_current: %d rows", rows)
	}
	return nil
}

// transformTrustlinesCurrent updates the latest trustline state.
func transformTrustlinesCurrent(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s.%s.trustlines_current
		WHERE (account_id, asset_code, asset_issuer) IN (
			SELECT DISTINCT account_id, asset_code, asset_issuer
			FROM %s.%s.trustlines_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
	`, cat, silver, cat, bronze)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.trustlines_current (
			account_id, asset_code, asset_issuer, asset_type,
			balance, trust_limit, buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			last_modified_ledger, updated_at
		)
		SELECT
			account_id, asset_code, asset_issuer, asset_type,
			balance, trust_limit, buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence, CURRENT_TIMESTAMP
		FROM (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY account_id, asset_code, asset_issuer
				ORDER BY ledger_sequence DESC
			) AS rn
			FROM %s.%s.trustlines_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		) sub WHERE rn = 1
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  trustlines_current: %d rows", rows)
	}
	return nil
}

// transformOffersCurrent updates the latest offer state.
func transformOffersCurrent(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s.%s.offers_current
		WHERE offer_id IN (
			SELECT DISTINCT offer_id FROM %s.%s.offers_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
	`, cat, silver, cat, bronze)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.offers_current (
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, last_modified_ledger, updated_at
		)
		SELECT
			offer_id, seller_account,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_sequence, closed_at
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY ledger_sequence DESC) AS rn
			FROM %s.%s.offers_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
		) sub WHERE rn = 1
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  offers_current: %d rows", rows)
	}
	return nil
}

// transformSilverTrades copies enriched trade data from bronze to silver.
func transformSilverTrades(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.trades (
			ledger_sequence, transaction_hash, operation_index,
			trade_index, trade_type, trade_timestamp,
			seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			price, ledger_range, pipeline_version
		)
		SELECT
			ledger_sequence, transaction_hash, operation_index,
			trade_index, trade_type, trade_timestamp,
			seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			price, ledger_range, pipeline_version
		FROM %s.%s.trades_row_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  trades: %d rows", rows)
	}
	return nil
}
