package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// TokenWriter writes discovered tokens to the registry
type TokenWriter struct {
	db *sql.DB
}

// NewTokenWriter creates a new token writer
func NewTokenWriter(db *sql.DB) *TokenWriter {
	return &TokenWriter{db: db}
}

// UpsertToken inserts or updates a discovered token
func (w *TokenWriter) UpsertToken(ctx context.Context, token *DiscoveredToken) error {
	query := `
		INSERT INTO discovered_tokens (
			contract_id,
			token_type,
			detection_method,
			name,
			symbol,
			decimals,
			is_sac,
			classic_asset_code,
			classic_asset_issuer,
			lp_pool_type,
			lp_asset_a,
			lp_asset_b,
			lp_fee_bps,
			first_seen_ledger,
			last_activity_ledger,
			discovered_at,
			updated_at,
			holder_count,
			transfer_count,
			total_supply,
			observed_functions,
			sep41_score
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
		ON CONFLICT (contract_id) DO UPDATE SET
			token_type = EXCLUDED.token_type,
			detection_method = EXCLUDED.detection_method,
			name = COALESCE(EXCLUDED.name, discovered_tokens.name),
			symbol = COALESCE(EXCLUDED.symbol, discovered_tokens.symbol),
			decimals = COALESCE(EXCLUDED.decimals, discovered_tokens.decimals),
			is_sac = EXCLUDED.is_sac,
			classic_asset_code = COALESCE(EXCLUDED.classic_asset_code, discovered_tokens.classic_asset_code),
			classic_asset_issuer = COALESCE(EXCLUDED.classic_asset_issuer, discovered_tokens.classic_asset_issuer),
			lp_pool_type = COALESCE(EXCLUDED.lp_pool_type, discovered_tokens.lp_pool_type),
			lp_asset_a = COALESCE(EXCLUDED.lp_asset_a, discovered_tokens.lp_asset_a),
			lp_asset_b = COALESCE(EXCLUDED.lp_asset_b, discovered_tokens.lp_asset_b),
			lp_fee_bps = COALESCE(EXCLUDED.lp_fee_bps, discovered_tokens.lp_fee_bps),
			last_activity_ledger = GREATEST(discovered_tokens.last_activity_ledger, EXCLUDED.last_activity_ledger),
			updated_at = $17,
			holder_count = EXCLUDED.holder_count,
			transfer_count = EXCLUDED.transfer_count,
			total_supply = COALESCE(EXCLUDED.total_supply, discovered_tokens.total_supply),
			observed_functions = (
				SELECT ARRAY(
					SELECT DISTINCT unnest
					FROM unnest(discovered_tokens.observed_functions || EXCLUDED.observed_functions)
				)
			),
			sep41_score = GREATEST(discovered_tokens.sep41_score, EXCLUDED.sep41_score)
	`

	now := time.Now()
	_, err := w.db.ExecContext(ctx, query,
		token.ContractID,
		token.TokenType,
		token.DetectionMethod,
		token.Name,
		token.Symbol,
		token.Decimals,
		token.IsSAC,
		token.ClassicAssetCode,
		token.ClassicAssetIssuer,
		token.LPPoolType,
		token.LPAssetA,
		token.LPAssetB,
		token.LPFeeBPS,
		token.FirstSeenLedger,
		token.LastActivityLedger,
		now,
		now,
		token.HolderCount,
		token.TransferCount,
		token.TotalSupply,
		pq.Array(token.ObservedFunctions),
		token.SEP41Score,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert token: %w", err)
	}

	return nil
}

// UpdateTokenStats updates cached statistics for a token
func (w *TokenWriter) UpdateTokenStats(ctx context.Context, contractID string, holderCount, transferCount int64, totalSupply *string) error {
	query := `
		UPDATE discovered_tokens
		SET holder_count = $2,
		    transfer_count = $3,
		    total_supply = $4,
		    updated_at = $5
		WHERE contract_id = $1
	`

	_, err := w.db.ExecContext(ctx, query, contractID, holderCount, transferCount, totalSupply, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update token stats: %w", err)
	}

	return nil
}

// GetTokenCount returns the total number of discovered tokens
func (w *TokenWriter) GetTokenCount(ctx context.Context) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM discovered_tokens").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count tokens: %w", err)
	}
	return count, nil
}

// GetTokenCountByType returns counts by token type
func (w *TokenWriter) GetTokenCountByType(ctx context.Context) (map[string]int64, error) {
	query := `
		SELECT token_type, COUNT(*) as count
		FROM discovered_tokens
		GROUP BY token_type
	`

	rows, err := w.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to count tokens by type: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var tokenType string
		var count int64
		if err := rows.Scan(&tokenType, &count); err != nil {
			return nil, fmt.Errorf("failed to scan token count: %w", err)
		}
		counts[tokenType] = count
	}

	return counts, nil
}

// GetExistingToken checks if a token already exists
func (w *TokenWriter) GetExistingToken(ctx context.Context, contractID string) (*DiscoveredToken, error) {
	query := `
		SELECT
			contract_id,
			token_type,
			detection_method,
			name,
			symbol,
			decimals,
			is_sac,
			classic_asset_code,
			classic_asset_issuer,
			lp_pool_type,
			lp_asset_a,
			lp_asset_b,
			lp_fee_bps,
			first_seen_ledger,
			last_activity_ledger,
			discovered_at,
			updated_at,
			holder_count,
			transfer_count,
			total_supply,
			observed_functions,
			sep41_score
		FROM discovered_tokens
		WHERE contract_id = $1
	`

	var token DiscoveredToken
	var functions pq.StringArray

	err := w.db.QueryRowContext(ctx, query, contractID).Scan(
		&token.ContractID,
		&token.TokenType,
		&token.DetectionMethod,
		&token.Name,
		&token.Symbol,
		&token.Decimals,
		&token.IsSAC,
		&token.ClassicAssetCode,
		&token.ClassicAssetIssuer,
		&token.LPPoolType,
		&token.LPAssetA,
		&token.LPAssetB,
		&token.LPFeeBPS,
		&token.FirstSeenLedger,
		&token.LastActivityLedger,
		&token.DiscoveredAt,
		&token.UpdatedAt,
		&token.HolderCount,
		&token.TransferCount,
		&token.TotalSupply,
		&functions,
		&token.SEP41Score,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get existing token: %w", err)
	}

	token.ObservedFunctions = []string(functions)
	return &token, nil
}
