package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AssetStatsProjector struct {
	network    string
	targetPool *pgxpool.Pool
}

func NewAssetStatsProjector(network string, targetPool *pgxpool.Pool) *AssetStatsProjector {
	return &AssetStatsProjector{network: network, targetPool: targetPool}
}

func (p *AssetStatsProjector) Name() string { return "asset_stats" }

func (p *AssetStatsProjector) RunOnce(ctx context.Context) (RunStats, error) {
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin asset stats tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Rebuild compact asset current + stats tables from serving balances.
	deleteStatsTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_asset_stats_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear asset stats: %w", err)
	}
	deleteAssetsTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_assets_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear assets current: %w", err)
	}

	insertAssetsTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_assets_current (
			asset_key, asset_code, asset_issuer, asset_type, is_native,
			holder_count, trustline_count, circulating_supply, updated_at
		)
		SELECT
			asset_key,
			MAX(asset_code) as asset_code,
			MAX(asset_issuer) as asset_issuer,
			MAX(asset_type) as asset_type,
			BOOL_OR(asset_type = 'native') as is_native,
			COUNT(*) FILTER (WHERE balance_stroops > 0) as holder_count,
			COUNT(*) as trustline_count,
			COALESCE(SUM(balance_display), 0) as circulating_supply,
			now()
		FROM serving.sv_account_balances_current
		GROUP BY asset_key
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate assets current: %w", err)
	}

	insertStatsTag, err := tx.Exec(ctx, `
		WITH totals AS (
			SELECT asset_key,
			       COUNT(*) FILTER (WHERE balance_stroops > 0) as total_holders,
			       COUNT(*) as total_trustlines,
			       COALESCE(SUM(balance_display), 0) as circulating_supply
			FROM serving.sv_account_balances_current
			GROUP BY asset_key
		), ranked AS (
			SELECT asset_key, balance_display,
			       ROW_NUMBER() OVER (PARTITION BY asset_key ORDER BY balance_stroops DESC, account_id ASC) as rn
			FROM serving.sv_account_balances_current
			WHERE balance_stroops > 0
		), top10 AS (
			SELECT asset_key, COALESCE(SUM(balance_display), 0) as top10_total
			FROM ranked
			WHERE rn <= 10
			GROUP BY asset_key
		)
		INSERT INTO serving.sv_asset_stats_current (
			asset_key, holder_count, trustline_count, circulating_supply,
			volume_24h, transfers_24h, unique_accounts_24h, top10_concentration, updated_at
		)
		SELECT
			t.asset_key,
			t.total_holders,
			t.total_trustlines,
			t.circulating_supply,
			0,
			0,
			0,
			CASE WHEN t.circulating_supply > 0 THEN COALESCE(tp.top10_total, 0) / t.circulating_supply ELSE 0 END,
			now()
		FROM totals t
		LEFT JOIN top10 tp USING (asset_key)
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate asset stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit asset stats tx: %w", err)
	}
	rowsApplied := insertAssetsTag.RowsAffected() + insertStatsTag.RowsAffected()
	rowsDeleted := deleteStatsTag.RowsAffected() + deleteAssetsTag.RowsAffected()
	log.Printf("projector=%s network=%s applied=%d deleted=%d rebuilt asset serving tables", p.Name(), p.network, rowsApplied, rowsDeleted)
	return RunStats{RowsApplied: rowsApplied, RowsDeleted: rowsDeleted}, nil
}
