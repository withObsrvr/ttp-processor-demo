package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type NetworkStatsProjector struct {
	network    string
	targetPool *pgxpool.Pool
}

func NewNetworkStatsProjector(network string, targetPool *pgxpool.Pool) *NetworkStatsProjector {
	return &NetworkStatsProjector{network: network, targetPool: targetPool}
}

func (p *NetworkStatsProjector) Name() string { return "network_stats" }

func (p *NetworkStatsProjector) RunOnce(ctx context.Context) (RunStats, error) {
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin network stats tx: %w", err)
	}
	defer tx.Rollback(ctx)

	type ledgerStats struct {
		LatestLedger    int64
		LatestClosedAt  *time.Time
		AvgClose        *float64
		ProtocolVersion *int32
	}
	var l ledgerStats
	err = tx.QueryRow(ctx, `
		WITH recent AS (
			SELECT ledger_sequence, closed_at, protocol_version, close_time_seconds
			FROM serving.sv_ledger_stats_recent
			ORDER BY ledger_sequence DESC
			LIMIT 100
		), latest AS (
			SELECT ledger_sequence, closed_at, protocol_version
			FROM serving.sv_ledger_stats_recent
			ORDER BY ledger_sequence DESC
			LIMIT 1
		)
		SELECT
			COALESCE((SELECT ledger_sequence FROM latest), 0),
			(SELECT closed_at FROM latest),
			AVG(close_time_seconds),
			(SELECT protocol_version FROM latest)
		FROM recent
	`).Scan(&l.LatestLedger, &l.LatestClosedAt, &l.AvgClose, &l.ProtocolVersion)
	if err != nil {
		return RunStats{}, fmt.Errorf("query ledger stats: %w", err)
	}

	type txStats struct {
		Total          int64
		Failed         int64
		ActiveAccounts int64
	}
	var t txStats
	err = tx.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE NOT successful),
			COUNT(DISTINCT source_account)
		FROM serving.sv_transactions_recent
		WHERE created_at > now() - interval '24 hours'
	`).Scan(&t.Total, &t.Failed, &t.ActiveAccounts)
	if err != nil {
		return RunStats{}, fmt.Errorf("query tx stats: %w", err)
	}

	cmdTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_network_stats_current (
			network,
			generated_at,
			latest_ledger,
			latest_ledger_closed_at,
			avg_close_time_seconds,
			protocol_version,
			tx_24h_total,
			tx_24h_failed,
			active_accounts_24h,
			created_accounts_24h
		) VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (network) DO UPDATE SET
			generated_at = now(),
			latest_ledger = EXCLUDED.latest_ledger,
			latest_ledger_closed_at = EXCLUDED.latest_ledger_closed_at,
			avg_close_time_seconds = EXCLUDED.avg_close_time_seconds,
			protocol_version = EXCLUDED.protocol_version,
			tx_24h_total = EXCLUDED.tx_24h_total,
			tx_24h_failed = EXCLUDED.tx_24h_failed,
			active_accounts_24h = EXCLUDED.active_accounts_24h,
			created_accounts_24h = EXCLUDED.created_accounts_24h
	`, p.network, l.LatestLedger, l.LatestClosedAt, l.AvgClose, nullableInt32Pointer(l.ProtocolVersion), t.Total, t.Failed, t.ActiveAccounts, 0)
	if err != nil {
		return RunStats{}, fmt.Errorf("upsert network stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit network stats tx: %w", err)
	}
	log.Printf("projector=%s network=%s latest_ledger=%d tx_24h_total=%d", p.Name(), p.network, l.LatestLedger, t.Total)
	return RunStats{RowsApplied: cmdTag.RowsAffected(), Checkpoint: l.LatestLedger}, nil
}
