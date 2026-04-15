package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ContractStatsProjector struct {
	network    string
	sourcePool *pgxpool.Pool
	targetPool *pgxpool.Pool
}

func NewContractStatsProjector(network string, sourcePool, targetPool *pgxpool.Pool) *ContractStatsProjector {
	return &ContractStatsProjector{network: network, sourcePool: sourcePool, targetPool: targetPool}
}

func (p *ContractStatsProjector) Name() string { return "contract_stats" }

func (p *ContractStatsProjector) RunOnce(ctx context.Context) (RunStats, error) {
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract stats tx: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteStatsTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_stats_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract stats: %w", err)
	}
	deleteFnTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_function_stats_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract function stats: %w", err)
	}

	insertStatsTag, err := tx.Exec(ctx, `
		WITH agg AS (
			SELECT
				contract_id,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours') as total_calls_24h,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '7 days') as total_calls_7d,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '30 days') as total_calls_30d,
				COUNT(DISTINCT source_account) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours') as unique_callers_24h,
				COUNT(DISTINCT source_account) FILTER (WHERE closed_at >= NOW() - INTERVAL '7 days') as unique_callers_7d,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours' AND successful) as success_count_24h,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours' AND NOT successful) as failure_count_24h,
				MAX(closed_at) as last_activity_at,
				MIN(closed_at) as first_seen_at
			FROM contract_invocations_raw
			GROUP BY contract_id
		), top_fn AS (
			SELECT DISTINCT ON (contract_id)
				contract_id,
				function_name,
				COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours') as cnt_24h
			FROM contract_invocations_raw
			WHERE function_name <> ''
			GROUP BY contract_id, function_name
			ORDER BY contract_id, cnt_24h DESC, function_name ASC
		)
		INSERT INTO serving.sv_contract_stats_current (
			contract_id, total_calls_24h, total_calls_7d, total_calls_30d,
			unique_callers_24h, unique_callers_7d,
			success_count_24h, failure_count_24h, success_rate_24h,
			top_function, last_activity_at, first_seen_at, updated_at
		)
		SELECT
			a.contract_id,
			a.total_calls_24h,
			a.total_calls_7d,
			a.total_calls_30d,
			a.unique_callers_24h,
			a.unique_callers_7d,
			a.success_count_24h,
			a.failure_count_24h,
			CASE WHEN (a.success_count_24h + a.failure_count_24h) > 0
				THEN a.success_count_24h::numeric / (a.success_count_24h + a.failure_count_24h)
				ELSE 0 END,
			t.function_name,
			a.last_activity_at,
			a.first_seen_at,
			now()
		FROM agg a
		LEFT JOIN top_fn t USING (contract_id)
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contract stats: %w", err)
	}

	insertFnTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_contract_function_stats_current (
			contract_id, function_name, calls_24h, calls_7d, calls_30d,
			success_count_24h, failure_count_24h, last_called_at, updated_at
		)
		SELECT
			contract_id,
			function_name,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours') as calls_24h,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '7 days') as calls_7d,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '30 days') as calls_30d,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours' AND successful) as success_count_24h,
			COUNT(*) FILTER (WHERE closed_at >= NOW() - INTERVAL '24 hours' AND NOT successful) as failure_count_24h,
			MAX(closed_at) as last_called_at,
			now()
		FROM contract_invocations_raw
		WHERE function_name <> ''
		GROUP BY contract_id, function_name
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contract function stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract stats tx: %w", err)
	}
	rowsApplied := insertStatsTag.RowsAffected() + insertFnTag.RowsAffected()
	rowsDeleted := deleteStatsTag.RowsAffected() + deleteFnTag.RowsAffected()
	log.Printf("projector=%s network=%s applied=%d deleted=%d rebuilt contract stats serving tables", p.Name(), p.network, rowsApplied, rowsDeleted)
	return RunStats{RowsApplied: rowsApplied, RowsDeleted: rowsDeleted}, nil
}
