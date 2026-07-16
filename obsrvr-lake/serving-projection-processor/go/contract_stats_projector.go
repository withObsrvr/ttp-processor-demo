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
	dataTime := resolveDataTime(ctx, p.targetPool, "serving.sv_contract_calls_recent", "created_at")
	var completeThru int64
	if err := p.targetPool.QueryRow(ctx, `SELECT COALESCE(MAX(ledger_sequence), 0) FROM serving.sv_contract_calls_recent`).Scan(&completeThru); err != nil {
		return RunStats{}, fmt.Errorf("resolve contract stats watermark: %w", err)
	}

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
	deleteActivityTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_activity_summary`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract activity summary: %w", err)
	}

	insertStatsTag, err := tx.Exec(ctx, `
		WITH agg AS (
			SELECT
				contract_id,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') as total_calls_24h,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '7 days') as total_calls_7d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days') as total_calls_30d,
				COUNT(DISTINCT caller_account) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') as unique_callers_24h,
				COUNT(DISTINCT caller_account) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '7 days') as unique_callers_7d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours' AND successful) as success_count_24h,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours' AND NOT successful) as failure_count_24h,
				MAX(created_at) as last_activity_at,
				MIN(created_at) as first_seen_at
			FROM serving.sv_contract_calls_recent
			GROUP BY contract_id
		), top_fn AS (
			SELECT DISTINCT ON (contract_id)
				contract_id,
				function_name,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') as cnt_24h
			FROM serving.sv_contract_calls_recent
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
	`, dataTime)
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
			COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') as calls_24h,
			COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '7 days') as calls_7d,
			COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days') as calls_30d,
			COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours' AND successful) as success_count_24h,
			COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours' AND NOT successful) as failure_count_24h,
			MAX(created_at) as last_called_at,
			now()
		FROM serving.sv_contract_calls_recent
		WHERE function_name <> ''
		GROUP BY contract_id, function_name
	`, dataTime)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contract function stats: %w", err)
	}

	insertActivityTag, err := tx.Exec(ctx, `
		WITH inv AS (
			SELECT
				contract_id,
				MIN(ledger_sequence) AS first_seen_ledger,
				MAX(ledger_sequence) AS last_seen_ledger,
				MIN(created_at) AS first_seen_at,
				MAX(created_at) AS last_seen_at,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') AS invocation_count_24h,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '7 days') AS invocation_count_7d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days') AS invocation_count_30d,
				COUNT(*) AS invocation_count_all,
				COUNT(DISTINCT caller_account) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days') AS unique_callers_30d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days' AND successful) AS successful_invocations_30d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days' AND NOT successful) AS failed_invocations_30d
			FROM serving.sv_contract_calls_recent
			GROUP BY contract_id
		), ev AS (
			SELECT
				contract_id,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '24 hours') AS event_count_24h,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '7 days') AS event_count_7d,
				COUNT(*) FILTER (WHERE created_at >= $1::timestamp - INTERVAL '30 days') AS event_count_30d
			FROM serving.sv_events_recent
			WHERE contract_id IS NOT NULL
			GROUP BY contract_id
		), ids AS (
			SELECT contract_id FROM inv
			UNION
			SELECT contract_id FROM ev
			UNION
			SELECT contract_id FROM serving.sv_contract_storage_summary
			UNION
			SELECT contract_id FROM serving.sv_smart_account_contracts_current
		)
		INSERT INTO serving.sv_contract_activity_summary (
			contract_id, first_seen_ledger, last_seen_ledger, first_seen_at, last_seen_at,
			invocation_count_24h, invocation_count_7d, invocation_count_30d, invocation_count_all,
			event_count_24h, event_count_7d, event_count_30d,
			unique_callers_30d, successful_invocations_30d, failed_invocations_30d,
			activity_classification, updated_at
		)
		SELECT
			ids.contract_id,
			inv.first_seen_ledger,
			inv.last_seen_ledger,
			inv.first_seen_at,
			inv.last_seen_at,
			COALESCE(inv.invocation_count_24h, 0),
			COALESCE(inv.invocation_count_7d, 0),
			COALESCE(inv.invocation_count_30d, 0),
			COALESCE(inv.invocation_count_all, 0),
			COALESCE(ev.event_count_24h, 0),
			COALESCE(ev.event_count_7d, 0),
			COALESCE(ev.event_count_30d, 0),
			COALESCE(inv.unique_callers_30d, 0),
			COALESCE(inv.successful_invocations_30d, 0),
			COALESCE(inv.failed_invocations_30d, 0),
			CASE
				WHEN sac.contract_id IS NOT NULL THEN 'smart_account'
				WHEN COALESCE(inv.invocation_count_30d, 0) > 0 AND COALESCE(ev.event_count_30d, 0) > 0 THEN 'active_contract'
				WHEN COALESCE(inv.invocation_count_30d, 0) > 0 THEN 'invoked_contract'
				WHEN COALESCE(ev.event_count_30d, 0) > 0 THEN 'event_emitter'
				WHEN css.contract_id IS NOT NULL THEN 'storage_only'
				ELSE 'unknown'
			END AS activity_classification,
			NOW()
		FROM ids
		LEFT JOIN inv ON inv.contract_id = ids.contract_id
		LEFT JOIN ev ON ev.contract_id = ids.contract_id
		LEFT JOIN serving.sv_contract_storage_summary css ON css.contract_id = ids.contract_id
		LEFT JOIN serving.sv_smart_account_contracts_current sac ON sac.contract_id = ids.contract_id
	`, dataTime)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contract activity summary: %w", err)
	}

	for _, table := range []string{
		"serving.sv_contract_stats_current",
		"serving.sv_contract_function_stats_current",
		"serving.sv_contract_activity_summary",
	} {
		if err := saveServingWatermark(ctx, tx, table, 0, completeThru); err != nil {
			return RunStats{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract stats tx: %w", err)
	}
	rowsApplied := insertStatsTag.RowsAffected() + insertFnTag.RowsAffected() + insertActivityTag.RowsAffected()
	rowsDeleted := deleteStatsTag.RowsAffected() + deleteFnTag.RowsAffected() + deleteActivityTag.RowsAffected()
	log.Printf("projector=%s network=%s applied=%d deleted=%d watermark=%d rebuilt contract stats serving tables", p.Name(), p.network, rowsApplied, rowsDeleted, completeThru)
	return RunStats{RowsApplied: rowsApplied, RowsDeleted: rowsDeleted, Checkpoint: completeThru}, nil
}
