package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ContractCallsRecentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type contractCallRecentRow struct {
	CallID         string
	TxHash         string
	LedgerSequence int64
	CreatedAt      time.Time
	ContractID     string
	CallerAccount  string
	FunctionName   string
	Successful     bool
	SummaryText    string
}

func NewContractCallsRecentProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *ContractCallsRecentProjector {
	return &ContractCallsRecentProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *ContractCallsRecentProjector) Name() string { return "contract_calls_recent" }

func (p *ContractCallsRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			ledger_sequence::text || ':' || transaction_index::text || ':' || operation_index::text as call_id,
			transaction_hash,
			ledger_sequence,
			closed_at,
			contract_id,
			source_account,
			function_name,
			successful,
			CASE
				WHEN function_name IS NOT NULL AND function_name <> '' THEN 'Invoke ' || contract_id || '.' || function_name
				ELSE 'Invoke ' || contract_id
			END as summary_text
		FROM contract_invocations_raw
		WHERE ledger_sequence > $1
		  AND closed_at >= NOW() - INTERVAL '30 days'
		ORDER BY ledger_sequence ASC, transaction_index ASC, operation_index ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query contract calls recent: %w", err)
	}
	defer rows.Close()

	var batch []contractCallRecentRow
	for rows.Next() {
		var r contractCallRecentRow
		if err := rows.Scan(&r.CallID, &r.TxHash, &r.LedgerSequence, &r.CreatedAt, &r.ContractID, &r.CallerAccount, &r.FunctionName, &r.Successful, &r.SummaryText); err != nil {
			return RunStats{}, fmt.Errorf("scan contract calls recent row: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate contract calls recent rows: %w", err)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract calls recent tx: %w", err)
	}
	defer tx.Rollback(ctx)

	retainedRows, err := applyRecentRetention(ctx, tx, "serving.sv_contract_calls_recent", "created_at", "30 days")
	if err != nil {
		return RunStats{}, err
	}

	if len(batch) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return RunStats{}, fmt.Errorf("commit contract calls retention-only tx: %w", err)
		}
		if retainedRows > 0 {
			log.Printf("projector=%s network=%s retention_deleted=%d checkpoint=%d", p.Name(), p.network, retainedRows, checkpoint)
		}
		return RunStats{RowsDeleted: retainedRows, Checkpoint: checkpoint}, nil
	}

	maxLedger := checkpoint
	var lastClosedAt *time.Time
	for _, r := range batch {
		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_contract_calls_recent (
				call_id,
				tx_hash,
				ledger_sequence,
				created_at,
				contract_id,
				caller_account,
				function_name,
				successful,
				summary_text
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (call_id) DO UPDATE SET
				tx_hash = EXCLUDED.tx_hash,
				ledger_sequence = EXCLUDED.ledger_sequence,
				created_at = EXCLUDED.created_at,
				contract_id = EXCLUDED.contract_id,
				caller_account = EXCLUDED.caller_account,
				function_name = EXCLUDED.function_name,
				successful = EXCLUDED.successful,
				summary_text = EXCLUDED.summary_text
		`, r.CallID, r.TxHash, r.LedgerSequence, r.CreatedAt, r.ContractID, r.CallerAccount, r.FunctionName, r.Successful, r.SummaryText)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert contract call recent %s: %w", r.CallID, err)
		}
		if r.LedgerSequence > maxLedger {
			maxLedger = r.LedgerSequence
		}
		t := r.CreatedAt
		lastClosedAt = &t
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastClosedAt); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract calls recent tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d retention_deleted=%d checkpoint=%d", p.Name(), p.network, len(batch), retainedRows, maxLedger)
	return RunStats{RowsApplied: int64(len(batch)), RowsDeleted: retainedRows, Checkpoint: maxLedger}, nil
}
