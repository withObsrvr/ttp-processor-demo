package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OperationsRecentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type operationsRecentRow struct {
	OperationID        string
	TxHash             string
	LedgerSequence     int64
	CreatedAt          time.Time
	OpIndex            int32
	TypeCode           *int32
	TypeName           string
	SourceAccount      *string
	DestinationAccount *string
	AssetKey           *string
	AmountStroops      *int64
	ContractID         *string
	FunctionName       *string
	Successful         *bool
	IsPaymentOp        bool
	IsSorobanOp        bool
	SummaryText        string
}

func NewOperationsRecentProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *OperationsRecentProjector {
	return &OperationsRecentProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *OperationsRecentProjector) Name() string { return "operations_recent" }

func (p *OperationsRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			transaction_hash || ':' || operation_index::text as operation_id,
			transaction_hash,
			ledger_sequence,
			COALESCE(ledger_closed_at, created_at, now()) as created_at,
			operation_index,
			type,
			COALESCE(NULLIF(type_string, ''), type::text) as type_name,
			source_account,
			COALESCE(destination, to_address, into_account, claimant) as destination_account,
			CASE
				WHEN contract_id IS NOT NULL AND contract_id <> '' THEN contract_id
				WHEN COALESCE(asset_type, '') = 'native' OR COALESCE(asset_code, '') = '' THEN 'XLM'
				WHEN asset_type = 'pool_share' AND liquidity_pool_id IS NOT NULL AND liquidity_pool_id <> '' THEN 'POOL:' || liquidity_pool_id
				WHEN asset_code IS NOT NULL AND asset_code <> '' AND asset_issuer IS NOT NULL AND asset_issuer <> '' THEN asset_code || ':' || asset_issuer
				ELSE NULL
			END as asset_key,
			amount,
			contract_id,
			function_name,
			tx_successful,
			COALESCE(is_payment_op, false),
			COALESCE(is_soroban_op, false),
			CASE
				WHEN COALESCE(is_soroban_op, false) AND contract_id IS NOT NULL AND function_name IS NOT NULL AND function_name <> ''
					THEN 'Invoke ' || contract_id || '.' || function_name
				WHEN COALESCE(is_soroban_op, false) AND contract_id IS NOT NULL
					THEN 'Invoke ' || contract_id
				WHEN COALESCE(is_payment_op, false) AND COALESCE(destination, to_address) IS NOT NULL
					THEN 'Payment to ' || COALESCE(destination, to_address)
				WHEN type_string IS NOT NULL AND type_string <> ''
					THEN replace(type_string, '_', ' ')
				ELSE 'Operation'
			END as summary_text
		FROM enriched_history_operations
		WHERE ledger_sequence > $1
		  AND COALESCE(ledger_closed_at, created_at, now()) >= NOW() - INTERVAL '30 days'
		ORDER BY ledger_sequence ASC, operation_index ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query operations recent: %w", err)
	}
	defer rows.Close()

	var batch []operationsRecentRow
	for rows.Next() {
		var r operationsRecentRow
		if err := rows.Scan(
			&r.OperationID,
			&r.TxHash,
			&r.LedgerSequence,
			&r.CreatedAt,
			&r.OpIndex,
			&r.TypeCode,
			&r.TypeName,
			&r.SourceAccount,
			&r.DestinationAccount,
			&r.AssetKey,
			&r.AmountStroops,
			&r.ContractID,
			&r.FunctionName,
			&r.Successful,
			&r.IsPaymentOp,
			&r.IsSorobanOp,
			&r.SummaryText,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan operations recent row: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate operations recent rows: %w", err)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin operations recent tx: %w", err)
	}
	defer tx.Rollback(ctx)

	retainedRows, err := applyRecentRetention(ctx, tx, "serving.sv_operations_recent", "created_at", "30 days")
	if err != nil {
		return RunStats{}, err
	}

	if len(batch) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return RunStats{}, fmt.Errorf("commit operations retention-only tx: %w", err)
		}
		if retainedRows > 0 {
			log.Printf("projector=%s network=%s retention_deleted=%d checkpoint=%d", p.Name(), p.network, retainedRows, checkpoint)
		}
		return RunStats{RowsDeleted: retainedRows, Checkpoint: checkpoint}, nil
	}

	maxLedger := checkpoint
	var lastCreatedAt *time.Time
	for _, r := range batch {
		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_operations_recent (
				operation_id,
				tx_hash,
				ledger_sequence,
				created_at,
				op_index,
				type_code,
				type_name,
				source_account,
				destination_account,
				asset_key,
				amount_stroops,
				contract_id,
				function_name,
				successful,
				is_payment_op,
				is_soroban_op,
				summary_text
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
			)
			ON CONFLICT (operation_id) DO UPDATE SET
				tx_hash = EXCLUDED.tx_hash,
				ledger_sequence = EXCLUDED.ledger_sequence,
				created_at = EXCLUDED.created_at,
				op_index = EXCLUDED.op_index,
				type_code = EXCLUDED.type_code,
				type_name = EXCLUDED.type_name,
				source_account = EXCLUDED.source_account,
				destination_account = EXCLUDED.destination_account,
				asset_key = EXCLUDED.asset_key,
				amount_stroops = EXCLUDED.amount_stroops,
				contract_id = EXCLUDED.contract_id,
				function_name = EXCLUDED.function_name,
				successful = EXCLUDED.successful,
				is_payment_op = EXCLUDED.is_payment_op,
				is_soroban_op = EXCLUDED.is_soroban_op,
				summary_text = EXCLUDED.summary_text
		`,
			r.OperationID,
			r.TxHash,
			r.LedgerSequence,
			r.CreatedAt,
			r.OpIndex,
			r.TypeCode,
			r.TypeName,
			r.SourceAccount,
			r.DestinationAccount,
			r.AssetKey,
			r.AmountStroops,
			r.ContractID,
			r.FunctionName,
			r.Successful,
			r.IsPaymentOp,
			r.IsSorobanOp,
			r.SummaryText,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert operation recent %s: %w", r.OperationID, err)
		}
		if r.LedgerSequence > maxLedger {
			maxLedger = r.LedgerSequence
		}
		t := r.CreatedAt
		lastCreatedAt = &t
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastCreatedAt); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit operations recent tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d retention_deleted=%d checkpoint=%d", p.Name(), p.network, len(batch), retainedRows, maxLedger)
	return RunStats{RowsApplied: int64(len(batch)), RowsDeleted: retainedRows, Checkpoint: maxLedger}, nil
}
