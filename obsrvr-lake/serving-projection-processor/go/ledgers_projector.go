package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LedgersRecentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type bronzeLedgerRow struct {
	Sequence          int64
	ClosedAt          time.Time
	LedgerHash        sql.NullString
	PreviousHash      sql.NullString
	ProtocolVersion   sql.NullInt32
	BaseFee           sql.NullInt32
	MaxTxSetSize      sql.NullInt32
	SuccessfulTxCount sql.NullInt32
	FailedTxCount     sql.NullInt32
	OperationCount    sql.NullInt32
	SorobanOpCount    sql.NullInt32
}

func NewLedgersRecentProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *LedgersRecentProjector {
	return &LedgersRecentProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *LedgersRecentProjector) Name() string {
	return "ledgers_recent"
}

func (p *LedgersRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			sequence,
			closed_at,
			ledger_hash,
			previous_ledger_hash,
			protocol_version,
			base_fee,
			max_tx_set_size,
			successful_tx_count,
			failed_tx_count,
			operation_count,
			soroban_op_count
		FROM ledgers_row_v2
		WHERE sequence > $1
		ORDER BY sequence ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query bronze ledgers: %w", err)
	}
	defer rows.Close()

	batch := make([]bronzeLedgerRow, 0, p.batchSize)
	for rows.Next() {
		var row bronzeLedgerRow
		if err := rows.Scan(
			&row.Sequence,
			&row.ClosedAt,
			&row.LedgerHash,
			&row.PreviousHash,
			&row.ProtocolVersion,
			&row.BaseFee,
			&row.MaxTxSetSize,
			&row.SuccessfulTxCount,
			&row.FailedTxCount,
			&row.OperationCount,
			&row.SorobanOpCount,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan bronze ledger: %w", err)
		}
		batch = append(batch, row)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate bronze ledgers: %w", err)
	}
	if len(batch) == 0 {
		return RunStats{Checkpoint: checkpoint}, nil
	}

	prevClosedAt, err := p.previousClosedAt(ctx, checkpoint, batch[0].Sequence)
	if err != nil {
		return RunStats{}, err
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin target tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for i := range batch {
		row := batch[i]
		var closeTimeSeconds *float64
		if prevClosedAt != nil {
			secs := row.ClosedAt.Sub(*prevClosedAt).Seconds()
			closeTimeSeconds = &secs
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_ledger_stats_recent (
				ledger_sequence,
				closed_at,
				ledger_hash,
				prev_hash,
				protocol_version,
				base_fee_stroops,
				max_tx_set_size,
				successful_tx_count,
				failed_tx_count,
				operation_count,
				soroban_op_count,
				close_time_seconds
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
			)
			ON CONFLICT (ledger_sequence) DO UPDATE SET
				closed_at = EXCLUDED.closed_at,
				ledger_hash = EXCLUDED.ledger_hash,
				prev_hash = EXCLUDED.prev_hash,
				protocol_version = EXCLUDED.protocol_version,
				base_fee_stroops = EXCLUDED.base_fee_stroops,
				max_tx_set_size = EXCLUDED.max_tx_set_size,
				successful_tx_count = EXCLUDED.successful_tx_count,
				failed_tx_count = EXCLUDED.failed_tx_count,
				operation_count = EXCLUDED.operation_count,
				soroban_op_count = EXCLUDED.soroban_op_count,
				close_time_seconds = EXCLUDED.close_time_seconds
		`,
			row.Sequence,
			row.ClosedAt,
			nullableString(row.LedgerHash),
			nullableString(row.PreviousHash),
			nullableInt32(row.ProtocolVersion),
			nullableInt32(row.BaseFee),
			nullableInt32(row.MaxTxSetSize),
			nullableInt32(row.SuccessfulTxCount),
			nullableInt32(row.FailedTxCount),
			nullableInt32(row.OperationCount),
			nullableInt32(row.SorobanOpCount),
			closeTimeSeconds,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving ledger %d: %w", row.Sequence, err)
		}

		prevClosedAt = &row.ClosedAt
	}

	last := batch[len(batch)-1]
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, last.Sequence, &last.ClosedAt); err != nil {
		return RunStats{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d checkpoint=%d", p.Name(), p.network, len(batch), last.Sequence)
	return RunStats{RowsApplied: int64(len(batch)), Checkpoint: last.Sequence}, nil
}

func (p *LedgersRecentProjector) previousClosedAt(ctx context.Context, checkpoint, firstSequence int64) (*time.Time, error) {
	var t time.Time
	if checkpoint > 0 {
		err := p.targetPool.QueryRow(ctx, `
			SELECT closed_at
			FROM serving.sv_ledger_stats_recent
			WHERE ledger_sequence <= $1
			ORDER BY ledger_sequence DESC
			LIMIT 1
		`, checkpoint).Scan(&t)
		if err == nil {
			return &t, nil
		}
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("load previous serving ledger time: %w", err)
		}
	}

	err := p.sourcePool.QueryRow(ctx, `
		SELECT closed_at
		FROM ledgers_row_v2
		WHERE sequence < $1
		ORDER BY sequence DESC
		LIMIT 1
	`, firstSequence).Scan(&t)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load previous bronze ledger time: %w", err)
	}
	return &t, nil
}

func nullableString(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}

func nullableInt32(v sql.NullInt32) any {
	if v.Valid {
		return int(v.Int32)
	}
	return nil
}
