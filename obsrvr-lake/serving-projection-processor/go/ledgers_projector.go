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
	Sequence                      int64
	ClosedAt                      time.Time
	LedgerHash                    sql.NullString
	PreviousHash                  sql.NullString
	ProtocolVersion               sql.NullInt32
	BaseFee                       sql.NullInt32
	MaxTxSetSize                  sql.NullInt32
	SuccessfulTxCount             sql.NullInt32
	FailedTxCount                 sql.NullInt32
	OperationCount                sql.NullInt32
	TxSetOperationCount           sql.NullInt32
	ValidatorNodeID               sql.NullString
	LedgerCloseSignature          sql.NullString
	SorobanOpCount                sql.NullInt32
	OperationCategories           ledgerOperationCategoryCounts
	SuccessfulOperationCategories ledgerOperationCategoryCounts
	OperationCategoriesComplete   bool
}

type ledgerOperationCategoryCounts struct {
	AccountCreation   sql.NullInt32
	Payments          sql.NullInt32
	OffersAndAMMs     sql.NullInt32
	Trustlines        sql.NullInt32
	ClaimableBalances sql.NullInt32
	Sponsorship       sql.NullInt32
	Soroban           sql.NullInt32
	Other             sql.NullInt32
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

func (p *LedgersRecentProjector) SourceHighWatermark(ctx context.Context) (int64, error) {
	var wm int64
	err := p.sourcePool.QueryRow(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM ledgers_row_v2`).Scan(&wm)
	if err != nil {
		return 0, fmt.Errorf("query ledgers recent watermark: %w", err)
	}
	return wm, nil
}

func (p *LedgersRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	dataTime := resolveDataTimeBy(ctx, p.sourcePool, "ledgers_row_v2", "closed_at", "sequence")

	rows, err := p.sourcePool.Query(ctx, `
		WITH next_ledgers AS (
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
			tx_set_operation_count,
			node_id,
			signature,
			soroban_op_count
			FROM ledgers_row_v2
			WHERE sequence > $1
			  AND closed_at >= $3::timestamp - INTERVAL '30 days'
			ORDER BY sequence ASC
			LIMIT $2
		), operation_categories AS (
			SELECT
				o.ledger_sequence,
				COUNT(*) FILTER (WHERE o.type = 0)::integer AS account_creation,
				COUNT(*) FILTER (WHERE o.type IN (1, 2, 13))::integer AS payments,
				COUNT(*) FILTER (WHERE o.type IN (3, 4, 12, 22, 23))::integer AS offers_and_amms,
				COUNT(*) FILTER (WHERE o.type IN (6, 7, 21))::integer AS trustlines,
				COUNT(*) FILTER (WHERE o.type IN (14, 15, 19))::integer AS claimable_balances,
				COUNT(*) FILTER (WHERE o.type IN (16, 17, 18))::integer AS sponsorship,
				COUNT(*) FILTER (WHERE o.type IN (24, 25, 26))::integer AS soroban,
				COUNT(*) FILTER (WHERE o.type IS NULL OR o.type NOT IN (0,1,2,3,4,6,7,12,13,14,15,16,17,18,19,21,22,23,24,25,26))::integer AS other,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type = 0)::integer AS successful_account_creation,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (1, 2, 13))::integer AS successful_payments,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (3, 4, 12, 22, 23))::integer AS successful_offers_and_amms,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (6, 7, 21))::integer AS successful_trustlines,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (14, 15, 19))::integer AS successful_claimable_balances,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (16, 17, 18))::integer AS successful_sponsorship,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND o.type IN (24, 25, 26))::integer AS successful_soroban,
				COUNT(*) FILTER (WHERE o.transaction_successful IS TRUE AND (o.type IS NULL OR o.type NOT IN (0,1,2,3,4,6,7,12,13,14,15,16,17,18,19,21,22,23,24,25,26)))::integer AS successful_other
			FROM operations_row_v2 o
			JOIN next_ledgers l ON l.sequence = o.ledger_sequence
			GROUP BY o.ledger_sequence
		)
		SELECT
			l.*,
			COALESCE(c.account_creation, 0),
			COALESCE(c.payments, 0),
			COALESCE(c.offers_and_amms, 0),
			COALESCE(c.trustlines, 0),
			COALESCE(c.claimable_balances, 0),
			COALESCE(c.sponsorship, 0),
			COALESCE(c.soroban, 0),
			COALESCE(c.other, 0),
			COALESCE(c.successful_account_creation, 0),
			COALESCE(c.successful_payments, 0),
			COALESCE(c.successful_offers_and_amms, 0),
			COALESCE(c.successful_trustlines, 0),
			COALESCE(c.successful_claimable_balances, 0),
			COALESCE(c.successful_sponsorship, 0),
			COALESCE(c.successful_soroban, 0),
			COALESCE(c.successful_other, 0)
		FROM next_ledgers l
		LEFT JOIN operation_categories c ON c.ledger_sequence = l.sequence
		ORDER BY l.sequence ASC
	`, checkpoint, p.batchSize, dataTime)
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
			&row.TxSetOperationCount,
			&row.ValidatorNodeID,
			&row.LedgerCloseSignature,
			&row.SorobanOpCount,
			&row.OperationCategories.AccountCreation,
			&row.OperationCategories.Payments,
			&row.OperationCategories.OffersAndAMMs,
			&row.OperationCategories.Trustlines,
			&row.OperationCategories.ClaimableBalances,
			&row.OperationCategories.Sponsorship,
			&row.OperationCategories.Soroban,
			&row.OperationCategories.Other,
			&row.SuccessfulOperationCategories.AccountCreation,
			&row.SuccessfulOperationCategories.Payments,
			&row.SuccessfulOperationCategories.OffersAndAMMs,
			&row.SuccessfulOperationCategories.Trustlines,
			&row.SuccessfulOperationCategories.ClaimableBalances,
			&row.SuccessfulOperationCategories.Sponsorship,
			&row.SuccessfulOperationCategories.Soroban,
			&row.SuccessfulOperationCategories.Other,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan bronze ledger: %w", err)
		}
		batch = append(batch, row)
		batch[len(batch)-1].OperationCategoriesComplete =
			categoryCountTotal(row.OperationCategories) == nullableInt32Value(row.TxSetOperationCount) &&
				categoryCountTotal(row.SuccessfulOperationCategories) == nullableInt32Value(row.OperationCount)
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
				tx_set_operation_count,
				validator_node_id,
				ledger_close_signature,
				soroban_op_count,
				op_category_account_creation,
				op_category_payments,
				op_category_offers_and_amms,
				op_category_trustlines,
				op_category_claimable_balances,
				op_category_sponsorship,
				op_category_soroban,
				op_category_other,
				successful_op_category_account_creation,
				successful_op_category_payments,
				successful_op_category_offers_and_amms,
				successful_op_category_trustlines,
				successful_op_category_claimable_balances,
				successful_op_category_sponsorship,
				successful_op_category_soroban,
				successful_op_category_other,
				operation_categories_complete,
				close_time_seconds
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32
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
				tx_set_operation_count = EXCLUDED.tx_set_operation_count,
				validator_node_id = EXCLUDED.validator_node_id,
				ledger_close_signature = EXCLUDED.ledger_close_signature,
				soroban_op_count = EXCLUDED.soroban_op_count,
				op_category_account_creation = EXCLUDED.op_category_account_creation,
				op_category_payments = EXCLUDED.op_category_payments,
				op_category_offers_and_amms = EXCLUDED.op_category_offers_and_amms,
				op_category_trustlines = EXCLUDED.op_category_trustlines,
				op_category_claimable_balances = EXCLUDED.op_category_claimable_balances,
				op_category_sponsorship = EXCLUDED.op_category_sponsorship,
				op_category_soroban = EXCLUDED.op_category_soroban,
				op_category_other = EXCLUDED.op_category_other,
				successful_op_category_account_creation = EXCLUDED.successful_op_category_account_creation,
				successful_op_category_payments = EXCLUDED.successful_op_category_payments,
				successful_op_category_offers_and_amms = EXCLUDED.successful_op_category_offers_and_amms,
				successful_op_category_trustlines = EXCLUDED.successful_op_category_trustlines,
				successful_op_category_claimable_balances = EXCLUDED.successful_op_category_claimable_balances,
				successful_op_category_sponsorship = EXCLUDED.successful_op_category_sponsorship,
				successful_op_category_soroban = EXCLUDED.successful_op_category_soroban,
				successful_op_category_other = EXCLUDED.successful_op_category_other,
				operation_categories_complete = EXCLUDED.operation_categories_complete,
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
			nullableInt32(row.TxSetOperationCount),
			nullableString(row.ValidatorNodeID),
			nullableString(row.LedgerCloseSignature),
			nullableInt32(row.SorobanOpCount),
			nullableInt32(row.OperationCategories.AccountCreation),
			nullableInt32(row.OperationCategories.Payments),
			nullableInt32(row.OperationCategories.OffersAndAMMs),
			nullableInt32(row.OperationCategories.Trustlines),
			nullableInt32(row.OperationCategories.ClaimableBalances),
			nullableInt32(row.OperationCategories.Sponsorship),
			nullableInt32(row.OperationCategories.Soroban),
			nullableInt32(row.OperationCategories.Other),
			nullableInt32(row.SuccessfulOperationCategories.AccountCreation),
			nullableInt32(row.SuccessfulOperationCategories.Payments),
			nullableInt32(row.SuccessfulOperationCategories.OffersAndAMMs),
			nullableInt32(row.SuccessfulOperationCategories.Trustlines),
			nullableInt32(row.SuccessfulOperationCategories.ClaimableBalances),
			nullableInt32(row.SuccessfulOperationCategories.Sponsorship),
			nullableInt32(row.SuccessfulOperationCategories.Soroban),
			nullableInt32(row.SuccessfulOperationCategories.Other),
			row.OperationCategoriesComplete,
			closeTimeSeconds,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving ledger %d: %w", row.Sequence, err)
		}

		prevClosedAt = &row.ClosedAt
	}

	last := batch[len(batch)-1]

	retainedRows, err := applyRecentRetentionWithReference(ctx, tx, "serving.sv_ledger_stats_recent", "closed_at", "30 days", dataTime)
	if err != nil {
		return RunStats{}, err
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, last.Sequence, &last.ClosedAt); err != nil {
		return RunStats{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d retention_deleted=%d checkpoint=%d", p.Name(), p.network, len(batch), retainedRows, last.Sequence)
	return RunStats{RowsApplied: int64(len(batch)), RowsDeleted: retainedRows, Checkpoint: last.Sequence}, nil
}

func categoryCountTotal(counts ledgerOperationCategoryCounts) int32 {
	return nullableInt32Value(counts.AccountCreation) +
		nullableInt32Value(counts.Payments) +
		nullableInt32Value(counts.OffersAndAMMs) +
		nullableInt32Value(counts.Trustlines) +
		nullableInt32Value(counts.ClaimableBalances) +
		nullableInt32Value(counts.Sponsorship) +
		nullableInt32Value(counts.Soroban) +
		nullableInt32Value(counts.Other)
}

func nullableInt32Value(value sql.NullInt32) int32 {
	if !value.Valid {
		return 0
	}
	return value.Int32
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
