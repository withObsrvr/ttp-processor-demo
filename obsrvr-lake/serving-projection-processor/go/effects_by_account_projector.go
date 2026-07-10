package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const effectsByAccountWatermarkTable = "serving.sv_effects_by_account"

type EffectsByAccountProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type effectsByAccountRow struct {
	AccountID      string
	LedgerSequence int64
	TxHash         string
	OpIndex        int32
	EffectIndex    int32
	OperationTOID  *int64
	EffectType     *int32
	EffectTypeName *string
	Amount         *string
	AssetCode      *string
	AssetIssuer    *string
	AssetType      *string
	DetailsJSON    *string
	TrustlineLimit *string
	AuthorizeFlag  *bool
	ClawbackFlag   *bool
	SignerAccount  *string
	SignerWeight   *int32
	OfferID        *int64
	SellerAccount  *string
	ClosedAt       time.Time
}

func NewEffectsByAccountProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *EffectsByAccountProjector {
	return &EffectsByAccountProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *EffectsByAccountProjector) Name() string { return "effects_by_account" }

func (p *EffectsByAccountProjector) SourceHighWatermark(ctx context.Context) (int64, error) {
	var wm int64
	err := p.sourcePool.QueryRow(ctx, `SELECT COALESCE(MAX(ledger_sequence), 0) FROM enriched_ledgers`).Scan(&wm)
	if err == nil {
		return wm, nil
	}
	err = p.sourcePool.QueryRow(ctx, `SELECT COALESCE(MAX(ledger_sequence), 0) FROM effects`).Scan(&wm)
	if err != nil {
		return 0, fmt.Errorf("query effects by account watermark: %w", err)
	}
	return wm, nil
}

func (p *EffectsByAccountProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	hasCompleteWatermark, completeFrom, completeThru, err := p.loadCompleteWatermark(ctx)
	if err != nil {
		return RunStats{}, err
	}
	startLedger := checkpoint
	if hasCompleteWatermark {
		startLedger = completeThru
	}

	batch, err := p.loadBatch(ctx, startLedger)
	if err != nil {
		return RunStats{}, err
	}
	if len(batch) == 0 {
		return p.advanceEmptyTail(ctx, startLedger, hasCompleteWatermark, completeFrom)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin effects by account tx: %w", err)
	}
	defer tx.Rollback(ctx)

	maxLedger := startLedger
	var lastClosedAt *time.Time
	for _, r := range batch {
		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_effects_by_account (
				account_id, ledger_sequence, tx_hash, op_index, effect_index,
				operation_toid, effect_type, effect_type_name, amount,
				asset_code, asset_issuer, asset_type, details_json,
				trustline_limit, authorize_flag, clawback_flag,
				signer_account, signer_weight, offer_id, seller_account,
				closed_at, materialized_at
			) VALUES (
				$1,$2,$3,$4,$5,
				$6,$7,$8,$9,
				$10,$11,$12,$13::jsonb,
				$14,$15,$16,
				$17,$18,$19,$20,
				$21,now()
			)
			ON CONFLICT (account_id, ledger_sequence, tx_hash, op_index, effect_index) DO UPDATE SET
				operation_toid = EXCLUDED.operation_toid,
				effect_type = EXCLUDED.effect_type,
				effect_type_name = EXCLUDED.effect_type_name,
				amount = EXCLUDED.amount,
				asset_code = EXCLUDED.asset_code,
				asset_issuer = EXCLUDED.asset_issuer,
				asset_type = EXCLUDED.asset_type,
				details_json = EXCLUDED.details_json,
				trustline_limit = EXCLUDED.trustline_limit,
				authorize_flag = EXCLUDED.authorize_flag,
				clawback_flag = EXCLUDED.clawback_flag,
				signer_account = EXCLUDED.signer_account,
				signer_weight = EXCLUDED.signer_weight,
				offer_id = EXCLUDED.offer_id,
				seller_account = EXCLUDED.seller_account,
				closed_at = EXCLUDED.closed_at,
				materialized_at = EXCLUDED.materialized_at
		`,
			r.AccountID,
			r.LedgerSequence,
			r.TxHash,
			r.OpIndex,
			r.EffectIndex,
			r.OperationTOID,
			r.EffectType,
			r.EffectTypeName,
			r.Amount,
			r.AssetCode,
			r.AssetIssuer,
			r.AssetType,
			r.DetailsJSON,
			r.TrustlineLimit,
			r.AuthorizeFlag,
			r.ClawbackFlag,
			r.SignerAccount,
			r.SignerWeight,
			r.OfferID,
			r.SellerAccount,
			r.ClosedAt,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert account effect %s/%d/%s/%d/%d: %w", r.AccountID, r.LedgerSequence, r.TxHash, r.OpIndex, r.EffectIndex, err)
		}
		if r.LedgerSequence > maxLedger {
			maxLedger = r.LedgerSequence
		}
		t := r.ClosedAt
		lastClosedAt = &t
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastClosedAt); err != nil {
		return RunStats{}, err
	}
	if hasCompleteWatermark && maxLedger > completeThru {
		if err := saveServingWatermark(ctx, tx, effectsByAccountWatermarkTable, completeFrom, maxLedger); err != nil {
			return RunStats{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit effects by account tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d checkpoint=%d watermark_advanced=%v", p.Name(), p.network, len(batch), maxLedger, hasCompleteWatermark && maxLedger > completeThru)
	return RunStats{RowsApplied: int64(len(batch)), Checkpoint: maxLedger}, nil
}

func (p *EffectsByAccountProjector) loadBatch(ctx context.Context, startLedger int64) ([]effectsByAccountRow, error) {
	rows, err := p.sourcePool.Query(ctx, `
		WITH candidate_ledgers AS (
			SELECT DISTINCT ledger_sequence
			FROM effects
			WHERE ledger_sequence > $1
			ORDER BY ledger_sequence ASC
			LIMIT $2
		)
		SELECT
			e.account_id,
			e.ledger_sequence,
			e.transaction_hash,
			e.operation_index,
			e.effect_index,
			COALESCE(e.operation_id, o.operation_id) AS operation_toid,
			e.effect_type,
			e.effect_type_string,
			e.amount,
			e.asset_code,
			e.asset_issuer,
			e.asset_type,
			NULLIF(e.details_json, '') AS details_json,
			e.trustline_limit,
			e.authorize_flag,
			e.clawback_flag,
			e.signer_account,
			e.signer_weight,
			e.offer_id,
			e.seller_account,
			COALESCE(e.created_at, now()) AS closed_at
		FROM effects e
		JOIN candidate_ledgers cl ON cl.ledger_sequence = e.ledger_sequence
		LEFT JOIN enriched_history_operations o ON o.ledger_sequence = e.ledger_sequence
			AND o.transaction_hash = e.transaction_hash
			AND o.operation_index = e.operation_index
		WHERE e.account_id IS NOT NULL
		  AND e.account_id <> ''
		ORDER BY e.ledger_sequence ASC, e.transaction_hash ASC, e.operation_index ASC, e.effect_index ASC
	`, startLedger, p.batchSize)
	if err != nil {
		return nil, fmt.Errorf("query effects by account: %w", err)
	}
	defer rows.Close()

	var batch []effectsByAccountRow
	for rows.Next() {
		var r effectsByAccountRow
		if err := rows.Scan(
			&r.AccountID,
			&r.LedgerSequence,
			&r.TxHash,
			&r.OpIndex,
			&r.EffectIndex,
			&r.OperationTOID,
			&r.EffectType,
			&r.EffectTypeName,
			&r.Amount,
			&r.AssetCode,
			&r.AssetIssuer,
			&r.AssetType,
			&r.DetailsJSON,
			&r.TrustlineLimit,
			&r.AuthorizeFlag,
			&r.ClawbackFlag,
			&r.SignerAccount,
			&r.SignerWeight,
			&r.OfferID,
			&r.SellerAccount,
			&r.ClosedAt,
		); err != nil {
			return nil, fmt.Errorf("scan effects by account row: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate effects by account rows: %w", err)
	}
	return batch, nil
}

func (p *EffectsByAccountProjector) advanceEmptyTail(ctx context.Context, startLedger int64, hasCompleteWatermark bool, completeFrom int64) (RunStats, error) {
	sourceHighWatermark, err := p.SourceHighWatermark(ctx)
	if err != nil {
		return RunStats{}, err
	}
	if sourceHighWatermark <= startLedger {
		return RunStats{Checkpoint: startLedger}, nil
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin effects by account empty-tail tx: %w", err)
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, sourceHighWatermark, &now); err != nil {
		return RunStats{}, err
	}
	if hasCompleteWatermark {
		if err := saveServingWatermark(ctx, tx, effectsByAccountWatermarkTable, completeFrom, sourceHighWatermark); err != nil {
			return RunStats{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit effects by account empty-tail tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=0 checkpoint=%d watermark_advanced=%v", p.Name(), p.network, sourceHighWatermark, hasCompleteWatermark)
	return RunStats{Checkpoint: sourceHighWatermark}, nil
}

func (p *EffectsByAccountProjector) loadCompleteWatermark(ctx context.Context) (bool, int64, int64, error) {
	var status string
	var completeFrom, completeThru int64
	err := p.targetPool.QueryRow(ctx, `
		SELECT status, complete_from, complete_thru
		FROM serving.sv_watermarks
		WHERE table_name = $1
	`, effectsByAccountWatermarkTable).Scan(&status, &completeFrom, &completeThru)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, 0, 0, nil
		}
		return false, 0, 0, fmt.Errorf("load effects by account watermark: %w", err)
	}
	return status == "complete", completeFrom, completeThru, nil
}
