package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AccountsCurrentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type silverAccountRow struct {
	AccountID          string
	BalanceText        *string
	SequenceNumber     *int64
	NumSubentries      *int32
	NumSponsoring      *int32
	NumSponsored       *int32
	HomeDomain         *string
	Sponsor            *string
	MasterWeight       *int32
	LowThreshold       *int32
	MedThreshold       *int32
	HighThreshold      *int32
	AuthRequired       *bool
	AuthRevocable      *bool
	AuthImmutable      *bool
	AuthClawback       *bool
	LastModifiedLedger int64
	SequenceLedger     *int64
	SequenceTime       *int64
	UpdatedAt          *time.Time
	CreatedAt          *time.Time
}

func NewAccountsCurrentProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *AccountsCurrentProjector {
	return &AccountsCurrentProjector{network: network, batchSize: batchSize, sourcePool: sourcePool, targetPool: targetPool, checkpoints: checkpoints}
}

func (p *AccountsCurrentProjector) Name() string { return "accounts_current" }

func (p *AccountsCurrentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			home_domain,
			sponsor_account,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			auth_required,
			auth_revocable,
			auth_immutable,
			auth_clawback_enabled,
			last_modified_ledger,
			sequence_ledger,
			sequence_time,
			updated_at,
			created_at
		FROM accounts_current
		WHERE last_modified_ledger > $1
		ORDER BY last_modified_ledger ASC, account_id ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query silver accounts_current: %w", err)
	}
	defer rows.Close()

	var batch []silverAccountRow
	for rows.Next() {
		var r silverAccountRow
		if err := rows.Scan(
			&r.AccountID,
			&r.BalanceText,
			&r.SequenceNumber,
			&r.NumSubentries,
			&r.NumSponsoring,
			&r.NumSponsored,
			&r.HomeDomain,
			&r.Sponsor,
			&r.MasterWeight,
			&r.LowThreshold,
			&r.MedThreshold,
			&r.HighThreshold,
			&r.AuthRequired,
			&r.AuthRevocable,
			&r.AuthImmutable,
			&r.AuthClawback,
			&r.LastModifiedLedger,
			&r.SequenceLedger,
			&r.SequenceTime,
			&r.UpdatedAt,
			&r.CreatedAt,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan silver account row: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate silver account rows: %w", err)
	}
	if len(batch) == 0 {
		return RunStats{Checkpoint: checkpoint}, nil
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin target tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var lastCreatedAt *time.Time
	maxLedger := checkpoint
	for _, r := range batch {
		balanceStroops := parseBalanceText(r.BalanceText)
		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_accounts_current (
				account_id,
				balance_stroops,
				sequence_number,
				num_subentries,
				num_sponsoring,
				num_sponsored,
				created_at,
				last_modified_ledger,
				sequence_ledger,
				sequence_time,
				updated_at,
				home_domain,
				sponsor,
				master_weight,
				low_threshold,
				med_threshold,
				high_threshold,
				auth_required,
				auth_revocable,
				auth_immutable,
				auth_clawback_enabled
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
			)
			ON CONFLICT (account_id) DO UPDATE SET
				balance_stroops = EXCLUDED.balance_stroops,
				sequence_number = EXCLUDED.sequence_number,
				num_subentries = EXCLUDED.num_subentries,
				num_sponsoring = EXCLUDED.num_sponsoring,
				num_sponsored = EXCLUDED.num_sponsored,
				created_at = COALESCE(serving.sv_accounts_current.created_at, EXCLUDED.created_at),
				last_modified_ledger = EXCLUDED.last_modified_ledger,
				sequence_ledger = EXCLUDED.sequence_ledger,
				sequence_time = EXCLUDED.sequence_time,
				updated_at = EXCLUDED.updated_at,
				home_domain = EXCLUDED.home_domain,
				sponsor = EXCLUDED.sponsor,
				master_weight = EXCLUDED.master_weight,
				low_threshold = EXCLUDED.low_threshold,
				med_threshold = EXCLUDED.med_threshold,
				high_threshold = EXCLUDED.high_threshold,
				auth_required = EXCLUDED.auth_required,
				auth_revocable = EXCLUDED.auth_revocable,
				auth_immutable = EXCLUDED.auth_immutable,
				auth_clawback_enabled = EXCLUDED.auth_clawback_enabled
		`,
			r.AccountID,
			balanceStroops,
			r.SequenceNumber,
			nullableInt32Ptr(r.NumSubentries),
			nullableInt32Ptr(r.NumSponsoring),
			nullableInt32Ptr(r.NumSponsored),
			r.CreatedAt,
			r.LastModifiedLedger,
			r.SequenceLedger,
			r.SequenceTime,
			r.UpdatedAt,
			r.HomeDomain,
			r.Sponsor,
			nullableInt32Ptr(r.MasterWeight),
			nullableInt32Ptr(r.LowThreshold),
			nullableInt32Ptr(r.MedThreshold),
			nullableInt32Ptr(r.HighThreshold),
			r.AuthRequired,
			r.AuthRevocable,
			r.AuthImmutable,
			r.AuthClawback,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving account %s: %w", r.AccountID, err)
		}
		if r.LastModifiedLedger > maxLedger {
			maxLedger = r.LastModifiedLedger
		}
		if r.CreatedAt != nil {
			lastCreatedAt = r.CreatedAt
		}
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastCreatedAt); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d checkpoint=%d", p.Name(), p.network, len(batch), maxLedger)
	return RunStats{RowsApplied: int64(len(batch)), Checkpoint: maxLedger}, nil
}
