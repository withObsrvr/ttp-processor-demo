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
	HomeDomain         *string
	MasterWeight       *int32
	LowThreshold       *int32
	MedThreshold       *int32
	HighThreshold      *int32
	LastModifiedLedger int64
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
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			last_modified_ledger,
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
			&r.HomeDomain,
			&r.MasterWeight,
			&r.LowThreshold,
			&r.MedThreshold,
			&r.HighThreshold,
			&r.LastModifiedLedger,
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
				created_at,
				last_modified_ledger,
				updated_at,
				home_domain,
				master_weight,
				low_threshold,
				med_threshold,
				high_threshold
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
			)
			ON CONFLICT (account_id) DO UPDATE SET
				balance_stroops = EXCLUDED.balance_stroops,
				sequence_number = EXCLUDED.sequence_number,
				num_subentries = EXCLUDED.num_subentries,
				created_at = COALESCE(serving.sv_accounts_current.created_at, EXCLUDED.created_at),
				last_modified_ledger = EXCLUDED.last_modified_ledger,
				updated_at = EXCLUDED.updated_at,
				home_domain = EXCLUDED.home_domain,
				master_weight = EXCLUDED.master_weight,
				low_threshold = EXCLUDED.low_threshold,
				med_threshold = EXCLUDED.med_threshold,
				high_threshold = EXCLUDED.high_threshold
		`,
			r.AccountID,
			balanceStroops,
			r.SequenceNumber,
			nullableInt32Ptr(r.NumSubentries),
			r.CreatedAt,
			r.LastModifiedLedger,
			r.UpdatedAt,
			r.HomeDomain,
			nullableInt32Ptr(r.MasterWeight),
			nullableInt32Ptr(r.LowThreshold),
			nullableInt32Ptr(r.MedThreshold),
			nullableInt32Ptr(r.HighThreshold),
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
