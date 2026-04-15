package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CheckpointStore struct {
	pool *pgxpool.Pool
}

func NewCheckpointStore(pool *pgxpool.Pool) *CheckpointStore {
	return &CheckpointStore{pool: pool}
}

func (s *CheckpointStore) Load(ctx context.Context, projectorName, network string) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `
		SELECT last_ledger_sequence
		FROM serving.sv_projection_checkpoints
		WHERE projection_name = $1 AND network = $2
	`, projectorName, network).Scan(&seq)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return seq, nil
}

func (s *CheckpointStore) Save(ctx context.Context, tx pgx.Tx, projectorName, network string, ledgerSequence int64, closedAt *time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_projection_checkpoints (
			projection_name, network, last_ledger_sequence, last_closed_at, updated_at
		) VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (projection_name, network)
		DO UPDATE SET
			last_ledger_sequence = EXCLUDED.last_ledger_sequence,
			last_closed_at = EXCLUDED.last_closed_at,
			updated_at = now()
	`, projectorName, network, ledgerSequence, closedAt)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}
