package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ContractsCurrentProjector struct {
	network    string
	sourcePool *pgxpool.Pool
	targetPool *pgxpool.Pool
}

func NewContractsCurrentProjector(network string, sourcePool, targetPool *pgxpool.Pool) *ContractsCurrentProjector {
	return &ContractsCurrentProjector{network: network, sourcePool: sourcePool, targetPool: targetPool}
}

func (p *ContractsCurrentProjector) Name() string { return "contracts_current" }

func (p *ContractsCurrentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contracts current tx: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contracts_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contracts current: %w", err)
	}

	insertTag, err := tx.Exec(ctx, `
		WITH storage AS (
			SELECT contract_id,
			       COUNT(*) as total_entries,
			       COUNT(*) FILTER (WHERE lower(durability) LIKE '%persistent') as persistent_entries,
			       COUNT(*) FILTER (WHERE lower(durability) LIKE '%temporary') as temporary_entries,
			       COUNT(*) FILTER (WHERE lower(durability) LIKE '%instance') as instance_entries,
			       COALESCE(SUM(LENGTH(data_value)), 0) as total_state_size_bytes
			FROM contract_data_current
			GROUP BY contract_id
		), token AS (
			SELECT contract_id, token_name, token_symbol, token_type
			FROM token_registry
		), meta AS (
			SELECT contract_id, creator_address, wasm_hash, created_ledger, created_at
			FROM contract_metadata
		), code AS (
			-- NOTE: n_data_segment_bytes is the closest size-like field currently available
			-- in contract_code_current. It is useful for explorer summaries, but it may not
			-- equal the full raw WASM byte length.
			SELECT contract_code_hash as wasm_hash,
			       n_data_segment_bytes::bigint as wasm_size_bytes
			FROM contract_code_current
		), all_contracts AS (
			SELECT contract_id FROM meta
			UNION
			SELECT contract_id FROM token
			UNION
			SELECT contract_id FROM storage
		)
		INSERT INTO serving.sv_contracts_current (
			contract_id, name, symbol, contract_type, creator_account,
			deploy_ledger, deploy_timestamp, wasm_hash, wasm_size_bytes,
			persistent_entries, temporary_entries, instance_entries,
			total_state_size_bytes, first_seen_at, last_seen_at, updated_at
		)
		SELECT
			ac.contract_id,
			COALESCE(t.token_name, t.token_symbol, ac.contract_id),
			t.token_symbol,
			COALESCE(t.token_type, 'contract'),
			m.creator_address,
			m.created_ledger,
			m.created_at,
			m.wasm_hash,
			c.wasm_size_bytes,
			COALESCE(s.persistent_entries, 0),
			COALESCE(s.temporary_entries, 0),
			COALESCE(s.instance_entries, 0),
			COALESCE(s.total_state_size_bytes, 0),
			m.created_at,
			NULL,
			now()
		FROM all_contracts ac
		LEFT JOIN token t ON t.contract_id = ac.contract_id
		LEFT JOIN meta m ON m.contract_id = ac.contract_id
		LEFT JOIN storage s ON s.contract_id = ac.contract_id
		LEFT JOIN code c ON c.wasm_hash = m.wasm_hash
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contracts current: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contracts current tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d deleted=%d rebuilt contract serving table", p.Name(), p.network, insertTag.RowsAffected(), deleteTag.RowsAffected())
	return RunStats{RowsApplied: insertTag.RowsAffected(), RowsDeleted: deleteTag.RowsAffected()}, nil
}
