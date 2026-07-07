package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ContractStorageProjector struct {
	network    string
	sourcePool *pgxpool.Pool
	targetPool *pgxpool.Pool
}

func NewContractStorageProjector(network string, sourcePool, targetPool *pgxpool.Pool) *ContractStorageProjector {
	return &ContractStorageProjector{network: network, sourcePool: sourcePool, targetPool: targetPool}
}

func (p *ContractStorageProjector) Name() string { return "contract_storage" }

func (p *ContractStorageProjector) RunOnce(ctx context.Context) (RunStats, error) {
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract storage tx: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_storage_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract storage current: %w", err)
	}

	rows, err := p.sourcePool.Query(ctx, `
		WITH current_ledger AS (
			SELECT GREATEST(
				COALESCE((SELECT MAX(ledger_sequence) FROM contract_data_current), 0),
				COALESCE((SELECT MAX(ledger_sequence) FROM ttl_current), 0)
			) AS ledger_sequence
		)
		SELECT
			cd.contract_id,
			cd.key_hash,
			cd.key_hash AS key,
			cd.durability,
			CASE
				WHEN lower(cd.durability) LIKE '%instance%' THEN 'instance'
				WHEN lower(cd.durability) LIKE '%temporary%' THEN 'temporary'
				WHEN lower(cd.durability) LIKE '%persistent%' THEN 'persistent'
				ELSE lower(cd.durability)
			END AS type,
			LENGTH(cd.data_value)::int AS size_bytes,
			cd.data_value,
			cd.last_modified_ledger,
			cd.closed_at,
			ttl.live_until_ledger_seq,
			CASE
				WHEN ttl.live_until_ledger_seq IS NULL THEN NULL
				ELSE (ttl.live_until_ledger_seq - cl.ledger_sequence)::int
			END AS ttl_remaining,
			CASE
				WHEN ttl.key_hash IS NULL THEN false
				WHEN ttl.expired THEN true
				WHEN ttl.live_until_ledger_seq < cl.ledger_sequence THEN true
				ELSE false
			END AS expired
		FROM contract_data_current cd
		CROSS JOIN current_ledger cl
		LEFT JOIN ttl_current ttl ON ttl.key_hash = cd.key_hash
		WHERE cd.contract_id IS NOT NULL
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("read source contract storage current: %w", err)
	}
	defer rows.Close()

	batch := make([][]interface{}, 0, 1000)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		copyCount, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"serving", "sv_contract_storage_current"},
			[]string{
				"contract_id", "key_hash", "key", "durability", "type", "size_bytes",
				"data_value", "last_modified_ledger", "closed_at", "live_until_ledger_seq",
				"ttl_remaining", "expired", "updated_at",
			},
			pgx.CopyFromRows(batch),
		)
		if err != nil {
			return fmt.Errorf("copy contract storage current: %w", err)
		}
		_ = copyCount
		batch = batch[:0]
		return nil
	}

	var inserted int64
	for rows.Next() {
		var contractID, keyHash, key, durability, typ string
		var sizeBytes *int32
		var dataValue *string
		var lastModified int64
		var closedAt *time.Time
		var liveUntil *int64
		var ttlRemaining *int32
		var expired bool
		if err := rows.Scan(&contractID, &keyHash, &key, &durability, &typ, &sizeBytes, &dataValue, &lastModified, &closedAt, &liveUntil, &ttlRemaining, &expired); err != nil {
			return RunStats{}, fmt.Errorf("scan contract storage current: %w", err)
		}
		batch = append(batch, []interface{}{
			contractID, keyHash, key, durability, typ, sizeBytes,
			dataValue, lastModified, closedAt, liveUntil,
			ttlRemaining, expired, time.Now().UTC(),
		})
		inserted++
		if len(batch) >= 1000 {
			if err := flush(); err != nil {
				return RunStats{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate contract storage current: %w", err)
	}
	if err := flush(); err != nil {
		return RunStats{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract storage tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d deleted=%d rebuilt contract storage serving table", p.Name(), p.network, inserted, deleteTag.RowsAffected())
	return RunStats{RowsApplied: inserted, RowsDeleted: deleteTag.RowsAffected()}, nil
}
