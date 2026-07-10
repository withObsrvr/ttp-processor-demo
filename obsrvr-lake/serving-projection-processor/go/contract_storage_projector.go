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
	var completeThru int64
	if err := p.sourcePool.QueryRow(ctx, `
		SELECT GREATEST(
			COALESCE((SELECT MAX(ledger_sequence) FROM contract_data_current), 0),
			COALESCE((SELECT MAX(ledger_sequence) FROM ttl_current), 0)
		)
	`).Scan(&completeThru); err != nil {
		return RunStats{}, fmt.Errorf("resolve contract storage watermark: %w", err)
	}

	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract storage tx: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_storage_current`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract storage current: %w", err)
	}
	deleteSummaryTag, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_storage_summary`)
	if err != nil {
		return RunStats{}, fmt.Errorf("clear contract storage summary: %w", err)
	}

	rows, err := p.sourcePool.Query(ctx, `
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
				ELSE (ttl.live_until_ledger_seq - $1::bigint)::int
			END AS ttl_remaining,
			CASE
				WHEN ttl.key_hash IS NULL THEN false
				WHEN ttl.expired THEN true
				WHEN ttl.live_until_ledger_seq < $1::bigint THEN true
				ELSE false
			END AS expired
		FROM contract_data_current cd
		LEFT JOIN ttl_current ttl ON ttl.key_hash = cd.key_hash
		WHERE cd.contract_id IS NOT NULL
	`, completeThru)
	if err != nil {
		return RunStats{}, fmt.Errorf("read source contract storage current: %w", err)
	}
	defer rows.Close()

	batch := make([][]interface{}, 0, 1000)
	now := time.Now().UTC()
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
			ttlRemaining, expired, now,
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

	summaryTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_contract_storage_summary (
			contract_id, total_entries, live_entries, expired_entries, deleted_entries,
			persistent_entries, temporary_entries, instance_entries, total_size_bytes,
			latest_ledger, latest_closed_at, updated_at
		)
		SELECT
			contract_id,
			COUNT(*)::bigint AS total_entries,
			COUNT(*) FILTER (WHERE expired = false)::bigint AS live_entries,
			COUNT(*) FILTER (WHERE expired = true)::bigint AS expired_entries,
			0::bigint AS deleted_entries,
			COUNT(*) FILTER (WHERE type = 'persistent')::bigint AS persistent_entries,
			COUNT(*) FILTER (WHERE type = 'temporary')::bigint AS temporary_entries,
			COUNT(*) FILTER (WHERE type = 'instance')::bigint AS instance_entries,
			COALESCE(SUM(size_bytes), 0)::bigint AS total_size_bytes,
			MAX(last_modified_ledger) AS latest_ledger,
			MAX(closed_at) AS latest_closed_at,
			NOW()
		FROM serving.sv_contract_storage_current
		GROUP BY contract_id
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate contract storage summary: %w", err)
	}

	for _, table := range []string{
		"serving.sv_contract_storage_current",
		"serving.sv_contract_storage_summary",
	} {
		if err := saveServingWatermark(ctx, tx, table, 0, completeThru); err != nil {
			return RunStats{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract storage tx: %w", err)
	}
	applied := inserted + summaryTag.RowsAffected()
	deleted := deleteTag.RowsAffected() + deleteSummaryTag.RowsAffected()
	log.Printf("projector=%s network=%s applied=%d deleted=%d watermark=%d rebuilt contract storage serving tables", p.Name(), p.network, applied, deleted, completeThru)
	return RunStats{RowsApplied: applied, RowsDeleted: deleted, Checkpoint: completeThru}, nil
}
