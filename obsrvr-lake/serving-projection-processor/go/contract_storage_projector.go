package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	contractStorageCurrentWatermarkTable = "serving.sv_contract_storage_current"
	contractStorageSummaryWatermarkTable = "serving.sv_contract_storage_summary"
)

const contractStorageUpsertSQL = `
	INSERT INTO serving.sv_contract_storage_current (
		contract_id, key_hash, key, durability, type, size_bytes, data_value,
		last_modified_ledger, closed_at, live_until_ledger_seq, ttl_remaining,
		expired, updated_at
	) VALUES ($1,$2,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
	ON CONFLICT (contract_id, key_hash) DO UPDATE SET
		key = EXCLUDED.key,
		durability = EXCLUDED.durability,
		type = EXCLUDED.type,
		size_bytes = EXCLUDED.size_bytes,
		data_value = EXCLUDED.data_value,
		last_modified_ledger = EXCLUDED.last_modified_ledger,
		closed_at = EXCLUDED.closed_at,
		live_until_ledger_seq = EXCLUDED.live_until_ledger_seq,
		ttl_remaining = EXCLUDED.ttl_remaining,
		expired = EXCLUDED.expired,
		updated_at = EXCLUDED.updated_at
	WHERE serving.sv_contract_storage_current.last_modified_ledger <= EXCLUDED.last_modified_ledger
`

const contractStorageDeleteSQL = `
	DELETE FROM serving.sv_contract_storage_current
	WHERE contract_id = $1 AND key_hash = $2 AND last_modified_ledger <= $3
	RETURNING contract_id
`

const contractStorageTTLUpdateSQL = `
	UPDATE serving.sv_contract_storage_current
	SET live_until_ledger_seq = $2,
	    ttl_remaining = ($2::bigint - $3::bigint)::int,
	    expired = ($2::bigint < $3::bigint),
	    updated_at = now()
	WHERE key_hash = $1
	RETURNING contract_id
`

type ContractStorageProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type contractStorageRow struct {
	ContractID         string
	KeyHash            string
	Durability         string
	Type               string
	SizeBytes          *int32
	DataValue          *string
	LastModifiedLedger int64
	ClosedAt           *time.Time
	LiveUntilLedgerSeq *int64
	TTLRemaining       *int32
	Expired            bool
}

type contractStorageTTLChange struct {
	KeyHash            string
	LiveUntilLedgerSeq int64
	LedgerSequence     int64
}

type contractStorageDeletion struct {
	ContractID     string
	KeyHash        string
	LedgerSequence int64
}

func NewContractStorageProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *ContractStorageProjector {
	return &ContractStorageProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *ContractStorageProjector) Name() string { return "contract_storage" }

func (p *ContractStorageProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	hasCompleteWatermark, completeFrom, completeThru, err := p.loadCompleteWatermark(ctx)
	if err != nil {
		return RunStats{}, err
	}
	startLedger := contractStorageStartLedger(checkpoint, hasCompleteWatermark, completeThru)

	batchEnd, found, err := p.loadBatchEnd(ctx, startLedger)
	if err != nil {
		return RunStats{}, err
	}
	if !found {
		return p.advanceEmptyTail(ctx, startLedger, hasCompleteWatermark, completeFrom)
	}

	storageRows, err := p.loadStorageRows(ctx, startLedger, batchEnd)
	if err != nil {
		return RunStats{}, err
	}
	ttlChanges, err := p.loadTTLChanges(ctx, startLedger, batchEnd)
	if err != nil {
		return RunStats{}, err
	}
	deletions, err := p.loadDeletions(ctx, startLedger, batchEnd)
	if err != nil {
		return RunStats{}, err
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract storage tx: %w", err)
	}
	defer tx.Rollback(ctx)

	affectedContracts := make(map[string]struct{})
	var applied, deleted int64
	for _, row := range storageRows {
		tag, err := tx.Exec(ctx, contractStorageUpsertSQL,
			row.ContractID, row.KeyHash, row.Durability, row.Type, row.SizeBytes,
			row.DataValue, row.LastModifiedLedger, row.ClosedAt,
			row.LiveUntilLedgerSeq, row.TTLRemaining, row.Expired,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert contract storage %s/%s: %w", row.ContractID, row.KeyHash, err)
		}
		applied += tag.RowsAffected()
		affectedContracts[row.ContractID] = struct{}{}
	}

	for _, ttl := range ttlChanges {
		rows, err := tx.Query(ctx, contractStorageTTLUpdateSQL, ttl.KeyHash, ttl.LiveUntilLedgerSeq, batchEnd)
		if err != nil {
			return RunStats{}, fmt.Errorf("apply TTL change %s: %w", ttl.KeyHash, err)
		}
		for rows.Next() {
			var contractID string
			if err := rows.Scan(&contractID); err != nil {
				rows.Close()
				return RunStats{}, fmt.Errorf("scan TTL-affected contract: %w", err)
			}
			affectedContracts[contractID] = struct{}{}
			applied++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return RunStats{}, fmt.Errorf("iterate TTL-affected contracts: %w", err)
		}
		rows.Close()
	}

	for _, deletion := range deletions {
		var contractID string
		err := tx.QueryRow(ctx, contractStorageDeleteSQL,
			deletion.ContractID, deletion.KeyHash, deletion.LedgerSequence,
		).Scan(&contractID)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			return RunStats{}, fmt.Errorf("delete contract storage %s/%s: %w", deletion.ContractID, deletion.KeyHash, err)
		}
		deleted++
		affectedContracts[contractID] = struct{}{}
	}

	for contractID := range affectedContracts {
		if err := p.rebuildSummary(ctx, tx, contractID, batchEnd); err != nil {
			return RunStats{}, err
		}
	}

	now := time.Now().UTC()
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, batchEnd, &now); err != nil {
		return RunStats{}, err
	}
	if hasCompleteWatermark {
		if err := saveServingWatermark(ctx, tx, contractStorageCurrentWatermarkTable, completeFrom, batchEnd); err != nil {
			return RunStats{}, err
		}
		if err := saveServingWatermark(ctx, tx, contractStorageSummaryWatermarkTable, completeFrom, batchEnd); err != nil {
			return RunStats{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract storage tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d deleted=%d contracts=%d checkpoint=%d", p.Name(), p.network, applied, deleted, len(affectedContracts), batchEnd)
	return RunStats{RowsApplied: applied, RowsDeleted: deleted, Checkpoint: batchEnd}, nil
}

func contractStorageStartLedger(checkpoint int64, hasCompleteWatermark bool, completeThru int64) int64 {
	if hasCompleteWatermark {
		return completeThru
	}
	return checkpoint
}

func (p *ContractStorageProjector) loadBatchEnd(ctx context.Context, startLedger int64) (int64, bool, error) {
	var batchEnd *int64
	err := p.sourcePool.QueryRow(ctx, `
		SELECT MAX(ledger_sequence)
		FROM (
			SELECT DISTINCT ledger_sequence
			FROM (
				SELECT ledger_sequence FROM contract_data_current WHERE ledger_sequence > $1
				UNION ALL
				SELECT ledger_sequence FROM ttl_current WHERE ledger_sequence > $1
				UNION ALL
				SELECT ledger_sequence FROM contract_data_deletions WHERE ledger_sequence > $1
				UNION ALL
				SELECT ledger_sequence FROM evicted_keys WHERE ledger_sequence > $1
			) changes
			ORDER BY ledger_sequence ASC
			LIMIT $2
		) batch
	`, startLedger, p.batchSize).Scan(&batchEnd)
	if err != nil {
		return 0, false, fmt.Errorf("resolve contract storage batch: %w", err)
	}
	if batchEnd == nil {
		return 0, false, nil
	}
	return *batchEnd, true, nil
}

func (p *ContractStorageProjector) loadStorageRows(ctx context.Context, startLedger, endLedger int64) ([]contractStorageRow, error) {
	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			cd.contract_id,
			cd.key_hash,
			cd.durability,
			CASE
				WHEN lower(cd.durability) LIKE '%instance%' THEN 'instance'
				WHEN lower(cd.durability) LIKE '%temporary%' THEN 'temporary'
				WHEN lower(cd.durability) LIKE '%persistent%' THEN 'persistent'
				ELSE lower(cd.durability)
			END AS type,
			length(cd.data_value)::int AS size_bytes,
			cd.data_value,
			cd.last_modified_ledger,
			cd.closed_at,
			ttl.live_until_ledger_seq,
			CASE WHEN ttl.live_until_ledger_seq IS NULL THEN NULL ELSE (ttl.live_until_ledger_seq - $2)::int END,
			CASE WHEN ttl.live_until_ledger_seq IS NULL THEN false ELSE ttl.live_until_ledger_seq < $2 END
		FROM contract_data_current cd
		LEFT JOIN ttl_current ttl ON ttl.key_hash = cd.key_hash
		WHERE cd.ledger_sequence > $1 AND cd.ledger_sequence <= $2
		ORDER BY cd.ledger_sequence, cd.contract_id, cd.key_hash
	`, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("query changed contract storage: %w", err)
	}
	defer rows.Close()

	var result []contractStorageRow
	for rows.Next() {
		var row contractStorageRow
		if err := rows.Scan(
			&row.ContractID, &row.KeyHash, &row.Durability, &row.Type,
			&row.SizeBytes, &row.DataValue, &row.LastModifiedLedger, &row.ClosedAt,
			&row.LiveUntilLedgerSeq, &row.TTLRemaining, &row.Expired,
		); err != nil {
			return nil, fmt.Errorf("scan changed contract storage: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changed contract storage: %w", err)
	}
	return result, nil
}

func (p *ContractStorageProjector) loadTTLChanges(ctx context.Context, startLedger, endLedger int64) ([]contractStorageTTLChange, error) {
	rows, err := p.sourcePool.Query(ctx, `
		SELECT key_hash, live_until_ledger_seq, ledger_sequence
		FROM ttl_current
		WHERE ledger_sequence > $1 AND ledger_sequence <= $2
		ORDER BY ledger_sequence, key_hash
	`, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("query contract storage TTL changes: %w", err)
	}
	defer rows.Close()

	var result []contractStorageTTLChange
	for rows.Next() {
		var row contractStorageTTLChange
		if err := rows.Scan(&row.KeyHash, &row.LiveUntilLedgerSeq, &row.LedgerSequence); err != nil {
			return nil, fmt.Errorf("scan contract storage TTL change: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contract storage TTL changes: %w", err)
	}
	return result, nil
}

func (p *ContractStorageProjector) loadDeletions(ctx context.Context, startLedger, endLedger int64) ([]contractStorageDeletion, error) {
	rows, err := p.sourcePool.Query(ctx, `
		SELECT contract_id, key_hash, ledger_sequence
		FROM (
			SELECT contract_id, key_hash, ledger_sequence
			FROM contract_data_deletions
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2
			UNION
			SELECT contract_id, key_hash, ledger_sequence
			FROM evicted_keys
			WHERE ledger_sequence > $1 AND ledger_sequence <= $2
		) deletions
		WHERE contract_id IS NOT NULL AND contract_id <> ''
		ORDER BY ledger_sequence, contract_id, key_hash
	`, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("query contract storage deletions: %w", err)
	}
	defer rows.Close()

	var result []contractStorageDeletion
	for rows.Next() {
		var row contractStorageDeletion
		if err := rows.Scan(&row.ContractID, &row.KeyHash, &row.LedgerSequence); err != nil {
			return nil, fmt.Errorf("scan contract storage deletion: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contract storage deletions: %w", err)
	}
	return result, nil
}

func (p *ContractStorageProjector) rebuildSummary(ctx context.Context, tx pgx.Tx, contractID string, currentLedger int64) error {
	if _, err := tx.Exec(ctx, `DELETE FROM serving.sv_contract_storage_summary WHERE contract_id = $1`, contractID); err != nil {
		return fmt.Errorf("clear contract storage summary %s: %w", contractID, err)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_contract_storage_summary (
			contract_id, total_entries, live_entries, expired_entries, deleted_entries,
			persistent_entries, temporary_entries, instance_entries, total_size_bytes,
			latest_ledger, latest_closed_at, updated_at
		)
		SELECT
			contract_id,
			count(*)::bigint,
			count(*) FILTER (WHERE live_until_ledger_seq IS NULL OR live_until_ledger_seq >= $2)::bigint,
			count(*) FILTER (WHERE live_until_ledger_seq IS NOT NULL AND live_until_ledger_seq < $2)::bigint,
			0::bigint,
			count(*) FILTER (WHERE type = 'persistent')::bigint,
			count(*) FILTER (WHERE type = 'temporary')::bigint,
			count(*) FILTER (WHERE type = 'instance')::bigint,
			coalesce(sum(size_bytes), 0)::bigint,
			max(last_modified_ledger),
			max(closed_at),
			now()
		FROM serving.sv_contract_storage_current
		WHERE contract_id = $1
		GROUP BY contract_id
	`, contractID, currentLedger)
	if err != nil {
		return fmt.Errorf("rebuild contract storage summary %s: %w", contractID, err)
	}
	return nil
}

func (p *ContractStorageProjector) sourceHighWatermark(ctx context.Context) (int64, error) {
	var watermark int64
	if err := p.sourcePool.QueryRow(ctx, `
		SELECT COALESCE(last_ledger_sequence, 0)
		FROM realtime_transformer_checkpoint
		WHERE id = 1
	`).Scan(&watermark); err != nil {
		return 0, fmt.Errorf("query contract storage source watermark: %w", err)
	}
	return watermark, nil
}

func (p *ContractStorageProjector) advanceEmptyTail(ctx context.Context, startLedger int64, hasCompleteWatermark bool, completeFrom int64) (RunStats, error) {
	sourceHighWatermark, err := p.sourceHighWatermark(ctx)
	if err != nil {
		return RunStats{}, err
	}
	if sourceHighWatermark <= startLedger {
		return RunStats{Checkpoint: startLedger}, nil
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin contract storage empty-tail tx: %w", err)
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, sourceHighWatermark, &now); err != nil {
		return RunStats{}, err
	}
	if hasCompleteWatermark {
		if err := saveServingWatermark(ctx, tx, contractStorageCurrentWatermarkTable, completeFrom, sourceHighWatermark); err != nil {
			return RunStats{}, err
		}
		if err := saveServingWatermark(ctx, tx, contractStorageSummaryWatermarkTable, completeFrom, sourceHighWatermark); err != nil {
			return RunStats{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit contract storage empty-tail tx: %w", err)
	}
	return RunStats{Checkpoint: sourceHighWatermark}, nil
}

func (p *ContractStorageProjector) loadCompleteWatermark(ctx context.Context) (bool, int64, int64, error) {
	var status string
	var completeFrom, completeThru int64
	err := p.targetPool.QueryRow(ctx, `
		SELECT status, complete_from, complete_thru
		FROM serving.sv_watermarks
		WHERE table_name = $1
	`, contractStorageCurrentWatermarkTable).Scan(&status, &completeFrom, &completeThru)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, 0, 0, nil
		}
		return false, 0, 0, fmt.Errorf("load contract storage watermark: %w", err)
	}
	return status == "complete", completeFrom, completeThru, nil
}
