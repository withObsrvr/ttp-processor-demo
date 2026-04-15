package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventsRecentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type bronzeEventRow struct {
	EventID                  string
	TxHash                   string
	LedgerSequence           int64
	CreatedAt                *time.Time
	EventIndex               *int32
	ContractID               *string
	Topic0                   *string
	Topic1                   *string
	Topic2                   *string
	Topic3                   *string
	EventType                *string
	InSuccessfulContractCall *bool
	TopicsJSON               *string
	TopicsDecoded            *string
	DataXDR                  *string
	DataDecoded              *string
	TopicCount               *int32
	OperationIndex           *int32
	DecodedSummary           *string
}

func NewEventsRecentProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *EventsRecentProjector {
	return &EventsRecentProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *EventsRecentProjector) Name() string { return "events_recent" }

func (p *EventsRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	startLedger := checkpoint
	if startLedger > 0 {
		startLedger-- // small replay overlap for idempotent upsert safety near ledger boundaries
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			COALESCE(NULLIF(event_id, ''), ledger_sequence::text || ':' || transaction_hash || ':' || COALESCE(event_index::text, '0')) as event_id,
			transaction_hash,
			ledger_sequence,
			COALESCE(closed_at, created_at, now()) as created_at,
			event_index,
			contract_id,
			CASE
				WHEN topics_decoded IS NOT NULL AND topics_decoded <> '' THEN topics_decoded::jsonb ->> 0
				WHEN topics_json IS NOT NULL AND topics_json <> '' THEN topics_json::jsonb ->> 0
				ELSE NULL
			END as topic0,
			CASE
				WHEN topics_decoded IS NOT NULL AND topics_decoded <> '' THEN topics_decoded::jsonb ->> 1
				WHEN topics_json IS NOT NULL AND topics_json <> '' THEN topics_json::jsonb ->> 1
				ELSE NULL
			END as topic1,
			CASE
				WHEN topics_decoded IS NOT NULL AND topics_decoded <> '' THEN topics_decoded::jsonb ->> 2
				WHEN topics_json IS NOT NULL AND topics_json <> '' THEN topics_json::jsonb ->> 2
				ELSE NULL
			END as topic2,
			CASE
				WHEN topics_decoded IS NOT NULL AND topics_decoded <> '' THEN topics_decoded::jsonb ->> 3
				WHEN topics_json IS NOT NULL AND topics_json <> '' THEN topics_json::jsonb ->> 3
				ELSE NULL
			END as topic3,
			event_type,
			in_successful_contract_call,
			topics_json,
			topics_decoded,
			data_xdr,
			data_decoded,
			topic_count,
			operation_index,
			COALESCE(NULLIF(data_decoded, ''), NULLIF(topics_decoded, ''), event_type) as decoded_summary
		FROM public.contract_events_stream_v1
		WHERE ledger_sequence >= $1
		  AND COALESCE(closed_at, created_at, now()) >= NOW() - INTERVAL '30 days'
		ORDER BY ledger_sequence ASC, transaction_hash ASC, event_index ASC
		LIMIT $2
	`, startLedger, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query source events recent: %w", err)
	}
	defer rows.Close()

	var batch []bronzeEventRow
	for rows.Next() {
		var r bronzeEventRow
		if err := rows.Scan(
			&r.EventID,
			&r.TxHash,
			&r.LedgerSequence,
			&r.CreatedAt,
			&r.EventIndex,
			&r.ContractID,
			&r.Topic0,
			&r.Topic1,
			&r.Topic2,
			&r.Topic3,
			&r.EventType,
			&r.InSuccessfulContractCall,
			&r.TopicsJSON,
			&r.TopicsDecoded,
			&r.DataXDR,
			&r.DataDecoded,
			&r.TopicCount,
			&r.OperationIndex,
			&r.DecodedSummary,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan source event row: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate source events recent: %w", err)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin events recent tx: %w", err)
	}
	defer tx.Rollback(ctx)

	retainedRows, err := applyRecentRetention(ctx, tx, "serving.sv_events_recent", "created_at", "30 days")
	if err != nil {
		return RunStats{}, err
	}

	if len(batch) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return RunStats{}, fmt.Errorf("commit events retention-only tx: %w", err)
		}
		if retainedRows > 0 {
			log.Printf("projector=%s network=%s retention_deleted=%d checkpoint=%d", p.Name(), p.network, retainedRows, checkpoint)
		}
		return RunStats{RowsDeleted: retainedRows, Checkpoint: checkpoint}, nil
	}

	inserted := 0
	maxLedger := checkpoint
	var lastCreatedAt *time.Time
	for _, r := range batch {
		rawEventJSON, err := json.Marshal(map[string]any{
			"event_id":                    r.EventID,
			"contract_id":                 r.ContractID,
			"ledger_sequence":             r.LedgerSequence,
			"transaction_hash":            r.TxHash,
			"closed_at":                   r.CreatedAt,
			"event_type":                  r.EventType,
			"in_successful_contract_call": r.InSuccessfulContractCall,
			"topics_json":                 r.TopicsJSON,
			"topics_decoded":              r.TopicsDecoded,
			"data_xdr":                    r.DataXDR,
			"data_decoded":                r.DataDecoded,
			"topic_count":                 r.TopicCount,
			"operation_index":             r.OperationIndex,
			"event_index":                 r.EventIndex,
		})
		if err != nil {
			return RunStats{}, fmt.Errorf("marshal serving event %s: %w", r.EventID, err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO serving.sv_events_recent (
				event_id,
				tx_hash,
				ledger_sequence,
				created_at,
				event_index,
				contract_id,
				topic0,
				topic1,
				topic2,
				topic3,
				event_type,
				raw_event_json,
				decoded_summary
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13
			)
			ON CONFLICT (event_id) DO UPDATE SET
				tx_hash = EXCLUDED.tx_hash,
				ledger_sequence = EXCLUDED.ledger_sequence,
				created_at = EXCLUDED.created_at,
				event_index = EXCLUDED.event_index,
				contract_id = EXCLUDED.contract_id,
				topic0 = EXCLUDED.topic0,
				topic1 = EXCLUDED.topic1,
				topic2 = EXCLUDED.topic2,
				topic3 = EXCLUDED.topic3,
				event_type = EXCLUDED.event_type,
				raw_event_json = EXCLUDED.raw_event_json,
				decoded_summary = EXCLUDED.decoded_summary
		`,
			r.EventID,
			r.TxHash,
			r.LedgerSequence,
			r.CreatedAt,
			r.EventIndex,
			r.ContractID,
			r.Topic0,
			r.Topic1,
			r.Topic2,
			r.Topic3,
			r.EventType,
			string(rawEventJSON),
			r.DecodedSummary,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving event %s: %w", r.EventID, err)
		}
		inserted++
		if r.LedgerSequence > maxLedger {
			maxLedger = r.LedgerSequence
		}
		if r.CreatedAt != nil {
			lastCreatedAt = r.CreatedAt
		}
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastCreatedAt); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit events recent tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d retention_deleted=%d checkpoint=%d", p.Name(), p.network, inserted, retainedRows, maxLedger)
	return RunStats{RowsApplied: int64(inserted), RowsDeleted: retainedRows, Checkpoint: maxLedger}, nil
}
