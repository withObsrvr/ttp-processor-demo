package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go/strkey"
)

type ExplorerEventsRecentProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	silverPool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

func NewExplorerEventsRecentProjector(network string, batchSize int, sourcePool, silverPool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *ExplorerEventsRecentProjector {
	return &ExplorerEventsRecentProjector{
		network:     network,
		batchSize:   batchSize,
		sourcePool:  sourcePool,
		silverPool:  silverPool,
		targetPool:  targetPool,
		checkpoints: checkpoints,
	}
}

func (p *ExplorerEventsRecentProjector) Name() string { return "explorer_events_recent" }

func explorerEventSuccessFields(transactionSuccessful, inSuccessfulContractCall *bool) (*bool, *bool) {
	_ = inSuccessfulContractCall // raw evidence is projected separately, not used for public status.
	// Compatibility `successful` must be an alias for transaction-level success,
	// never for `in_successful_contract_call`. Preserve nil as unknown.
	return transactionSuccessful, transactionSuccessful
}

func (p *ExplorerEventsRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}
	startLedger := checkpoint
	if startLedger > 0 {
		startLedger--
	}

	dataTime := resolveDataTime(ctx, p.sourcePool, "public.contract_events_stream_v1", "closed_at")

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			COALESCE(NULLIF(ce.event_id, ''), ce.ledger_sequence::text || ':' || ce.transaction_hash || ':' || COALESCE(ce.event_index::text, '0')) as event_id,
			ce.transaction_hash,
			ce.ledger_sequence,
			COALESCE(ce.closed_at, ce.created_at, now()) as created_at,
			ce.event_index,
			ce.contract_id,
			CASE
				WHEN ce.topics_decoded IS NOT NULL AND ce.topics_decoded <> '' THEN ce.topics_decoded::jsonb ->> 0
				WHEN ce.topics_json IS NOT NULL AND ce.topics_json <> '' THEN ce.topics_json::jsonb ->> 0
				ELSE NULL
			END as topic0,
			CASE
				WHEN ce.topics_decoded IS NOT NULL AND ce.topics_decoded <> '' THEN ce.topics_decoded::jsonb ->> 1
				WHEN ce.topics_json IS NOT NULL AND ce.topics_json <> '' THEN ce.topics_json::jsonb ->> 1
				ELSE NULL
			END as topic1,
			CASE
				WHEN ce.topics_decoded IS NOT NULL AND ce.topics_decoded <> '' THEN ce.topics_decoded::jsonb ->> 2
				WHEN ce.topics_json IS NOT NULL AND ce.topics_json <> '' THEN ce.topics_json::jsonb ->> 2
				ELSE NULL
			END as topic2,
			CASE
				WHEN ce.topics_decoded IS NOT NULL AND ce.topics_decoded <> '' THEN ce.topics_decoded::jsonb ->> 3
				WHEN ce.topics_json IS NOT NULL AND ce.topics_json <> '' THEN ce.topics_json::jsonb ->> 3
				ELSE NULL
			END as topic3,
			COALESCE(tx.successful, ce.successful),
			ce.in_successful_contract_call,
			ce.topics_decoded,
			ce.data_decoded,
			ce.operation_index
		FROM public.contract_events_stream_v1 ce
		LEFT JOIN public.transactions_row_v2 tx
		  ON tx.transaction_hash = ce.transaction_hash
		 AND tx.ledger_sequence = ce.ledger_sequence
		WHERE ce.ledger_sequence >= $1
		  AND COALESCE(ce.closed_at, ce.created_at, now()) >= $3::timestamp - INTERVAL '30 days'
		ORDER BY ce.ledger_sequence ASC, ce.transaction_hash ASC, ce.event_index ASC
		LIMIT $2
	`, startLedger, p.batchSize, dataTime)
	if err != nil {
		return RunStats{}, fmt.Errorf("query explorer source events: %w", err)
	}
	defer rows.Close()

	var batch []bronzeEventRow
	contractIDs := map[string]struct{}{}
	for rows.Next() {
		var r bronzeEventRow
		if err := rows.Scan(&r.EventID, &r.TxHash, &r.LedgerSequence, &r.CreatedAt, &r.EventIndex, &r.ContractID, &r.Topic0, &r.Topic1, &r.Topic2, &r.Topic3, &r.TransactionSuccessful, &r.InSuccessfulContractCall, &r.TopicsDecoded, &r.DataDecoded, &r.OperationIndex); err != nil {
			return RunStats{}, fmt.Errorf("scan explorer source event: %w", err)
		}
		batch = append(batch, r)
		if r.ContractID != nil && *r.ContractID != "" {
			contractIDs[*r.ContractID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate explorer source events: %w", err)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin explorer events tx: %w", err)
	}
	defer tx.Rollback(ctx)

	retainedRows, err := applyRecentRetentionWithReference(ctx, tx, "serving.sv_explorer_events_recent", "created_at", "30 days", dataTime)
	if err != nil {
		return RunStats{}, err
	}
	if len(batch) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return RunStats{}, fmt.Errorf("commit explorer retention-only tx: %w", err)
		}
		return RunStats{RowsDeleted: retainedRows, Checkpoint: checkpoint}, nil
	}

	classifier, err := loadExplorerEventClassifier(ctx, p.silverPool)
	if err != nil {
		return RunStats{}, fmt.Errorf("load event classification rules: %w", err)
	}
	registry, err := p.loadContractDisplayInfo(ctx, contractIDs)
	if err != nil {
		return RunStats{}, fmt.Errorf("load contract display info: %w", err)
	}

	inserted := 0
	maxLedger := checkpoint
	var lastCreatedAt *time.Time
	for _, r := range batch {
		classification := classifier.Classify(r.ContractID, r.Topic0, r.TopicsDecoded)
		var contractAddress *string
		if r.ContractID != nil {
			if converted, err := hexContractToStrKey(*r.ContractID); err == nil {
				contractAddress = &converted
			}
		}
		var contractName, contractSymbol, contractCategory *string
		if contractAddress != nil {
			if info, ok := registry[*contractAddress]; ok {
				contractName = info.name
				contractSymbol = info.symbol
				contractCategory = info.category
			}
		}
		// Public explorer success is transaction-scoped. Keep `successful` as a
		// deprecated compatibility alias for `transaction_successful`; preserve
		// Soroban call-context evidence separately in `in_successful_contract_call`.
		transactionSuccessful, successful := explorerEventSuccessFields(r.TransactionSuccessful, r.InSuccessfulContractCall)

		_, err = tx.Exec(ctx, `
			INSERT INTO serving.sv_explorer_events_recent (
				event_id, tx_hash, ledger_sequence, created_at, event_index, operation_index,
				contract_id, contract_address, topic0, topic1, topic2, topic3,
				topics_decoded, data_decoded, transaction_successful, in_successful_contract_call,
				successful, explorer_type, protocol, contract_name, contract_symbol, contract_category
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
			)
			ON CONFLICT (event_id) DO UPDATE SET
				tx_hash = EXCLUDED.tx_hash,
				ledger_sequence = EXCLUDED.ledger_sequence,
				created_at = EXCLUDED.created_at,
				event_index = EXCLUDED.event_index,
				operation_index = EXCLUDED.operation_index,
				contract_id = EXCLUDED.contract_id,
				contract_address = EXCLUDED.contract_address,
				topic0 = EXCLUDED.topic0,
				topic1 = EXCLUDED.topic1,
				topic2 = EXCLUDED.topic2,
				topic3 = EXCLUDED.topic3,
				topics_decoded = EXCLUDED.topics_decoded,
				data_decoded = EXCLUDED.data_decoded,
				transaction_successful = EXCLUDED.transaction_successful,
				in_successful_contract_call = EXCLUDED.in_successful_contract_call,
				successful = EXCLUDED.successful,
				explorer_type = EXCLUDED.explorer_type,
				protocol = EXCLUDED.protocol,
				contract_name = EXCLUDED.contract_name,
				contract_symbol = EXCLUDED.contract_symbol,
				contract_category = EXCLUDED.contract_category
		`, r.EventID, r.TxHash, r.LedgerSequence, r.CreatedAt, r.EventIndex, r.OperationIndex,
			r.ContractID, contractAddress, r.Topic0, r.Topic1, r.Topic2, r.Topic3,
			r.TopicsDecoded, r.DataDecoded, transactionSuccessful, r.InSuccessfulContractCall,
			successful, classification.EventType, classification.Protocol,
			contractName, contractSymbol, contractCategory)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving explorer event %s: %w", r.EventID, err)
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
		return RunStats{}, fmt.Errorf("commit explorer events tx: %w", err)
	}
	log.Printf("projector=%s network=%s applied=%d retention_deleted=%d checkpoint=%d", p.Name(), p.network, inserted, retainedRows, maxLedger)
	return RunStats{RowsApplied: int64(inserted), RowsDeleted: retainedRows, Checkpoint: maxLedger}, nil
}

type contractDisplayInfo struct {
	name     *string
	symbol   *string
	category *string
}

func (p *ExplorerEventsRecentProjector) loadContractDisplayInfo(ctx context.Context, contractIDs map[string]struct{}) (map[string]contractDisplayInfo, error) {
	result := map[string]contractDisplayInfo{}
	if len(contractIDs) == 0 {
		return result, nil
	}
	ids := make([]string, 0, len(contractIDs))
	for id := range contractIDs {
		if strkeyID, err := hexContractToStrKey(id); err == nil {
			ids = append(ids, strkeyID)
		}
	}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := p.silverPool.Query(ctx, `
		SELECT cr.contract_id, cr.display_name, tr.token_symbol, cr.category
		FROM contract_registry cr
		LEFT JOIN token_registry tr ON cr.contract_id = tr.contract_id
		WHERE cr.contract_id = ANY($1)
	`, ids)
	if err != nil {
		if !strings.Contains(err.Error(), "contract_registry") {
			return nil, err
		}
		return result, nil
	}
	defer rows.Close()
	for rows.Next() {
		var contractID string
		var name, symbol, category *string
		if err := rows.Scan(&contractID, &name, &symbol, &category); err != nil {
			return nil, err
		}
		result[contractID] = contractDisplayInfo{name: name, symbol: symbol, category: category}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func hexContractToStrKey(hexID string) (string, error) {
	v := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(hexID)), "0x")
	if v == "" {
		return "", fmt.Errorf("empty contract id")
	}
	if len(v)%2 != 0 {
		v = "0" + v
	}
	raw, err := hex.DecodeString(v)
	if err != nil {
		return "", err
	}
	return strkey.Encode(strkey.VersionByteContract, raw)
}
