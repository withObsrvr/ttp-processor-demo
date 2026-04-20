package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/withObsrvr/obsrvr-lake/serving-projection-processor/txview"
)

// TxReceiptsProjector materializes per-transaction "receipt" rows into
// serving.sv_tx_receipts. Purpose: collapse Prism's 5–7 parallel calls
// (/full, /semantic, /effects, /diffs, /events, etc.) on a transaction
// detail page into a single serving-layer PK lookup.
//
// Reads from silver_hot:
//   - enriched_history_operations   → op list + tx-level metadata
//   - effects                       → per-op effects (for effects_json + diffs derivation)
//   - token_transfers_raw           → token events (events_json)
//   - semantic_activities           → tx_type + description
//
// Writes one row per (distinct transaction_hash) in the watermark window.
//
// Checkpoint: last ledger_sequence successfully projected. Same pattern as
// the other *_recent projectors.
type TxReceiptsProjector struct {
	network     string
	batchSize   int
	bronzePool  *pgxpool.Pool
	silverPool  *pgxpool.Pool
	servingPool *pgxpool.Pool
	checkpoints *CheckpointStore
}

func NewTxReceiptsProjector(network string, batchSize int, bronzePool, silverPool, servingPool *pgxpool.Pool, checkpoints *CheckpointStore) *TxReceiptsProjector {
	return &TxReceiptsProjector{
		network:     network,
		batchSize:   batchSize,
		bronzePool:  bronzePool,
		silverPool:  silverPool,
		servingPool: servingPool,
		checkpoints: checkpoints,
	}
}

func (p *TxReceiptsProjector) Name() string { return "tx_receipts" }

func (p *TxReceiptsProjector) SourceHighWatermark(ctx context.Context) (int64, error) {
	var wm int64
	err := p.silverPool.QueryRow(ctx, `SELECT COALESCE(MAX(ledger_sequence), 0) FROM enriched_history_operations`, pgx.QueryExecModeSimpleProtocol).Scan(&wm)
	if err != nil {
		return 0, fmt.Errorf("query tx receipts watermark: %w", err)
	}
	return wm, nil
}

type txMeta struct {
	TxHash                       string
	LedgerSequence               int64
	CreatedAt                    time.Time
	SourceAccount                string
	Successful                   bool
	Fee                          int64
	MaxFee                       *int64
	AccountSequence              *int64
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
}

func (p *TxReceiptsProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	// Always process complete ledgers before advancing the checkpoint.
	// Limiting directly by tx hash count and then checkpointing by ledger can
	// skip receipts permanently whenever a later-ledger tx lands in the same
	// batch as unprocessed txs from earlier ledgers.
	candidates, err := p.loadCandidates(ctx, checkpoint)
	if err != nil {
		return RunStats{}, err
	}
	if len(candidates) == 0 {
		return RunStats{Checkpoint: checkpoint}, nil
	}

	// 2. For each candidate tx, load the per-tx data in parallel-by-nature
	//    via 4 batched queries keyed by tx_hash IN (...). Build the JSON
	//    blobs in-memory and UPSERT.
	hashes := make([]string, len(candidates))
	for i, c := range candidates {
		hashes[i] = c.TxHash
	}

	operations, err := p.loadOperations(ctx, hashes)
	if err != nil {
		return RunStats{}, err
	}
	effects, err := p.loadEffects(ctx, hashes)
	if err != nil {
		return RunStats{}, err
	}
	tokenEvents, err := p.loadTokenEvents(ctx, hashes)
	if err != nil {
		return RunStats{}, err
	}
	txDetails, err := p.loadTransactionDetails(ctx, hashes)
	if err != nil {
		return RunStats{}, err
	}
	semanticByTx, err := p.loadSemantic(ctx, hashes)
	if err != nil {
		return RunStats{}, err
	}

	// 3. Assemble + UPSERT in a single transaction for atomicity
	tx, err := p.servingPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin tx-receipts tx: %w", err)
	}
	defer tx.Rollback(ctx)

	maxLedger := checkpoint
	var lastCreated *time.Time
	applied := 0
	for _, c := range candidates {
		ops := operations[c.TxHash]
		effs := effects[c.TxHash]
		toks := tokenEvents[c.TxHash]
		detail := txDetails[c.TxHash]
		if detail.Fee != 0 {
			c.Fee = detail.Fee
		}
		if detail.MaxFee != nil {
			c.MaxFee = detail.MaxFee
		}
		if detail.AccountSequence != nil {
			c.AccountSequence = detail.AccountSequence
		}
		if detail.SorobanResourcesInstructions != nil {
			c.SorobanResourcesInstructions = detail.SorobanResourcesInstructions
		}
		if detail.SorobanResourcesReadBytes != nil {
			c.SorobanResourcesReadBytes = detail.SorobanResourcesReadBytes
		}
		if detail.SorobanResourcesWriteBytes != nil {
			c.SorobanResourcesWriteBytes = detail.SorobanResourcesWriteBytes
		}

		summary := buildReceiptSummary(ops, toks)
		fallbackSummary := semanticByTx[c.TxHash]
		if (summary.Type == "" || summary.Type == "unknown") && fallbackSummary.Type != "" {
			summary = fallbackSummary
		}
		semanticPayload := buildReceiptSemanticPayload(c, ops, toks, summary)

		diffs := deriveBalanceDiffs(effs)
		involvedContracts := collectContractIDs(ops, toks)
		involvedAccounts := collectAccountIDs(ops, effs, toks, c.SourceAccount)
		primaryContract := firstOrNil(involvedContracts)
		txType := summary.Type

		fullJSON, _ := json.Marshal(map[string]any{
			"tx_hash":                        c.TxHash,
			"ledger_sequence":                c.LedgerSequence,
			"created_at":                     c.CreatedAt,
			"source_account":                 nullable(c.SourceAccount),
			"successful":                     c.Successful,
			"fee":                            c.Fee,
			"max_fee":                        c.MaxFee,
			"account_sequence":               c.AccountSequence,
			"soroban_resources_instructions": c.SorobanResourcesInstructions,
			"soroban_resources_read_bytes":   c.SorobanResourcesReadBytes,
			"soroban_resources_write_bytes":  c.SorobanResourcesWriteBytes,
			"summary":                        summary,
			"operations":                     receiptOpsToTxViewOps(ops),
			"events":                         receiptEventsToTxViewEvents(toks),
		})
		semanticJSON, _ := json.Marshal(semanticPayload)
		effectsJSON, _ := json.Marshal(effs)
		diffsJSON, _ := json.Marshal(diffs)
		eventsJSON, _ := json.Marshal(toks)

		ct := c.CreatedAt
		_, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_tx_receipts (
				tx_hash, ledger_sequence, created_at, source_account, successful,
				operation_count, full_json, semantic_json, effects_json, diffs_json,
				events_json, involved_contracts, involved_accounts,
				primary_contract_id, tx_type, materialized_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, now())
			ON CONFLICT (tx_hash) DO UPDATE SET
				ledger_sequence     = EXCLUDED.ledger_sequence,
				created_at          = EXCLUDED.created_at,
				source_account      = EXCLUDED.source_account,
				successful          = EXCLUDED.successful,
				operation_count     = EXCLUDED.operation_count,
				full_json           = EXCLUDED.full_json,
				semantic_json       = EXCLUDED.semantic_json,
				effects_json        = EXCLUDED.effects_json,
				diffs_json          = EXCLUDED.diffs_json,
				events_json         = EXCLUDED.events_json,
				involved_contracts  = EXCLUDED.involved_contracts,
				involved_accounts   = EXCLUDED.involved_accounts,
				primary_contract_id = EXCLUDED.primary_contract_id,
				tx_type             = EXCLUDED.tx_type,
				materialized_at     = now()
		`,
			c.TxHash, c.LedgerSequence, c.CreatedAt, nullable(c.SourceAccount), c.Successful,
			len(ops), fullJSON, semanticJSON, effectsJSON, diffsJSON,
			eventsJSON, involvedContracts, involvedAccounts,
			primaryContract, nullable(txType),
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert tx receipt %s: %w", c.TxHash, err)
		}
		if c.LedgerSequence > maxLedger {
			maxLedger = c.LedgerSequence
		}
		lastCreated = &ct
		applied++
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastCreated); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit tx-receipts tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d checkpoint=%d", p.Name(), p.network, applied, maxLedger)
	return RunStats{RowsApplied: int64(applied), Checkpoint: maxLedger}, nil
}

// ---- Data shapes held only in memory while building the row ----------------

type receiptOperation struct {
	Index         int     `json:"operation_index"`
	Type          int     `json:"type"`
	TypeName      string  `json:"type_name,omitempty"`
	SourceAccount *string `json:"source_account,omitempty"`
	ContractID    *string `json:"contract_id,omitempty"`
	FunctionName  *string `json:"function_name,omitempty"`
	Destination   *string `json:"destination,omitempty"`
	AssetCode     *string `json:"asset_code,omitempty"`
	Amount        *string `json:"amount,omitempty"`
	IsSoroban     bool    `json:"is_soroban,omitempty"`
}

type receiptEffect struct {
	OperationIndex int     `json:"operation_index"`
	EffectIndex    int     `json:"effect_index"`
	EffectType     string  `json:"effect_type"`
	AccountID      *string `json:"account_id,omitempty"`
	AssetCode      *string `json:"asset_code,omitempty"`
	AssetIssuer    *string `json:"asset_issuer,omitempty"`
	Amount         *string `json:"amount,omitempty"`
}

type receiptTokenEvent struct {
	EventID        string  `json:"event_id"`
	LedgerSequence int64   `json:"ledger_sequence"`
	TxHash         string  `json:"tx_hash"`
	ClosedAt       string  `json:"closed_at"`
	EventType      string  `json:"event_type"`
	From           *string `json:"from,omitempty"`
	To             *string `json:"to,omitempty"`
	ContractID     *string `json:"contract_id,omitempty"`
	AssetCode      *string `json:"asset_code,omitempty"`
	AssetIssuer    *string `json:"asset_issuer,omitempty"`
	Amount         *string `json:"amount,omitempty"`
	SourceType     string  `json:"source_type"`
	OperationType  *int32  `json:"operation_type,omitempty"`
	EventIndex     int     `json:"event_index"`
}

type balanceDiff struct {
	Account string `json:"account"`
	Asset   string `json:"asset"`
	Delta   string `json:"delta"` // signed string (+/- amount)
}

// ---- Per-column loaders (batched by tx_hash IN (...)) ----------------------
//
// Each loader keys the returned map by tx_hash. Missing keys just mean no
// rows for that tx, which is fine — the projector emits empty arrays.

func (p *TxReceiptsProjector) loadOperations(ctx context.Context, hashes []string) (map[string][]receiptOperation, error) {
	rows, err := p.silverPool.Query(ctx, `
		SELECT transaction_hash, operation_index, type, source_account, contract_id,
		       function_name, destination, asset_code, amount, is_soroban_op
		  FROM enriched_history_operations
		 WHERE transaction_hash = ANY($1)
		 ORDER BY transaction_hash, operation_index
	`, pgx.QueryExecModeSimpleProtocol, hashes)
	if err != nil {
		return nil, fmt.Errorf("query tx-receipt operations: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]receiptOperation, len(hashes))
	for rows.Next() {
		var hash string
		var op receiptOperation
		var soroban *bool
		if err := rows.Scan(&hash, &op.Index, &op.Type, &op.SourceAccount, &op.ContractID,
			&op.FunctionName, &op.Destination, &op.AssetCode, &op.Amount, &soroban); err != nil {
			return nil, err
		}
		op.TypeName = operationTypeName(int32(op.Type))
		if soroban != nil {
			op.IsSoroban = *soroban
		}
		out[hash] = append(out[hash], op)
	}
	return out, rows.Err()
}

func (p *TxReceiptsProjector) loadEffects(ctx context.Context, hashes []string) (map[string][]receiptEffect, error) {
	rows, err := p.silverPool.Query(ctx, `
		SELECT transaction_hash, operation_index, effect_index, effect_type_string,
		       account_id, asset_code, asset_issuer, amount
		  FROM effects
		 WHERE transaction_hash = ANY($1)
		 ORDER BY transaction_hash, operation_index, effect_index
	`, pgx.QueryExecModeSimpleProtocol, hashes)
	if err != nil {
		return nil, fmt.Errorf("query tx-receipt effects: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]receiptEffect, len(hashes))
	for rows.Next() {
		var hash string
		var e receiptEffect
		if err := rows.Scan(&hash, &e.OperationIndex, &e.EffectIndex, &e.EffectType,
			&e.AccountID, &e.AssetCode, &e.AssetIssuer, &e.Amount); err != nil {
			return nil, err
		}
		out[hash] = append(out[hash], e)
	}
	return out, rows.Err()
}

func (p *TxReceiptsProjector) loadTokenEvents(ctx context.Context, hashes []string) (map[string][]receiptTokenEvent, error) {
	rows, err := p.silverPool.Query(ctx, `
		SELECT transaction_hash, ledger_sequence, timestamp, source_type, operation_type,
		       COALESCE(event_index, -1), from_account, to_account,
		       token_contract_id, asset_code, asset_issuer, amount::text
		  FROM token_transfers_raw
		 WHERE transaction_hash = ANY($1) AND transaction_successful = true
		 ORDER BY transaction_hash, ledger_sequence, timestamp, COALESCE(event_index, -1)
	`, pgx.QueryExecModeSimpleProtocol, hashes)
	if err != nil {
		return nil, fmt.Errorf("query tx-receipt token events: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]receiptTokenEvent, len(hashes))
	for rows.Next() {
		var hash string
		var ev receiptTokenEvent
		var ts time.Time
		var opType *int32
		if err := rows.Scan(&hash, &ev.LedgerSequence, &ts, &ev.SourceType, &opType, &ev.EventIndex, &ev.From, &ev.To,
			&ev.ContractID, &ev.AssetCode, &ev.AssetIssuer, &ev.Amount); err != nil {
			return nil, err
		}
		ev.TxHash = hash
		ev.ClosedAt = ts.UTC().Format(time.RFC3339)
		ev.OperationType = opType
		switch {
		case ev.From == nil && ev.To != nil:
			ev.EventType = "mint"
		case ev.To == nil && ev.From != nil:
			ev.EventType = "burn"
		default:
			ev.EventType = "transfer"
		}
		ev.EventID = fmt.Sprintf("%s:%d:%d", hash, ev.LedgerSequence, ev.EventIndex)
		out[hash] = append(out[hash], ev)
	}
	return out, rows.Err()
}

func (p *TxReceiptsProjector) loadSemantic(ctx context.Context, hashes []string) (map[string]txview.TxSummary, error) {
	// semantic_activities remains a fallback-only source of summary text during
	// resets. Canonical receipt semantics are now derived from shared txview
	// logic so the serving projection and /semantic stay aligned.
	rows, err := p.silverPool.Query(ctx, `
		SELECT DISTINCT ON (transaction_hash) transaction_hash, activity_type, description
		  FROM semantic_activities
		 WHERE transaction_hash = ANY($1)
		 ORDER BY transaction_hash, ledger_sequence
	`, pgx.QueryExecModeSimpleProtocol, hashes)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			log.Printf("projector=tx_receipts semantic_activities unavailable (non-fatal): %v", err)
			return map[string]txview.TxSummary{}, nil
		}
		return nil, fmt.Errorf("query tx-receipt semantic: %w", err)
	}
	defer rows.Close()
	out := make(map[string]txview.TxSummary, len(hashes))
	for rows.Next() {
		var hash, activityType string
		var desc *string
		if err := rows.Scan(&hash, &activityType, &desc); err != nil {
			return nil, err
		}
		out[hash] = txview.TxSummary{Type: activityType, Description: derefString(desc)}
	}
	return out, rows.Err()
}

func (p *TxReceiptsProjector) loadTransactionDetails(ctx context.Context, hashes []string) (map[string]txMeta, error) {
	rows, err := p.bronzePool.Query(ctx, `
		SELECT transaction_hash, fee_charged, max_fee, account_sequence,
		       soroban_resources_instructions, soroban_resources_read_bytes,
		       soroban_resources_write_bytes
		  FROM transactions_row_v2
		 WHERE transaction_hash = ANY($1)
	`, pgx.QueryExecModeSimpleProtocol, hashes)
	if err != nil {
		return nil, fmt.Errorf("query tx-receipt transaction details: %w", err)
	}
	defer rows.Close()
	out := make(map[string]txMeta, len(hashes))
	for rows.Next() {
		var hash string
		var detail txMeta
		if err := rows.Scan(&hash, &detail.Fee, &detail.MaxFee, &detail.AccountSequence,
			&detail.SorobanResourcesInstructions, &detail.SorobanResourcesReadBytes,
			&detail.SorobanResourcesWriteBytes); err != nil {
			return nil, err
		}
		out[hash] = detail
	}
	return out, rows.Err()
}

// ---- Helpers --------------------------------------------------------------

// deriveBalanceDiffs collapses effects into per-(account, asset) signed deltas.
// This is the expensive aggregation previously computed on every /diffs request;
// now done once at materialization time.
func deriveBalanceDiffs(effs []receiptEffect) []balanceDiff {
	type key struct{ account, asset string }
	acc := make(map[key]*big.Rat)
	for _, e := range effs {
		if e.AccountID == nil || e.Amount == nil {
			continue
		}
		signedAmount, ok := signedEffectAmount(e)
		if !ok {
			continue
		}
		k := key{account: *e.AccountID, asset: normalizeEffectAsset(e)}
		if acc[k] == nil {
			acc[k] = new(big.Rat)
		}
		acc[k].Add(acc[k], signedAmount)
	}
	out := make([]balanceDiff, 0, len(acc))
	for k, total := range acc {
		if total == nil || total.Sign() == 0 {
			continue
		}
		out = append(out, balanceDiff{Account: k.account, Asset: k.asset, Delta: formatRatDecimal(total)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Account != out[j].Account {
			return out[i].Account < out[j].Account
		}
		if out[i].Asset != out[j].Asset {
			return out[i].Asset < out[j].Asset
		}
		return out[i].Delta < out[j].Delta
	})
	return out
}

func signedEffectAmount(e receiptEffect) (*big.Rat, bool) {
	if e.Amount == nil || *e.Amount == "" {
		return nil, false
	}
	amt, ok := new(big.Rat).SetString(*e.Amount)
	if !ok {
		return nil, false
	}
	switch e.EffectType {
	case "account_credited", "contract_credited":
		return amt, true
	case "account_debited", "contract_debited":
		return amt.Neg(amt), true
	default:
		return nil, false
	}
}

func normalizeEffectAsset(e receiptEffect) string {
	asset := "native"
	if e.AssetCode != nil && *e.AssetCode != "" {
		asset = *e.AssetCode
		if e.AssetIssuer != nil && *e.AssetIssuer != "" {
			asset = asset + ":" + *e.AssetIssuer
		}
	}
	return asset
}

func formatRatDecimal(r *big.Rat) string {
	if r == nil {
		return "0"
	}
	// Effects amounts currently arrive as decimal strings with fixed-scale
	// formatting (e.g. 0.2900000). Preserve that representation for receipts.
	return r.FloatString(7)
}

func collectContractIDs(ops []receiptOperation, toks []receiptTokenEvent) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(s *string) {
		if s == nil || *s == "" {
			return
		}
		if _, ok := seen[*s]; ok {
			return
		}
		seen[*s] = struct{}{}
		out = append(out, *s)
	}
	for _, o := range ops {
		add(o.ContractID)
	}
	for _, t := range toks {
		add(t.ContractID)
	}
	sort.Strings(out)
	return out
}

func collectAccountIDs(ops []receiptOperation, effs []receiptEffect, toks []receiptTokenEvent, source string) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(s *string) {
		if s == nil || *s == "" {
			return
		}
		if _, ok := seen[*s]; ok {
			return
		}
		seen[*s] = struct{}{}
		out = append(out, *s)
	}
	if source != "" {
		add(&source)
	}
	for _, o := range ops {
		add(o.SourceAccount)
		add(o.Destination)
	}
	for _, e := range effs {
		add(e.AccountID)
	}
	for _, t := range toks {
		add(t.From)
		add(t.To)
	}
	sort.Strings(out)
	return out
}

func (p *TxReceiptsProjector) loadCandidates(ctx context.Context, checkpoint int64) ([]txMeta, error) {
	// Pull one extra row so we can detect whether the last ledger in the batch
	// is only partially represented. We only checkpoint through fully included
	// ledgers.
	candidates, err := p.queryCandidateBatch(ctx, `WHERE ledger_sequence > $1`, checkpoint, p.batchSize+1)
	if err != nil {
		return nil, err
	}

	selected, overflowLedger, needFullLedger := selectCompleteLedgerBatch(candidates, p.batchSize)
	if !needFullLedger {
		return selected, nil
	}

	// The first unseen ledger alone contains more transactions than the nominal
	// batch size. Process that whole ledger in one go so we make forward progress
	// without ever checkpointing a partial ledger.
	return p.queryCandidateBatch(ctx, `WHERE ledger_sequence = $1`, overflowLedger, 0)
}

func selectCompleteLedgerBatch(candidates []txMeta, batchSize int) (selected []txMeta, overflowLedger int64, needFullLedger bool) {
	if len(candidates) <= batchSize {
		return candidates, 0, false
	}

	overflowLedger = candidates[batchSize].LedgerSequence
	cut := batchSize
	for cut > 0 && candidates[cut-1].LedgerSequence == overflowLedger {
		cut--
	}
	if cut > 0 {
		return candidates[:cut], overflowLedger, false
	}
	return nil, overflowLedger, true
}

func (p *TxReceiptsProjector) queryCandidateBatch(ctx context.Context, whereClause string, arg any, limit int) ([]txMeta, error) {
	query := fmt.Sprintf(`
		SELECT
			transaction_hash,
			MIN(ledger_sequence) AS ledger_sequence,
			COALESCE(MIN(ledger_closed_at), MIN(created_at), now()) AS created_at,
			MIN(source_account) FILTER (WHERE source_account IS NOT NULL AND source_account <> '') AS source_account,
			BOOL_OR(COALESCE(tx_successful, false)) AS successful
		FROM enriched_history_operations
		%s
		GROUP BY transaction_hash
		ORDER BY MIN(ledger_sequence) ASC, transaction_hash ASC
	`, whereClause)
	args := []any{arg}
	if limit > 0 {
		query += ` LIMIT $2`
		args = append(args, limit)
	}

	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, pgx.QueryExecModeSimpleProtocol)
	queryArgs = append(queryArgs, args...)

	rows, err := p.silverPool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query tx-receipt candidates: %w", err)
	}
	defer rows.Close()

	var candidates []txMeta
	for rows.Next() {
		var m txMeta
		var src *string
		if err := rows.Scan(&m.TxHash, &m.LedgerSequence, &m.CreatedAt, &src, &m.Successful); err != nil {
			return nil, fmt.Errorf("scan tx-receipt candidate: %w", err)
		}
		if src != nil {
			m.SourceAccount = *src
		}
		candidates = append(candidates, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tx-receipt candidates: %w", err)
	}
	return candidates, nil
}

func buildReceiptSummary(ops []receiptOperation, toks []receiptTokenEvent) txview.TxSummary {
	summary := generateProjectorTxSummary(receiptOpsToProjectorOps(ops), receiptEventsToProjectorEvents(toks))
	return txview.TxSummary{
		Description:       summary.Description,
		Type:              summary.Type,
		InvolvedContracts: summary.InvolvedContracts,
		Transfer:          convertTransferDetail(summary.Transfer),
		Mint:              convertTransferDetail(summary.Mint),
		Burn:              convertTransferDetail(summary.Burn),
		Swap:              convertSwapDetail(summary.Swap),
	}
}

func buildReceiptSemanticPayload(meta txMeta, ops []receiptOperation, toks []receiptTokenEvent, summary txview.TxSummary) txview.SemanticTransactionResponse {
	closedAt := meta.CreatedAt.UTC().Format(time.RFC3339)
	return txview.BuildSemanticResponse(txview.TransactionInfo{
		TxHash:                       meta.TxHash,
		LedgerSequence:               meta.LedgerSequence,
		ClosedAt:                     closedAt,
		Successful:                   meta.Successful,
		Fee:                          meta.Fee,
		OperationCount:               len(ops),
		SourceAccount:                stringPtrIfNotEmpty(meta.SourceAccount),
		AccountSequence:              meta.AccountSequence,
		MaxFee:                       meta.MaxFee,
		SorobanResourcesInstructions: meta.SorobanResourcesInstructions,
		SorobanResourcesReadBytes:    meta.SorobanResourcesReadBytes,
		SorobanResourcesWriteBytes:   meta.SorobanResourcesWriteBytes,
	}, receiptOpsToTxViewOps(ops), receiptEventsToTxViewEvents(toks), summary)
}

func receiptOpsToProjectorOps(ops []receiptOperation) []projectorDecodedOperation {
	out := make([]projectorDecodedOperation, 0, len(ops))
	for _, op := range ops {
		out = append(out, projectorDecodedOperation{
			Index:         op.Index,
			Type:          int32(op.Type),
			TypeName:      op.TypeName,
			SourceAccount: derefString(op.SourceAccount),
			ContractID:    op.ContractID,
			FunctionName:  op.FunctionName,
			Destination:   op.Destination,
			AssetCode:     op.AssetCode,
			Amount:        op.Amount,
			IsSorobanOp:   op.IsSoroban,
		})
	}
	return out
}

func receiptEventsToProjectorEvents(events []receiptTokenEvent) []projectorUnifiedEvent {
	out := make([]projectorUnifiedEvent, 0, len(events))
	for _, ev := range events {
		out = append(out, projectorUnifiedEvent{
			ContractID: ev.ContractID,
			EventType:  ev.EventType,
			From:       ev.From,
			To:         ev.To,
			Amount:     ev.Amount,
			AssetCode:  ev.AssetCode,
		})
	}
	return out
}

func receiptOpsToTxViewOps(ops []receiptOperation) []txview.Operation {
	out := make([]txview.Operation, 0, len(ops))
	for _, op := range ops {
		out = append(out, txview.Operation{
			Index:         op.Index,
			Type:          int32(op.Type),
			TypeName:      op.TypeName,
			SourceAccount: derefString(op.SourceAccount),
			ContractID:    op.ContractID,
			FunctionName:  op.FunctionName,
			Destination:   op.Destination,
			AssetCode:     op.AssetCode,
			Amount:        op.Amount,
			IsSorobanOp:   op.IsSoroban,
		})
	}
	return out
}

func receiptEventsToTxViewEvents(events []receiptTokenEvent) []txview.Event {
	out := make([]txview.Event, 0, len(events))
	for _, ev := range events {
		out = append(out, txview.Event{
			EventID:        ev.EventID,
			ContractID:     ev.ContractID,
			LedgerSequence: ev.LedgerSequence,
			TxHash:         ev.TxHash,
			ClosedAt:       ev.ClosedAt,
			EventType:      ev.EventType,
			From:           ev.From,
			To:             ev.To,
			Amount:         ev.Amount,
			AssetCode:      ev.AssetCode,
			AssetIssuer:    ev.AssetIssuer,
			SourceType:     ev.SourceType,
			OperationType:  ev.OperationType,
			EventIndex:     ev.EventIndex,
		})
	}
	return out
}

func convertTransferDetail(in *TransferDetail) *txview.TransferDetail {
	if in == nil {
		return nil
	}
	return &txview.TransferDetail{Asset: in.Asset, Amount: in.Amount, From: in.From, To: in.To}
}

func convertSwapDetail(in *SwapDetail) *txview.SwapDetail {
	if in == nil {
		return nil
	}
	return &txview.SwapDetail{
		SoldAsset:    in.SoldAsset,
		SoldAmount:   in.SoldAmount,
		BoughtAsset:  in.BoughtAsset,
		BoughtAmount: in.BoughtAmount,
		Router:       in.Router,
		Trader:       in.Trader,
	}
}

func stringPtrIfNotEmpty(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func firstOrNil(s []string) *string {
	if len(s) == 0 {
		return nil
	}
	return &s[0]
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
