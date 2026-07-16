package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/amount"
)

// Silver effect type codes (Horizon-aligned) used to reconcile removals.
const (
	effectTypeAccountRemoved   = 1
	effectTypeTrustlineRemoved = 21
)

// balanceRemovalEffectOverlapLedgers re-scans a small window behind the
// checkpoint when loading removal effects. The transformer commits effects and
// trustlines_current in parallel per-table transactions, so an effect for a
// ledger at or below the checkpoint can become visible after the checkpoint
// has already advanced. Removal deletes are idempotent and guarded by
// last_modified_ledger, so re-scanning the overlap is harmless.
const balanceRemovalEffectOverlapLedgers = 100

type AccountBalancesProjector struct {
	network     string
	batchSize   int
	sourcePool  *pgxpool.Pool
	targetPool  *pgxpool.Pool
	checkpoints *CheckpointStore
}

type changedAccount struct {
	AccountID        string
	Ledger           int64
	AccountChanged   bool
	TrustlineChanged bool
}

type trustlineCurrentRow struct {
	AccountID          string
	AssetType          string
	AssetIssuer        *string
	AssetCode          *string
	LiquidityPoolID    *string
	Balance            *int64
	TrustLineLimit     *int64
	BuyingLiabilities  *int64
	SellingLiabilities *int64
	Flags              *int32
	Sponsor            *string
	LastModifiedLedger int64
	UpdatedAt          *time.Time
}

func NewAccountBalancesProjector(network string, batchSize int, sourcePool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *AccountBalancesProjector {
	return &AccountBalancesProjector{network: network, batchSize: batchSize, sourcePool: sourcePool, targetPool: targetPool, checkpoints: checkpoints}
}

func (p *AccountBalancesProjector) Name() string { return "account_balances" }

func (p *AccountBalancesProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	changed, err := p.loadChangedAccounts(ctx, checkpoint)
	if err != nil {
		return RunStats{}, err
	}
	removals, err := p.loadBalanceRemovals(ctx, checkpoint)
	if err != nil {
		return RunStats{}, err
	}
	if len(changed) == 0 && len(removals) == 0 {
		return RunStats{Checkpoint: checkpoint}, nil
	}

	affected := make([]string, 0, len(changed))
	maxLedger := checkpoint
	for _, c := range changed {
		affected = append(affected, c.AccountID)
		if c.Ledger > maxLedger {
			maxLedger = c.Ledger
		}
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin target tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var rowsApplied int64
	for _, c := range changed {
		projectXLM, projectTrustlines := balanceProjectionSteps(c)
		if projectXLM {
			inserted, err := p.projectAccountXLM(ctx, tx, c.AccountID)
			if err != nil {
				return RunStats{}, err
			}
			rowsApplied += inserted
		}

		if projectTrustlines {
			inserted, err := p.projectAccountTrustlines(ctx, tx, c.AccountID)
			if err != nil {
				return RunStats{}, err
			}
			rowsApplied += inserted
		}
	}

	rowsDeleted, err := p.applyBalanceRemovals(ctx, tx, removals)
	if err != nil {
		return RunStats{}, err
	}

	now := time.Now().UTC()
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, &now); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s accounts=%d applied=%d deleted=%d checkpoint=%d", p.Name(), p.network, len(affected), rowsApplied, rowsDeleted, maxLedger)
	return RunStats{RowsApplied: rowsApplied, RowsDeleted: rowsDeleted, Checkpoint: maxLedger}, nil
}

// balanceRemoval identifies serving balance rows that the chain removed. An
// empty AssetKey means the whole account was removed (account merge).
type balanceRemoval struct {
	AccountID      string
	AssetKey       string
	LedgerSequence int64
}

// loadBalanceRemovals reads trustline_removed / account_removed effects past
// the checkpoint (minus a small overlap). These effects are the only provable
// removal signal: hot trustlines_current is a pruned window, not complete
// current state, so a serving row being absent from the source must never
// justify a delete — that would wipe balances for trustlines that simply
// haven't changed recently (see
// TestBalanceProjectionStepsPreserveTrustlinesOnAccountOnlyChange). The
// remaining gap: removals older than the hot effects retention window (e.g.
// across extended projector downtime) are missed.
func (p *AccountBalancesProjector) loadBalanceRemovals(ctx context.Context, checkpoint int64) ([]balanceRemoval, error) {
	scanFrom := checkpoint - balanceRemovalEffectOverlapLedgers
	if scanFrom < 0 {
		scanFrom = 0
	}
	rows, err := p.sourcePool.Query(ctx, `
		SELECT account_id, effect_type, asset_type, asset_code, asset_issuer, details_json, ledger_sequence
		FROM effects
		WHERE effect_type = ANY($1)
		  AND ledger_sequence > $2
		  AND account_id IS NOT NULL
		  AND account_id <> ''
		ORDER BY ledger_sequence ASC
	`, []int32{effectTypeAccountRemoved, effectTypeTrustlineRemoved}, scanFrom)
	if err != nil {
		return nil, fmt.Errorf("query balance removal effects: %w", err)
	}
	defer rows.Close()

	var removals []balanceRemoval
	for rows.Next() {
		var accountID string
		var effectType int32
		var assetType, assetCode, assetIssuer, detailsJSON *string
		var ledgerSequence int64
		if err := rows.Scan(&accountID, &effectType, &assetType, &assetCode, &assetIssuer, &detailsJSON, &ledgerSequence); err != nil {
			return nil, fmt.Errorf("scan balance removal effect: %w", err)
		}
		removal := balanceRemoval{AccountID: accountID, LedgerSequence: ledgerSequence}
		if effectType == effectTypeTrustlineRemoved {
			key, err := balanceRemovalAssetKey(assetType, assetCode, assetIssuer, detailsJSON)
			if err != nil {
				// An unresolvable asset must not degrade into "delete nothing"
				// (stale balance) or "delete everything" (data loss).
				return nil, fmt.Errorf("trustline_removed effect for %s at ledger %d: %w", accountID, ledgerSequence, err)
			}
			removal.AssetKey = key
		}
		removals = append(removals, removal)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate balance removal effects: %w", err)
	}
	return removals, nil
}

// balanceRemovalAssetKey maps a trustline_removed effect onto the serving
// asset_key. Pool-share trustlines carry only liquidity_pool_id (in
// details_json); classic trustlines carry the asset columns.
func balanceRemovalAssetKey(assetType, assetCode, assetIssuer, detailsJSON *string) (string, error) {
	if detailsJSON != nil && *detailsJSON != "" {
		var details struct {
			LiquidityPoolID string `json:"liquidity_pool_id"`
		}
		if err := json.Unmarshal([]byte(*detailsJSON), &details); err == nil && details.LiquidityPoolID != "" {
			return "POOL:" + details.LiquidityPoolID, nil
		}
	}
	if coalesceString(assetCode) == "" && coalesceString(assetIssuer) == "" {
		return "", fmt.Errorf("no asset or liquidity pool identity on effect")
	}
	return assetKey(coalesceString(assetType), assetCode, assetIssuer, nil), nil
}

// applyBalanceRemovals deletes the serving rows for removed trustlines and
// merged accounts. The last_modified_ledger guard makes deletes safe against
// re-creation inside the same batch and against overlap re-scans: a trustline
// re-established after the removal carries a newer last_modified_ledger and is
// left alone.
func (p *AccountBalancesProjector) applyBalanceRemovals(ctx context.Context, tx pgx.Tx, removals []balanceRemoval) (int64, error) {
	var deleted int64
	for _, r := range removals {
		query := `
			DELETE FROM serving.sv_account_balances_current
			WHERE account_id = $1 AND asset_key = $2 AND last_modified_ledger <= $3`
		args := []interface{}{r.AccountID, r.AssetKey, r.LedgerSequence}
		if r.AssetKey == "" {
			// Account removed: every balance row (including native) goes.
			query = `
				DELETE FROM serving.sv_account_balances_current
				WHERE account_id = $1 AND last_modified_ledger <= $2`
			args = []interface{}{r.AccountID, r.LedgerSequence}
		}
		cmdTag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return deleted, fmt.Errorf("delete removed balance %s/%s: %w", r.AccountID, r.AssetKey, err)
		}
		deleted += cmdTag.RowsAffected()
	}
	return deleted, nil
}

func (p *AccountBalancesProjector) loadChangedAccounts(ctx context.Context, checkpoint int64) ([]changedAccount, error) {
	rows, err := p.sourcePool.Query(ctx, `
		WITH changed AS (
			SELECT account_id, last_modified_ledger, true AS account_changed, false AS trustline_changed
			FROM accounts_current
			WHERE last_modified_ledger > $1
			UNION ALL
			SELECT account_id, last_modified_ledger, false AS account_changed, true AS trustline_changed
			FROM trustlines_current
			WHERE last_modified_ledger > $1
		)
		SELECT account_id,
		       MAX(last_modified_ledger) as max_ledger,
		       BOOL_OR(account_changed) as account_changed,
		       BOOL_OR(trustline_changed) as trustline_changed
		FROM changed
		GROUP BY account_id
		ORDER BY max_ledger ASC, account_id ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return nil, fmt.Errorf("query changed accounts: %w", err)
	}
	defer rows.Close()

	var changed []changedAccount
	for rows.Next() {
		var c changedAccount
		if err := rows.Scan(&c.AccountID, &c.Ledger, &c.AccountChanged, &c.TrustlineChanged); err != nil {
			return nil, fmt.Errorf("scan changed account: %w", err)
		}
		changed = append(changed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changed accounts: %w", err)
	}
	return changed, nil
}

func balanceProjectionSteps(c changedAccount) (projectXLM bool, projectTrustlines bool) {
	return c.AccountChanged, c.TrustlineChanged
}

func (p *AccountBalancesProjector) projectAccountXLM(ctx context.Context, tx pgx.Tx, accountID string) (int64, error) {
	var balanceText *string
	var lastModified int64
	var updatedAt *time.Time
	err := p.sourcePool.QueryRow(ctx, `
		SELECT balance, last_modified_ledger, updated_at
		FROM accounts_current
		WHERE account_id = $1
	`, accountID).Scan(&balanceText, &lastModified, &updatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("load XLM balance for %s: %w", accountID, err)
	}

	balance := parseBalanceText(balanceText)
	cmdTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_account_balances_current (
			account_id,
			asset_key,
			asset_code,
			asset_issuer,
			asset_type,
			balance_stroops,
			balance_display,
			last_modified_ledger,
			updated_at
		) VALUES ($1,'XLM','XLM',NULL,'native',$2,$3,$4,$5)
		ON CONFLICT (account_id, asset_key) DO UPDATE SET
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			asset_type = EXCLUDED.asset_type,
			balance_stroops = EXCLUDED.balance_stroops,
			balance_display = EXCLUDED.balance_display,
			limit_stroops = EXCLUDED.limit_stroops,
			buying_liabilities_stroops = EXCLUDED.buying_liabilities_stroops,
			selling_liabilities_stroops = EXCLUDED.selling_liabilities_stroops,
			is_authorized = EXCLUDED.is_authorized,
			is_authorized_to_maintain_liabilities = EXCLUDED.is_authorized_to_maintain_liabilities,
			is_clawback_enabled = EXCLUDED.is_clawback_enabled,
			sponsor = EXCLUDED.sponsor,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			updated_at = EXCLUDED.updated_at
	`, accountID, balance, stroopsToDisplay(balance), lastModified, updatedAt)
	if err != nil {
		return 0, fmt.Errorf("insert XLM balance for %s: %w", accountID, err)
	}
	return cmdTag.RowsAffected(), nil
}

func (p *AccountBalancesProjector) projectAccountTrustlines(ctx context.Context, tx pgx.Tx, accountID string) (int64, error) {
	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			account_id,
			asset_type,
			asset_issuer,
			asset_code,
			liquidity_pool_id,
			balance,
			trust_line_limit,
			buying_liabilities,
			selling_liabilities,
			flags,
			sponsor,
			last_modified_ledger,
			updated_at
		FROM trustlines_current
		WHERE account_id = $1
	`, accountID)
	if err != nil {
		return 0, fmt.Errorf("query trustlines for %s: %w", accountID, err)
	}
	defer rows.Close()

	var inserted int64
	for rows.Next() {
		var r trustlineCurrentRow
		if err := rows.Scan(
			&r.AccountID,
			&r.AssetType,
			&r.AssetIssuer,
			&r.AssetCode,
			&r.LiquidityPoolID,
			&r.Balance,
			&r.TrustLineLimit,
			&r.BuyingLiabilities,
			&r.SellingLiabilities,
			&r.Flags,
			&r.Sponsor,
			&r.LastModifiedLedger,
			&r.UpdatedAt,
		); err != nil {
			return 0, fmt.Errorf("scan trustline for %s: %w", accountID, err)
		}

		assetKey := assetKey(r.AssetType, r.AssetCode, r.AssetIssuer, r.LiquidityPoolID)
		cmdTag, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_account_balances_current (
				account_id,
				asset_key,
				asset_code,
				asset_issuer,
				asset_type,
				balance_stroops,
				balance_display,
				limit_stroops,
				is_authorized,
				buying_liabilities_stroops,
				selling_liabilities_stroops,
				is_authorized_to_maintain_liabilities,
				is_clawback_enabled,
				sponsor,
				last_modified_ledger,
				updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
			ON CONFLICT (account_id, asset_key) DO UPDATE SET
				asset_code = EXCLUDED.asset_code,
				asset_issuer = EXCLUDED.asset_issuer,
				asset_type = EXCLUDED.asset_type,
				balance_stroops = EXCLUDED.balance_stroops,
				balance_display = EXCLUDED.balance_display,
				limit_stroops = EXCLUDED.limit_stroops,
				is_authorized = EXCLUDED.is_authorized,
				buying_liabilities_stroops = EXCLUDED.buying_liabilities_stroops,
				selling_liabilities_stroops = EXCLUDED.selling_liabilities_stroops,
				is_authorized_to_maintain_liabilities = EXCLUDED.is_authorized_to_maintain_liabilities,
				is_clawback_enabled = EXCLUDED.is_clawback_enabled,
				sponsor = EXCLUDED.sponsor,
				last_modified_ledger = EXCLUDED.last_modified_ledger,
				updated_at = EXCLUDED.updated_at
		`,
			r.AccountID,
			assetKey,
			coalesceString(r.AssetCode),
			r.AssetIssuer,
			r.AssetType,
			r.Balance,
			stroopsToDisplay(r.Balance),
			r.TrustLineLimit,
			authorizedFromFlags(r.Flags),
			r.BuyingLiabilities,
			r.SellingLiabilities,
			authorizedToMaintainFromFlags(r.Flags),
			clawbackFromFlags(r.Flags),
			r.Sponsor,
			r.LastModifiedLedger,
			r.UpdatedAt,
		)
		if err != nil {
			return 0, fmt.Errorf("insert trustline balance for %s: %w", accountID, err)
		}
		inserted += cmdTag.RowsAffected()
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return inserted, nil
}

func assetKey(assetType string, assetCode, assetIssuer, liquidityPoolID *string) string {
	if assetType == "native" {
		return "XLM"
	}
	if liquidityPoolID != nil && *liquidityPoolID != "" {
		return "POOL:" + *liquidityPoolID
	}
	parts := []string{coalesceString(assetCode), coalesceString(assetIssuer)}
	parts = trimRightEmpty(parts)
	return strings.Join(parts, ":")
}

func trimRightEmpty(parts []string) []string {
	idx := len(parts)
	for idx > 0 && parts[idx-1] == "" {
		idx--
	}
	return parts[:idx]
}

func coalesceString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func authorizedFromFlags(flags *int32) *bool {
	if flags == nil {
		return nil
	}
	v := ((*flags) & 1) != 0
	return &v
}

func authorizedToMaintainFromFlags(flags *int32) *bool {
	if flags == nil {
		return nil
	}
	v := ((*flags) & 2) != 0
	return &v
}

func clawbackFromFlags(flags *int32) *bool {
	if flags == nil {
		return nil
	}
	v := ((*flags) & 4) != 0
	return &v
}

func stroopsToDisplay(v *int64) *string {
	if v == nil {
		return nil
	}
	s := amount.StringFromInt64(*v)
	return &s
}
