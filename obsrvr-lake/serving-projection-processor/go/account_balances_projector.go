package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/amount"
)

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
	if len(changed) == 0 {
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

	now := time.Now().UTC()
	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, &now); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s accounts=%d applied=%d deleted=%d checkpoint=%d", p.Name(), p.network, len(affected), rowsApplied, int64(0), maxLedger)
	return RunStats{RowsApplied: rowsApplied, RowsDeleted: 0, Checkpoint: maxLedger}, nil
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
