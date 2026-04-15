package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionsRecentProjector struct {
	network         string
	batchSize       int
	sourcePool      *pgxpool.Pool
	silverPool      *pgxpool.Pool
	targetPool      *pgxpool.Pool
	checkpoints     *CheckpointStore
	summaryEnricher *summaryEnricher
}

type bronzeTransactionRow struct {
	LedgerSequence               int64
	TransactionHash              string
	SourceAccount                *string
	FeeCharged                   *int64
	MaxFee                       *int64
	Successful                   bool
	OperationCount               *int32
	MemoType                     *string
	Memo                         *string
	CreatedAt                    *time.Time
	AccountSequence              *int64
	SorobanResourcesInstructions *int64
	SorobanResourcesReadBytes    *int64
	SorobanResourcesWriteBytes   *int64
	SorobanContractID            *string
}

func NewTransactionsRecentProjector(network string, batchSize int, sourcePool, silverPool, targetPool *pgxpool.Pool, checkpoints *CheckpointStore) *TransactionsRecentProjector {
	return &TransactionsRecentProjector{
		network:         network,
		batchSize:       batchSize,
		sourcePool:      sourcePool,
		silverPool:      silverPool,
		targetPool:      targetPool,
		checkpoints:     checkpoints,
		summaryEnricher: newSummaryEnricher(silverPool),
	}
}

func (p *TransactionsRecentProjector) Name() string { return "transactions_recent" }

func (p *TransactionsRecentProjector) RunOnce(ctx context.Context) (RunStats, error) {
	checkpoint, err := p.checkpoints.Load(ctx, p.Name(), p.network)
	if err != nil {
		return RunStats{}, err
	}

	rows, err := p.sourcePool.Query(ctx, `
		SELECT
			ledger_sequence,
			transaction_hash,
			source_account,
			fee_charged,
			max_fee,
			successful,
			operation_count,
			memo_type,
			memo,
			created_at,
			account_sequence,
			soroban_resources_instructions,
			soroban_resources_read_bytes,
			soroban_resources_write_bytes,
			soroban_contract_id
		FROM transactions_row_v2
		WHERE ledger_sequence > $1
		ORDER BY ledger_sequence ASC, transaction_hash ASC
		LIMIT $2
	`, checkpoint, p.batchSize)
	if err != nil {
		return RunStats{}, fmt.Errorf("query bronze transactions: %w", err)
	}
	defer rows.Close()

	var batch []bronzeTransactionRow
	for rows.Next() {
		var r bronzeTransactionRow
		if err := rows.Scan(
			&r.LedgerSequence,
			&r.TransactionHash,
			&r.SourceAccount,
			&r.FeeCharged,
			&r.MaxFee,
			&r.Successful,
			&r.OperationCount,
			&r.MemoType,
			&r.Memo,
			&r.CreatedAt,
			&r.AccountSequence,
			&r.SorobanResourcesInstructions,
			&r.SorobanResourcesReadBytes,
			&r.SorobanResourcesWriteBytes,
			&r.SorobanContractID,
		); err != nil {
			return RunStats{}, fmt.Errorf("scan bronze transaction: %w", err)
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return RunStats{}, fmt.Errorf("iterate bronze transactions: %w", err)
	}
	if len(batch) == 0 {
		return RunStats{Checkpoint: checkpoint}, nil
	}

	summaries, err := p.summaryEnricher.BuildSummaries(ctx, batch)
	if err != nil {
		return RunStats{}, fmt.Errorf("build transaction summaries: %w", err)
	}

	tx, err := p.targetPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RunStats{}, fmt.Errorf("begin target tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var lastCreatedAt *time.Time
	var maxLedger int64
	for _, r := range batch {
		isSoroban := r.SorobanResourcesInstructions != nil || r.SorobanContractID != nil
		summaryObj, ok := summaries[r.TransactionHash]
		if !ok {
			summaryObj = fallbackSummaryFromBronze(r)
		}
		txType := summaryObj.Type
		if txType == "" {
			txType = classifyTransactionType(isSoroban, r.OperationCount)
		}
		summary := summaryObj.Description
		if summary == "" {
			summary = buildTransactionSummary(r)
		}
		summaryObj.Type = txType
		summaryObj.Description = summary
		if summaryObj.InvolvedContracts == nil {
			summaryObj.InvolvedContracts = []string{}
		}
		summaryJSON, err := json.Marshal(summaryObj)
		if err != nil {
			return RunStats{}, fmt.Errorf("marshal transaction summary %s: %w", r.TransactionHash, err)
		}

		primaryContractID := r.SorobanContractID
		if len(summaryObj.InvolvedContracts) > 0 {
			primaryContractID = &summaryObj.InvolvedContracts[0]
		}
		primaryAssetKey, primaryAmount := derivePrimaryAssetAndAmount(summaryObj)

		_, err = tx.Exec(ctx, `
			INSERT INTO serving.sv_transactions_recent (
				tx_hash,
				ledger_sequence,
				created_at,
				source_account,
				fee_charged_stroops,
				max_fee_stroops,
				successful,
				operation_count,
				tx_type,
				summary_text,
				summary_json,
				primary_contract_id,
				primary_asset_key,
				primary_amount_stroops,
				memo_type,
				memo_value,
				account_sequence,
				is_soroban,
				cpu_insns,
				read_bytes,
				write_bytes
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
			)
			ON CONFLICT (tx_hash) DO UPDATE SET
				ledger_sequence = EXCLUDED.ledger_sequence,
				created_at = EXCLUDED.created_at,
				source_account = EXCLUDED.source_account,
				fee_charged_stroops = EXCLUDED.fee_charged_stroops,
				max_fee_stroops = EXCLUDED.max_fee_stroops,
				successful = EXCLUDED.successful,
				operation_count = EXCLUDED.operation_count,
				tx_type = EXCLUDED.tx_type,
				summary_text = EXCLUDED.summary_text,
				summary_json = EXCLUDED.summary_json,
				primary_contract_id = EXCLUDED.primary_contract_id,
				primary_asset_key = EXCLUDED.primary_asset_key,
				primary_amount_stroops = EXCLUDED.primary_amount_stroops,
				memo_type = EXCLUDED.memo_type,
				memo_value = EXCLUDED.memo_value,
				account_sequence = EXCLUDED.account_sequence,
				is_soroban = EXCLUDED.is_soroban,
				cpu_insns = EXCLUDED.cpu_insns,
				read_bytes = EXCLUDED.read_bytes,
				write_bytes = EXCLUDED.write_bytes
		`,
			r.TransactionHash,
			r.LedgerSequence,
			r.CreatedAt,
			r.SourceAccount,
			r.FeeCharged,
			r.MaxFee,
			r.Successful,
			nullableInt32Ptr(r.OperationCount),
			txType,
			summary,
			string(summaryJSON),
			primaryContractID,
			primaryAssetKey,
			primaryAmount,
			r.MemoType,
			r.Memo,
			r.AccountSequence,
			isSoroban,
			r.SorobanResourcesInstructions,
			r.SorobanResourcesReadBytes,
			r.SorobanResourcesWriteBytes,
		)
		if err != nil {
			return RunStats{}, fmt.Errorf("upsert serving transaction %s: %w", r.TransactionHash, err)
		}

		if r.CreatedAt != nil {
			lastCreatedAt = r.CreatedAt
		}
		if r.LedgerSequence > maxLedger {
			maxLedger = r.LedgerSequence
		}
	}

	if err := p.checkpoints.Save(ctx, tx, p.Name(), p.network, maxLedger, lastCreatedAt); err != nil {
		return RunStats{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit target tx: %w", err)
	}

	log.Printf("projector=%s network=%s applied=%d checkpoint=%d", p.Name(), p.network, len(batch), maxLedger)
	return RunStats{RowsApplied: int64(len(batch)), Checkpoint: maxLedger}, nil
}

func classifyTransactionType(isSoroban bool, operationCount *int32) string {
	if isSoroban {
		return "contract_call"
	}
	if operationCount != nil && *operationCount > 1 {
		return "multi_op"
	}
	return "classic"
}

func buildTransactionSummary(r bronzeTransactionRow) string {
	if r.SorobanContractID != nil && *r.SorobanContractID != "" {
		return fmt.Sprintf("Invoked contract %s", *r.SorobanContractID)
	}
	if r.SourceAccount != nil && *r.SourceAccount != "" {
		return fmt.Sprintf("Transaction from %s", *r.SourceAccount)
	}
	return "Transaction"
}

func derivePrimaryAssetAndAmount(summary TxSummary) (*string, *int64) {
	pick := func(asset, amount string) (*string, *int64) {
		var assetPtr *string
		if asset != "" {
			assetCopy := asset
			assetPtr = &assetCopy
		}
		if amount == "" {
			return assetPtr, nil
		}
		amt, ok := new(big.Int).SetString(amount, 10)
		if !ok || !amt.IsInt64() {
			return assetPtr, nil
		}
		v := amt.Int64()
		return assetPtr, &v
	}
	if summary.Transfer != nil {
		return pick(summary.Transfer.Asset, summary.Transfer.Amount)
	}
	if summary.Mint != nil {
		return pick(summary.Mint.Asset, summary.Mint.Amount)
	}
	if summary.Burn != nil {
		return pick(summary.Burn.Asset, summary.Burn.Amount)
	}
	if summary.Swap != nil {
		return pick(summary.Swap.SoldAsset, summary.Swap.SoldAmount)
	}
	return nil, nil
}

func nullableInt32Ptr(v *int32) any {
	if v == nil {
		return nil
	}
	return int(*v)
}
