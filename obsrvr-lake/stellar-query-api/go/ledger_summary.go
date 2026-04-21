package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/stellar/go/strkey"
)

type LedgerSummaryHandler struct {
	queryService *QueryService
	unified      *UnifiedDuckDBReader
	hot          *SilverHotReader
}

func NewLedgerSummaryHandler(queryService *QueryService, unified *UnifiedDuckDBReader, hot *SilverHotReader) *LedgerSummaryHandler {
	return &LedgerSummaryHandler{
		queryService: queryService,
		unified:      unified,
		hot:          hot,
	}
}

type LedgerSummaryResponse struct {
	Ledger                     LedgerSummaryLedger              `json:"ledger"`
	Totals                     LedgerSummaryTotals              `json:"totals"`
	ClassificationCounts       LedgerSummaryClassifications     `json:"classification_counts"`
	SorobanUtilization         *LedgerSummarySorobanUtilization `json:"soroban_utilization,omitempty"`
	Sampling                   *LedgerSummarySampling           `json:"sampling,omitempty"`
	RepresentativeTransactions []LedgerRepresentativeTx         `json:"representative_transactions,omitempty"`
	Composition                *LedgerSummaryComposition        `json:"composition,omitempty"`
	Provenance                 LedgerSummaryProvenance          `json:"provenance"`
}

type LedgerSummaryLedger struct {
	Sequence          int64   `json:"sequence"`
	ClosedAt          string  `json:"closed_at"`
	CloseTimeSeconds  float64 `json:"close_time_seconds,omitempty"`
	ClosedByNodeID    string  `json:"closed_by_node_id,omitempty"`
	ClosedByValidator string  `json:"closed_by_validator,omitempty"`
	ProtocolVersion   int     `json:"protocol_version,omitempty"`
	Hash              string  `json:"hash,omitempty"`
	PreviousHash      string  `json:"previous_hash,omitempty"`
}

type LedgerSummaryTotals struct {
	TransactionCount   int64 `json:"transaction_count"`
	SuccessfulTxCount  int64 `json:"successful_tx_count,omitempty"`
	FailedTxCount      int64 `json:"failed_tx_count,omitempty"`
	OperationCount     int64 `json:"operation_count,omitempty"`
	ContractEventCount int64 `json:"contract_event_count,omitempty"`
	SorobanOpCount     int64 `json:"soroban_op_count,omitempty"`
	TotalFeeCharged    int64 `json:"total_fee_charged,omitempty"`
}

type LedgerSummaryClassifications struct {
	SwapTxCount         int64 `json:"swap_tx_count,omitempty"`
	ContractCallTxCount int64 `json:"contract_call_tx_count,omitempty"`
	ClassicTxCount      int64 `json:"classic_tx_count,omitempty"`
	PaymentTxCount      int64 `json:"payment_tx_count,omitempty"`
	WalletTxCount       int64 `json:"wallet_tx_count,omitempty"`
	DeploymentTxCount   int64 `json:"deployment_tx_count,omitempty"`
	SorobanTxCount      int64 `json:"soroban_tx_count,omitempty"`
}

type LedgerSummarySorobanUtilization struct {
	InstructionsUsed    int64   `json:"instructions_used,omitempty"`
	InstructionsLimit   int64   `json:"instructions_limit,omitempty"`
	InstructionsPct     float64 `json:"instructions_pct,omitempty"`
	ReadBytesUsed       int64   `json:"read_bytes_used,omitempty"`
	WriteBytesUsed      int64   `json:"write_bytes_used,omitempty"`
	ReadWriteBytesUsed  int64   `json:"read_write_bytes_used,omitempty"`
	ReadWriteBytesLimit int64   `json:"read_write_bytes_limit,omitempty"`
	ReadWritePct        float64 `json:"read_write_pct,omitempty"`
	RentBurned          float64 `json:"rent_burned,omitempty"`
}

type LedgerSummarySampling struct {
	Strategy                    string `json:"strategy"`
	SampleCount                 int    `json:"sample_count"`
	RepresentedTransactionCount int64  `json:"represented_transaction_count"`
	TotalTransactionCount       int64  `json:"total_transaction_count"`
}

type LedgerRepresentativeTx struct {
	TxHash         string                             `json:"tx_hash"`
	Category       string                             `json:"category"`
	CategoryLabel  string                             `json:"category_label,omitempty"`
	CoverageCount  int64                              `json:"coverage_count,omitempty"`
	Classification LedgerRepresentativeClassification `json:"classification"`
	Summary        LedgerRepresentativeSummary        `json:"summary"`
	Actors         *LedgerRepresentativeActors        `json:"actors,omitempty"`
}

type LedgerRepresentativeClassification struct {
	TxType     string `json:"tx_type,omitempty"`
	Subtype    string `json:"subtype,omitempty"`
	Confidence string `json:"confidence,omitempty"`
}

type LedgerRepresentativeSummary struct {
	Description        string `json:"description"`
	FunctionName       string `json:"function_name,omitempty"`
	ProtocolLabel      string `json:"protocol_label,omitempty"`
	ProtocolContractID string `json:"protocol_contract_id,omitempty"`
	SoldAmount         string `json:"sold_amount,omitempty"`
	SoldAsset          string `json:"sold_asset,omitempty"`
	BoughtAmount       string `json:"bought_amount,omitempty"`
	BoughtAsset        string `json:"bought_asset,omitempty"`
	Amount             string `json:"amount,omitempty"`
	AmountDisplay      string `json:"amount_display,omitempty"`
	Asset              string `json:"asset,omitempty"`
}

type LedgerRepresentativeActorRef struct {
	ID    string `json:"id,omitempty"`
	Label string `json:"label,omitempty"`
	Type  string `json:"type,omitempty"`
}

type LedgerRepresentativeActors struct {
	Primary   *LedgerRepresentativeActorRef `json:"primary,omitempty"`
	Secondary *LedgerRepresentativeActorRef `json:"secondary,omitempty"`
}

type LedgerSummaryComposition struct {
	DominantTxType      string  `json:"dominant_tx_type,omitempty"`
	DominantTxTypeCount int64   `json:"dominant_tx_type_count,omitempty"`
	SorobanSharePct     float64 `json:"soroban_share_pct,omitempty"`
	FailedSharePct      float64 `json:"failed_share_pct,omitempty"`
}

type LedgerSummaryProvenance struct {
	ClassificationSource string `json:"classification_source,omitempty"`
	UtilizationSource    string `json:"utilization_source,omitempty"`
	SamplingSource       string `json:"sampling_source,omitempty"`
	Partial              bool   `json:"partial"`
}

type ledgerSummaryTxAgg struct {
	TxHash              string
	LedgerSequence      int64
	ClosedAt            string
	SourceAccount       string
	FeeCharged          int64
	OpCount             int64
	Successful          bool
	HasSoroban          bool
	HasPayment          bool
	HasClassicSwap      bool
	HasWalletInvolved   bool
	HasDeployment       bool
	PrimaryContract     string
	PrimaryContractName string
	PrimaryContractType string
	FunctionName        string
	PaymentAssetCode    string
	PaymentAmount       string
	ActivityType        string
	ActivityDescription string
	DestinationAccount  string
	PrimaryCategory     string
}

type txSummaryPreview struct {
	TxType          string
	Description     string
	FunctionName    string
	PrimaryContract string
}

type ledgerSemanticActivity struct {
	TransactionHash    string
	ActivityType       string
	Description        string
	ContractID         string
	SourceAccount      string
	DestinationAccount string
	AssetCode          string
	Amount             string
	FunctionName       string
}

type ledgerContractLabel struct {
	ContractID     string
	DisplayName    string
	Category       string
	WalletType     string
	DeployedLedger int64
}

// HandleLedgerSummary returns a compact ledger-level summary.
// @Summary Get ledger summary
// @Description Returns a structured ledger summary with totals, semantic composition, Soroban utilization, representative transactions, and provenance metadata.
// @Tags Ledgers
// @Accept json
// @Produce json
// @Param seq path int true "Ledger sequence number"
// @Success 200 {object} LedgerSummaryResponse "Ledger summary"
// @Failure 400 {object} map[string]interface{} "Invalid ledger sequence"
// @Failure 404 {object} map[string]interface{} "Ledger not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/ledgers/{seq}/summary [get]
func (h *LedgerSummaryHandler) HandleLedgerSummary(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.queryService == nil {
		respondError(w, "ledger summary handler unavailable", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	seqStr := vars["seq"]
	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		respondError(w, "invalid ledger sequence", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	ledgerRow, err := h.getLedgerRow(ctx, seq)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ledgerRow == nil {
		respondError(w, fmt.Sprintf("ledger not found: %d", seq), http.StatusNotFound)
		return
	}

	resp := LedgerSummaryResponse{
		Ledger: LedgerSummaryLedger{
			Sequence: seq,
			ClosedAt: ledgerTimeString(ledgerRow["closed_at"]),
		},
		Totals: LedgerSummaryTotals{
			TransactionCount:   mapInt64(ledgerRow["transaction_count"]),
			SuccessfulTxCount:  mapInt64(ledgerRow["successful_tx_count"]),
			FailedTxCount:      mapInt64(ledgerRow["failed_tx_count"]),
			OperationCount:     mapInt64(ledgerRow["operation_count"]),
			ContractEventCount: mapInt64(ledgerRow["contract_events_count"]),
			SorobanOpCount:     mapInt64(ledgerRow["soroban_op_count"]),
			TotalFeeCharged:    mapInt64(ledgerRow["total_fee_charged"]),
		},
		Provenance: LedgerSummaryProvenance{
			ClassificationSource: "derived_from_enriched_history_operations_v1",
			UtilizationSource:    "transactions_row_v2_plus_config_settings_current_v1",
			SamplingSource:       "deterministic_primary_category_ranking_v1",
			Partial:              false,
		},
	}

	if hash, ok := ledgerRow["ledger_hash"].(string); ok {
		resp.Ledger.Hash = hash
	}
	if prev, ok := ledgerRow["previous_ledger_hash"].(string); ok {
		resp.Ledger.PreviousHash = prev
	}
	resp.Ledger.ProtocolVersion = int(mapInt64(ledgerRow["protocol_version"]))
	if closedBy, ok := ledgerRow["node_id"].(string); ok {
		resp.Ledger.ClosedByNodeID = closedBy
		resp.Ledger.ClosedByValidator = decodeValidatorAccountID(closedBy)
	}

	txAggs, err := h.getLedgerTxAggs(ctx, seq)
	if err != nil {
		resp.Provenance.Partial = true
	} else {
		activities, activityErr := h.getLedgerSemanticActivities(ctx, seq)
		if activityErr != nil {
			resp.Provenance.Partial = true
		}
		labels, labelErr := h.getContractLabels(ctx, contractIDsFromTxAggs(txAggs))
		if labelErr != nil {
			resp.Provenance.Partial = true
		}
		applySemanticEnhancement(txAggs, activities, labels)
		hydratePrimaryCategories(txAggs)
		resp.ClassificationCounts = buildClassificationCounts(txAggs)
		resp.Composition = buildComposition(resp.Totals, txAggs)
		resp.Sampling, resp.RepresentativeTransactions, err = h.buildSamplingAndSamples(ctx, txAggs)
		if err != nil {
			resp.Provenance.Partial = true
		}
	}

	utilization, err := h.getSorobanUtilization(ctx, seq)
	if err != nil {
		resp.Provenance.Partial = true
	} else if utilization != nil {
		resp.SorobanUtilization = utilization
	}

	respondJSON(w, resp)
}

func (h *LedgerSummaryHandler) getLedgerRow(ctx context.Context, seq int64) (map[string]interface{}, error) {
	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := h.queryService.determineSource(seq, seq)

	var ledger map[string]interface{}
	if queryHot {
		rows, err := h.queryService.hot.QueryLedgers(ctx, hotStart, hotEnd, 1, "sequence_asc")
		if err != nil {
			return nil, err
		}
		results, err := scanLedgers(rows)
		rows.Close()
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			ledger = results[0]
		}
	}
	if ledger == nil && queryCold {
		rows, err := h.queryService.cold.QueryLedgers(ctx, coldStart, coldEnd, 1, "sequence_asc")
		if err != nil {
			return nil, err
		}
		results, err := scanLedgers(rows)
		rows.Close()
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			ledger = results[0]
		}
	}
	return ledger, nil
}

func (h *LedgerSummaryHandler) getLedgerTxAggs(ctx context.Context, seq int64) ([]ledgerSummaryTxAgg, error) {
	if h.unified == nil {
		return nil, fmt.Errorf("unified reader unavailable")
	}

	summaryQuery := fmt.Sprintf(`
		WITH dedup_ops AS (
			%s
		)
		SELECT transaction_hash,
		       MIN(ledger_sequence) as ledger_seq,
		       MIN(ledger_closed_at) as closed_at,
		       MIN(source_account) as source_account,
		       MIN(tx_fee_charged) as fee_charged,
		       COUNT(*) as op_count,
		       BOOL_AND(tx_successful) as successful,
		       BOOL_OR(is_soroban_op) as has_soroban,
		       MIN(contract_id) FILTER (WHERE contract_id IS NOT NULL AND contract_id <> '') as primary_contract
		FROM dedup_ops
		GROUP BY transaction_hash
		ORDER BY MIN(operation_index)
	`, h.unifiedLedgerOpsQuery(`
		SELECT DISTINCT ON (ledger_sequence, operation_index)
			transaction_hash,
			ledger_sequence,
			ledger_closed_at,
			source_account,
			tx_fee_charged,
			operation_index,
			tx_successful,
			is_soroban_op,
			contract_id
		FROM combined
		ORDER BY ledger_sequence, operation_index, source
	`))

	rows, err := h.unified.db.QueryContext(ctx, summaryQuery, seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ledgerSummaryTxAgg
	indexByHash := map[string]int{}
	for rows.Next() {
		var item ledgerSummaryTxAgg
		var closedAt sql.NullString
		var sourceAccount sql.NullString
		var feeCharged sql.NullInt64
		var hasSoroban sql.NullBool
		var primaryContract sql.NullString
		if err := rows.Scan(
			&item.TxHash,
			&item.LedgerSequence,
			&closedAt,
			&sourceAccount,
			&feeCharged,
			&item.OpCount,
			&item.Successful,
			&hasSoroban,
			&primaryContract,
		); err != nil {
			return nil, err
		}
		if closedAt.Valid {
			item.ClosedAt = closedAt.String
		}
		if sourceAccount.Valid {
			item.SourceAccount = sourceAccount.String
		}
		if feeCharged.Valid {
			item.FeeCharged = feeCharged.Int64
		}
		if hasSoroban.Valid {
			item.HasSoroban = hasSoroban.Bool
		}
		if primaryContract.Valid {
			item.PrimaryContract = primaryContract.String
		}
		indexByHash[item.TxHash] = len(out)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return h.getServingLedgerTxAggs(ctx, seq)
	}

	opQuery := fmt.Sprintf(`
		WITH dedup_ops AS (
			%s
		)
		SELECT transaction_hash,
		       COALESCE(is_payment_op, false),
		       COALESCE(type, 0),
		       COALESCE(contract_id, ''),
		       COALESCE(function_name, ''),
		       COALESCE(asset_code, ''),
		       COALESCE(CAST(amount AS VARCHAR), ''),
		       COALESCE(destination, '')
		FROM dedup_ops
		ORDER BY transaction_hash, operation_index
	`, h.unifiedLedgerOpsQuery(`
		SELECT DISTINCT ON (ledger_sequence, operation_index)
			transaction_hash,
			is_payment_op,
			type,
			contract_id,
			function_name,
			asset_code,
			amount,
			destination,
			operation_index
		FROM combined
		ORDER BY ledger_sequence, operation_index, source
	`))

	opRows, err := h.unified.db.QueryContext(ctx, opQuery, seq)
	if err != nil {
		return nil, err
	}
	defer opRows.Close()

	for opRows.Next() {
		var txHash, contractID, functionName, assetCode, amount, destination string
		var isPayment bool
		var opType int32
		if err := opRows.Scan(&txHash, &isPayment, &opType, &contractID, &functionName, &assetCode, &amount, &destination); err != nil {
			return nil, err
		}
		idx, ok := indexByHash[txHash]
		if !ok {
			continue
		}
		item := &out[idx]
		if isPayment {
			item.HasPayment = true
			if item.PaymentAssetCode == "" {
				item.PaymentAssetCode = assetCode
			}
			if item.PaymentAmount == "" {
				item.PaymentAmount = amount
			}
			if item.DestinationAccount == "" {
				item.DestinationAccount = destination
			}
		}
		if opType == 2 || opType == 3 || opType == 4 || opType == 12 || opType == 13 || opType == 22 || opType == 23 {
			item.HasClassicSwap = true
		}
		if item.PrimaryContract == "" && contractID != "" {
			item.PrimaryContract = contractID
		}
		if item.FunctionName == "" && functionName != "" {
			item.FunctionName = functionName
		}
	}
	if err := opRows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (h *LedgerSummaryHandler) unifiedLedgerOpsQuery(selectFromCombined string) string {
	combined := fmt.Sprintf(`
		WITH combined AS (
			SELECT *, 1 AS source
			FROM %s.enriched_history_operations
			WHERE ledger_sequence = $1
		`, h.unified.hotSchema)
	if h.unified.coldSchema != "" {
		combined += fmt.Sprintf(`
			UNION ALL
			SELECT *, 2 AS source
			FROM %s.enriched_history_operations
			WHERE ledger_sequence = $1
		`, h.unified.coldSchema)
	}
	combined += fmt.Sprintf(`
		)
		%s
	`, selectFromCombined)
	return combined
}

func (h *LedgerSummaryHandler) getServingLedgerTxAggs(ctx context.Context, seq int64) ([]ledgerSummaryTxAgg, error) {
	if h.hot == nil || h.hot.db == nil {
		return nil, nil
	}

	rows, err := h.hot.db.QueryContext(ctx, `
		SELECT tx_hash, ledger_sequence, created_at, COALESCE(source_account, ''), successful,
		       COALESCE(fee_charged_stroops, 0), COALESCE(operation_count, 0),
		       COALESCE(tx_type, ''), COALESCE(summary_text, ''), summary_json, COALESCE(primary_contract_id, '')
		FROM serving.sv_transactions_recent
		WHERE ledger_sequence = $1
		ORDER BY created_at DESC, tx_hash DESC
	`, seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ledgerSummaryTxAgg
	for rows.Next() {
		var (
			item            ledgerSummaryTxAgg
			createdAt       time.Time
			txType          string
			summaryText     string
			summaryJSON     sql.NullString
			primaryContract string
		)
		if err := rows.Scan(&item.TxHash, &item.LedgerSequence, &createdAt, &item.SourceAccount, &item.Successful, &item.FeeCharged, &item.OpCount, &txType, &summaryText, &summaryJSON, &primaryContract); err != nil {
			return nil, err
		}
		item.ClosedAt = createdAt.UTC().Format(time.RFC3339)
		item.PrimaryContract = primaryContract
		applyServingSummary(&item, txType, summaryText, summaryJSON)
		out = append(out, item)
	}
	return out, rows.Err()
}

func applyServingSummary(item *ledgerSummaryTxAgg, txType, summaryText string, summaryJSON sql.NullString) {
	if item == nil {
		return
	}
	item.ActivityType = txType
	item.ActivityDescription = summaryText

	var summary TxSummary
	if summaryJSON.Valid && summaryJSON.String != "" {
		_ = json.Unmarshal([]byte(summaryJSON.String), &summary)
	}
	if summary.Description != "" {
		item.ActivityDescription = summary.Description
	}
	if summary.Type != "" {
		item.ActivityType = summary.Type
	}
	if summary.Transfer != nil {
		item.HasPayment = true
		item.PaymentAssetCode = summary.Transfer.Asset
		item.PaymentAmount = summary.Transfer.Amount
		item.DestinationAccount = summary.Transfer.To
	}
	if summary.Swap != nil {
		item.HasClassicSwap = true
		item.PaymentAssetCode = summary.Swap.SoldAsset
		item.PaymentAmount = summary.Swap.SoldAmount
	}
	if len(summary.InvolvedContracts) > 0 && item.PrimaryContract == "" {
		item.PrimaryContract = summary.InvolvedContracts[0]
	}
	if txType == "contract_call" || txType == "swap" || txType == "multi_op" || txType == "invoke_host_function" {
		item.HasSoroban = txType == "contract_call" || txType == "invoke_host_function"
	}
	if txType == "payment" || txType == "path_payment" {
		item.HasPayment = true
	}
}

func (h *LedgerSummaryHandler) getLedgerSemanticActivities(ctx context.Context, seq int64) (map[string][]ledgerSemanticActivity, error) {
	if h.unified == nil {
		return nil, fmt.Errorf("unified reader unavailable")
	}

	query := fmt.Sprintf(`
		SELECT transaction_hash,
		       COALESCE(activity_type, ''),
		       COALESCE(description, ''),
		       COALESCE(contract_id, ''),
		       COALESCE(source_account, ''),
		       COALESCE(destination_account, ''),
		       COALESCE(asset_code, ''),
		       COALESCE(CAST(amount AS VARCHAR), ''),
		       COALESCE(soroban_function_name, '')
		FROM %s.semantic_activities
		WHERE ledger_sequence = $1
	`, h.unified.hotSchema)

	rows, err := h.unified.db.QueryContext(ctx, query, seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string][]ledgerSemanticActivity{}
	for rows.Next() {
		var item ledgerSemanticActivity
		if err := rows.Scan(
			&item.TransactionHash,
			&item.ActivityType,
			&item.Description,
			&item.ContractID,
			&item.SourceAccount,
			&item.DestinationAccount,
			&item.AssetCode,
			&item.Amount,
			&item.FunctionName,
		); err != nil {
			return nil, err
		}
		out[item.TransactionHash] = append(out[item.TransactionHash], item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *LedgerSummaryHandler) getContractLabels(ctx context.Context, contractIDs []string) (map[string]ledgerContractLabel, error) {
	if h.unified == nil || len(contractIDs) == 0 {
		return map[string]ledgerContractLabel{}, nil
	}
	placeholders := make([]string, len(contractIDs))
	args := make([]interface{}, len(contractIDs))
	for i, id := range contractIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT sec.contract_id,
		       COALESCE(cr.display_name, sec.token_name, sec.token_symbol, sec.contract_id) AS display_name,
		       COALESCE(cr.category, sec.contract_type, ''),
		       COALESCE(sec.wallet_type, ''),
		       COALESCE(sec.deployed_ledger, 0)
		FROM %s.semantic_entities_contracts sec
		LEFT JOIN %s.contract_registry cr ON cr.contract_id = sec.contract_id
		WHERE sec.contract_id IN (%s)
	`, h.unified.hotSchema, h.unified.hotSchema, strings.Join(placeholders, ", "))

	rows, err := h.unified.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]ledgerContractLabel{}
	for rows.Next() {
		var item ledgerContractLabel
		if err := rows.Scan(&item.ContractID, &item.DisplayName, &item.Category, &item.WalletType, &item.DeployedLedger); err != nil {
			return nil, err
		}
		out[item.ContractID] = item
	}
	return out, rows.Err()
}

func applySemanticEnhancement(items []ledgerSummaryTxAgg, activities map[string][]ledgerSemanticActivity, labels map[string]ledgerContractLabel) {
	for i := range items {
		if label, ok := labels[items[i].PrimaryContract]; ok {
			items[i].PrimaryContractName = label.DisplayName
			items[i].PrimaryContractType = label.Category
			items[i].HasWalletInvolved = label.WalletType != ""
			items[i].HasDeployment = label.DeployedLedger > 0 && label.DeployedLedger == items[i].LedgerSequence
		}
		for _, act := range activities[items[i].TxHash] {
			if items[i].ActivityType == "" {
				items[i].ActivityType = act.ActivityType
			}
			if items[i].ActivityDescription == "" && act.Description != "" {
				items[i].ActivityDescription = act.Description
			}
			if items[i].FunctionName == "" && act.FunctionName != "" {
				items[i].FunctionName = act.FunctionName
			}
			if items[i].PrimaryContract == "" && act.ContractID != "" {
				items[i].PrimaryContract = act.ContractID
				if label, ok := labels[act.ContractID]; ok {
					items[i].PrimaryContractName = label.DisplayName
					items[i].PrimaryContractType = label.Category
					items[i].HasWalletInvolved = label.WalletType != ""
					items[i].HasDeployment = label.DeployedLedger > 0 && label.DeployedLedger == items[i].LedgerSequence
				}
			}
			if items[i].PaymentAssetCode == "" && act.AssetCode != "" {
				items[i].PaymentAssetCode = act.AssetCode
			}
			if items[i].PaymentAmount == "" && act.Amount != "" {
				items[i].PaymentAmount = act.Amount
			}
			if items[i].DestinationAccount == "" && act.DestinationAccount != "" {
				items[i].DestinationAccount = act.DestinationAccount
			}
			if act.ActivityType == "payment" || act.ActivityType == "path_payment" {
				items[i].HasPayment = true
			}
			if act.ActivityType == "contract_call" || act.ActivityType == "invoke_host_function" {
				items[i].HasSoroban = true
			}
		}
	}
}

func contractIDsFromTxAggs(items []ledgerSummaryTxAgg) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item.PrimaryContract == "" {
			continue
		}
		if _, ok := seen[item.PrimaryContract]; ok {
			continue
		}
		seen[item.PrimaryContract] = struct{}{}
		out = append(out, item.PrimaryContract)
	}
	return out
}

func (h *LedgerSummaryHandler) getSorobanUtilization(ctx context.Context, seq int64) (*LedgerSummarySorobanUtilization, error) {
	if h.unified == nil || !h.unified.hasBronze() {
		return nil, fmt.Errorf("bronze/unified reader unavailable")
	}

	usageQuery := fmt.Sprintf(`
		WITH txs AS (
			SELECT DISTINCT * FROM (
				%s
			) bronze_txs
		)
		SELECT
			COALESCE(SUM(soroban_resources_instructions), 0) AS instructions_used,
			COALESCE(SUM(soroban_resources_read_bytes), 0) AS read_bytes_used,
			COALESCE(SUM(soroban_resources_write_bytes), 0) AS write_bytes_used,
			COALESCE(SUM(rent_fee_charged), 0) AS rent_burned
		FROM txs
	`, h.unified.bronzeUnionQuery(
		"transaction_hash, soroban_resources_instructions, soroban_resources_read_bytes, soroban_resources_write_bytes, rent_fee_charged",
		"transactions_row_v2",
		"WHERE ledger_sequence = $1",
	))

	var instructionsUsed, readBytesUsed, writeBytesUsed, rentBurned sql.NullInt64
	if err := h.unified.db.QueryRowContext(ctx, usageQuery, seq).Scan(&instructionsUsed, &readBytesUsed, &writeBytesUsed, &rentBurned); err != nil {
		return nil, err
	}

	cfg, err := h.unified.GetSorobanConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg == nil && h.hot != nil {
		cfg, err = h.hot.GetSorobanConfig(ctx)
		if err != nil {
			return nil, err
		}
	}

	resp := &LedgerSummarySorobanUtilization{
		InstructionsUsed:   instructionsUsed.Int64,
		ReadBytesUsed:      readBytesUsed.Int64,
		WriteBytesUsed:     writeBytesUsed.Int64,
		ReadWriteBytesUsed: readBytesUsed.Int64 + writeBytesUsed.Int64,
		RentBurned:         float64(rentBurned.Int64),
	}

	if cfg != nil {
		resp.InstructionsLimit = cfg.Instructions.LedgerMax
		resp.ReadWriteBytesLimit = cfg.LedgerLimits.MaxReadBytes + cfg.LedgerLimits.MaxWriteBytes
		resp.InstructionsPct = pct(resp.InstructionsUsed, resp.InstructionsLimit)
		resp.ReadWritePct = pct(resp.ReadWriteBytesUsed, resp.ReadWriteBytesLimit)
	}

	if resp.InstructionsUsed == 0 && resp.ReadWriteBytesUsed == 0 && resp.RentBurned == 0 {
		return nil, nil
	}
	return resp, nil
}

func hydratePrimaryCategories(items []ledgerSummaryTxAgg) {
	for i := range items {
		switch {
		case items[i].HasDeployment:
			items[i].PrimaryCategory = "deployment"
		case items[i].HasClassicSwap || strings.Contains(strings.ToLower(items[i].ActivityDescription), "swapped"):
			items[i].PrimaryCategory = "swap"
		case items[i].HasSoroban:
			items[i].PrimaryCategory = "contract_call"
		case items[i].HasPayment:
			items[i].PrimaryCategory = "classic_payment"
		default:
			items[i].PrimaryCategory = "classic"
		}
	}
}

func buildClassificationCounts(items []ledgerSummaryTxAgg) LedgerSummaryClassifications {
	var counts LedgerSummaryClassifications
	for _, item := range items {
		if item.HasClassicSwap || strings.Contains(strings.ToLower(item.ActivityDescription), "swapped") {
			counts.SwapTxCount++
		}
		if item.HasSoroban {
			counts.ContractCallTxCount++
			counts.SorobanTxCount++
		}
		if !item.HasSoroban {
			counts.ClassicTxCount++
		}
		if item.HasPayment {
			counts.PaymentTxCount++
		}
		if item.HasWalletInvolved {
			counts.WalletTxCount++
		}
		if item.HasDeployment {
			counts.DeploymentTxCount++
		}
	}
	return counts
}

func buildComposition(totals LedgerSummaryTotals, items []ledgerSummaryTxAgg) *LedgerSummaryComposition {
	if totals.TransactionCount == 0 {
		return nil
	}
	primaryCounts := map[string]int64{}
	for _, item := range items {
		primaryCounts[item.PrimaryCategory]++
	}

	var dominantType string
	var dominantCount int64
	order := []string{"contract_call", "swap", "deployment", "classic_payment", "classic"}
	for _, key := range order {
		if primaryCounts[key] > dominantCount {
			dominantType = key
			dominantCount = primaryCounts[key]
		}
	}

	sorobanCount := int64(0)
	for _, item := range items {
		if item.HasSoroban {
			sorobanCount++
		}
	}

	return &LedgerSummaryComposition{
		DominantTxType:      dominantType,
		DominantTxTypeCount: dominantCount,
		SorobanSharePct:     pct(sorobanCount, totals.TransactionCount),
		FailedSharePct:      pct(totals.FailedTxCount, totals.TransactionCount),
	}
}

func (h *LedgerSummaryHandler) buildSamplingAndSamples(ctx context.Context, items []ledgerSummaryTxAgg) (*LedgerSummarySampling, []LedgerRepresentativeTx, error) {
	if len(items) == 0 {
		return &LedgerSummarySampling{
			Strategy:              "one_per_dominant_kind",
			SampleCount:           0,
			TotalTransactionCount: 0,
		}, nil, nil
	}

	counts := map[string]int64{}
	for _, item := range items {
		counts[item.PrimaryCategory]++
	}

	categories := make([]string, 0, len(counts))
	for category, count := range counts {
		if count > 0 {
			categories = append(categories, category)
		}
	}
	sort.Slice(categories, func(i, j int) bool {
		if counts[categories[i]] == counts[categories[j]] {
			return sampleCategoryRank(categories[i]) < sampleCategoryRank(categories[j])
		}
		return counts[categories[i]] > counts[categories[j]]
	})
	if len(categories) > 3 {
		categories = categories[:3]
	}

	previews, err := h.fetchSamplePreviews(ctx, selectedHashesForCategories(items, categories))
	if err != nil {
		return nil, nil, err
	}
	represented := int64(0)
	samples := make([]LedgerRepresentativeTx, 0, len(categories))
	for _, category := range categories {
		best, ok := chooseBestSample(items, category)
		if !ok {
			continue
		}
		represented += counts[category]
		samples = append(samples, buildRepresentativeTx(best, counts[category], previews[best.TxHash]))
	}

	return &LedgerSummarySampling{
		Strategy:                    "one_per_dominant_kind",
		SampleCount:                 len(samples),
		RepresentedTransactionCount: represented,
		TotalTransactionCount:       int64(len(items)),
	}, samples, nil
}

func selectedHashesForCategories(items []ledgerSummaryTxAgg, categories []string) []string {
	hashes := make([]string, 0, len(categories))
	for _, category := range categories {
		if best, ok := chooseBestSample(items, category); ok {
			hashes = append(hashes, best.TxHash)
		}
	}
	return hashes
}

func chooseBestSample(items []ledgerSummaryTxAgg, category string) (ledgerSummaryTxAgg, bool) {
	var chosen ledgerSummaryTxAgg
	found := false
	for _, item := range items {
		if item.PrimaryCategory != category {
			continue
		}
		if !found || betterSample(item, chosen) {
			chosen = item
			found = true
		}
	}
	return chosen, found
}

func betterSample(a, b ledgerSummaryTxAgg) bool {
	if a.Successful != b.Successful {
		return a.Successful
	}
	if a.OpCount != b.OpCount {
		return a.OpCount > b.OpCount
	}
	if a.FunctionName != b.FunctionName {
		return a.FunctionName < b.FunctionName
	}
	return a.TxHash < b.TxHash
}

func buildRepresentativeTx(item ledgerSummaryTxAgg, coverage int64, preview txSummaryPreview) LedgerRepresentativeTx {
	classification := LedgerRepresentativeClassification{
		TxType:     classificationTxType(item.PrimaryCategory),
		Confidence: "medium",
	}
	if item.FunctionName != "" && (item.PrimaryCategory == "contract_call" || item.PrimaryCategory == "deployment") {
		classification.Subtype = item.FunctionName
	}

	summary := LedgerRepresentativeSummary{
		Description: normalizeSingleLine(representativeDescription(item, preview)),
	}
	if item.FunctionName != "" && (item.PrimaryCategory == "contract_call" || item.PrimaryCategory == "deployment") {
		summary.FunctionName = item.FunctionName
	}
	if item.PaymentAmount != "" {
		summary.Amount = item.PaymentAmount
		summary.AmountDisplay = item.PaymentAmount
	}
	if item.PaymentAssetCode != "" {
		summary.Asset = item.PaymentAssetCode
	}
	if item.PrimaryContractName != "" {
		summary.ProtocolLabel = item.PrimaryContractName
	} else if item.PrimaryContract != "" {
		summary.ProtocolLabel = abbreviateAddr(item.PrimaryContract)
	} else if preview.PrimaryContract != "" {
		summary.ProtocolLabel = abbreviateAddr(preview.PrimaryContract)
	}
	if item.PrimaryContract != "" {
		summary.ProtocolContractID = item.PrimaryContract
	} else if preview.PrimaryContract != "" {
		summary.ProtocolContractID = preview.PrimaryContract
	}

	return LedgerRepresentativeTx{
		TxHash:         item.TxHash,
		Category:       item.PrimaryCategory,
		CategoryLabel:  categoryLabel(item.PrimaryCategory),
		CoverageCount:  coverage,
		Classification: classification,
		Summary:        summary,
		Actors:         representativeActors(item),
	}
}

func representativeDescription(item ledgerSummaryTxAgg, preview txSummaryPreview) string {
	if item.ActivityDescription != "" {
		return normalizeSingleLine(item.ActivityDescription)
	}
	if preview.Description != "" {
		return normalizeSingleLine(preview.Description)
	}
	switch item.PrimaryCategory {
	case "swap":
		return "Representative swap transaction"
	case "deployment":
		if item.PrimaryContractName != "" {
			return fmt.Sprintf("Deployed or initialized %s", item.PrimaryContractName)
		}
		return "Representative deployment transaction"
	case "contract_call":
		if item.FunctionName != "" && item.PrimaryContractName != "" {
			return fmt.Sprintf("Called %s on %s", item.FunctionName, item.PrimaryContractName)
		}
		if item.FunctionName != "" {
			return fmt.Sprintf("Called %s on a Soroban contract", item.FunctionName)
		}
		return "Representative Soroban contract call"
	case "classic_payment":
		if item.PaymentAmount != "" && item.PaymentAssetCode != "" {
			return fmt.Sprintf("Sent %s %s", item.PaymentAmount, item.PaymentAssetCode)
		}
		return "Representative classic payment"
	default:
		return "Representative classic transaction"
	}
}

func (h *LedgerSummaryHandler) fetchSamplePreviews(ctx context.Context, txHashes []string) (map[string]txSummaryPreview, error) {
	if h.hot == nil || h.hot.db == nil || len(txHashes) == 0 {
		return map[string]txSummaryPreview{}, nil
	}
	placeholders := make([]string, len(txHashes))
	args := make([]interface{}, len(txHashes))
	for i, hash := range txHashes {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = hash
	}

	query := fmt.Sprintf(`
		SELECT tx_hash, COALESCE(tx_type, ''), COALESCE(summary_text, ''), summary_json, COALESCE(primary_contract_id, '')
		FROM serving.sv_transactions_recent
		WHERE tx_hash IN (%s)
	`, strings.Join(placeholders, ", "))

	rows, err := h.hot.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]txSummaryPreview{}
	for rows.Next() {
		var hash, txType, summaryText, primaryContract string
		var summaryJSON sql.NullString
		if err := rows.Scan(&hash, &txType, &summaryText, &summaryJSON, &primaryContract); err != nil {
			continue
		}
		preview := txSummaryPreview{TxType: txType, Description: summaryText, PrimaryContract: primaryContract}
		if summaryJSON.Valid && summaryJSON.String != "" {
			var summary TxSummary
			if err := json.Unmarshal([]byte(summaryJSON.String), &summary); err == nil {
				if summary.Description != "" {
					preview.Description = summary.Description
				}
				if summary.Type != "" {
					preview.TxType = summary.Type
				}
			}
		}
		out[hash] = preview
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func pct(used, limit int64) float64 {
	if limit <= 0 {
		return 0
	}
	value := (float64(used) / float64(limit)) * 100
	return math.Round(value*100) / 100
}

func classificationTxType(category string) string {
	switch category {
	case "contract_call":
		return "contract_call"
	case "classic_payment":
		return "simple_payment"
	case "deployment":
		return "deployment"
	case "swap":
		return "swap"
	case "classic":
		return "classic"
	default:
		return category
	}
}

func normalizeSingleLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return strings.Join(strings.Fields(s), " ")
}

func decodeValidatorAccountID(nodeID string) string {
	if strings.TrimSpace(nodeID) == "" {
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(nodeID)
	if err != nil || len(raw) != 32 {
		return ""
	}
	accountID, err := strkey.Encode(strkey.VersionByteAccountID, raw)
	if err != nil {
		return ""
	}
	return accountID
}

func representativeActors(item ledgerSummaryTxAgg) *LedgerRepresentativeActors {
	actors := &LedgerRepresentativeActors{}
	switch item.PrimaryCategory {
	case "contract_call", "deployment":
		if item.PrimaryContract != "" || item.PrimaryContractName != "" {
			actors.Primary = &LedgerRepresentativeActorRef{
				ID:    item.PrimaryContract,
				Label: nonEmpty(item.PrimaryContractName, abbreviateAddr(item.PrimaryContract)),
				Type:  nonEmpty(item.PrimaryContractType, "contract"),
			}
		}
		if item.SourceAccount != "" {
			actors.Secondary = &LedgerRepresentativeActorRef{
				ID:    item.SourceAccount,
				Label: abbreviateAddr(item.SourceAccount),
				Type:  "classic_account",
			}
		}
	case "classic_payment", "swap":
		if item.SourceAccount != "" {
			actors.Primary = &LedgerRepresentativeActorRef{
				ID:    item.SourceAccount,
				Label: abbreviateAddr(item.SourceAccount),
				Type:  "classic_account",
			}
		}
		if item.DestinationAccount != "" {
			actors.Secondary = &LedgerRepresentativeActorRef{
				ID:    item.DestinationAccount,
				Label: abbreviateAddr(item.DestinationAccount),
				Type:  "classic_account",
			}
		}
	default:
		if item.SourceAccount != "" {
			actors.Primary = &LedgerRepresentativeActorRef{
				ID:    item.SourceAccount,
				Label: abbreviateAddr(item.SourceAccount),
				Type:  "account",
			}
		}
	}
	if actors.Primary == nil {
		return nil
	}
	return actors
}

func categoryLabel(category string) string {
	switch category {
	case "contract_call":
		return "Contract Call"
	case "classic_payment":
		return "Classic Payment"
	case "deployment":
		return "Deployment"
	case "swap":
		return "Swap"
	case "classic":
		return "Classic"
	default:
		return titleSnakeCase(category)
	}
}

func titleSnakeCase(s string) string {
	parts := strings.Split(strings.ReplaceAll(s, "-", "_"), "_")
	for i, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func sampleCategoryRank(category string) int {
	switch category {
	case "contract_call":
		return 0
	case "swap":
		return 1
	case "deployment":
		return 2
	case "classic_payment":
		return 3
	case "classic":
		return 4
	default:
		return 100
	}
}

func mapInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func ledgerTimeString(v interface{}) string {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case string:
		return t
	default:
		return ""
	}
}
