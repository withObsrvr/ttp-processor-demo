package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

// ExplorerHomeSummaryHandler serves the explorer/home summary payload used by Prism.
type ExplorerHomeSummaryHandler struct {
	hot     *SilverHotReader
	unified *UnifiedDuckDBReader
	network string
}

func NewExplorerHomeSummaryHandler(hot *SilverHotReader, unified *UnifiedDuckDBReader, network string) *ExplorerHomeSummaryHandler {
	if network == "" {
		network = "testnet"
	}
	return &ExplorerHomeSummaryHandler{hot: hot, unified: unified, network: network}
}

type ExplorerHomeSummaryResponse struct {
	Network                   string                        `json:"network"`
	GeneratedAt               string                        `json:"generated_at"`
	Header                    ExplorerHomeHeader            `json:"header"`
	Hero                      ExplorerHomeHero              `json:"hero"`
	Alert                     *ExplorerHomeAlert            `json:"alert,omitempty"`
	ContractsNeedingAttention []ExplorerAttentionContract   `json:"contracts_needing_attention,omitempty"`
	Leaders                   []ExplorerLeader              `json:"leaders,omitempty"`
	Utilization               *ExplorerHomeUtilization      `json:"utilization,omitempty"`
	Meta                      *ExplorerHomeMeta             `json:"meta,omitempty"`
	Provenance                ExplorerHomeSummaryProvenance `json:"provenance"`
}

type ExplorerHomeHeader struct {
	LatestLedgerSequence int64  `json:"latest_ledger_sequence"`
	LatestLedgerClosedAt string `json:"latest_ledger_closed_at,omitempty"`
}

type ExplorerHomeHero struct {
	Health       ExplorerHomeHealth      `json:"health"`
	LatestLedger ExplorerHeroLedger      `json:"latest_ledger"`
	Cadence      ExplorerHomeCadence     `json:"cadence"`
	Contracts    ExplorerHomeContracts   `json:"contracts"`
	Soroban      ExplorerHomeSoroban     `json:"soroban"`
	Trends       ExplorerHomeTrends      `json:"trends"`
	TTL          ExplorerHomeTTL         `json:"ttl"`
	ActivityMix  ExplorerHomeActivityMix `json:"activity_mix"`
}

type ExplorerHomeHealth struct {
	Status       string `json:"status,omitempty"`
	LoadBand     string `json:"load_band,omitempty"`
	ActivityBand string `json:"activity_band,omitempty"`
}

type ExplorerHeroLedger struct {
	Sequence         int64  `json:"sequence"`
	ClosedAt         string `json:"closed_at,omitempty"`
	TransactionCount int64  `json:"transaction_count,omitempty"`
	OperationCount   int64  `json:"operation_count,omitempty"`
}

type ExplorerHomeCadence struct {
	AvgCloseSeconds       float64 `json:"avg_close_seconds,omitempty"`
	TxPerLedgerRecentAvg  int64   `json:"tx_per_ledger_recent_avg,omitempty"`
	OpsPerLedgerRecentAvg int64   `json:"ops_per_ledger_recent_avg,omitempty"`
}

type ExplorerHomeContracts struct {
	Active24h int64 `json:"active_24h,omitempty"`
}

type ExplorerHomeSoroban struct {
	InstructionPct float64 `json:"instruction_pct,omitempty"`
	ReadWritePct   float64 `json:"read_write_pct,omitempty"`
}

type ExplorerHomeTrends struct {
	TxVs24hAvgPct       float64 `json:"tx_vs_24h_avg_pct,omitempty"`
	AgentActivityWoWPct float64 `json:"agent_activity_wow_pct,omitempty"`
	AnomalyDetected     bool    `json:"anomaly_detected"`
}

type ExplorerHomeTTL struct {
	ExpiringContractCount int64 `json:"expiring_contract_count,omitempty"`
	WorstRemainingHours   int64 `json:"worst_remaining_hours,omitempty"`
	WorstRemainingLedgers int64 `json:"worst_remaining_ledgers,omitempty"`
}

type ExplorerHomeActivityMix struct {
	AgentTx24h        int64 `json:"agent_tx_24h,omitempty"`
	SwapTx24h         int64 `json:"swap_tx_24h,omitempty"`
	ContractCallTx24h int64 `json:"contract_call_tx_24h,omitempty"`
}

type ExplorerHomeAlert struct {
	Type                  string   `json:"type,omitempty"`
	Severity              string   `json:"severity,omitempty"`
	AffectedContractCount int64    `json:"affected_contract_count,omitempty"`
	WorstRemainingHours   int64    `json:"worst_remaining_hours,omitempty"`
	TopContracts          []string `json:"top_contracts,omitempty"`
}

type ExplorerAttentionContract struct {
	ContractID       string  `json:"contract_id"`
	ProtocolName     string  `json:"protocol_name,omitempty"`
	ContractName     string  `json:"contract_name,omitempty"`
	Severity         string  `json:"severity,omitempty"`
	RemainingLedgers int64   `json:"remaining_ledgers,omitempty"`
	RemainingHours   int64   `json:"remaining_hours,omitempty"`
	RemainingHuman   string  `json:"remaining_human,omitempty"`
	RunwayPct        float64 `json:"runway_pct,omitempty"`
	Status           string  `json:"status,omitempty"`
}

type ExplorerLeader struct {
	ContractID       string   `json:"contract_id"`
	ProtocolName     string   `json:"protocol_name,omitempty"`
	ContractName     string   `json:"contract_name,omitempty"`
	CallCount24h     int64    `json:"call_count_24h,omitempty"`
	UniqueCallers24h int64    `json:"unique_callers_24h,omitempty"`
	DominantActions  []string `json:"dominant_actions,omitempty"`
	GrowthPct        float64  `json:"growth_pct,omitempty"`
}

type ExplorerHomeUtilization struct {
	InstructionPct      float64 `json:"instruction_pct,omitempty"`
	InstructionUsed     int64   `json:"instruction_used,omitempty"`
	InstructionLimit    int64   `json:"instruction_limit,omitempty"`
	ReadWritePct        float64 `json:"read_write_pct,omitempty"`
	ReadWriteUsedBytes  int64   `json:"read_write_used_bytes,omitempty"`
	ReadWriteLimitBytes int64   `json:"read_write_limit_bytes,omitempty"`
	TxSizePct           float64 `json:"tx_size_pct,omitempty"`
	AvgTxSizeBytes      int64   `json:"avg_tx_size_bytes,omitempty"`
	TxSizeLimitBytes    int64   `json:"tx_size_limit_bytes,omitempty"`
	SourceLedger        int64   `json:"source_ledger,omitempty"`
}

type ExplorerHomeMeta struct {
	LatestLedgerAgeSeconds int64 `json:"latest_ledger_age_seconds,omitempty"`
}

type ExplorerHomeSummaryProvenance struct {
	Route         string   `json:"route"`
	DataSource    string   `json:"data_source"`
	Partial       bool     `json:"partial"`
	Warnings      []string `json:"warnings,omitempty"`
	GeneratedFrom []string `json:"generated_from,omitempty"`
}

type contractIdentity struct {
	DisplayName string
	Category    string
}

type attentionAggregate struct {
	ContractID       string
	RemainingLedgers int64
	RemainingHours   int64
	WorstLiveUntil   int64
}

type ttlAttentionSummary struct {
	ExpiringContractCount int64
	WorstRemainingHours   int64
	WorstRemainingLedgers int64
}

// HandleExplorerSummary returns the aggregated explorer home-summary payload.
// @Summary Get explorer home summary
// @Description Returns the aggregated summary payload for the explorer home page, including header, hero, alert, leaders, contracts needing attention, utilization, meta, and provenance. This handler is exposed on both /api/v1/explorer/summary and /api/v1/home/summary.
// @Tags Explorer
// @Accept json
// @Produce json
// @Param limit query int false "Maximum number of leaders/contracts-needing-attention to return" default(4)
// @Success 200 {object} ExplorerHomeSummaryResponse "Explorer home summary"
// @Failure 503 {object} map[string]interface{} "Explorer summary unavailable"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/explorer/summary [get]
// @Router /api/v1/home/summary [get]
func (h *ExplorerHomeSummaryHandler) HandleExplorerSummary(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.hot == nil || h.hot.DB() == nil {
		respondError(w, "explorer summary unavailable", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	dataSource := "silver_hot_plus_unified_silver"
	if h.unified == nil {
		dataSource = "silver_hot"
	}

	resp := ExplorerHomeSummaryResponse{
		Network:     h.network,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Provenance: ExplorerHomeSummaryProvenance{
			Route:      r.URL.Path,
			DataSource: dataSource,
			Partial:    false,
			GeneratedFrom: []string{
				"serving.sv_network_stats_current",
				"serving.sv_ledger_stats_recent",
				"serving.sv_contract_stats_current",
				"silver_hot.ttl_current",
				"silver_hot.enriched_history_operations",
			},
		},
	}

	latestLedger, recentAvg, servingStats, err := h.loadHeaderAndCadence(ctx, &resp)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	currentLedger := latestLedger.LedgerSequence
	if currentLedger == 0 {
		currentLedger = resp.Header.LatestLedgerSequence
	}

	utilization, utilWarning := h.loadUtilization(ctx, currentLedger)
	if utilWarning != "" {
		resp.Provenance.Partial = true
		resp.Provenance.Warnings = append(resp.Provenance.Warnings, utilWarning)
	}
	resp.Utilization = utilization
	if utilization != nil {
		resp.Hero.Soroban.InstructionPct = utilization.InstructionPct
		resp.Hero.Soroban.ReadWritePct = utilization.ReadWritePct
	}

	swap24h, contractCall24h, mixWarning := h.loadActivityMix(ctx)
	if mixWarning != "" {
		resp.Provenance.Partial = true
		resp.Provenance.Warnings = append(resp.Provenance.Warnings, mixWarning)
	}
	resp.Hero.ActivityMix = ExplorerHomeActivityMix{
		AgentTx24h:        0,
		SwapTx24h:         swap24h,
		ContractCallTx24h: contractCall24h,
	}

	attentionLimit := parseLimit(r, 4, 20)
	attention, ttlSummary, attentionWarning := h.loadContractsNeedingAttention(ctx, currentLedger, attentionLimit)
	if attentionWarning != "" {
		resp.Provenance.Partial = true
		resp.Provenance.Warnings = append(resp.Provenance.Warnings, attentionWarning)
	}
	resp.ContractsNeedingAttention = attention
	if ttlSummary != nil {
		resp.Hero.TTL.ExpiringContractCount = ttlSummary.ExpiringContractCount
		resp.Hero.TTL.WorstRemainingHours = ttlSummary.WorstRemainingHours
		resp.Hero.TTL.WorstRemainingLedgers = ttlSummary.WorstRemainingLedgers
	}
	if len(attention) > 0 && ttlSummary != nil {
		resp.Alert = &ExplorerHomeAlert{
			Type:                  "ttl_risk",
			Severity:              maxSeverityFromAttention(attention),
			AffectedContractCount: ttlSummary.ExpiringContractCount,
			WorstRemainingHours:   ttlSummary.WorstRemainingHours,
			TopContracts:          topAttentionNames(attention, 3),
		}
	}

	leaders, leadersWarning := h.loadLeaders(ctx, parseLimit(r, 4, 20))
	if leadersWarning != "" {
		resp.Provenance.Partial = true
		resp.Provenance.Warnings = append(resp.Provenance.Warnings, leadersWarning)
	}
	resp.Leaders = leaders

	if servingStats != nil {
		if servingStats.Soroban != nil {
			resp.Hero.Contracts.Active24h = servingStats.Soroban.ActiveContracts24h
		}
		resp.Hero.Trends.AgentActivityWoWPct = 0
		if recentAvg.TxPerLedgerRecentAvg > 0 {
			resp.Hero.Trends.TxVs24hAvgPct = pctDelta(float64(latestLedger.SuccessfulTxCount+latestLedger.FailedTxCount), float64(recentAvg.TxPerLedgerRecentAvg))
			resp.Hero.Trends.AnomalyDetected = math.Abs(resp.Hero.Trends.TxVs24hAvgPct) >= 200
		}
	}
	if resp.Hero.Contracts.Active24h == 0 {
		active24h, activeWarning := h.loadActiveContracts24h(ctx)
		if activeWarning != "" {
			resp.Provenance.Partial = true
			resp.Provenance.Warnings = append(resp.Provenance.Warnings, activeWarning)
		} else {
			resp.Hero.Contracts.Active24h = active24h
		}
	}

	resp.Hero.Health = classifyHomeHealth(resp.Header.LatestLedgerClosedAt, resp.Hero.Trends.TxVs24hAvgPct, resp.Hero.Soroban)
	resp.Meta = buildHomeMeta(resp.Header.LatestLedgerClosedAt)

	respondJSON(w, resp)
}

func (h *ExplorerHomeSummaryHandler) loadHeaderAndCadence(ctx context.Context, resp *ExplorerHomeSummaryResponse) (ServingRecentLedger, ExplorerHomeCadence, *NetworkStats, error) {
	var latest ServingRecentLedger
	var cadence ExplorerHomeCadence

	servingStats, err := h.hot.GetServingNetworkStats(ctx)
	if err != nil {
		resp.Provenance.Partial = true
		resp.Provenance.Warnings = append(resp.Provenance.Warnings, "serving network stats unavailable")
	}

	_, ledgers, err := h.hot.GetServingRecentLedgers(ctx, 20)
	if err != nil {
		return latest, cadence, servingStats, fmt.Errorf("failed to load recent ledgers: %w", err)
	}
	if len(ledgers) == 0 {
		return latest, cadence, servingStats, fmt.Errorf("no recent ledgers available")
	}
	latest = ledgers[0]
	resp.Header = ExplorerHomeHeader{
		LatestLedgerSequence: latest.LedgerSequence,
		LatestLedgerClosedAt: latest.ClosedAt,
	}
	resp.Hero.LatestLedger = ExplorerHeroLedger{
		Sequence:         latest.LedgerSequence,
		ClosedAt:         latest.ClosedAt,
		TransactionCount: int64(latest.SuccessfulTxCount + latest.FailedTxCount),
		OperationCount:   int64(latest.OperationCount),
	}

	var txSum, opSum int64
	for _, l := range ledgers {
		txSum += int64(l.SuccessfulTxCount + l.FailedTxCount)
		opSum += int64(l.OperationCount)
	}
	if len(ledgers) > 0 {
		cadence.TxPerLedgerRecentAvg = int64(math.Round(float64(txSum) / float64(len(ledgers))))
		cadence.OpsPerLedgerRecentAvg = int64(math.Round(float64(opSum) / float64(len(ledgers))))
	}
	cadence.AvgCloseSeconds = 5.0
	if servingStats != nil && servingStats.Ledger.AvgCloseTimeSeconds > 0 {
		cadence.AvgCloseSeconds = servingStats.Ledger.AvgCloseTimeSeconds
	}
	resp.Hero.Cadence = cadence

	return latest, cadence, servingStats, nil
}

func (h *ExplorerHomeSummaryHandler) loadUtilization(ctx context.Context, ledgerSeq int64) (*ExplorerHomeUtilization, string) {
	if ledgerSeq == 0 || h.unified == nil {
		return nil, "utilization snapshot unavailable without unified reader"
	}
	ledgerHandler := NewLedgerSummaryHandler(nil, h.unified, h.hot)
	util, err := ledgerHandler.getSorobanUtilization(ctx, ledgerSeq)
	if err != nil {
		return nil, "failed to compute Soroban utilization snapshot"
	}
	if util == nil {
		return nil, "latest ledger has no Soroban utilization data"
	}
	resp := &ExplorerHomeUtilization{
		InstructionPct:      util.InstructionsPct,
		InstructionUsed:     util.InstructionsUsed,
		InstructionLimit:    util.InstructionsLimit,
		ReadWritePct:        util.ReadWritePct,
		ReadWriteUsedBytes:  util.ReadWriteBytesUsed,
		ReadWriteLimitBytes: util.ReadWriteBytesLimit,
		SourceLedger:        ledgerSeq,
		TxSizeLimitBytes:    131072,
	}
	if resp.InstructionLimit == 0 || resp.ReadWriteLimitBytes == 0 {
		cfg, cfgErr := h.loadSorobanConfig(ctx)
		if cfgErr == nil && cfg != nil {
			if resp.InstructionLimit == 0 {
				resp.InstructionLimit = cfg.Instructions.LedgerMax
			}
			if resp.ReadWriteLimitBytes == 0 {
				resp.ReadWriteLimitBytes = cfg.LedgerLimits.MaxReadBytes + cfg.LedgerLimits.MaxWriteBytes
			}
			if resp.InstructionPct == 0 && resp.InstructionLimit > 0 {
				resp.InstructionPct = pct(resp.InstructionUsed, resp.InstructionLimit)
			}
			if resp.ReadWritePct == 0 && resp.ReadWriteLimitBytes > 0 {
				resp.ReadWritePct = pct(resp.ReadWriteUsedBytes, resp.ReadWriteLimitBytes)
			}
		}
	}
	return resp, ""
}

func (h *ExplorerHomeSummaryHandler) loadActivityMix(ctx context.Context) (int64, int64, string) {
	if h.unified == nil || h.unified.db == nil {
		return 0, 0, "activity mix unavailable without unified reader"
	}
	query := h.unifiedEnriched24hQuery(`
		SELECT
			COUNT(DISTINCT transaction_hash) FILTER (
				WHERE type IN (2, 3, 4, 12, 13, 22, 23)
			) AS swap_tx_24h,
			COUNT(DISTINCT transaction_hash) FILTER (
				WHERE COALESCE(is_soroban_op, false)
			) AS contract_call_tx_24h
		FROM combined
	`)
	var swapTx24h, contractCallTx24h sql.NullInt64
	if err := h.unified.db.QueryRowContext(ctx, query).Scan(&swapTx24h, &contractCallTx24h); err != nil {
		return 0, 0, "failed to query 24h activity mix"
	}
	return swapTx24h.Int64, contractCallTx24h.Int64, ""
}

func (h *ExplorerHomeSummaryHandler) loadContractsNeedingAttention(ctx context.Context, currentLedger int64, limit int) ([]ExplorerAttentionContract, *ttlAttentionSummary, string) {
	if h.hot == nil || h.hot.DB() == nil {
		return nil, nil, "contracts-needing-attention unavailable without hot reader"
	}
	if currentLedger == 0 {
		var err error
		currentLedger, err = h.currentLedger(ctx)
		if err != nil {
			return nil, nil, "failed to determine current ledger for TTL ranking"
		}
	}

	const withinLedgers int64 = 100000
	threshold := currentLedger + withinLedgers

	var totalCount, worstRemaining sql.NullInt64
	if err := h.hot.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) AS contract_count,
		       COALESCE(MIN(t.live_until_ledger_seq - $1), 0) AS worst_remaining_ledgers
		FROM (
			SELECT cd.contract_id, MIN(t.live_until_ledger_seq) AS live_until_ledger_seq
			FROM ttl_current t
			JOIN contract_data_current cd ON cd.key_hash = t.key_hash
			WHERE COALESCE(t.expired, false) = false
			  AND t.live_until_ledger_seq > $1
			  AND t.live_until_ledger_seq <= $2
			GROUP BY cd.contract_id
		) t
	`, currentLedger, threshold).Scan(&totalCount, &worstRemaining); err != nil {
		return nil, nil, "failed to load TTL-expiring contracts"
	}

	if totalCount.Int64 == 0 {
		return nil, &ttlAttentionSummary{}, ""
	}

	rows, err := h.hot.DB().QueryContext(ctx, `
		SELECT cd.contract_id,
		       MIN(t.live_until_ledger_seq - $1) AS remaining_ledgers,
		       MIN(t.live_until_ledger_seq) AS worst_live_until
		FROM ttl_current t
		JOIN contract_data_current cd ON cd.key_hash = t.key_hash
		WHERE COALESCE(t.expired, false) = false
		  AND t.live_until_ledger_seq > $1
		  AND t.live_until_ledger_seq <= $2
		GROUP BY cd.contract_id
		ORDER BY remaining_ledgers ASC, cd.contract_id ASC
		LIMIT $3
	`, currentLedger, threshold, limit)
	if err != nil {
		return nil, nil, "failed to load TTL-expiring contracts"
	}
	defer rows.Close()

	items := make([]attentionAggregate, 0, limit)
	for rows.Next() {
		var item attentionAggregate
		if err := rows.Scan(&item.ContractID, &item.RemainingLedgers, &item.WorstLiveUntil); err != nil {
			return nil, nil, "failed to load TTL-expiring contracts"
		}
		if item.RemainingLedgers < 0 {
			item.RemainingLedgers = 0
		}
		item.RemainingHours = ledgersToHours(item.RemainingLedgers)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "failed to load TTL-expiring contracts"
	}

	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ContractID)
	}
	identities := h.lookupContractIdentities(ctx, ids)

	out := make([]ExplorerAttentionContract, 0, len(items))
	for _, item := range items {
		identity := identities[item.ContractID]
		protocolName, contractName := splitProtocolAndContract(identity.DisplayName, item.ContractID)
		out = append(out, ExplorerAttentionContract{
			ContractID:       item.ContractID,
			ProtocolName:     protocolName,
			ContractName:     contractName,
			Severity:         ttlSeverity(item.RemainingHours),
			RemainingLedgers: item.RemainingLedgers,
			RemainingHours:   item.RemainingHours,
			RemainingHuman:   humanizeTTLHours(item.RemainingHours),
			RunwayPct:        ttlRunwayPct(item.RemainingLedgers),
			Status:           ttlStatus(item.RemainingHours),
		})
	}

	return out, &ttlAttentionSummary{
		ExpiringContractCount: totalCount.Int64,
		WorstRemainingLedgers: worstRemaining.Int64,
		WorstRemainingHours:   ledgersToHours(worstRemaining.Int64),
	}, ""
}

func (h *ExplorerHomeSummaryHandler) loadLeaders(ctx context.Context, limit int) ([]ExplorerLeader, string) {
	if h.hot == nil || h.hot.DB() == nil {
		return nil, "failed to load leaders from serving stats"
	}
	rows, err := h.hot.DB().QueryContext(ctx, `
		SELECT contract_id,
		       COALESCE(total_calls_24h, 0) AS total_calls_24h,
		       COALESCE(unique_callers_24h, 0) AS unique_callers_24h,
		       COALESCE(total_calls_7d, 0) AS total_calls_7d,
		       COALESCE(top_function, '') AS top_function
		FROM serving.sv_contract_stats_current
		WHERE COALESCE(total_calls_24h, 0) > 0
		ORDER BY total_calls_24h DESC, contract_id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, "failed to load leaders from serving stats"
	}
	defer rows.Close()

	type leaderStat struct {
		ContractID  string
		Calls24h    int64
		Unique24h   int64
		Calls7d     int64
		TopFunction string
	}
	stats := make([]leaderStat, 0, limit)
	ids := make([]string, 0, limit)
	for rows.Next() {
		var s leaderStat
		if err := rows.Scan(&s.ContractID, &s.Calls24h, &s.Unique24h, &s.Calls7d, &s.TopFunction); err != nil {
			return nil, "failed to load leaders from serving stats"
		}
		stats = append(stats, s)
		ids = append(ids, s.ContractID)
	}
	if err := rows.Err(); err != nil {
		return nil, "failed to load leaders from serving stats"
	}
	identities := h.lookupContractIdentities(ctx, ids)

	out := make([]ExplorerLeader, 0, len(stats))
	for _, s := range stats {
		identity := identities[s.ContractID]
		protocolName, contractName := splitProtocolAndContract(identity.DisplayName, s.ContractID)
		dominant := []string{}
		if s.TopFunction != "" {
			dominant = append(dominant, s.TopFunction)
		}
		growth := 0.0
		if s.Calls7d > 0 {
			growth = pctDelta(float64(s.Calls24h), float64(s.Calls7d)/7.0)
		}
		out = append(out, ExplorerLeader{
			ContractID:       s.ContractID,
			ProtocolName:     protocolName,
			ContractName:     contractName,
			CallCount24h:     s.Calls24h,
			UniqueCallers24h: s.Unique24h,
			DominantActions:  dominant,
			GrowthPct:        growth,
		})
	}
	return out, ""
}

func (h *ExplorerHomeSummaryHandler) lookupContractIdentities(ctx context.Context, ids []string) map[string]contractIdentity {
	out := map[string]contractIdentity{}
	if h.hot == nil || h.hot.DB() == nil || len(ids) == 0 {
		return out
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf(`
		SELECT cr.contract_id,
		       COALESCE(NULLIF(cr.display_name, ''), NULLIF(svc.name, ''), NULLIF(svc.symbol, ''), NULLIF(sec.token_name, ''), NULLIF(sec.token_symbol, ''), NULLIF(tr.token_name, ''), NULLIF(tr.token_symbol, ''), cr.contract_id) AS display_name,
		       COALESCE(NULLIF(cr.category, ''), NULLIF(svc.contract_type, ''), NULLIF(sec.contract_type, ''), '') AS category
		FROM contract_registry cr
		LEFT JOIN token_registry tr ON tr.contract_id = cr.contract_id
		LEFT JOIN serving.sv_contracts_current svc ON svc.contract_id = cr.contract_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = cr.contract_id
		WHERE cr.contract_id IN (%s)
	`, strings.Join(placeholders, ", "))
	rows, err := h.hot.DB().QueryContext(ctx, query, args...)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, name, category string
			if err := rows.Scan(&id, &name, &category); err != nil {
				continue
			}
			out[id] = contractIdentity{DisplayName: name, Category: category}
		}
	}

	missing := make([]string, 0)
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return out
	}
	placeholders = make([]string, len(missing))
	args = make([]interface{}, len(missing))
	for i, id := range missing {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	semanticFallbackQuery := fmt.Sprintf(`
		SELECT contract_id,
		       COALESCE(NULLIF(token_name, ''), NULLIF(token_symbol, ''), contract_id) AS display_name,
		       COALESCE(NULLIF(contract_type, ''), 'contract') AS category
		FROM semantic_entities_contracts
		WHERE contract_id IN (%s)
	`, strings.Join(placeholders, ", "))
	semanticRows, err := h.hot.DB().QueryContext(ctx, semanticFallbackQuery, args...)
	if err == nil {
		defer semanticRows.Close()
		for semanticRows.Next() {
			var id, name, category string
			if err := semanticRows.Scan(&id, &name, &category); err != nil {
				continue
			}
			out[id] = contractIdentity{DisplayName: name, Category: category}
		}
	}

	missing = missing[:0]
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return out
	}
	placeholders = make([]string, len(missing))
	args = make([]interface{}, len(missing))
	for i, id := range missing {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	fallbackQuery := fmt.Sprintf(`
		SELECT contract_id,
		       COALESCE(NULLIF(name, ''), NULLIF(symbol, ''), contract_id) AS display_name,
		       COALESCE(NULLIF(contract_type, ''), 'contract') AS category
		FROM serving.sv_contracts_current
		WHERE contract_id IN (%s)
	`, strings.Join(placeholders, ", "))
	fallbackRows, err := h.hot.DB().QueryContext(ctx, fallbackQuery, args...)
	if err == nil {
		defer fallbackRows.Close()
		for fallbackRows.Next() {
			var id, name, category string
			if err := fallbackRows.Scan(&id, &name, &category); err != nil {
				continue
			}
			out[id] = contractIdentity{DisplayName: name, Category: category}
		}
	}

	missing = missing[:0]
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return out
	}
	placeholders = make([]string, len(missing))
	args = make([]interface{}, len(missing))
	for i, id := range missing {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	tokenFallbackQuery := fmt.Sprintf(`
		SELECT contract_id,
		       COALESCE(token_name, token_symbol, contract_id) AS display_name,
		       'token' AS category
		FROM token_registry
		WHERE contract_id IN (%s)
	`, strings.Join(placeholders, ", "))
	tokenRows, err := h.hot.DB().QueryContext(ctx, tokenFallbackQuery, args...)
	if err != nil {
		return out
	}
	defer tokenRows.Close()
	for tokenRows.Next() {
		var id, name, category string
		if err := tokenRows.Scan(&id, &name, &category); err != nil {
			continue
		}
		out[id] = contractIdentity{DisplayName: name, Category: category}
	}
	return out
}

func (h *ExplorerHomeSummaryHandler) unifiedEnriched24hQuery(selectBody string) string {
	combined := fmt.Sprintf(`
		WITH combined AS (
			SELECT *
			FROM %s.enriched_history_operations
			WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`, h.unified.hotSchema)
	if h.unified.coldSchema != "" {
		combined += fmt.Sprintf(`
			UNION ALL
			SELECT *
			FROM %s.enriched_history_operations
			WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
		`, h.unified.coldSchema)
	}
	combined += fmt.Sprintf(`
		)
		%s
	`, selectBody)
	return combined
}

func (h *ExplorerHomeSummaryHandler) loadActiveContracts24h(ctx context.Context) (int64, string) {
	if h.hot == nil || h.hot.DB() == nil {
		return 0, "active contract count unavailable"
	}
	var count sql.NullInt64
	if err := h.hot.DB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM serving.sv_contract_stats_current
		WHERE COALESCE(total_calls_24h, 0) > 0
	`).Scan(&count); err == nil && count.Int64 > 0 {
		return count.Int64, ""
	}
	if err := h.hot.DB().QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT contract_id)
		FROM contract_invocations_raw
		WHERE closed_at > NOW() - INTERVAL '24 hours'
	`).Scan(&count); err != nil {
		return 0, "failed to query active contracts in 24h"
	}
	return count.Int64, ""
}

func (h *ExplorerHomeSummaryHandler) loadSorobanConfig(ctx context.Context) (*SorobanConfig, error) {
	if h.unified != nil {
		if cfg, err := h.unified.GetSorobanConfig(ctx); err == nil && cfg != nil {
			return cfg, nil
		}
	}
	if h.hot != nil {
		if cfg, err := h.hot.GetSorobanConfig(ctx); err == nil && cfg != nil {
			return cfg, nil
		}
	}
	return defaultSorobanConfig(), nil
}

func defaultSorobanConfig() *SorobanConfig {
	return &SorobanConfig{
		Instructions: SorobanInstructionLimits{
			LedgerMax: 100000000,
			TxMax:     100000000,
		},
		Memory: SorobanMemoryLimits{
			TxLimitBytes: 41943040,
		},
		LedgerLimits: SorobanIOLimits{
			MaxReadEntries:  200,
			MaxReadBytes:    200000,
			MaxWriteEntries: 100,
			MaxWriteBytes:   66560,
		},
		Contract: SorobanContractLimits{
			MaxSizeBytes: 65536,
		},
	}
}

func (h *ExplorerHomeSummaryHandler) currentLedger(ctx context.Context) (int64, error) {
	if h.hot != nil {
		if seq, err := h.hot.GetServingLatestLedgerSequence(ctx); err == nil && seq > 0 {
			return seq, nil
		}
		if seq, err := h.hot.GetCurrentLedger(ctx); err == nil && seq > 0 {
			return seq, nil
		}
	}
	if h.unified != nil {
		return h.unified.GetCurrentLedger(ctx)
	}
	return 0, fmt.Errorf("no reader available")
}

func classifyHomeHealth(closedAt string, txVsAvgPct float64, soroban ExplorerHomeSoroban) ExplorerHomeHealth {
	status := "unknown"
	if closedAt != "" {
		if ts, err := time.Parse(time.RFC3339, closedAt); err == nil {
			age := time.Since(ts)
			switch {
			case age <= 30*time.Second:
				status = "healthy"
			case age <= 2*time.Minute:
				status = "degraded"
			default:
				status = "halted"
			}
		}
	}
	activityBand := "normal"
	switch {
	case txVsAvgPct >= 50:
		activityBand = "busy"
	case txVsAvgPct <= -50:
		activityBand = "quiet"
	}
	loadBand := "light"
	maxPct := math.Max(soroban.InstructionPct, soroban.ReadWritePct)
	switch {
	case maxPct >= 85:
		loadBand = "heavy"
	case maxPct >= 50:
		loadBand = "moderate"
	}
	return ExplorerHomeHealth{Status: status, LoadBand: loadBand, ActivityBand: activityBand}
}

func buildHomeMeta(closedAt string) *ExplorerHomeMeta {
	if closedAt == "" {
		return nil
	}
	ts, err := time.Parse(time.RFC3339, closedAt)
	if err != nil {
		return nil
	}
	age := int64(time.Since(ts).Seconds())
	if age < 0 {
		age = 0
	}
	return &ExplorerHomeMeta{LatestLedgerAgeSeconds: age}
}

func pctDelta(current, baseline float64) float64 {
	if baseline <= 0 {
		return 0
	}
	return math.Round((((current-baseline)/baseline)*100)*100) / 100
}

func ledgersToHours(ledgers int64) int64 {
	if ledgers <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(ledgers*5) / 3600.0))
}

func humanizeTTLHours(hours int64) string {
	switch {
	case hours <= 1:
		return "under 1 hour"
	case hours < 24:
		return fmt.Sprintf("%d hours", hours)
	case hours < 48:
		return "1 day"
	default:
		return fmt.Sprintf("%d days", int64(math.Ceil(float64(hours)/24.0)))
	}
}

func ttlSeverity(hours int64) string {
	switch {
	case hours <= 24:
		return "bad"
	case hours <= 72:
		return "warn"
	default:
		return "watch"
	}
}

func ttlStatus(hours int64) string {
	switch {
	case hours <= 24:
		return "expiring_soon"
	case hours <= 72:
		return "needs_attention"
	default:
		return "watchlist"
	}
}

func ttlRunwayPct(remainingLedgers int64) float64 {
	const window = 100000.0
	pct := (float64(remainingLedgers) / window) * 100
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return math.Round(pct*100) / 100
}

func splitProtocolAndContract(displayName, contractID string) (string, string) {
	name := strings.TrimSpace(displayName)
	if name == "" || name == contractID {
		return abbreviateAddr(contractID), ""
	}
	parts := strings.SplitN(name, "·", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	parts = strings.SplitN(name, "-", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return name, ""
}

func topAttentionNames(items []ExplorerAttentionContract, limit int) []string {
	out := make([]string, 0, limit)
	for _, item := range items {
		name := item.ProtocolName
		if name == "" {
			name = item.ContractName
		}
		if name == "" {
			name = abbreviateAddr(item.ContractID)
		}
		out = append(out, name)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func maxSeverityFromAttention(items []ExplorerAttentionContract) string {
	for _, item := range items {
		if item.Severity == "bad" {
			return "warn"
		}
	}
	if len(items) > 0 {
		return "info"
	}
	return ""
}
