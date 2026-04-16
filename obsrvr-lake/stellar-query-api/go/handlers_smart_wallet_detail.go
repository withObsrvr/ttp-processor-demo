package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

type SmartWalletDetailResponse struct {
	ContractID      string                         `json:"contract_id"`
	DisplayName     *string                        `json:"display_name,omitempty"`
	IsSmartWallet   bool                           `json:"is_smart_wallet"`
	Meta            SmartWalletDetailMeta          `json:"meta"`
	Wallet          SmartWalletIdentitySection     `json:"wallet"`
	Account         SmartWalletAccountSection      `json:"account"`
	SignerConfig    SmartWalletSignerConfigSection `json:"signer_config"`
	Policies        SmartWalletPoliciesSection     `json:"policies"`
	SessionKeys     SmartWalletSessionKeysSection  `json:"session_keys"`
	Contract        SmartWalletContractSection     `json:"contract"`
	Rent            SmartWalletRentSection         `json:"rent"`
	ActivitySummary SmartWalletActivitySummary     `json:"activity_summary"`
	Timeline        []SmartWalletTimelineItem      `json:"timeline"`
}

type SmartWalletDetailMeta struct {
	GeneratedAt string   `json:"generated_at"`
	Sources     []string `json:"sources"`
	Partial     bool     `json:"partial"`
}

type SmartWalletIdentitySection struct {
	WalletType           string   `json:"wallet_type,omitempty"`
	Implementation       string   `json:"implementation,omitempty"`
	ClassificationSource string   `json:"classification_source,omitempty"`
	Confidence           float64  `json:"confidence"`
	HasCheckAuth         bool     `json:"has_check_auth"`
	Evidence             []string `json:"evidence,omitempty"`
}

type SmartWalletAccountSection struct {
	CreatedAt      *string                     `json:"created_at,omitempty"`
	CreatedLedger  *int64                      `json:"created_ledger,omitempty"`
	LastActivityAt *string                     `json:"last_activity_at,omitempty"`
	SequenceNumber *string                     `json:"sequence_number,omitempty"`
	NumSubentries  *int64                      `json:"num_subentries,omitempty"`
	NativeBalance  *string                     `json:"native_balance,omitempty"`
	Balances       []SmartWalletBalanceSummary `json:"balances"`
	Source         string                      `json:"source,omitempty"`
}

type SmartWalletBalanceSummary struct {
	AssetCode       string  `json:"asset_code"`
	AssetType       string  `json:"asset_type"`
	AssetIssuer     *string `json:"asset_issuer,omitempty"`
	Balance         string  `json:"balance"`
	ValueUSD        *string `json:"value_usd,omitempty"`
	Decimals        *int    `json:"decimals,omitempty"`
	Symbol          *string `json:"symbol,omitempty"`
	TokenContractID *string `json:"token_contract_id,omitempty"`
	BalanceSource   string  `json:"balance_source,omitempty"`
}

type SmartWalletSignerConfigSection struct {
	Decoded        bool                      `json:"decoded"`
	Source         string                    `json:"source"`
	SignerCount    int                       `json:"signer_count"`
	Signers        []SmartWalletSignerDetail `json:"signers"`
	Thresholds     *SmartWalletThresholds    `json:"thresholds,omitempty"`
	RequiredWeight *int                      `json:"required_weight,omitempty"`
	TotalWeight    *int                      `json:"total_weight,omitempty"`
	ApprovalModel  SmartWalletApprovalModel  `json:"approval_model"`
}

type SmartWalletSignerDetail struct {
	ID      string  `json:"id"`
	KeyType string  `json:"key_type,omitempty"`
	Role    *string `json:"role,omitempty"`
	Label   *string `json:"label,omitempty"`
	Weight  *int    `json:"weight,omitempty"`
	AddedAt *string `json:"added_at,omitempty"`
}

type SmartWalletThresholds struct {
	Low    *int `json:"low,omitempty"`
	Medium *int `json:"medium,omitempty"`
	High   *int `json:"high,omitempty"`
}

type SmartWalletApprovalModel struct {
	Type               string `json:"type"`
	Summary            string `json:"summary"`
	MinSignersEstimate *int   `json:"min_signers_estimate,omitempty"`
}

type SmartWalletPoliciesSection struct {
	Decoded bool                          `json:"decoded"`
	Items   []SmartWalletPolicyDetailItem `json:"items"`
}

type SmartWalletPolicyDetailItem struct {
	PolicyType  string         `json:"policy_type"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Status      string         `json:"status"`
	Config      map[string]any `json:"config,omitempty"`
}

type SmartWalletSessionKeysSection struct {
	Supported bool                           `json:"supported"`
	Count     int                            `json:"count"`
	Items     []SmartWalletSessionKeySummary `json:"items"`
}

type SmartWalletSessionKeySummary struct {
	Key        string  `json:"key"`
	KeyType    string  `json:"key_type,omitempty"`
	Scope      *string `json:"scope,omitempty"`
	ExpiresAt  *string `json:"expires_at,omitempty"`
	SpendLimit *string `json:"spend_limit,omitempty"`
	UsedAmount *string `json:"used_amount,omitempty"`
	Status     string  `json:"status,omitempty"`
}

type SmartWalletContractSection struct {
	WasmHash          *string  `json:"wasm_hash,omitempty"`
	Deployer          *string  `json:"deployer,omitempty"`
	InterfaceType     string   `json:"interface_type,omitempty"`
	ExportedFunctions []string `json:"exported_functions,omitempty"`
	ObservedFunctions []string `json:"observed_functions,omitempty"`
	StorageEntries    *int     `json:"storage_entries,omitempty"`
	PersistentEntries *int     `json:"persistent_entries,omitempty"`
	TemporaryEntries  *int     `json:"temporary_entries,omitempty"`
	StateSizeBytes    *int64   `json:"state_size_bytes,omitempty"`
}

type SmartWalletRentSection struct {
	RentStatus                  string  `json:"rent_status"`
	TTLLedgers                  *int64  `json:"ttl_ledgers,omitempty"`
	TTLExpiresAt                *string `json:"ttl_expires_at,omitempty"`
	EstimatedMonthlyRentStroops *int64  `json:"estimated_monthly_rent_stroops,omitempty"`
}

type SmartWalletActivitySummary struct {
	TotalTransactions7d      int64                       `json:"total_transactions_7d"`
	SuccessfulTransactions7d int64                       `json:"successful_transactions_7d"`
	PolicyUpdates30d         int64                       `json:"policy_updates_30d"`
	Approvals30d             int64                       `json:"approvals_30d"`
	ProtocolInteractions30d  int64                       `json:"protocol_interactions_30d"`
	UniqueCallers30d         int64                       `json:"unique_callers_30d"`
	ActiveWindows            []string                    `json:"active_windows"`
	CommonFunctions          []SmartWalletCommonFunction `json:"common_functions"`
	CommonProtocols          []SmartWalletCommonProtocol `json:"common_protocols"`
}

type SmartWalletCommonFunction struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type SmartWalletCommonProtocol struct {
	ContractID       string  `json:"contract_id"`
	DisplayName      *string `json:"display_name,omitempty"`
	InteractionCount int64   `json:"interaction_count"`
}

type SmartWalletTimelineItem struct {
	Type           string   `json:"type"`
	Subtype        string   `json:"subtype,omitempty"`
	Timestamp      string   `json:"timestamp"`
	LedgerSequence int64    `json:"ledger_sequence"`
	TxHash         string   `json:"tx_hash"`
	Successful     bool     `json:"successful"`
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	Actors         []string `json:"actors,omitempty"`
	Evidence       []string `json:"evidence,omitempty"`
}

type smartWalletTxSummary struct {
	TxType   string
	Summary  TxSummary
	Contract *string
}

type SmartWalletBalancesResponse struct {
	ContractID          string                      `json:"contract_id"`
	NativeBalance       *string                     `json:"native_balance,omitempty"`
	NativeBalanceSource string                      `json:"native_balance_source,omitempty"`
	Balances            []SmartWalletBalanceSummary `json:"balances"`
	Count               int                         `json:"count"`
	Partial             bool                        `json:"partial"`
	BalanceStatus       string                      `json:"balance_status,omitempty"`
}

func (h *SmartWalletHandlers) SetUnifiedDuckDBReader(reader *UnifiedDuckDBReader) {
	h.unifiedReader = reader
}

// HandleSmartWalletDetail serves GET /api/v1/silver/smart-wallets/{contract_id}
func (h *SmartWalletHandlers) HandleSmartWalletDetail(w http.ResponseWriter, r *http.Request) {
	contractID := mux.Vars(r)["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	detail, err := h.GetSmartWalletDetail(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, detail)
}

func (h *SmartWalletHandlers) HandleSmartWalletBalances(w http.ResponseWriter, r *http.Request) {
	contractID := mux.Vars(r)["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}
	resp, err := h.GetSmartWalletBalances(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, resp)
}

func (h *SmartWalletHandlers) GetSmartWalletDetail(ctx context.Context, contractID string) (*SmartWalletDetailResponse, error) {
	meta := &ContractMetadataResponse{ContractID: contractID}
	if h.hotReader != nil {
		h.hotReader.enrichContractMetadata(ctx, meta)
		h.hotReader.enrichContractFunctions(ctx, meta)
		h.hotReader.enrichContractRegistry(ctx, meta)
		h.hotReader.enrichContractStorage(ctx, meta)
		if meta.WasmHash != nil {
			h.hotReader.enrichContractWasm(ctx, meta)
		}
		h.hotReader.enrichContractRentEstimate(ctx, meta)
	}
	if meta.CreatorAddress == nil && h.unifiedReader != nil {
		h.unifiedReader.enrichContractMetadata(ctx, meta)
	}
	if len(meta.ExportedFunctions) == 0 && h.unifiedReader != nil {
		h.unifiedReader.enrichContractFunctions(ctx, meta)
	}

	walletInfo, walletEvidence, classificationSource, evidenceStrings, usedSources := h.detectWalletDetail(ctx, contractID)
	if walletInfo == nil {
		walletInfo = &SmartWalletInfo{ContractID: contractID}
	}

	balanceResp, balanceErr := h.GetSmartWalletBalances(ctx, contractID)
	accountSection, accountPartial := h.buildSmartWalletAccountSection(ctx, contractID, meta, balanceResp)
	if balanceErr != nil {
		accountPartial = true
	}
	signerSection := buildSmartWalletSignerConfig(walletInfo)
	policies := buildSmartWalletPolicies(walletInfo)
	contractSection := buildSmartWalletContractSection(meta, walletEvidence)
	rentSection := SmartWalletRentSection{
		RentStatus:                  smartWalletRentStatus(meta),
		EstimatedMonthlyRentStroops: meta.EstimatedMonthlyRentStroops,
	}
	activitySummary, timeline, activityPartial := h.buildSmartWalletActivity(ctx, contractID)

	partial := accountPartial || activityPartial || !walletInfo.IsSmartWallet
	if meta.CreatorAddress == nil && meta.WasmHash == nil && len(meta.ExportedFunctions) == 0 {
		partial = true
	}

	return &SmartWalletDetailResponse{
		ContractID:    contractID,
		DisplayName:   meta.DisplayName,
		IsSmartWallet: walletInfo.IsSmartWallet,
		Meta: SmartWalletDetailMeta{
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Sources:     dedupeStrings(append([]string{"smart_wallet_detail"}, usedSources...)),
			Partial:     partial,
		},
		Wallet: SmartWalletIdentitySection{
			WalletType:           walletInfo.WalletType,
			Implementation:       walletInfo.Implementation,
			ClassificationSource: classificationSource,
			Confidence:           walletInfo.Confidence,
			HasCheckAuth:         walletInfo.HasCheckAuth,
			Evidence:             evidenceStrings,
		},
		Account:      accountSection,
		SignerConfig: signerSection,
		Policies:     policies,
		SessionKeys: SmartWalletSessionKeysSection{
			Supported: false,
			Count:     0,
			Items:     []SmartWalletSessionKeySummary{},
		},
		Contract:        contractSection,
		Rent:            rentSection,
		ActivitySummary: activitySummary,
		Timeline:        timeline,
	}, nil
}

func (h *SmartWalletHandlers) detectWalletDetail(ctx context.Context, contractID string) (*SmartWalletInfo, *WalletEvidence, string, []string, []string) {
	var sources []string
	if info, err := h.getSmartWalletFromSemantic(ctx, contractID); err == nil && info.IsSmartWallet {
		sources = append(sources, "semantic")
		evidence := []string{"wallet_type materialized in semantic_entities_contracts"}
		if info.WalletType != "" {
			evidence = append(evidence, fmt.Sprintf("wallet_type=%s", info.WalletType))
		}
		return info, nil, "classified", evidence, sources
	}

	evidence, err := h.assembleWalletEvidence(ctx, contractID)
	if err != nil {
		return &SmartWalletInfo{ContractID: contractID}, nil, "unavailable", nil, sources
	}
	if evidence.WasmHash != "" {
		sources = append(sources, "metadata_or_rpc")
	}
	if len(evidence.ObservedFunctions) > 0 {
		sources = append(sources, "observed_functions")
	}
	if len(evidence.InstanceStorage) > 0 {
		sources = append(sources, "instance_storage")
	}
	result := NewWalletDetectorRegistry().Detect(*evidence)
	info := mapDetectionResult(contractID, evidence, result)
	return info, evidence, "detected", describeWalletEvidence(evidence, info), sources
}

func describeWalletEvidence(evidence *WalletEvidence, info *SmartWalletInfo) []string {
	if evidence == nil {
		return nil
	}
	out := []string{}
	if evidence.WasmHash != "" && info != nil && info.WalletType != "" {
		out = append(out, fmt.Sprintf("wasm hash matched trusted %s registry", info.WalletType))
	}
	if evidence.HasCheckAuth {
		out = append(out, "check_auth implemented")
	}
	if len(evidence.InstanceStorage) > 0 {
		out = append(out, fmt.Sprintf("instance storage entries observed: %d", len(evidence.InstanceStorage)))
	}
	if len(evidence.ObservedFunctions) > 0 {
		interesting := filterWalletFunctions(evidence.ObservedFunctions)
		if len(interesting) > 0 {
			out = append(out, fmt.Sprintf("wallet-management functions observed: %s", strings.Join(interesting, ", ")))
		}
	}
	return dedupeStrings(out)
}

func (h *SmartWalletHandlers) buildSmartWalletAccountSection(ctx context.Context, contractID string, meta *ContractMetadataResponse, balanceResp *SmartWalletBalancesResponse) (SmartWalletAccountSection, bool) {
	section := SmartWalletAccountSection{Balances: []SmartWalletBalanceSummary{}, Source: "partial"}
	partial := false

	var account *AccountCurrent
	if h.hotReader != nil {
		acc, err := h.hotReader.GetAccountCurrent(ctx, contractID)
		if err == nil && acc != nil {
			account = acc
			section.Source = "hot"
		}
	}
	if account == nil && h.coldReader != nil {
		acc, err := h.coldReader.GetAccountCurrent(ctx, contractID)
		if err == nil && acc != nil {
			account = acc
			section.Source = "cold"
		}
	}

	if account != nil {
		section.SequenceNumber = &account.SequenceNumber
		ns := account.NumSubentries
		section.NumSubentries = &ns
		section.NativeBalance = &account.Balance
	}

	if balanceResp != nil {
		section.Balances = append(section.Balances, balanceResp.Balances...)
		if balanceResp.NativeBalance != nil {
			section.NativeBalance = balanceResp.NativeBalance
		}
	}
	if len(section.Balances) == 0 && section.NativeBalance != nil {
		decimals := 7
		symbol := "XLM"
		section.Balances = append(section.Balances, SmartWalletBalanceSummary{AssetCode: "XLM", AssetType: "native", Balance: *section.NativeBalance, Decimals: &decimals, Symbol: &symbol, BalanceSource: "classic_account_state"})
	}

	if meta != nil {
		section.CreatedAt = meta.CreatedAt
		section.CreatedLedger = meta.CreatedLedger
		section.LastActivityAt = inferLastActivityAt(meta)
	}
	if section.CreatedAt == nil && h.hotReader != nil {
		if createdAt, err := h.hotReader.GetAccountCreatedAt(ctx, contractID); err == nil {
			section.CreatedAt = createdAt
		}
	}
	if section.LastActivityAt == nil {
		partial = true
	}
	if account == nil {
		partial = true
	}
	return section, partial
}

func (h *SmartWalletHandlers) GetSmartWalletBalances(ctx context.Context, contractID string) (*SmartWalletBalancesResponse, error) {
	resp := &SmartWalletBalancesResponse{
		ContractID:    contractID,
		Balances:      []SmartWalletBalanceSummary{},
		Partial:       true,
		BalanceStatus: "not_materialized",
	}

	if h.hotReader != nil {
		if addressBalances, err := h.hotReader.GetAddressBalancesCurrent(ctx, contractID); err == nil && len(addressBalances) > 0 {
			for _, b := range addressBalances {
				assetCode := derefOrDefault(b.AssetCode, "")
				if assetCode == "" && b.AssetType == "native" {
					assetCode = "XLM"
				}
				bal := SmartWalletBalanceSummary{
					AssetCode:       assetCode,
					AssetType:       b.AssetType,
					AssetIssuer:     b.AssetIssuer,
					Balance:         b.BalanceDisplay,
					Decimals:        b.Decimals,
					Symbol:          b.Symbol,
					TokenContractID: b.TokenContractID,
					BalanceSource:   b.BalanceSource,
				}
				resp.Balances = append(resp.Balances, bal)
				if b.AssetType == "native" {
					resp.NativeBalance = &bal.Balance
					resp.NativeBalanceSource = b.BalanceSource
				}
			}
			resp.Partial = false
			resp.BalanceStatus = "materialized"
			resp.Count = len(resp.Balances)
			return resp, nil
		}
	}

	if h.hotReader != nil {
		if balances, err := h.hotReader.GetServingAccountBalances(ctx, contractID); err == nil && balances != nil {
			for _, b := range balances.Balances {
				bal := SmartWalletBalanceSummary{
					AssetCode:     b.AssetCode,
					AssetType:     b.AssetType,
					AssetIssuer:   b.AssetIssuer,
					Balance:       b.Balance,
					BalanceSource: "classic_account_state",
				}
				if b.AssetType == "native" {
					resp.NativeBalance = &bal.Balance
					resp.NativeBalanceSource = "classic_account_state"
					decimals := 7
					symbol := "XLM"
					bal.Decimals = &decimals
					bal.Symbol = &symbol
				}
				resp.Balances = append(resp.Balances, bal)
			}
			resp.Partial = false
			resp.BalanceStatus = "hot_account_state"
		}
	}

	if h.unifiedReader != nil {
		if holdings, err := h.unifiedReader.GetAddressTokenPortfolio(ctx, contractID); err == nil {
			for _, holding := range holdings {
				assetCode := derefOrDefault(holding.AssetCode, derefOrDefault(holding.Symbol, "unknown"))
				assetType := "token"
				if holding.TokenType != "" {
					assetType = holding.TokenType
				}
				bal := SmartWalletBalanceSummary{
					AssetCode:       assetCode,
					AssetType:       assetType,
					AssetIssuer:     holding.AssetIssuer,
					Balance:         holding.Balance,
					Decimals:        &holding.Decimals,
					Symbol:          holding.Symbol,
					TokenContractID: holding.ContractID,
					BalanceSource:   "indexed_transfer_history",
				}
				resp.Balances = appendOrReplaceBalance(resp.Balances, bal)
			}
			if len(holdings) > 0 && resp.BalanceStatus == "not_materialized" {
				resp.BalanceStatus = "indexed_transfer_history"
			}
		}
	}

	if resp.NativeBalance == nil {
		resp.NativeBalanceSource = "unavailable"
	}
	resp.Count = len(resp.Balances)
	return resp, nil
}

func appendOrReplaceBalance(balances []SmartWalletBalanceSummary, bal SmartWalletBalanceSummary) []SmartWalletBalanceSummary {
	for i := range balances {
		if balances[i].AssetType == bal.AssetType && balances[i].AssetCode == bal.AssetCode && derefOrDefault(balances[i].TokenContractID, "") == derefOrDefault(bal.TokenContractID, "") {
			balances[i] = bal
			return balances
		}
	}
	return append(balances, bal)
}

func inferLastActivityAt(meta *ContractMetadataResponse) *string {
	return nil
}

func buildSmartWalletSignerConfig(info *SmartWalletInfo) SmartWalletSignerConfigSection {
	section := SmartWalletSignerConfigSection{
		Decoded: false,
		Source:  "wallet_detection",
		Signers: []SmartWalletSignerDetail{},
		ApprovalModel: SmartWalletApprovalModel{
			Type:    "unknown",
			Summary: "Threshold model not decoded from indexed state",
		},
	}
	if info == nil {
		return section
	}
	section.SignerCount = info.SignerCount
	for _, s := range info.Signers {
		section.Signers = append(section.Signers, SmartWalletSignerDetail{ID: s.ID, KeyType: s.KeyType, Weight: s.Weight})
	}
	if len(section.Signers) > 0 {
		section.Decoded = true
		section.Source = "wallet_detection"
		total := 0
		weighted := false
		for _, s := range section.Signers {
			if s.Weight != nil {
				total += *s.Weight
				weighted = true
			}
		}
		if weighted {
			section.TotalWeight = &total
			section.ApprovalModel.Type = "weighted_multisig"
			section.ApprovalModel.Summary = fmt.Sprintf("%d signers observed; exact threshold not decoded", len(section.Signers))
		} else if len(section.Signers) > 0 {
			section.ApprovalModel.Type = "multisig"
			section.ApprovalModel.Summary = fmt.Sprintf("%d signer(s) observed", len(section.Signers))
		}
		if len(section.Signers) > 0 {
			min := 1
			section.ApprovalModel.MinSignersEstimate = &min
		}
	}
	return section
}

func buildSmartWalletPolicies(info *SmartWalletInfo) SmartWalletPoliciesSection {
	section := SmartWalletPoliciesSection{Decoded: false, Items: []SmartWalletPolicyDetailItem{}}
	if info == nil {
		return section
	}
	for _, p := range info.Policies {
		section.Items = append(section.Items, SmartWalletPolicyDetailItem{
			PolicyType:  "wallet_policy_hint",
			Name:        p,
			Description: p,
			Status:      "heuristic",
		})
	}
	if len(section.Items) > 0 {
		section.Decoded = true
	}
	return section
}

func buildSmartWalletContractSection(meta *ContractMetadataResponse, evidence *WalletEvidence) SmartWalletContractSection {
	section := SmartWalletContractSection{InterfaceType: "smart_wallet"}
	if meta != nil {
		section.WasmHash = meta.WasmHash
		section.Deployer = meta.CreatorAddress
		section.StateSizeBytes = meta.TotalStateSizeBytes
		if meta.TotalEntries > 0 {
			v := meta.TotalEntries
			section.StorageEntries = &v
		}
		if meta.PersistentEntries > 0 {
			v := meta.PersistentEntries
			section.PersistentEntries = &v
		}
		if meta.TemporaryEntries > 0 {
			v := meta.TemporaryEntries
			section.TemporaryEntries = &v
		}
		for _, fn := range meta.ExportedFunctions {
			section.ExportedFunctions = append(section.ExportedFunctions, fn.Name)
			section.ObservedFunctions = append(section.ObservedFunctions, fn.Name)
		}
	}
	if section.WasmHash == nil && evidence != nil && evidence.WasmHash != "" {
		section.WasmHash = &evidence.WasmHash
	}
	if len(section.ObservedFunctions) == 0 && evidence != nil && len(evidence.ObservedFunctions) > 0 {
		section.ObservedFunctions = append(section.ObservedFunctions, evidence.ObservedFunctions...)
	}
	section.ExportedFunctions = dedupeStrings(section.ExportedFunctions)
	section.ObservedFunctions = dedupeStrings(section.ObservedFunctions)
	return section
}

func smartWalletRentStatus(meta *ContractMetadataResponse) string {
	if meta == nil {
		return "unknown"
	}
	if meta.EstimatedMonthlyRentStroops != nil || meta.TotalEntries > 0 {
		return "healthy"
	}
	return "unknown"
}

func (h *SmartWalletHandlers) buildSmartWalletActivity(ctx context.Context, contractID string) (SmartWalletActivitySummary, []SmartWalletTimelineItem, bool) {
	summary := SmartWalletActivitySummary{
		ActiveWindows:   []string{},
		CommonFunctions: []SmartWalletCommonFunction{},
		CommonProtocols: []SmartWalletCommonProtocol{},
	}
	partial := false

	calls, err := h.loadWalletRecentCalls(ctx, contractID, 200)
	if err != nil {
		return summary, nil, true
	}
	if len(calls) == 0 {
		return summary, []SmartWalletTimelineItem{}, true
	}

	now := time.Now().UTC()
	funcCounts := map[string]int64{}
	callerSet := map[string]struct{}{}
	hourCounts := map[int]int{}
	tx7d := map[string]bool{}
	success7d := map[string]bool{}
	approvalTx30d := map[string]struct{}{}
	policyTx30d := map[string]struct{}{}
	protocolTx30d := map[string]struct{}{}
	timeline := make([]SmartWalletTimelineItem, 0, 20)
	txSummaries, _ := h.fetchWalletTxSummaries(ctx, collectWalletTxHashes(calls))

	for _, call := range calls {
		fn := strings.ToLower(strings.TrimSpace(derefOrDefault(call.FunctionName, "")))
		funcCounts[fn]++
		if call.SourceAccount != "" {
			callerSet[call.SourceAccount] = struct{}{}
		}
		ts, err := time.Parse(time.RFC3339, call.ClosedAt)
		if err == nil {
			hourCounts[ts.UTC().Hour()]++
			if now.Sub(ts) <= 7*24*time.Hour {
				tx7d[call.TransactionHash] = true
				if call.Successful {
					success7d[call.TransactionHash] = true
				}
			}
			if now.Sub(ts) <= 30*24*time.Hour {
				txSummary := txSummaries[call.TransactionHash]
				switch txSummary.TxType {
				case "smart_wallet_multisig_approval":
					approvalTx30d[call.TransactionHash] = struct{}{}
				case "smart_wallet_policy_update":
					policyTx30d[call.TransactionHash] = struct{}{}
				case "wallet_mediated_protocol_interaction":
					protocolTx30d[call.TransactionHash] = struct{}{}
				default:
					if isApprovalFunction(fn) {
						approvalTx30d[call.TransactionHash] = struct{}{}
					}
					if isPolicyUpdateFunction(fn) {
						policyTx30d[call.TransactionHash] = struct{}{}
					}
					if isProtocolInteractionFunction(fn) {
						protocolTx30d[call.TransactionHash] = struct{}{}
					}
				}
			}
		}
		if len(timeline) < 20 {
			timeline = append(timeline, classifyWalletTimeline(call, txSummaries[call.TransactionHash]))
		}
	}

	summary.TotalTransactions7d = int64(len(tx7d))
	summary.SuccessfulTransactions7d = int64(len(success7d))
	summary.PolicyUpdates30d = int64(len(policyTx30d))
	summary.Approvals30d = int64(len(approvalTx30d))
	summary.ProtocolInteractions30d = int64(len(protocolTx30d))
	summary.UniqueCallers30d = int64(len(callerSet))
	summary.ActiveWindows = summarizeActiveWindows(hourCounts)
	summary.CommonFunctions = topWalletFunctions(funcCounts, 5)

	return summary, timeline, partial
}

func (h *SmartWalletHandlers) loadWalletRecentCalls(ctx context.Context, contractID string, limit int) ([]ContractCallRecord, error) {
	if h.hotReader != nil {
		calls, _, _, err := h.hotReader.GetServingContractRecentCalls(ctx, contractID, limit, nil, "desc")
		if err == nil && len(calls) > 0 {
			return calls, nil
		}
	}
	if h.unifiedReader != nil {
		calls, _, _, err := h.unifiedReader.GetRecentContractCallsWithCursor(ctx, contractID, limit, nil, "desc")
		if err == nil {
			return calls, nil
		}
	}
	return nil, fmt.Errorf("recent calls unavailable")
}

func classifyWalletTimeline(call ContractCallRecord, txSummary smartWalletTxSummary) SmartWalletTimelineItem {
	fn := strings.ToLower(strings.TrimSpace(derefOrDefault(call.FunctionName, "")))
	item := SmartWalletTimelineItem{
		Type:           "activity",
		Timestamp:      call.ClosedAt,
		LedgerSequence: call.LedgerSequence,
		TxHash:         call.TransactionHash,
		Successful:     call.Successful,
		Actors:         compactActors(call.SourceAccount),
		Evidence:       []string{fmt.Sprintf("function:%s", fn)},
		Title:          humanizeWalletFunctionTitle(fn),
		Description:    humanizeWalletFunctionDescription(fn),
	}
	if txSummary.TxType != "" {
		item.Evidence = append(item.Evidence, fmt.Sprintf("semantic_tx_type:%s", txSummary.TxType))
	}
	if txSummary.Summary.Description != "" {
		item.Description = txSummary.Summary.Description
	}
	switch txSummary.TxType {
	case "smart_wallet_multisig_approval":
		item.Type = "approval"
		item.Subtype = nonEmpty(fn, "multisig_approval")
		item.Title = "Transfer approved"
		if txSummary.Summary.Description == "" {
			item.Description = "A signer approved a pending wallet action"
		}
		return item
	case "smart_wallet_policy_update":
		item.Type = "policy_update"
		item.Subtype = nonEmpty(fn, "policy_updated")
		item.Title = humanizePolicyTitle(fn)
		if txSummary.Summary.Description == "" {
			item.Description = humanizePolicyDescription(fn)
		}
		return item
	case "wallet_mediated_protocol_interaction":
		item.Type = "execution"
		item.Subtype = "protocol_interaction"
		item.Title = "Protocol interaction executed"
		if txSummary.Summary.Description == "" {
			item.Description = "The smart wallet executed a protocol interaction"
		}
		return item
	case "smart_wallet_transfer":
		item.Type = "execution"
		item.Subtype = "wallet_transfer"
		item.Title = "Wallet transfer executed"
		if txSummary.Summary.Description == "" {
			item.Description = "The smart wallet executed a transfer"
		}
		return item
	case "smart_wallet_swap":
		item.Type = "execution"
		item.Subtype = "wallet_swap"
		item.Title = "Wallet swap executed"
		if txSummary.Summary.Description == "" {
			item.Description = "The smart wallet executed a swap"
		}
		return item
	}
	switch {
	case fn == "add_signer":
		item.Type = "policy_update"
		item.Subtype = "signer_added"
	case fn == "remove_signer" || fn == "revoke_signer":
		item.Type = "policy_update"
		item.Subtype = "signer_removed"
	case fn == "set_threshold" || fn == "update_signer":
		item.Type = "policy_update"
		item.Subtype = "threshold_changed"
	case isApprovalFunction(fn):
		item.Type = "approval"
		item.Subtype = "multisig_approval"
	case fn == "execute":
		item.Type = "execution"
		item.Subtype = "wallet_execution"
	case isPolicyUpdateFunction(fn):
		item.Type = "policy_update"
		item.Subtype = "policy_updated"
	}
	return item
}

func humanizeWalletFunctionTitle(fn string) string {
	switch fn {
	case "add_signer":
		return "Signer added"
	case "remove_signer", "revoke_signer":
		return "Signer removed"
	case "approve":
		return "Action approved"
	case "execute":
		return "Wallet execution"
	case "set_threshold":
		return "Threshold updated"
	default:
		if fn == "" {
			return "Wallet activity"
		}
		return strings.ReplaceAll(strings.Title(strings.ReplaceAll(fn, "_", " ")), "  ", " ")
	}
}

func humanizeWalletFunctionDescription(fn string) string {
	switch fn {
	case "add_signer":
		return "Added a new signer to the wallet"
	case "remove_signer", "revoke_signer":
		return "Removed an existing signer from the wallet"
	case "approve":
		return "A signer approved a pending wallet action"
	case "execute":
		return "The wallet executed a contract action"
	case "set_threshold":
		return "The wallet approval threshold was updated"
	default:
		if fn == "" {
			return "Observed wallet-related contract activity"
		}
		return fmt.Sprintf("Observed wallet function call: %s", fn)
	}
}

func summarizeActiveWindows(hourCounts map[int]int) []string {
	if len(hourCounts) == 0 {
		return []string{}
	}
	type hc struct{ hour, count int }
	list := make([]hc, 0, len(hourCounts))
	for hour, count := range hourCounts {
		list = append(list, hc{hour, count})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].count == list[j].count {
			return list[i].hour < list[j].hour
		}
		return list[i].count > list[j].count
	})
	if len(list) > 3 {
		list = list[:3]
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		out = append(out, fmt.Sprintf("%02d:00-%02d:00 UTC", e.hour, (e.hour+1)%24))
	}
	return out
}

func topWalletFunctions(counts map[string]int64, limit int) []SmartWalletCommonFunction {
	type kv struct {
		name  string
		count int64
	}
	items := make([]kv, 0, len(counts))
	for name, count := range counts {
		if name == "" {
			continue
		}
		items = append(items, kv{name, count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].name < items[j].name
		}
		return items[i].count > items[j].count
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]SmartWalletCommonFunction, 0, len(items))
	for _, item := range items {
		out = append(out, SmartWalletCommonFunction{Name: item.name, Count: item.count})
	}
	return out
}

func filterWalletFunctions(functions []string) []string {
	allowed := map[string]struct{}{
		"add_signer": {}, "remove_signer": {}, "set_signer": {}, "get_signers": {},
		"update_signer": {}, "revoke_signer": {}, "approve": {}, "execute": {},
		"set_threshold": {}, "install_plugin": {}, "uninstall_plugin": {},
		"add_context_rule": {}, "remove_guardian": {}, "add_guardian": {}, "recover": {},
	}
	out := []string{}
	for _, fn := range functions {
		if _, ok := allowed[strings.ToLower(fn)]; ok {
			out = append(out, fn)
		}
	}
	sort.Strings(out)
	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func isApprovalFunction(fn string) bool {
	switch fn {
	case "approve", "confirm", "submit_approval":
		return true
	default:
		return false
	}
}

func isPolicyUpdateFunction(fn string) bool {
	switch fn {
	case "add_signer", "remove_signer", "set_signer", "update_signer", "revoke_signer", "set_threshold", "install_plugin", "uninstall_plugin", "add_context_rule", "remove_guardian", "add_guardian", "recover":
		return true
	default:
		return false
	}
}

func isProtocolInteractionFunction(fn string) bool {
	return fn == "execute"
}

func compactActors(actor string) []string {
	if strings.TrimSpace(actor) == "" {
		return nil
	}
	return []string{actor}
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func (h *SmartWalletHandlers) fetchWalletTxSummaries(ctx context.Context, txHashes []string) (map[string]smartWalletTxSummary, error) {
	out := make(map[string]smartWalletTxSummary, len(txHashes))
	if h.hotReader == nil || len(txHashes) == 0 {
		return out, nil
	}
	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT tx_hash, tx_type, summary_text, summary_json, primary_contract_id
		FROM serving.sv_transactions_recent
		WHERE tx_hash = ANY($1)
	`, pq.Array(txHashes))
	if err != nil {
		return out, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			hash            string
			txType          string
			summaryText     string
			summaryJSON     string
			primaryContract sql.NullString
		)
		if err := rows.Scan(&hash, &txType, &summaryText, &summaryJSON, &primaryContract); err != nil {
			return out, err
		}
		rec := smartWalletTxSummary{TxType: txType}
		if summaryJSON != "" {
			_ = json.Unmarshal([]byte(summaryJSON), &rec.Summary)
		}
		if rec.Summary.Description == "" {
			rec.Summary = buildFallbackRecentTxSummary(txType, summaryText, primaryContract)
		}
		if primaryContract.Valid {
			rec.Contract = &primaryContract.String
		}
		out[hash] = rec
	}
	return out, rows.Err()
}

func collectWalletTxHashes(calls []ContractCallRecord) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		if call.TransactionHash == "" {
			continue
		}
		if _, ok := seen[call.TransactionHash]; ok {
			continue
		}
		seen[call.TransactionHash] = struct{}{}
		out = append(out, call.TransactionHash)
	}
	return out
}

func humanizePolicyTitle(fn string) string {
	switch fn {
	case "add_signer":
		return "Signer added"
	case "remove_signer", "revoke_signer":
		return "Signer removed"
	case "set_threshold":
		return "Threshold changed"
	default:
		return "Wallet policy updated"
	}
}

func humanizePolicyDescription(fn string) string {
	switch fn {
	case "add_signer":
		return "Added a new signer to the wallet"
	case "remove_signer", "revoke_signer":
		return "Removed a signer from the wallet"
	case "set_threshold":
		return "Changed the wallet approval threshold"
	default:
		return "Updated the wallet policy configuration"
	}
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func dedupeStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
