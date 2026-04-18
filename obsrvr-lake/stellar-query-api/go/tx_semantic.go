package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// SemanticTransactionResponse is an actor-centric semantic view of a transaction.
type SemanticTransactionResponse struct {
	Transaction    SemanticTransactionInfo           `json:"transaction"`
	Classification SemanticTransactionClassification `json:"classification"`
	Actors         []SemanticActor                   `json:"actors"`
	Assets         SemanticAssetContext              `json:"assets"`
	Operations     []DecodedOperation                `json:"operations"`
	Events         []UnifiedEvent                    `json:"events"`
	Diffs          *TxDiffs                          `json:"diffs,omitempty"`
	CallGraph      []SemanticCallEdge                `json:"call_graph,omitempty"`
	LegacySummary  TxSummary                         `json:"legacy_summary"`
}

type SemanticTransactionInfo struct {
	TxHash          string  `json:"tx_hash"`
	LedgerSequence  int64   `json:"ledger_sequence"`
	ClosedAt        string  `json:"closed_at"`
	Successful      bool    `json:"successful"`
	Fee             int64   `json:"fee"`
	OperationCount  int     `json:"operation_count"`
	SourceAccount   *string `json:"source_account,omitempty"`
	AccountSequence *int64  `json:"account_sequence,omitempty"`
	MaxFee          *int64  `json:"max_fee,omitempty"`
}

type SemanticTransactionClassification struct {
	TxType             string   `json:"tx_type"`
	Subtype            string   `json:"subtype,omitempty"`
	Confidence         string   `json:"confidence"`
	OperationTypes     []string `json:"operation_types,omitempty"`
	WalletInvolved     bool     `json:"wallet_involved"`
	EffectiveActorType string   `json:"effective_actor_type,omitempty"`
}

type SemanticActor struct {
	ActorID    string                 `json:"actor_id"`
	ActorType  string                 `json:"actor_type"`
	Label      *string                `json:"label,omitempty"`
	Roles      []string               `json:"roles"`
	Wallet     *SemanticWalletContext `json:"wallet,omitempty"`
	ContractID *string                `json:"contract_id,omitempty"`
}

type SemanticWalletContext struct {
	WalletType     string             `json:"wallet_type,omitempty"`
	Implementation string             `json:"implementation,omitempty"`
	Confidence     float64            `json:"confidence,omitempty"`
	HasCheckAuth   bool               `json:"has_check_auth,omitempty"`
	SignerCount    int                `json:"signer_count,omitempty"`
	Signers        []WalletSignerInfo `json:"signers,omitempty"`
}

type SemanticAssetContext struct {
	Sent      *SemanticAssetMovement  `json:"sent,omitempty"`
	Received  *SemanticAssetMovement  `json:"received,omitempty"`
	Movements []SemanticAssetMovement `json:"movements,omitempty"`
	Path      []string                `json:"path,omitempty"`
}

type SemanticAssetMovement struct {
	Amount   string  `json:"amount"`
	Asset    string  `json:"asset"`
	From     *string `json:"from,omitempty"`
	To       *string `json:"to,omitempty"`
	Contract *string `json:"contract,omitempty"`
	Kind     string  `json:"kind,omitempty"`
}

type SemanticCallEdge struct {
	From       string `json:"from"`
	To         string `json:"to"`
	Function   string `json:"function"`
	Depth      int    `json:"depth"`
	Order      int    `json:"order"`
	Successful bool   `json:"successful"`
}

// HandleSemanticTransaction returns an actor-centric semantic view of a transaction.
// @Summary Get semantic transaction view
// @Description Returns an actor-centric semantic transaction response with classification, actors, and asset movement context.
// @Tags Transactions
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} SemanticTransactionResponse "Semantic transaction"
// @Failure 400 {object} map[string]interface{} "Missing transaction hash"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/semantic [get]
func (h *DecodeHandlers) HandleSemanticTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	options := SemanticBuildOptions{
		DeepEnrichment: r.URL.Query().Get("include_deep") == "true",
	}

	decoded, err := h.getTransactionForSemantic(r.Context(), txHash, options)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		respondError(w, err.Error(), status)
		return
	}

	if decoded.OpCount == 0 && len(decoded.Events) == 0 {
		respondError(w, "transaction not found", http.StatusNotFound)
		return
	}

	semantic, err := h.BuildSemanticTransaction(r.Context(), decoded, options)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, semantic)
}

type SemanticBuildOptions struct {
	DeepEnrichment bool
}

// BuildSemanticTransaction converts a decoded transaction into an actor-centric semantic view.
func (h *DecodeHandlers) BuildSemanticTransaction(ctx context.Context, decoded *DecodedTransaction, options SemanticBuildOptions) (*SemanticTransactionResponse, error) {
	walletCache := make(map[string]*SmartWalletInfo)
	labelCache := make(map[string]*semanticLabelInfo)
	builder := newSemanticActorBuilder()
	var diffs *TxDiffs
	var callGraph []ContractCall
	if options.DeepEnrichment {
		diffs = h.getSemanticTxDiffs(ctx, decoded.TxHash)
		callGraph = h.getSemanticCallGraph(ctx, decoded.TxHash)
	}

	// Token contracts observed in CAP-67 events are a good first token-contract signal.
	tokenContracts := map[string]bool{}
	heuristicWalletContracts := map[string]bool{}
	for _, event := range decoded.Events {
		if event.ContractID != nil && (hasValue(event.TokenName) || hasValue(event.TokenSymbol) || hasValue(event.AssetCode) || event.TokenType != nil) {
			tokenContracts[*event.ContractID] = true
		}
	}
	for _, op := range decoded.Operations {
		if op.ContractID != nil && op.FunctionName != nil {
			fn := strings.ToLower(*op.FunctionName)
			args := strings.ToLower(derefOrDefault(op.ArgumentsJSON, ""))
			if isLikelySmartWalletFunction(fn, args) {
				heuristicWalletContracts[*op.ContractID] = true
			}
		}
	}

	addActor := func(actorID, role string) {
		if actorID == "" {
			return
		}
		actorType, walletCtx, label, contractID := h.resolveSemanticActor(ctx, actorID, tokenContracts, heuristicWalletContracts, walletCache, labelCache, options)
		builder.add(actorID, actorType, role, label, walletCtx, contractID)
	}

	submitter := ""
	if decoded.SourceAccount != nil && *decoded.SourceAccount != "" {
		submitter = *decoded.SourceAccount
	} else if len(decoded.Operations) > 0 && decoded.Operations[0].SourceAccount != "" {
		submitter = decoded.Operations[0].SourceAccount
	}
	if submitter != "" {
		addActor(submitter, "submitter")
		addActor(submitter, "fee_payer")
	}

	for _, op := range decoded.Operations {
		if op.SourceAccount != "" {
			addActor(op.SourceAccount, "counterparty")
			if op.IsSorobanOp {
				addActor(op.SourceAccount, "invoker")
			} else {
				addActor(op.SourceAccount, "effective_actor")
			}
		}
		if op.Destination != nil {
			addActor(*op.Destination, "receiver")
			addActor(*op.Destination, "beneficiary")
		}
		if op.ContractID != nil {
			addActor(*op.ContractID, "protocol")
		}
	}

	for _, event := range decoded.Events {
		if event.From != nil {
			addActor(*event.From, "sender")
			addActor(*event.From, "asset_owner")
			addActor(*event.From, "effective_actor")
		}
		if event.To != nil {
			addActor(*event.To, "receiver")
			addActor(*event.To, "beneficiary")
		}
		if event.ContractID != nil {
			addActor(*event.ContractID, "protocol")
			if info := walletCache[*event.ContractID]; (info != nil && info.IsSmartWallet) || heuristicWalletContracts[*event.ContractID] {
				addActor(*event.ContractID, "effective_actor")
				addActor(*event.ContractID, "asset_owner")
			}
		}
	}

	for _, op := range decoded.Operations {
		if op.ContractID != nil {
			if info := walletCache[*op.ContractID]; (info != nil && info.IsSmartWallet) || heuristicWalletContracts[*op.ContractID] {
				addActor(*op.ContractID, "effective_actor")
				addActor(*op.ContractID, "asset_owner")
			}
		}
	}

	// If no better effective actor was found, default to submitter/source account.
	if submitter != "" && !builder.hasRole("effective_actor") {
		addActor(submitter, "effective_actor")
	}

	actors := builder.list()
	classification := classifySemanticTransaction(decoded, actors, diffs, callGraph)
	assets := deriveSemanticAssets(decoded, diffs)

	return &SemanticTransactionResponse{
		Transaction: SemanticTransactionInfo{
			TxHash:          decoded.TxHash,
			LedgerSequence:  decoded.LedgerSeq,
			ClosedAt:        decoded.ClosedAt,
			Successful:      decoded.Successful,
			Fee:             decoded.Fee,
			OperationCount:  decoded.OpCount,
			SourceAccount:   decoded.SourceAccount,
			AccountSequence: decoded.AccountSequence,
			MaxFee:          decoded.MaxFee,
		},
		Classification: *classification,
		Actors:         actors,
		Assets:         *assets,
		Operations:     decoded.Operations,
		Events:         decoded.Events,
		Diffs:          diffs,
		CallGraph:      toSemanticCallEdges(callGraph),
		LegacySummary:  decoded.Summary,
	}, nil
}

func (h *DecodeHandlers) resolveSemanticActor(ctx context.Context, actorID string, tokenContracts map[string]bool, heuristicWalletContracts map[string]bool, walletCache map[string]*SmartWalletInfo, labelCache map[string]*semanticLabelInfo, options SemanticBuildOptions) (string, *SemanticWalletContext, *string, *string) {
	if actorID == "" {
		return "unknown_actor", nil, nil, nil
	}

	switch {
	case strings.HasPrefix(actorID, "G"):
		if meta := h.lookupSemanticLabel(ctx, actorID, labelCache); meta != nil && meta.ActorType != "" {
			return meta.ActorType, nil, meta.Label, nil
		}
		return "classic_account", nil, nil, nil
	case strings.HasPrefix(actorID, "C"):
		meta := h.lookupSemanticLabel(ctx, actorID, labelCache)
		if options.DeepEnrichment && walletCache[actorID] == nil {
			walletHandlers := NewSmartWalletHandlers(h.hotReader, h.coldReader, h.bronzeCold)
			info, err := walletHandlers.GetSmartWalletInfo(ctx, actorID)
			if err == nil {
				walletCache[actorID] = info
			} else {
				walletCache[actorID] = &SmartWalletInfo{ContractID: actorID}
			}
		}

		if info := walletCache[actorID]; info != nil && info.IsSmartWallet {
			label := meta.Label
			if label == nil {
				derived := info.Implementation
				if derived == "" {
					derived = info.WalletType
				}
				label = stringPtr(derived)
			}
			return "smart_wallet", &SemanticWalletContext{
				WalletType:     info.WalletType,
				Implementation: info.Implementation,
				Confidence:     info.Confidence,
				HasCheckAuth:   info.HasCheckAuth,
				SignerCount:    info.SignerCount,
				Signers:        info.Signers,
			}, label, &actorID
		}

		if heuristicWalletContracts[actorID] {
			label := meta.Label
			if label == nil {
				label = stringPtr("smart_wallet")
			}
			return "smart_wallet", &SemanticWalletContext{
				WalletType:     "heuristic",
				Implementation: "function_inference",
				Confidence:     0.6,
			}, label, &actorID
		}

		if tokenContracts[actorID] {
			if meta != nil && meta.Label != nil {
				return "token_contract", nil, meta.Label, &actorID
			}
			return "token_contract", nil, nil, &actorID
		}
		if meta != nil {
			actorType := meta.ActorType
			if actorType == "" {
				actorType = "contract"
			}
			return actorType, nil, meta.Label, &actorID
		}
		return "contract", nil, nil, &actorID
	default:
		return "unknown_actor", nil, nil, nil
	}
}

func classifySemanticTransaction(decoded *DecodedTransaction, actors []SemanticActor, diffs *TxDiffs, callGraph []ContractCall) *SemanticTransactionClassification {
	operationTypes := make([]string, 0, len(decoded.Operations))
	seenTypes := map[string]bool{}
	walletInvolved := false
	effectiveActorType := ""
	nonWalletProtocol := false

	for _, actor := range actors {
		if actor.ActorType == "smart_wallet" {
			walletInvolved = true
		}
		if effectiveActorType == "" && hasRole(actor.Roles, "effective_actor") {
			effectiveActorType = actor.ActorType
		}
		if hasRole(actor.Roles, "protocol") && actor.ActorType != "smart_wallet" {
			nonWalletProtocol = true
		}
	}
	if !nonWalletProtocol {
		nonWalletProtocol = hasExternalProtocolCall(callGraph, actors)
	}

	for _, op := range decoded.Operations {
		if !seenTypes[op.TypeName] {
			operationTypes = append(operationTypes, op.TypeName)
			seenTypes[op.TypeName] = true
		}
	}
	if len(operationTypes) == 0 && decoded.Summary.Type != "" {
		operationTypes = append(operationTypes, decoded.Summary.Type)
	}

	txType := "unknown"
	confidence := "medium"
	subtype := ""

	if walletInvolved {
		if inferredType, inferredSubtype, inferredConfidence, ok := inferSmartWalletIntent(decoded, nonWalletProtocol); ok {
			txType = inferredType
			subtype = inferredSubtype
			confidence = inferredConfidence
		}
	}

	if txType == "unknown" {
		switch decoded.Summary.Type {
		case "payment":
			txType = "simple_payment"
			confidence = "high"
		case "path_payment":
			txType = "path_payment"
			confidence = "high"
		case "swap":
			if walletInvolved {
				txType = "smart_wallet_swap"
			} else {
				txType = "dex_swap"
			}
			confidence = "high"
		case "transfer":
			if walletInvolved {
				if nonWalletProtocol {
					txType = "wallet_mediated_protocol_interaction"
				} else {
					txType = "smart_wallet_transfer"
				}
			} else {
				txType = "token_transfer"
			}
			confidence = "high"
		case "contract_call":
			if walletInvolved && nonWalletProtocol {
				txType = "wallet_mediated_protocol_interaction"
			} else if walletInvolved {
				txType = "smart_wallet_activity"
			} else {
				txType = "contract_call"
			}
			confidence = "high"
		case "change_trust":
			txType = "trustline_change"
			confidence = "high"
		case "create_account":
			txType = "account_funding"
			confidence = "high"
		case "account_merge":
			txType = "account_merge"
			confidence = "high"
		case "manage_sell_offer", "manage_buy_offer", "create_passive_sell_offer":
			txType = "offer_management"
			subtype = decoded.Summary.Type
			confidence = "high"
		case "multi_transfer":
			txType = "multi_transfer"
			confidence = "medium"
		case "multi_op":
			txType = "composite_transaction"
			confidence = "medium"
		default:
			if len(decoded.Events) > 0 {
				if walletInvolved {
					txType = "smart_wallet_activity"
				} else if hasSorobanOperation(decoded.Operations) {
					txType = "contract_call"
				} else {
					txType = decoded.Summary.Type
				}
			} else if hasSorobanOperation(decoded.Operations) {
				if walletInvolved {
					txType = "smart_wallet_activity"
				} else {
					txType = "contract_call"
				}
			}
		}
	}

	if walletInvolved {
		if hasSwapEvidence(decoded, diffs) {
			txType = "smart_wallet_swap"
		} else if hasTransferEvidence(decoded, diffs) {
			if nonWalletProtocol {
				txType = "wallet_mediated_protocol_interaction"
			} else {
				txType = "smart_wallet_transfer"
			}
		} else if txType == "contract_call" {
			if nonWalletProtocol {
				txType = "wallet_mediated_protocol_interaction"
			} else {
				txType = "smart_wallet_activity"
			}
		}
	}

	if txType == "unknown" && len(operationTypes) == 1 {
		txType = operationTypes[0]
	}

	return &SemanticTransactionClassification{
		TxType:             txType,
		Subtype:            subtype,
		Confidence:         confidence,
		OperationTypes:     operationTypes,
		WalletInvolved:     walletInvolved,
		EffectiveActorType: effectiveActorType,
	}
}

func deriveSemanticAssets(decoded *DecodedTransaction, diffs *TxDiffs) *SemanticAssetContext {
	ctx := &SemanticAssetContext{}

	if decoded.Summary.Swap != nil {
		ctx.Sent = &SemanticAssetMovement{
			Amount: decoded.Summary.Swap.SoldAmount,
			Asset:  decoded.Summary.Swap.SoldAsset,
			From:   emptyStringPtr(decoded.Summary.Swap.Trader),
			Kind:   "sent",
		}
		ctx.Received = &SemanticAssetMovement{
			Amount: decoded.Summary.Swap.BoughtAmount,
			Asset:  decoded.Summary.Swap.BoughtAsset,
			To:     emptyStringPtr(decoded.Summary.Swap.Trader),
			Kind:   "received",
		}
		ctx.Path = []string{decoded.Summary.Swap.SoldAsset, decoded.Summary.Swap.BoughtAsset}
	}

	if decoded.Summary.Transfer != nil {
		ctx.Sent = &SemanticAssetMovement{
			Amount: decoded.Summary.Transfer.Amount,
			Asset:  decoded.Summary.Transfer.Asset,
			From:   emptyStringPtr(decoded.Summary.Transfer.From),
			To:     emptyStringPtr(decoded.Summary.Transfer.To),
			Kind:   "sent",
		}
	}
	if decoded.Summary.Mint != nil {
		ctx.Received = &SemanticAssetMovement{
			Amount: decoded.Summary.Mint.Amount,
			Asset:  decoded.Summary.Mint.Asset,
			To:     emptyStringPtr(decoded.Summary.Mint.To),
			Kind:   "mint",
		}
	}
	if decoded.Summary.Burn != nil {
		ctx.Sent = &SemanticAssetMovement{
			Amount: decoded.Summary.Burn.Amount,
			Asset:  decoded.Summary.Burn.Asset,
			From:   emptyStringPtr(decoded.Summary.Burn.From),
			Kind:   "burn",
		}
	}

	for _, event := range decoded.Events {
		movement := SemanticAssetMovement{
			Amount: derefOrDefault(event.Amount, ""),
			Asset:  semanticAssetName(event),
			From:   event.From,
			To:     event.To,
			Kind:   event.EventType,
		}
		if event.ContractID != nil {
			movement.Contract = event.ContractID
		}
		ctx.Movements = append(ctx.Movements, movement)
	}

	if ctx.Sent == nil && len(ctx.Movements) > 0 {
		for _, movement := range ctx.Movements {
			if movement.Kind == "transfer" || movement.Kind == "burn" {
				m := movement
				ctx.Sent = &m
				break
			}
		}
	}
	if ctx.Received == nil && len(ctx.Movements) > 0 {
		for _, movement := range ctx.Movements {
			if movement.Kind == "transfer" || movement.Kind == "mint" {
				m := movement
				ctx.Received = &m
				break
			}
		}
	}

	if diffs != nil {
		for _, change := range diffs.BalanceChanges {
			if change.Delta == "0" || change.Delta == "+0" || change.Delta == "-0" || change.Delta == "0.0000000" {
				continue
			}
			movement := SemanticAssetMovement{
				Amount:   strings.TrimPrefix(change.Delta, "+"),
				Asset:    change.AssetCode,
				From:     nil,
				To:       nil,
				Contract: nil,
				Kind:     "balance_change",
			}
			addr := change.Address
			if strings.HasPrefix(change.Delta, "-") {
				movement.From = &addr
				movement.Amount = strings.TrimPrefix(change.Delta, "-")
			} else {
				movement.To = &addr
			}
			ctx.Movements = append(ctx.Movements, movement)
		}
	}

	if ctx.Sent == nil || ctx.Received == nil {
		ctx.inferPrimaryMovementsFromDiffs(diffs)
	}

	if len(ctx.Path) == 0 && ctx.Sent != nil && ctx.Received != nil && ctx.Sent.Asset != ctx.Received.Asset {
		ctx.Path = []string{ctx.Sent.Asset, ctx.Received.Asset}
	}

	return ctx
}

func semanticAssetName(event UnifiedEvent) string {
	if event.TokenSymbol != nil && *event.TokenSymbol != "" {
		return *event.TokenSymbol
	}
	if event.AssetCode != nil && *event.AssetCode != "" {
		return *event.AssetCode
	}
	if event.TokenName != nil && *event.TokenName != "" {
		return *event.TokenName
	}
	if event.ContractID != nil {
		return *event.ContractID
	}
	return "unknown_asset"
}

func hasSorobanOperation(ops []DecodedOperation) bool {
	for _, op := range ops {
		if op.IsSorobanOp {
			return true
		}
	}
	return false
}

type semanticActorBuilder struct {
	actors map[string]*SemanticActor
}

func newSemanticActorBuilder() *semanticActorBuilder {
	return &semanticActorBuilder{actors: map[string]*SemanticActor{}}
}

func (b *semanticActorBuilder) add(actorID, actorType, role string, label *string, wallet *SemanticWalletContext, contractID *string) {
	if actorID == "" {
		return
	}
	actor, ok := b.actors[actorID]
	if !ok {
		actor = &SemanticActor{
			ActorID:    actorID,
			ActorType:  actorType,
			Roles:      []string{},
			Label:      label,
			Wallet:     wallet,
			ContractID: contractID,
		}
		b.actors[actorID] = actor
	}
	if actor.ActorType == "unknown_actor" && actorType != "unknown_actor" {
		actor.ActorType = actorType
	}
	if actor.Label == nil && label != nil {
		actor.Label = label
	}
	if actor.Wallet == nil && wallet != nil {
		actor.Wallet = wallet
	}
	if actor.ContractID == nil && contractID != nil {
		actor.ContractID = contractID
	}
	if !hasRole(actor.Roles, role) {
		actor.Roles = append(actor.Roles, role)
	}
}

func (b *semanticActorBuilder) hasRole(role string) bool {
	for _, actor := range b.actors {
		if hasRole(actor.Roles, role) {
			return true
		}
	}
	return false
}

func (b *semanticActorBuilder) list() []SemanticActor {
	result := make([]SemanticActor, 0, len(b.actors))
	for _, actor := range b.actors {
		sort.Strings(actor.Roles)
		result = append(result, *actor)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ActorID < result[j].ActorID
	})
	return result
}

func hasRole(roles []string, role string) bool {
	for _, existing := range roles {
		if existing == role {
			return true
		}
	}
	return false
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func emptyStringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefOrDefault(s *string, fallback string) string {
	if s == nil || *s == "" {
		return fallback
	}
	return *s
}

func hasValue(s *string) bool {
	return s != nil && *s != ""
}

func inferSmartWalletIntent(decoded *DecodedTransaction, nonWalletProtocol bool) (string, string, string, bool) {
	for _, op := range decoded.Operations {
		if !op.IsSorobanOp || op.FunctionName == nil {
			continue
		}
		fn := strings.ToLower(*op.FunctionName)
		args := strings.ToLower(derefOrDefault(op.ArgumentsJSON, ""))

		switch {
		case isSmartWalletDeploymentFunction(fn, args):
			return "smart_wallet_deployment", fn, "high", true
		case fn == "swap" || fn == "swap_exact_in" || fn == "swap_exact_out" || strings.Contains(fn, "swap") || isProtocolSwapHint(fn, args):
			if nonWalletProtocol || isProtocolRouterHint(fn, args) {
				return "wallet_mediated_protocol_interaction", "swap", "high", true
			}
			return "smart_wallet_swap", fn, "high", true
		case fn == "transfer" || fn == "transfer_from" || strings.Contains(fn, "payment") || strings.Contains(fn, "send") || isTokenTransferHint(fn, args):
			if nonWalletProtocol || isProtocolRouterHint(fn, args) {
				return "wallet_mediated_protocol_interaction", "transfer", "high", true
			}
			return "smart_wallet_transfer", fn, "high", true
		case isSignerRotationFunction(fn):
			return "smart_wallet_policy_update", "signer_rotation", "high", true
		case isSignerAddRemoveFunction(fn):
			return "smart_wallet_policy_update", fn, "high", true
		case isSmartWalletPolicyFunction(fn) || containsWalletPolicyHint(args):
			return "smart_wallet_policy_update", fn, "high", true
		case isSmartWalletApprovalFunction(fn):
			return "smart_wallet_multisig_approval", fn, "high", true
		case nonWalletProtocol || isProtocolRouterHint(fn, args):
			return "wallet_mediated_protocol_interaction", fn, "medium", true
		}
	}
	return "", "", "", false
}

func isSmartWalletPolicyFunction(fn string) bool {
	switch fn {
	case "allow_signing_key", "add_signer", "remove_signer", "set_signer", "update_signer",
		"set_policy", "update_policy", "set_threshold", "set_limits", "set_spend_limit",
		"set_admin", "upgrade", "set_guardian", "set_owner", "set_recovery", "rotate_key",
		"set_hook", "remove_hook", "set_timelock", "set_daily_limit", "set_session_key":
		return true
	default:
		return false
	}
}

func isSmartWalletApprovalFunction(fn string) bool {
	switch fn {
	case "approve", "approve_call", "confirm", "confirm_tx", "sign", "sign_tx", "submit_approval", "execute_approved":
		return true
	default:
		return false
	}
}

func containsWalletPolicyHint(args string) bool {
	if args == "" {
		return false
	}
	hints := []string{"signer", "threshold", "policy", "guardian", "owner", "recovery", "weight", "spend_limit", "signing_key", "session_key", "timelock", "daily_limit", "admin"}
	for _, hint := range hints {
		if strings.Contains(args, hint) {
			return true
		}
	}
	return false
}

func isSignerRotationFunction(fn string) bool {
	switch fn {
	case "rotate_key", "rotate_signer", "rotate_owner_key", "replace_signer":
		return true
	default:
		return false
	}
}

func isSignerAddRemoveFunction(fn string) bool {
	switch fn {
	case "add_signer", "remove_signer", "allow_signing_key", "revoke_signing_key", "set_signer", "update_signer":
		return true
	default:
		return false
	}
}

func isSmartWalletDeploymentFunction(fn, args string) bool {
	if fn == "init" || fn == "initialize" || fn == "constructor" || fn == "deploy" || fn == "create_wallet" {
		return true
	}
	return strings.Contains(args, "initial_signers") || strings.Contains(args, "initialize") || strings.Contains(args, "create_wallet")
}

func isLikelySmartWalletFunction(fn, args string) bool {
	return isSmartWalletPolicyFunction(fn) || isSmartWalletApprovalFunction(fn) || isSignerRotationFunction(fn) || isSignerAddRemoveFunction(fn) || isSmartWalletDeploymentFunction(fn, args) || containsWalletPolicyHint(args)
}

func isProtocolRouterHint(fn, args string) bool {
	hints := []string{"router", "route", "soroswap", "blend", "pool", "liquidity", "oracle", "vault", "amm"}
	for _, hint := range hints {
		if strings.Contains(fn, hint) || strings.Contains(args, hint) {
			return true
		}
	}
	return false
}

func isProtocolSwapHint(fn, args string) bool {
	hints := []string{"path", "exact_in", "exact_out", "minimum_out", "pool", "router", "swap"}
	for _, hint := range hints {
		if strings.Contains(fn, hint) || strings.Contains(args, hint) {
			return true
		}
	}
	return false
}

func isTokenTransferHint(fn, args string) bool {
	hints := []string{"transfer", "recipient", "recipient_id", "amount", "token", "asset", "destination"}
	matches := 0
	for _, hint := range hints {
		if strings.Contains(fn, hint) || strings.Contains(args, hint) {
			matches++
		}
	}
	return matches >= 2
}

func (h *DecodeHandlers) getSemanticTxDiffs(ctx context.Context, txHash string) *TxDiffs {
	if txHash == "" {
		return nil
	}
	if h.hotPathReader != nil {
		subCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		if diffs, err := h.hotPathReader.GetTransactionDiffs(subCtx, txHash); err == nil {
			return diffs
		}
	}
	if h.bronzeCold != nil {
		subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if diffs, err := h.bronzeCold.GetTransactionDiffs(subCtx, txHash); err == nil {
			return diffs
		}
	}
	return nil
}

func (h *DecodeHandlers) getSemanticCallGraph(ctx context.Context, txHash string) []ContractCall {
	if h.silverReader == nil || txHash == "" {
		return nil
	}
	subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	calls, err := h.silverReader.GetTransactionCallGraph(subCtx, txHash)
	if err != nil {
		return nil
	}
	return calls
}

func toSemanticCallEdges(calls []ContractCall) []SemanticCallEdge {
	if len(calls) == 0 {
		return nil
	}
	result := make([]SemanticCallEdge, 0, len(calls))
	for _, call := range calls {
		result = append(result, SemanticCallEdge{
			From:       call.FromContract,
			To:         call.ToContract,
			Function:   call.FunctionName,
			Depth:      call.CallDepth,
			Order:      call.ExecutionOrder,
			Successful: call.Successful,
		})
	}
	return result
}

func hasSwapEvidence(decoded *DecodedTransaction, diffs *TxDiffs) bool {
	if decoded.Summary.Swap != nil || decoded.Summary.Type == "swap" {
		return true
	}
	assets := map[string]bool{}
	for _, event := range decoded.Events {
		asset := semanticAssetName(event)
		if asset != "" && asset != "unknown_asset" {
			assets[asset] = true
		}
	}
	if diffs != nil {
		for _, change := range diffs.BalanceChanges {
			if change.Delta != "" && change.Delta != "0" && change.AssetCode != "" {
				assets[change.AssetCode] = true
			}
		}
	}
	return len(assets) > 1
}

func hasTransferEvidence(decoded *DecodedTransaction, diffs *TxDiffs) bool {
	if decoded.Summary.Transfer != nil || decoded.Summary.Mint != nil || decoded.Summary.Burn != nil {
		return true
	}
	if len(decoded.Events) > 0 {
		return true
	}
	if diffs != nil {
		nonZero := 0
		for _, change := range diffs.BalanceChanges {
			if change.Delta != "" && change.Delta != "0" && change.Delta != "+0" && change.Delta != "-0" && change.Delta != "0.0000000" {
				nonZero++
			}
		}
		return nonZero >= 1
	}
	return false
}

func hasExternalProtocolCall(callGraph []ContractCall, actors []SemanticActor) bool {
	if len(callGraph) == 0 {
		return false
	}
	actorTypes := map[string]string{}
	for _, actor := range actors {
		actorTypes[actor.ActorID] = actor.ActorType
	}
	for _, call := range callGraph {
		if call.ToContract == "" {
			continue
		}
		if actorTypes[call.ToContract] != "smart_wallet" {
			return true
		}
	}
	return false
}

func (ctx *SemanticAssetContext) inferPrimaryMovementsFromDiffs(diffs *TxDiffs) {
	if diffs == nil {
		return
	}
	for _, change := range diffs.BalanceChanges {
		if change.Delta == "" || change.Delta == "0" || change.Delta == "+0" || change.Delta == "-0" || change.Delta == "0.0000000" {
			continue
		}
		addr := change.Address
		if strings.HasPrefix(change.Delta, "-") && ctx.Sent == nil {
			ctx.Sent = &SemanticAssetMovement{
				Amount: strings.TrimPrefix(change.Delta, "-"),
				Asset:  change.AssetCode,
				From:   &addr,
				Kind:   "balance_change",
			}
		}
		if !strings.HasPrefix(change.Delta, "-") && ctx.Received == nil {
			ctx.Received = &SemanticAssetMovement{
				Amount: change.Delta,
				Asset:  change.AssetCode,
				To:     &addr,
				Kind:   "balance_change",
			}
		}
	}
}

type semanticLabelInfo struct {
	Label     *string
	ActorType string
}

func (h *DecodeHandlers) lookupSemanticLabel(ctx context.Context, actorID string, cache map[string]*semanticLabelInfo) *semanticLabelInfo {
	if meta, ok := cache[actorID]; ok {
		return meta
	}
	meta := &semanticLabelInfo{}
	if h.hotReader == nil || h.hotReader.db == nil {
		cache[actorID] = meta
		return meta
	}

	if strings.HasPrefix(actorID, "C") {
		var displayName, category sql.NullString
		err := h.hotReader.db.QueryRowContext(ctx, `
			SELECT cr.display_name, cr.category
			FROM contract_registry cr
			WHERE cr.contract_id = $1
		`, actorID).Scan(&displayName, &category)
		if err == nil {
			if displayName.Valid && displayName.String != "" {
				meta.Label = &displayName.String
			}
			meta.ActorType = actorTypeFromCategory(category)
		}
		if meta.Label == nil {
			var tokenName, tokenSymbol, tokenType sql.NullString
			err = h.hotReader.db.QueryRowContext(ctx, `
				SELECT token_name, token_symbol, token_type
				FROM token_registry
				WHERE contract_id = $1
			`, actorID).Scan(&tokenName, &tokenSymbol, &tokenType)
			if err == nil {
				if tokenSymbol.Valid && tokenSymbol.String != "" {
					meta.Label = &tokenSymbol.String
				} else if tokenName.Valid && tokenName.String != "" {
					meta.Label = &tokenName.String
				}
				if meta.ActorType == "" {
					meta.ActorType = "token_contract"
				}
				_ = tokenType
			}
		}
		if meta.ActorType == "" {
			var contractType, tokenName, tokenSymbol, walletType sql.NullString
			err = h.hotReader.db.QueryRowContext(ctx, `
				SELECT contract_type, token_name, token_symbol, wallet_type
				FROM semantic_entities_contracts
				WHERE contract_id = $1
			`, actorID).Scan(&contractType, &tokenName, &tokenSymbol, &walletType)
			if err == nil {
				if meta.Label == nil {
					if tokenSymbol.Valid && tokenSymbol.String != "" {
						meta.Label = &tokenSymbol.String
					} else if tokenName.Valid && tokenName.String != "" {
						meta.Label = &tokenName.String
					}
				}
				if walletType.Valid && walletType.String != "" {
					meta.ActorType = "smart_wallet"
				} else if contractType.Valid && contractType.String != "" {
					meta.ActorType = actorTypeFromContractType(contractType.String)
				}
			}
		}
	}

	cache[actorID] = meta
	return meta
}

func actorTypeFromCategory(category sql.NullString) string {
	if !category.Valid {
		return ""
	}
	switch strings.ToLower(category.String) {
	case "token", "sep41_token":
		return "token_contract"
	case "pool", "liquidity_pool":
		return "pool"
	case "wallet", "smart_wallet":
		return "smart_wallet"
	case "protocol", "dex", "lending", "oracle":
		return "protocol"
	default:
		return "contract"
	}
}

func actorTypeFromContractType(contractType string) string {
	switch strings.ToLower(contractType) {
	case "sep41_token", "token", "sac", "custom_soroban":
		return "token_contract"
	case "smart_wallet":
		return "smart_wallet"
	case "pool", "liquidity_pool":
		return "pool"
	case "protocol", "dex", "lending", "oracle":
		return "protocol"
	default:
		return "contract"
	}
}
