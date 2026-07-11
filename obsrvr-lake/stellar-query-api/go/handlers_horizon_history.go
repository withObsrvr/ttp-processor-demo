package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	hbase "github.com/stellar/go-stellar-sdk/protocols/horizon/base"
	heffects "github.com/stellar/go-stellar-sdk/protocols/horizon/effects"
	hoperations "github.com/stellar/go-stellar-sdk/protocols/horizon/operations"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const horizonInvokeContractHostFunction = "HostFunctionTypeHostFunctionTypeInvokeContract"
const maxHorizonEffectType = 97

func (h *HorizonCompatHandlers) HandleOperations(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{})
}

func (h *HorizonCompatHandlers) HandleOperation(w http.ResponseWriter, r *http.Request) {
	if h.operationReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility operation lookup requires the unified DuckDB reader.",
		))
		return
	}

	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil || id <= 0 {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", "operation id must be a positive integer"))
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	op, err := h.operationReader.GetOperationByID(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, errHorizonOperationNotFound):
			renderHorizonProblem(w, r, horizonProblem(http.StatusNotFound, "not_found", "Resource Missing", "Operation not found."))
		case isQueryTimeout(err):
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
		default:
			renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		}
		return
	}

	if err := writeHorizonJSON(w, http.StatusOK, horizonOperationRecord(r, *op, "asc")); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func (h *HorizonCompatHandlers) HandlePayments(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{PaymentsOnly: true})
}

func (h *HorizonCompatHandlers) HandleTransactionOperations(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{TxHash: mux.Vars(r)["hash"]})
}

func (h *HorizonCompatHandlers) HandleTransactionPayments(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{TxHash: mux.Vars(r)["hash"], PaymentsOnly: true})
}

func (h *HorizonCompatHandlers) HandleAccountOperations(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{AccountID: mux.Vars(r)["id"]})
}

func (h *HorizonCompatHandlers) HandleAccountPayments(w http.ResponseWriter, r *http.Request) {
	h.handleOperationCollection(w, r, OperationFilters{AccountID: mux.Vars(r)["id"], PaymentsOnly: true})
}

func (h *HorizonCompatHandlers) handleOperationCollection(w http.ResponseWriter, r *http.Request, filters OperationFilters) {
	if h.operationReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility operation collections require the unified DuckDB reader.",
		))
		return
	}

	page, err := parseHorizonPageQuery(r)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}
	cursor, err := decodeHorizonOperationCursor(page.Cursor)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}

	filters.Limit = int(page.Limit)
	filters.Order = page.Order
	filters.Cursor = cursor

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	ops, _, _, err := h.operationReader.GetEnrichedOperationsWithCursor(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
			return
		}
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		return
	}

	records := make([]hoperations.Operation, 0, len(ops))
	for _, op := range ops {
		records = append(records, horizonOperationRecord(r, op, page.Order))
	}

	var firstCursor, lastCursor string
	if len(ops) > 0 {
		firstCursor = horizonOperationPagingToken(ops[0], page.Order)
		lastCursor = horizonOperationPagingToken(ops[len(ops)-1], page.Order)
	}

	var out hoperations.OperationsPage
	out.Links = horizonCompatCollectionLinks(r, page, firstCursor, lastCursor)
	out.Embedded.Records = records

	if err := writeHorizonJSON(w, http.StatusOK, out); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func (h *HorizonCompatHandlers) HandleEffects(w http.ResponseWriter, r *http.Request) {
	h.handleEffectCollection(w, r, EffectFilters{})
}

func (h *HorizonCompatHandlers) HandleOperationEffects(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil || id <= 0 {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", "operation id must be a positive integer"))
		return
	}
	filters := EffectFilters{OperationID: &id}
	if ledgerSequence := int64(toid.Parse(id).LedgerSequence); ledgerSequence > 0 {
		filters.LedgerSequence = ledgerSequence
	}
	h.handleEffectCollection(w, r, filters)
}

func (h *HorizonCompatHandlers) HandleTransactionEffects(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]
	filters := EffectFilters{TransactionHash: hash}
	if h.txReader != nil && h.txReader.Available() {
		ctx, cancel := withInteractiveQueryTimeout(r.Context())
		tx, err := h.txReader.GetTransactionByHash(ctx, hash)
		cancel()
		if err == nil && tx != nil && tx.Ledger > 0 {
			filters.LedgerSequence = int64(tx.Ledger)
		} else if err != nil && !errors.Is(err, errHorizonTransactionNotFound) {
			// The lookup only provides the ledger bound; proceed unbounded, but a
			// failing lookup would otherwise be visible only as mysteriously slow
			// effect queries.
			log.Printf("horizon_effects path=tx_ledger_hint_error tx=%s err=%v; querying without ledger bound", hash, err)
		}
	}
	h.handleEffectCollection(w, r, filters)
}

func (h *HorizonCompatHandlers) HandleAccountEffects(w http.ResponseWriter, r *http.Request) {
	h.handleEffectCollection(w, r, EffectFilters{AccountID: mux.Vars(r)["id"]})
}

func (h *HorizonCompatHandlers) handleEffectCollection(w http.ResponseWriter, r *http.Request, filters EffectFilters) {
	if h.effectReader == nil {
		renderHorizonProblem(w, r, horizonProblem(
			http.StatusServiceUnavailable,
			"data_unavailable",
			"Data Unavailable",
			"Horizon compatibility effect collections require the unified DuckDB reader.",
		))
		return
	}

	page, err := parseHorizonPageQuery(r)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}
	cursor, err := decodeHorizonEffectCursor(page.Cursor)
	if err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusBadRequest, "bad_request", "Bad Request", err.Error()))
		return
	}

	filters.Limit = int(page.Limit)
	filters.Order = page.Order
	filters.Cursor = cursor
	filters.HorizonOrder = true
	filters.MaxEffectType = maxHorizonEffectType

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	effects, _, _, err := h.effectReader.GetEffects(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			renderHorizonProblem(w, r, horizonProblem(http.StatusGatewayTimeout, "timeout", "Timeout", err.Error()))
			return
		}
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
		return
	}

	records := make([]heffects.Effect, 0, len(effects))
	for _, effect := range effects {
		records = append(records, horizonEffectRecord(r, effect, page.Order))
	}

	var firstCursor, lastCursor string
	if len(effects) > 0 {
		firstCursor = horizonEffectPagingToken(effects[0], page.Order)
		lastCursor = horizonEffectPagingToken(effects[len(effects)-1], page.Order)
	}

	var out heffects.EffectsPage
	out.Links = horizonCompatCollectionLinks(r, page, firstCursor, lastCursor)
	out.Embedded.Records = records

	if err := writeHorizonJSON(w, http.StatusOK, out); err != nil {
		renderHorizonProblem(w, r, horizonProblem(http.StatusInternalServerError, "server_error", "Internal Server Error", err.Error()))
	}
}

func decodeHorizonOperationCursor(raw string) (*OperationCursor, error) {
	if raw == "" {
		return nil, nil
	}
	if raw == "now" {
		return nil, nil
	}
	// Horizon operation paging tokens are numeric TOIDs (that is all this layer
	// ever emits). Do NOT fall back to the legacy base64 ledger:op_index cursor:
	// its op_index is a per-transaction index, and funneling it into the TOID
	// comparison would silently return a wrong page. /silver cursors belong to
	// the /silver endpoints.
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("cursor %q is not a Horizon operation paging token", raw)
	}
	parsed := toid.Parse(id)
	return &OperationCursor{
		LedgerSequence: int64(parsed.LedgerSequence),
		OperationIndex: id,
	}, nil
}

func decodeHorizonEffectCursor(raw string) (*EffectCursor, error) {
	if raw == "" {
		return nil, nil
	}
	if raw == "now" {
		return nil, nil
	}
	if op, idx, ok, err := parseHorizonEffectPair(raw); ok || err != nil {
		if err != nil {
			return nil, err
		}
		effectIndex := int(idx) - 1
		if effectIndex < 0 {
			effectIndex = -1
		}
		return &EffectCursor{OperationID: &op, EffectIndex: effectIndex}, nil
	}
	return DecodeEffectCursor(raw)
}

func parseHorizonEffectPair(raw string) (int64, int64, bool, error) {
	opRaw, idxRaw, found := strings.Cut(raw, "-")
	if !found || opRaw == "" || idxRaw == "" || strings.Contains(idxRaw, "-") {
		return 0, 0, false, nil
	}
	op, err := strconv.ParseInt(opRaw, 10, 64)
	if err != nil {
		return 0, 0, false, nil
	}
	idx, err := strconv.ParseInt(idxRaw, 10, 64)
	if err != nil {
		return 0, 0, true, err
	}
	if op <= 0 {
		return 0, 0, true, errors.New("effect cursor operation id must be positive")
	}
	return op, idx, true, nil
}

func horizonOperationRecord(r *http.Request, op EnrichedOperation, order string) hoperations.Operation {
	base := horizonOperationBase(r, op, order)
	switch xdr.OperationType(op.Type) {
	case xdr.OperationTypeCreateAccount:
		return hoperations.CreateAccount{
			Base:            base,
			StartingBalance: derefString(op.Amount),
			Funder:          op.SourceAccount,
			Account:         derefString(op.Destination),
		}
	case xdr.OperationTypePayment:
		return hoperations.Payment{
			Base:   base,
			Asset:  horizonAsset(op.AssetCode, op.AssetIssuer),
			From:   op.SourceAccount,
			To:     derefString(op.Destination),
			Amount: derefString(op.Amount),
		}
	// Path payments are part of the /payments predicate; rendering them as bare
	// Base records would hand SDK clients zero-valued amounts with no unmarshal
	// error. The enriched columns carry the destination leg (from/to/asset/
	// amount); the source leg (source_amount/source_max/destination_min, path)
	// is not captured at ingest and is emitted empty.
	case xdr.OperationTypePathPaymentStrictReceive:
		return hoperations.PathPayment{
			Payment: hoperations.Payment{
				Base:   base,
				Asset:  horizonAsset(op.AssetCode, op.AssetIssuer),
				From:   op.SourceAccount,
				To:     derefString(op.Destination),
				Amount: derefString(op.Amount),
			},
			Path: []hbase.Asset{},
		}
	case xdr.OperationTypePathPaymentStrictSend:
		return hoperations.PathPaymentStrictSend{
			Payment: hoperations.Payment{
				Base:   base,
				Asset:  horizonAsset(op.AssetCode, op.AssetIssuer),
				From:   op.SourceAccount,
				To:     derefString(op.Destination),
				Amount: derefString(op.Amount),
			},
			Path: []hbase.Asset{},
		}
	case xdr.OperationTypeAccountMerge:
		return hoperations.AccountMerge{
			Base:    base,
			Account: op.SourceAccount,
			Into:    derefString(op.Destination),
		}
	case xdr.OperationTypeInvokeHostFunction:
		return hoperations.InvokeHostFunction{
			Base:                base,
			Function:            horizonHostFunctionName(op),
			Parameters:          horizonHostFunctionParameters(op.SorobanArgsJSON),
			Address:             "",
			AssetBalanceChanges: []hoperations.AssetContractBalanceChange{},
		}
	}
	return base
}

func horizonHostFunctionName(op EnrichedOperation) string {
	if op.SorobanContractID != nil || op.SorobanFunction != nil {
		return horizonInvokeContractHostFunction
	}
	return ""
}

func horizonHostFunctionParameters(raw *string) []hoperations.HostFunctionParameter {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return []hoperations.HostFunctionParameter{}
	}
	value := strings.TrimSpace(*raw)

	var params []hoperations.HostFunctionParameter
	if err := json.Unmarshal([]byte(value), &params); err == nil {
		return params
	}

	var values []string
	if err := json.Unmarshal([]byte(value), &values); err == nil {
		params = make([]hoperations.HostFunctionParameter, 0, len(values))
		for _, v := range values {
			params = append(params, hoperations.HostFunctionParameter{Value: v})
		}
		return params
	}

	return []hoperations.HostFunctionParameter{{Value: value}}
}

func horizonOperationBase(r *http.Request, op EnrichedOperation, order string) hoperations.Base {
	links := newHorizonCompatLinkBuilder(r)
	id := strconv.FormatInt(op.OperationID, 10)
	pt := horizonOperationPagingToken(op, order)
	base := hoperations.Base{
		ID:                    id,
		PT:                    pt,
		TransactionSuccessful: op.TxSuccessful,
		SourceAccount:         op.SourceAccount,
		Type:                  horizonOperationTypeName(op.Type, op.TypeName),
		TypeI:                 op.Type,
		LedgerCloseTime:       parseHorizonTimestamp(op.LedgerClosedAt),
		TransactionHash:       op.TransactionHash,
	}
	base.Links.Self = links.Link("/operations", id)
	base.Links.Transaction = links.Link("/transactions", op.TransactionHash)
	base.Links.Effects = links.PagedLink("/operations", id, "effects")
	base.Links.Succeeds = links.Linkf("/operations?order=desc&cursor=%s", pt)
	base.Links.Precedes = links.Linkf("/operations?order=asc&cursor=%s", pt)
	return base
}

func horizonEffectRecord(r *http.Request, effect SilverEffect, order string) heffects.Effect {
	base := horizonEffectBase(r, effect, order)
	switch heffects.EffectType(effect.EffectType) {
	case heffects.EffectAccountCredited:
		return heffects.AccountCredited{Base: base, Asset: horizonAssetInfo(effect.Asset), Amount: derefString(effect.Amount)}
	case heffects.EffectAccountDebited:
		return heffects.AccountDebited{Base: base, Asset: horizonAssetInfo(effect.Asset), Amount: derefString(effect.Amount)}
	default:
		return base
	}
}

func horizonEffectBase(r *http.Request, effect SilverEffect, order string) heffects.Base {
	links := newHorizonCompatLinkBuilder(r)
	pt := horizonEffectPagingToken(effect, order)
	id := horizonEffectID(effect)
	opID := strconv.Itoa(effect.OperationIndex)
	if effect.OperationID != nil {
		opID = strconv.FormatInt(*effect.OperationID, 10)
	}
	base := heffects.Base{
		ID:              id,
		PT:              pt,
		Account:         derefString(effect.AccountID),
		Type:            horizonEffectTypeName(effect),
		TypeI:           int32(effect.EffectType),
		LedgerCloseTime: effect.Timestamp.UTC(),
	}
	base.Links.Operation = links.Link("/operations", opID)
	base.Links.Succeeds = links.Linkf("/effects?order=desc&cursor=%s", pt)
	base.Links.Precedes = links.Linkf("/effects?order=asc&cursor=%s", pt)
	return base
}

func horizonOperationPagingToken(op EnrichedOperation, order string) string {
	return strconv.FormatInt(op.OperationID, 10)
}

func horizonEffectPagingToken(effect SilverEffect, order string) string {
	if effect.OperationID != nil {
		return fmt.Sprintf("%d-%d", *effect.OperationID, horizonEffectOrder(effect))
	}
	return EffectCursor{
		LedgerSequence:  effect.LedgerSequence,
		TransactionHash: effect.TransactionHash,
		OperationIndex:  effect.OperationIndex,
		EffectIndex:     effect.EffectIndex,
		Order:           order,
	}.Encode()
}

func horizonEffectID(effect SilverEffect) string {
	if effect.OperationID != nil {
		return fmt.Sprintf("%019d-%010d", *effect.OperationID, horizonEffectOrder(effect))
	}
	return strconv.FormatInt(effect.LedgerSequence, 10) + "-" + effect.TransactionHash + "-" +
		strconv.Itoa(effect.OperationIndex) + "-" + strconv.Itoa(effect.EffectIndex)
}

func horizonEffectOrder(effect SilverEffect) int {
	return effect.EffectIndex + 1
}

func horizonOperationTypeName(opType int32, fallback string) string {
	if name, ok := hoperations.TypeNames[xdr.OperationType(opType)]; ok {
		return name
	}
	return strings.ToLower(fallback)
}

func horizonEffectTypeName(effect SilverEffect) string {
	if name, ok := heffects.EffectTypeNames[heffects.EffectType(effect.EffectType)]; ok {
		return name
	}
	return effect.EffectTypeString
}

func horizonAsset(code, issuer *string) hbase.Asset {
	assetCode := derefString(code)
	assetIssuer := derefString(issuer)
	if assetCode == "" || assetCode == "XLM" || assetIssuer == "" {
		return hbase.Asset{Type: "native"}
	}
	assetType := "credit_alphanum4"
	if len(assetCode) > 4 {
		assetType = "credit_alphanum12"
	}
	return hbase.Asset{Type: assetType, Code: assetCode, Issuer: assetIssuer}
}

func horizonAssetInfo(asset *AssetInfo) hbase.Asset {
	if asset == nil {
		return hbase.Asset{Type: "native"}
	}
	return horizonAsset(&asset.Code, asset.Issuer)
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func parseHorizonTimestamp(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999-07", "2006-01-02 15:04:05-07"} {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}
