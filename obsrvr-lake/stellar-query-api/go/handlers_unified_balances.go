package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/stellar/go-stellar-sdk/strkey"
)

// UnifiedAddressBalance is a normalized balance row for XLM, classic trustlines,
// and Soroban token holdings.
type UnifiedAddressBalance struct {
	AssetType         string  `json:"asset_type"`
	AssetCode         string  `json:"asset_code,omitempty"`
	AssetIssuer       *string `json:"asset_issuer,omitempty"`
	ContractID        *string `json:"contract_id,omitempty"`
	Symbol            *string `json:"symbol,omitempty"`
	BalanceRaw        string  `json:"balance_raw"`
	BalanceDisplay    string  `json:"balance"`
	Decimals          *int    `json:"decimals,omitempty"`
	DecimalsSource    string  `json:"decimals_source,omitempty"`
	BalanceSource     string  `json:"balance_source"`
	LastUpdatedLedger *int64  `json:"last_updated_ledger,omitempty"`
	LastUpdatedAt     *string `json:"last_updated_at,omitempty"`
}

// UnifiedAddressBalancesResponse is returned by /api/v1/silver/addresses/{addr}/balances.
type UnifiedAddressBalancesResponse struct {
	Address       string                  `json:"address"`
	Balances      []UnifiedAddressBalance `json:"balances"`
	TotalBalances int                     `json:"total_balances"`
	Sources       []string                `json:"sources"`
	Partial       bool                    `json:"partial"`
	Warnings      []string                `json:"warnings,omitempty"`
}

// HandleUnifiedAddressBalances returns XLM, classic trustlines, and Soroban token
// balances for an address in one response.
// @Summary Get unified address balances
// @Description Returns XLM, classic trustlines, and arbitrary Soroban token holdings for a Stellar address
// @Tags Accounts
// @Accept json
// @Produce json
// @Param addr path string true "Stellar account or contract address"
// @Success 200 {object} UnifiedAddressBalancesResponse "Unified address balances"
// @Failure 400 {object} map[string]interface{} "Missing or invalid address"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/addresses/{addr}/balances [get]
func (h *SilverHandlers) HandleUnifiedAddressBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	addr := vars["addr"]
	if addr == "" {
		addr = r.URL.Query().Get("address")
	}
	if addr == "" {
		respondError(w, "address required", http.StatusBadRequest)
		return
	}
	if !isValidStellarAddress(addr) {
		respondError(w, "invalid address: must be a Stellar account (G...) or contract (C...) address", http.StatusBadRequest)
		return
	}

	resp, err := h.GetUnifiedAddressBalances(r.Context(), addr)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, resp)
}

func (h *SilverHandlers) GetUnifiedAddressBalances(ctx context.Context, addr string) (*UnifiedAddressBalancesResponse, error) {
	resp := &UnifiedAddressBalancesResponse{Address: addr}
	byKey := make(map[string]UnifiedAddressBalance)
	sourceSet := make(map[string]bool)
	add := func(b UnifiedAddressBalance) {
		key := unifiedBalanceKey(b)
		if _, exists := byKey[key]; exists {
			return
		}
		byKey[key] = b
		if b.BalanceSource != "" {
			sourceSet[b.BalanceSource] = true
		}
	}

	// Best path: materialized generic address balances already normalize native,
	// classic, SAC, and custom Soroban token holdings.
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		addressBalances, err := h.legacyReader.hot.GetAddressBalancesCurrent(ctx, addr)
		if err == nil && len(addressBalances) > 0 {
			for _, b := range addressBalances {
				assetCode := derefOrDefault(b.AssetCode, "")
				if assetCode == "" && b.AssetType == "native" {
					assetCode = "XLM"
				}
				decimalsSource := "asset_metadata"
				if b.Decimals == nil {
					decimalsSource = "unknown"
				}
				add(UnifiedAddressBalance{
					AssetType:         b.AssetType,
					AssetCode:         assetCode,
					AssetIssuer:       b.AssetIssuer,
					ContractID:        b.TokenContractID,
					Symbol:            b.Symbol,
					BalanceRaw:        b.BalanceRaw,
					BalanceDisplay:    b.BalanceDisplay,
					Decimals:          b.Decimals,
					DecimalsSource:    decimalsSource,
					BalanceSource:     b.BalanceSource,
					LastUpdatedLedger: b.LastUpdatedLedger,
					LastUpdatedAt:     b.LastUpdatedAt,
				})
			}
		}
	}

	// Compose classic account state when the generic materialized table is absent
	// or incomplete for the address.
	classicBalances, classicErr := h.getClassicAccountBalances(ctx, addr)
	if classicErr == nil && classicBalances != nil {
		decimals := 7
		for _, b := range classicBalances.Balances {
			add(UnifiedAddressBalance{
				AssetType:      b.AssetType,
				AssetCode:      b.AssetCode,
				AssetIssuer:    b.AssetIssuer,
				BalanceRaw:     strconv.FormatInt(b.BalanceStroops, 10),
				BalanceDisplay: b.Balance,
				Decimals:       &decimals,
				DecimalsSource: "stellar_asset",
				BalanceSource:  "classic_account_state",
			})
		}
	} else if classicErr != nil && !strings.Contains(classicErr.Error(), "not found") {
		resp.Partial = true
		resp.Warnings = append(resp.Warnings, "classic balances unavailable: "+classicErr.Error())
	}

	// Add arbitrary Soroban token holdings derived from token transfer history.
	if h.unifiedReader != nil {
		holdings, err := h.unifiedReader.GetAddressTokenPortfolio(ctx, addr)
		if err == nil {
			for _, holding := range holdings {
				assetCode := derefOrDefault(holding.AssetCode, derefOrDefault(holding.Symbol, ""))
				assetType := holding.TokenType
				if assetType == "" {
					assetType = "soroban_token"
				}
				decimals := holding.Decimals
				add(UnifiedAddressBalance{
					AssetType:         assetType,
					AssetCode:         assetCode,
					AssetIssuer:       holding.AssetIssuer,
					ContractID:        holding.ContractID,
					Symbol:            holding.Symbol,
					BalanceRaw:        strconv.FormatInt(holding.BalanceRaw, 10),
					BalanceDisplay:    holding.Balance,
					Decimals:          &decimals,
					DecimalsSource:    "token_registry_or_default",
					BalanceSource:     "indexed_transfer_history",
					LastUpdatedLedger: &holding.LastLedger,
					LastUpdatedAt:     &holding.LastSeen,
				})
			}
		} else {
			resp.Partial = true
			resp.Warnings = append(resp.Warnings, "soroban token balances unavailable: "+err.Error())
		}
	} else if len(byKey) == 0 {
		resp.Partial = true
		resp.Warnings = append(resp.Warnings, "soroban token balances unavailable: unified reader not configured")
	}

	for _, b := range byKey {
		resp.Balances = append(resp.Balances, b)
	}
	sort.Slice(resp.Balances, func(i, j int) bool {
		return compareUnifiedBalances(resp.Balances[i], resp.Balances[j])
	})
	for source := range sourceSet {
		resp.Sources = append(resp.Sources, source)
	}
	sort.Strings(resp.Sources)
	resp.TotalBalances = len(resp.Balances)
	return resp, nil
}

func (h *SilverHandlers) getClassicAccountBalances(ctx context.Context, addr string) (*AccountBalancesResponse, error) {
	if h.legacyReader != nil && h.legacyReader.hot != nil {
		if balances, err := h.legacyReader.hot.GetServingAccountBalances(ctx, addr); err == nil && balances != nil {
			return balances, nil
		}
	}
	if h.unifiedReader != nil {
		return h.unifiedReader.GetAccountBalances(ctx, addr)
	}
	return nil, fmt.Errorf("classic balances unavailable: no reader configured")
}

func isValidStellarAddress(addr string) bool {
	if _, err := strkey.Decode(strkey.VersionByteAccountID, addr); err == nil {
		return true
	}
	if _, err := strkey.Decode(strkey.VersionByteContract, addr); err == nil {
		return true
	}
	return false
}

func unifiedBalanceKey(b UnifiedAddressBalance) string {
	return strings.Join([]string{
		b.AssetType,
		b.AssetCode,
		derefOrDefault(b.AssetIssuer, ""),
		derefOrDefault(b.ContractID, ""),
	}, "|")
}

func compareUnifiedBalances(a, b UnifiedAddressBalance) bool {
	if a.AssetType == "native" && b.AssetType != "native" {
		return true
	}
	if b.AssetType == "native" && a.AssetType != "native" {
		return false
	}
	return unifiedBalanceKey(a) < unifiedBalanceKey(b)
}
