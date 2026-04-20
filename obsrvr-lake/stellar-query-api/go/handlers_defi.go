package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// DefiAsset represents a normalized DeFi asset shape.
type DefiAsset struct {
	AssetType       *string `json:"asset_type,omitempty"`
	AssetCode       *string `json:"asset_code,omitempty"`
	AssetIssuer     *string `json:"asset_issuer,omitempty"`
	AssetContractID *string `json:"asset_contract_id,omitempty"`
	Symbol          *string `json:"symbol,omitempty"`
	Decimals        *int    `json:"decimals,omitempty"`
}

// DefiProtocol represents a supported DeFi protocol.
type DefiProtocol struct {
	ProtocolID           string         `json:"protocol_id"`
	Network              string         `json:"network"`
	DisplayName          string         `json:"display_name"`
	Slug                 string         `json:"slug"`
	Version              *string        `json:"version,omitempty"`
	Category             string         `json:"category"`
	Status               string         `json:"status"`
	AdapterName          string         `json:"adapter_name"`
	PricingModel         string         `json:"pricing_model"`
	HealthModel          *string        `json:"health_model,omitempty"`
	SupportsHistory      bool           `json:"supports_history"`
	SupportsRewards      bool           `json:"supports_rewards"`
	SupportsHealthFactor bool           `json:"supports_health_factor"`
	WebsiteURL           *string        `json:"website_url,omitempty"`
	DocsURL              *string        `json:"docs_url,omitempty"`
	IconURL              *string        `json:"icon_url,omitempty"`
	Verified             bool           `json:"verified"`
	ConfigJSON           map[string]any `json:"config_json"`
}

type DefiMarket struct {
	MarketID              string         `json:"market_id"`
	ProtocolID            string         `json:"protocol_id"`
	MarketType            string         `json:"market_type"`
	MarketAddress         *string        `json:"market_address,omitempty"`
	PoolAddress           *string        `json:"pool_address,omitempty"`
	RouterAddress         *string        `json:"router_address,omitempty"`
	OracleContractID      *string        `json:"oracle_contract_id,omitempty"`
	InputAsset1           *DefiAsset     `json:"input_asset_1,omitempty"`
	InputAsset2           *DefiAsset     `json:"input_asset_2,omitempty"`
	TVLValueUSD           *string        `json:"tvl_value_usd,omitempty"`
	TotalDepositValueUSD  *string        `json:"total_deposit_value_usd,omitempty"`
	TotalBorrowedValueUSD *string        `json:"total_borrowed_value_usd,omitempty"`
	APRDeposit            *string        `json:"apr_deposit,omitempty"`
	APRBorrow             *string        `json:"apr_borrow,omitempty"`
	APRRewards            *string        `json:"apr_rewards,omitempty"`
	IsActive              bool           `json:"is_active"`
	MetadataJSON          map[string]any `json:"metadata_json"`
	AsOfLedger            int64          `json:"as_of_ledger"`
	AsOfTime              string         `json:"as_of_time"`
}

type DefiPositionComponent struct {
	ComponentID   string         `json:"component_id"`
	ComponentType string         `json:"component_type"`
	Asset         *DefiAsset     `json:"asset,omitempty"`
	Amount        *string        `json:"amount,omitempty"`
	Value         *string        `json:"value,omitempty"`
	Price         *string        `json:"price,omitempty"`
	PriceSource   *string        `json:"price_source,omitempty"`
	MetadataJSON  map[string]any `json:"metadata_json"`
}

type DefiPosition struct {
	PositionID            string         `json:"position_id"`
	ProtocolID            string         `json:"protocol_id"`
	ProtocolVersion       *string        `json:"protocol_version,omitempty"`
	PositionType          string         `json:"position_type"`
	Status                string         `json:"status"`
	OwnerAddress          string         `json:"owner_address"`
	AccountAddress        *string        `json:"account_address,omitempty"`
	RelatedAddress        *string        `json:"related_address,omitempty"`
	MarketID              *string        `json:"market_id,omitempty"`
	MarketAddress         *string        `json:"market_address,omitempty"`
	PositionKeyHash       *string        `json:"position_key_hash,omitempty"`
	UnderlyingAsset       *DefiAsset     `json:"underlying_asset,omitempty"`
	QuoteCurrency         string         `json:"quote_currency"`
	DepositAmount         *string        `json:"deposit_amount,omitempty"`
	BorrowAmount          *string        `json:"borrow_amount,omitempty"`
	ShareAmount           *string        `json:"share_amount,omitempty"`
	ClaimableRewardAmount *string        `json:"claimable_reward_amount,omitempty"`
	DepositValue          *string        `json:"deposit_value,omitempty"`
	BorrowedValue         *string        `json:"borrowed_value,omitempty"`
	CurrentValue          *string        `json:"current_value,omitempty"`
	NetValue              *string        `json:"net_value,omitempty"`
	CurrentReturnValue    *string        `json:"current_return_value,omitempty"`
	CurrentReturnPercent  *string        `json:"current_return_percent,omitempty"`
	ClaimableRewardsValue *string        `json:"claimable_rewards_value,omitempty"`
	HealthFactor          *string        `json:"health_factor,omitempty"`
	LTV                   *string        `json:"ltv,omitempty"`
	CollateralRatio       *string        `json:"collateral_ratio,omitempty"`
	LiquidationThreshold  *string        `json:"liquidation_threshold,omitempty"`
	RiskStatus            *string        `json:"risk_status,omitempty"`
	ProtocolStateJSON     map[string]any `json:"protocol_state_json"`
	ValuationJSON         map[string]any `json:"valuation_json"`
	SourceJSON            map[string]any `json:"source_json"`
	AsOfLedger            int64          `json:"as_of_ledger"`
	AsOfTime              string         `json:"as_of_time"`
	LastUpdatedLedger     int64          `json:"last_updated_ledger"`
	LastUpdatedAt         string         `json:"last_updated_at"`
}

type DefiExposureSummary struct {
	TotalValue                 string  `json:"total_value"`
	TotalDepositValue          string  `json:"total_deposit_value"`
	TotalBorrowedValue         string  `json:"total_borrowed_value"`
	NetValue                   string  `json:"net_value"`
	TotalClaimableRewardsValue string  `json:"total_claimable_rewards_value"`
	OpenPositionCount          int     `json:"open_position_count"`
	ProtocolCount              int     `json:"protocol_count"`
	LowestHealthFactor         *string `json:"lowest_health_factor,omitempty"`
	PositionsAtRisk            int     `json:"positions_at_risk"`
}

type DefiExposureByProtocol struct {
	ProtocolID         string `json:"protocol_id"`
	TotalValue         string `json:"total_value"`
	TotalDepositValue  string `json:"total_deposit_value"`
	TotalBorrowedValue string `json:"total_borrowed_value"`
	NetValue           string `json:"net_value"`
	PositionCount      int    `json:"position_count"`
}

type DefiExposureResponse struct {
	Address          string                   `json:"address"`
	Quote            string                   `json:"quote"`
	AsOfLedger       int64                    `json:"as_of_ledger"`
	AsOfTime         string                   `json:"as_of_time"`
	FreshnessSeconds int64                    `json:"freshness_seconds"`
	DataStatus       string                   `json:"data_status"`
	Totals           DefiExposureSummary      `json:"totals"`
	ByProtocol       []DefiExposureByProtocol `json:"by_protocol"`
}

type DefiPositionsResponse struct {
	Address          string              `json:"address"`
	Quote            string              `json:"quote"`
	AsOfLedger       int64               `json:"as_of_ledger"`
	AsOfTime         string              `json:"as_of_time"`
	FreshnessSeconds int64               `json:"freshness_seconds"`
	DataStatus       string              `json:"data_status"`
	Summary          DefiExposureSummary `json:"summary"`
	Positions        []DefiPosition      `json:"positions"`
	HasMore          bool                `json:"has_more"`
	NextCursor       *string             `json:"next_cursor"`
}

type DefiStatusProtocol struct {
	ProtocolID           string  `json:"protocol_id"`
	Status               string  `json:"status"`
	Reason               *string `json:"reason,omitempty"`
	LastSuccessfulLedger *int64  `json:"last_successful_ledger,omitempty"`
	LastSuccessfulTime   *string `json:"last_successful_time,omitempty"`
	FreshnessSeconds     *int    `json:"freshness_seconds,omitempty"`
	SourceDivergence     bool    `json:"source_divergence"`
}

type DefiStatusResponse struct {
	Status           string               `json:"status"`
	AsOfLedger       *int64               `json:"as_of_ledger,omitempty"`
	FreshnessSeconds *int                 `json:"freshness_seconds,omitempty"`
	Protocols        []DefiStatusProtocol `json:"protocols"`
}

type DefiProtocolFilters struct {
	Status   string
	Category string
	Network  string
}

type DefiMarketFilters struct {
	Protocol   string
	MarketType string
	ActiveOnly bool
	Limit      int
	Cursor     *DefiCursor
}

type DefiPositionsFilters struct {
	Address       string
	Quote         string
	Protocol      string
	IncludeClosed bool
	Limit         int
	Cursor        *DefiCursor
}

type DefiCursor struct {
	ID string
}

func (c DefiCursor) Encode() string {
	return base64.StdEncoding.EncodeToString([]byte(c.ID))
}

func DecodeDefiCursor(raw string) (*DefiCursor, error) {
	if raw == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	if len(decoded) == 0 {
		return nil, errors.New("empty cursor")
	}
	return &DefiCursor{ID: string(decoded)}, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableInt32(v sql.NullInt32) *int {
	if !v.Valid {
		return nil
	}
	i := int(v.Int32)
	return &i
}

func jsonObject(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func jsonByProtocol(raw []byte) []DefiExposureByProtocol {
	if len(raw) == 0 {
		return []DefiExposureByProtocol{}
	}
	var out []DefiExposureByProtocol
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return []DefiExposureByProtocol{}
	}
	return out
}

func buildDefiAsset(assetType, assetCode, assetIssuer, assetContractID, symbol sql.NullString, decimals sql.NullInt32) *DefiAsset {
	if !assetType.Valid && !assetCode.Valid && !assetIssuer.Valid && !assetContractID.Valid && !symbol.Valid && !decimals.Valid {
		return nil
	}
	asset := &DefiAsset{}
	if assetType.Valid {
		asset.AssetType = &assetType.String
	}
	if assetCode.Valid {
		asset.AssetCode = &assetCode.String
	}
	if assetIssuer.Valid {
		asset.AssetIssuer = &assetIssuer.String
	}
	if assetContractID.Valid {
		asset.AssetContractID = &assetContractID.String
	}
	if symbol.Valid {
		asset.Symbol = &symbol.String
	}
	asset.Decimals = nullableInt32(decimals)
	return asset
}

func parseBoolQueryParam(r *http.Request, name string, defaultVal bool) (bool, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return defaultVal, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal, err
	}
	return b, nil
}

func freshnessSeconds(ts time.Time) int64 {
	if ts.IsZero() {
		return 0
	}
	sec := int64(time.Since(ts).Seconds())
	if sec < 0 {
		return 0
	}
	return sec
}

// HandleDefiProtocols serves GET /api/v1/semantic/defi/protocols
// @Summary List supported DeFi protocols
// @Tags DeFi Registry
// @Produce json
// @Param status query string false "Filter by protocol status"
// @Param category query string false "Filter by protocol category"
// @Param network query string false "Network name" default(testnet)
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/protocols [get]
func (h *SemanticHandlers) HandleDefiProtocols(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	filters := DefiProtocolFilters{
		Status:   strings.TrimSpace(r.URL.Query().Get("status")),
		Category: strings.TrimSpace(r.URL.Query().Get("category")),
		Network:  strings.TrimSpace(r.URL.Query().Get("network")),
	}
	if filters.Network == "" {
		filters.Network = "testnet"
	}

	protocols, err := h.unified.hot.GetDefiProtocols(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]any{"protocols": protocols})
}

// HandleDefiMarkets serves GET /api/v1/semantic/defi/markets
// @Summary List DeFi markets
// @Tags DeFi Markets
// @Produce json
// @Param protocol query string false "Filter by protocol id"
// @Param market_type query string false "Filter by market type"
// @Param active_only query bool false "Only active markets" default(true)
// @Param limit query int false "Max results" default(100)
// @Param cursor query string false "Pagination cursor"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/markets [get]
func (h *SemanticHandlers) HandleDefiMarkets(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	activeOnly, err := parseBoolQueryParam(r, "active_only", true)
	if err != nil {
		respondError(w, "invalid active_only: must be true or false", http.StatusBadRequest)
		return
	}
	cursor, err := DecodeDefiCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, "invalid cursor", http.StatusBadRequest)
		return
	}

	filters := DefiMarketFilters{
		Protocol:   strings.TrimSpace(r.URL.Query().Get("protocol")),
		MarketType: strings.TrimSpace(r.URL.Query().Get("market_type")),
		ActiveOnly: activeOnly,
		Limit:      parseLimit(r, 100, 500),
		Cursor:     cursor,
	}

	markets, nextCursor, hasMore, err := h.unified.hot.GetDefiMarkets(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"markets":     markets,
		"count":       len(markets),
		"has_more":    hasMore,
		"next_cursor": nil,
	}
	if nextCursor != "" {
		resp["next_cursor"] = nextCursor
	}
	respondJSON(w, resp)
}

// HandleDefiStatus serves GET /api/v1/semantic/defi/status
// @Summary Get DeFi protocol freshness and degradation status
// @Tags DeFi Status
// @Produce json
// @Success 200 {object} DefiStatusResponse
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/status [get]
func (h *SemanticHandlers) HandleDefiStatus(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	status, err := h.unified.hot.GetDefiStatus(r.Context())
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, status)
}

// HandleDefiExposure serves GET /api/v1/semantic/defi/exposure
// @Summary Get aggregated DeFi exposure for a user
// @Tags DeFi Positions
// @Produce json
// @Param address query string true "User address"
// @Param quote query string false "Quote currency" default(USD)
// @Param protocol query string false "Optional protocol filter"
// @Success 200 {object} DefiExposureResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/exposure [get]
func (h *SemanticHandlers) HandleDefiExposure(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	address := strings.TrimSpace(r.URL.Query().Get("address"))
	if address == "" {
		respondError(w, "address parameter is required", http.StatusBadRequest)
		return
	}
	quote := strings.TrimSpace(r.URL.Query().Get("quote"))
	if quote == "" {
		quote = "USD"
	}

	resp, err := h.unified.hot.GetDefiExposure(r.Context(), address, quote, strings.TrimSpace(r.URL.Query().Get("protocol")))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			respondError(w, "defi exposure not found", http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, resp)
}

// HandleDefiPositions serves GET /api/v1/semantic/defi/positions
// @Summary List DeFi positions for a user
// @Tags DeFi Positions
// @Produce json
// @Param address query string true "User address"
// @Param quote query string false "Quote currency" default(USD)
// @Param protocol query string false "Optional protocol filter"
// @Param include_closed query bool false "Include closed positions" default(false)
// @Param limit query int false "Max results" default(100)
// @Param cursor query string false "Pagination cursor"
// @Success 200 {object} DefiPositionsResponse
// @Failure 400 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/positions [get]
func (h *SemanticHandlers) HandleDefiPositions(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	address := strings.TrimSpace(r.URL.Query().Get("address"))
	if address == "" {
		respondError(w, "address parameter is required", http.StatusBadRequest)
		return
	}
	includeClosed, err := parseBoolQueryParam(r, "include_closed", false)
	if err != nil {
		respondError(w, "invalid include_closed: must be true or false", http.StatusBadRequest)
		return
	}
	cursor, err := DecodeDefiCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, "invalid cursor", http.StatusBadRequest)
		return
	}
	quote := strings.TrimSpace(r.URL.Query().Get("quote"))
	if quote == "" {
		quote = "USD"
	}

	resp, err := h.unified.hot.GetDefiPositions(r.Context(), DefiPositionsFilters{
		Address:       address,
		Quote:         quote,
		Protocol:      strings.TrimSpace(r.URL.Query().Get("protocol")),
		IncludeClosed: includeClosed,
		Limit:         parseLimit(r, 100, 500),
		Cursor:        cursor,
	})
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, resp)
}

// HandleDefiPosition serves GET /api/v1/semantic/defi/positions/{position_id}
// @Summary Get a single DeFi position
// @Tags DeFi Positions
// @Produce json
// @Param position_id path string true "Position ID"
// @Param quote query string false "Quote currency" default(USD)
// @Success 200 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/semantic/defi/positions/{position_id} [get]
func (h *SemanticHandlers) HandleDefiPosition(w http.ResponseWriter, r *http.Request) {
	if h.unified == nil || h.unified.hot == nil {
		respondError(w, "defi endpoints require silver hot reader", http.StatusServiceUnavailable)
		return
	}

	positionID := mux.Vars(r)["position_id"]
	if strings.TrimSpace(positionID) == "" {
		respondError(w, "position_id required", http.StatusBadRequest)
		return
	}
	quote := strings.TrimSpace(r.URL.Query().Get("quote"))
	if quote == "" {
		quote = "USD"
	}

	position, components, err := h.unified.hot.GetDefiPosition(r.Context(), positionID, quote)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			respondError(w, "position not found", http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]any{
		"position":   position,
		"components": components,
	})
}
