package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// AssetSummary represents a single asset in the listing
type AssetSummary struct {
	AssetCode          string  `json:"asset_code"`
	AssetIssuer        string  `json:"asset_issuer"`
	AssetType          string  `json:"asset_type"`
	HolderCount        int64   `json:"holder_count"`
	CirculatingSupply  string  `json:"circulating_supply"`
	Volume24h          string  `json:"volume_24h"`
	Transfers24h       int64   `json:"transfers_24h"`
	Top10Concentration float64 `json:"top_10_concentration"`
	FirstSeen          string  `json:"first_seen,omitempty"`
	LastActivity       string  `json:"last_activity,omitempty"`
}

// AssetListResponse is the response for GET /api/v1/silver/assets
type AssetListResponse struct {
	Assets      []AssetSummary `json:"assets"`
	TotalAssets int            `json:"total_assets"`
	Cursor      string         `json:"cursor,omitempty"`
	HasMore     bool           `json:"has_more"`
	GeneratedAt string         `json:"generated_at"`
}

// AssetIssuerMetadata captures issuer/home-domain level trust signals derivable
// from on-chain account state without external TOML fetching.
type AssetIssuerMetadata struct {
	AccountID           string  `json:"account_id"`
	HomeDomain          *string `json:"home_domain,omitempty"`
	AuthRequired        bool    `json:"auth_required"`
	AuthRevocable       bool    `json:"auth_revocable"`
	AuthImmutable       bool    `json:"auth_immutable"`
	AuthClawbackEnabled bool    `json:"auth_clawback_enabled"`
}

// LinkedTokenSummary describes a classic↔token relationship observable from
// token_registry today. This is useful but not yet authoritative/canonical.
type LinkedTokenSummary struct {
	ContractID    string  `json:"contract_id"`
	TokenType     string  `json:"token_type"`
	TokenName     *string `json:"token_name,omitempty"`
	TokenSymbol   *string `json:"token_symbol,omitempty"`
	TokenDecimals *int    `json:"token_decimals,omitempty"`
}

// AssetPairSummary describes a related market/pair for an asset.
type AssetPairSummary struct {
	CounterAsset     AssetInfo `json:"counter_asset"`
	TradeCount24h    int64     `json:"trade_count_24h"`
	BaseVolume24h    string    `json:"base_volume_24h"`
	CounterVolume24h string    `json:"counter_volume_24h"`
	LastPrice        *string   `json:"last_price,omitempty"`
}

// AssetDetailResponse is a composite explorer-oriented asset response built
// entirely from currently available silver/query-api data sources.
type AssetDetailResponse struct {
	Asset            AssetInfo              `json:"asset"`
	CanonicalSlug    string                 `json:"canonical_slug"`
	DisplayName      *string                `json:"display_name,omitempty"`
	Symbol           *string                `json:"symbol,omitempty"`
	Decimals         *int                   `json:"decimals,omitempty"`
	Verified         *bool                  `json:"verified,omitempty"`
	TokenType        *string                `json:"token_type,omitempty"`
	LinkedContractID *string                `json:"linked_contract_id,omitempty"`
	Issuer           *AssetIssuerMetadata   `json:"issuer,omitempty"`
	Stats            any                    `json:"stats,omitempty"`
	TopHolders       any                    `json:"top_holders,omitempty"`
	RecentTransfers  []TokenTransfer        `json:"recent_transfers,omitempty"`
	LinkedTokens     []LinkedTokenSummary   `json:"linked_tokens,omitempty"`
	TopPairs         []AssetPairSummary     `json:"top_pairs,omitempty"`
	LiquidityPools   []LiquidityPoolCurrent `json:"liquidity_pools,omitempty"`
	GeneratedAt      string                 `json:"generated_at"`
}

// AssetListFilters contains filter options for the asset list query
type AssetListFilters struct {
	SortBy       string // holder_count, volume_24h, transfers_24h, circulating_supply
	SortOrder    string // asc, desc
	MinHolders   *int64
	MinVolume24h *int64 // in stroops
	AssetType    string // credit_alphanum4, credit_alphanum12, native
	Search       string // search by asset code prefix
	Limit        int
	Cursor       *AssetListCursor
}

// AssetListCursor for pagination
// Format: holder_count:volume_24h:sort_by:sort_order:asset_code:asset_issuer
type AssetListCursor struct {
	HolderCount int64
	Volume24h   int64
	AssetCode   string
	AssetIssuer string
	SortBy      string
	SortOrder   string
}

// Encode encodes the cursor to a base64 string
func (c *AssetListCursor) Encode() string {
	raw := fmt.Sprintf("%d:%d:%s:%s:%s:%s", c.HolderCount, c.Volume24h, c.SortBy, c.SortOrder, c.AssetCode, c.AssetIssuer)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeAssetListCursor decodes a base64 cursor string
func DecodeAssetListCursor(s string) (*AssetListCursor, error) {
	if s == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 6)
	if len(parts) != 6 {
		return nil, fmt.Errorf("invalid cursor format: expected holder_count:volume_24h:sort_by:sort_order:asset_code:asset_issuer")
	}

	holderCount, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid holder_count in cursor: %w", err)
	}

	volume24h, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid volume_24h in cursor: %w", err)
	}

	return &AssetListCursor{
		HolderCount: holderCount,
		Volume24h:   volume24h,
		SortBy:      parts[2],
		SortOrder:   parts[3],
		AssetCode:   parts[4],
		AssetIssuer: parts[5],
	}, nil
}
