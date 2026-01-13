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
