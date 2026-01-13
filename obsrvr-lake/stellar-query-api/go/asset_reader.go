package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

// GetAssetList returns a paginated list of all assets on the network
// This queries trustlines_current to aggregate unique assets with holder counts and supply
func (u *UnifiedSilverReader) GetAssetList(ctx context.Context, filters AssetListFilters) (*AssetListResponse, error) {
	// Query hot storage (PostgreSQL) for asset aggregations
	assets, cursor, hasMore, err := u.hot.GetAssetListWithCursor(ctx, filters)
	if err != nil {
		log.Printf("Warning: failed to query hot storage for asset list: %v", err)
		return nil, err
	}

	// Get total count of assets matching filters (excluding pagination)
	totalCount, err := u.hot.GetAssetCount(ctx, filters)
	if err != nil {
		log.Printf("Warning: failed to get asset count: %v", err)
		// Fall back to page count if count query fails
		totalCount = len(assets)
	}

	return &AssetListResponse{
		Assets:      assets,
		TotalAssets: totalCount,
		Cursor:      cursor,
		HasMore:     hasMore,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetAssetListWithCursor queries PostgreSQL for asset list with pagination
func (r *SilverHotReader) GetAssetListWithCursor(ctx context.Context, filters AssetListFilters) ([]AssetSummary, string, bool, error) {
	requestedLimit := filters.Limit
	if requestedLimit <= 0 {
		requestedLimit = 100
	}
	if requestedLimit > 1000 {
		requestedLimit = 1000
	}

	// Determine sort field and order
	sortBy := filters.SortBy
	if sortBy == "" {
		sortBy = "holder_count"
	}
	sortOrder := filters.SortOrder
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Validate sort field
	validSortFields := map[string]string{
		"holder_count":       "holder_count",
		"volume_24h":         "volume_24h",
		"transfers_24h":      "transfers_24h",
		"circulating_supply": "circulating_supply",
	}
	dbSortField, ok := validSortFields[sortBy]
	if !ok {
		dbSortField = "holder_count"
		sortBy = "holder_count"
	}

	// Validate sort order
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	// Build the main query
	// This aggregates trustlines to get unique assets with holder counts and supply
	// Then joins with token_transfers_raw for 24h volume/transfer stats
	query := `
		WITH asset_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				asset_type,
				COUNT(*) FILTER (WHERE balance > 0) as holder_count,
				SUM(balance) as circulating_supply,
				MIN(created_at) as first_seen,
				MAX(updated_at) as last_activity
			FROM trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
			GROUP BY asset_code, asset_issuer, asset_type
		),
		transfer_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				COUNT(*) as transfers_24h,
				COALESCE(SUM(amount), 0) as volume_24h
			FROM token_transfers_raw
			WHERE timestamp >= NOW() - INTERVAL '24 hours'
			  AND transaction_successful = true
			GROUP BY asset_code, asset_issuer
		)
		SELECT
			a.asset_code,
			COALESCE(a.asset_issuer, '') as asset_issuer,
			COALESCE(a.asset_type, 'credit_alphanum4') as asset_type,
			a.holder_count,
			COALESCE(a.circulating_supply::text, '0') as circulating_supply,
			COALESCE(t.transfers_24h, 0) as transfers_24h,
			COALESCE(t.volume_24h, 0) as volume_24h,
			a.first_seen,
			a.last_activity
		FROM asset_stats a
		LEFT JOIN transfer_stats t
			ON a.asset_code = t.asset_code
			AND COALESCE(a.asset_issuer, '') = COALESCE(t.asset_issuer, '')
		WHERE a.holder_count > 0
	`

	args := []interface{}{}
	argIndex := 1

	// Apply filters
	if filters.MinHolders != nil && *filters.MinHolders > 0 {
		query += fmt.Sprintf(" AND a.holder_count >= $%d", argIndex)
		args = append(args, *filters.MinHolders)
		argIndex++
	}

	if filters.MinVolume24h != nil && *filters.MinVolume24h > 0 {
		query += fmt.Sprintf(" AND COALESCE(t.volume_24h, 0) >= $%d", argIndex)
		args = append(args, *filters.MinVolume24h)
		argIndex++
	}

	if filters.AssetType != "" {
		query += fmt.Sprintf(" AND a.asset_type = $%d", argIndex)
		args = append(args, filters.AssetType)
		argIndex++
	}

	if filters.Search != "" {
		query += fmt.Sprintf(" AND UPPER(a.asset_code) LIKE UPPER($%d)", argIndex)
		args = append(args, filters.Search+"%")
		argIndex++
	}

	// Apply cursor for pagination
	if filters.Cursor != nil {
		// Validate cursor matches current sort settings
		if filters.Cursor.SortBy != sortBy || filters.Cursor.SortOrder != sortOrder {
			return nil, "", false, fmt.Errorf("cursor was created with different sort settings")
		}

		if sortOrder == "desc" {
			switch sortBy {
			case "holder_count":
				query += fmt.Sprintf(" AND (a.holder_count < $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			case "volume_24h":
				query += fmt.Sprintf(" AND (COALESCE(t.volume_24h, 0) < $%d OR (COALESCE(t.volume_24h, 0) = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.Volume24h, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			default:
				// Default to holder_count cursor logic
				query += fmt.Sprintf(" AND (a.holder_count < $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			}
		} else {
			// Ascending order
			switch sortBy {
			case "holder_count":
				query += fmt.Sprintf(" AND (a.holder_count > $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			case "volume_24h":
				query += fmt.Sprintf(" AND (COALESCE(t.volume_24h, 0) > $%d OR (COALESCE(t.volume_24h, 0) = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.Volume24h, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			default:
				query += fmt.Sprintf(" AND (a.holder_count > $%d OR (a.holder_count = $%d AND (a.asset_code > $%d OR (a.asset_code = $%d AND COALESCE(a.asset_issuer, '') > $%d))))",
					argIndex, argIndex, argIndex+1, argIndex+1, argIndex+2)
				args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode, filters.Cursor.AssetIssuer)
				argIndex += 3
			}
		}
	}

	// Add ORDER BY
	orderDir := "DESC"
	if sortOrder == "asc" {
		orderDir = "ASC"
	}
	query += fmt.Sprintf(" ORDER BY %s %s, a.asset_code ASC, a.asset_issuer ASC", dbSortField, orderDir)

	// Request one extra to detect has_more
	query += fmt.Sprintf(" LIMIT $%d", argIndex)
	args = append(args, requestedLimit+1)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("failed to query asset list: %w", err)
	}
	defer rows.Close()

	var assets []AssetSummary
	for rows.Next() {
		var a AssetSummary
		var circulatingSupplyStr sql.NullString // Use string to handle large numeric values
		var volume24h int64
		var firstSeen, lastActivity sql.NullTime

		err := rows.Scan(
			&a.AssetCode,
			&a.AssetIssuer,
			&a.AssetType,
			&a.HolderCount,
			&circulatingSupplyStr,
			&a.Transfers24h,
			&volume24h,
			&firstSeen,
			&lastActivity,
		)
		if err != nil {
			return nil, "", false, fmt.Errorf("failed to scan asset row: %w", err)
		}

		// Convert circulating supply - handle large numbers that overflow int64
		a.CirculatingSupply = formatBigNumericStroops(circulatingSupplyStr.String)
		a.Volume24h = formatStroops(volume24h)

		if firstSeen.Valid {
			a.FirstSeen = firstSeen.Time.Format(time.RFC3339)
		}
		if lastActivity.Valid {
			a.LastActivity = lastActivity.Time.Format(time.RFC3339)
		}

		assets = append(assets, a)
	}

	if err := rows.Err(); err != nil {
		return nil, "", false, fmt.Errorf("error iterating asset rows: %w", err)
	}

	// Determine has_more and trim to requested limit
	hasMore := len(assets) > requestedLimit
	if hasMore {
		assets = assets[:requestedLimit]
	}

	// Generate next cursor from last result
	var nextCursor string
	if len(assets) > 0 && hasMore {
		last := assets[len(assets)-1]
		// Parse volume back to stroops for cursor
		volume24hStroops := parseFormattedToStroops(last.Volume24h)

		cursor := AssetListCursor{
			HolderCount: last.HolderCount,
			Volume24h:   volume24hStroops,
			AssetCode:   last.AssetCode,
			AssetIssuer: last.AssetIssuer,
			SortBy:      sortBy,
			SortOrder:   sortOrder,
		}
		nextCursor = cursor.Encode()
	}

	return assets, nextCursor, hasMore, nil
}

// parseFormattedToStroops converts formatted string back to stroops
func parseFormattedToStroops(formatted string) int64 {
	// Simple parsing - assumes format "X.XXXXXXX"
	var whole, frac int64
	_, err := fmt.Sscanf(formatted, "%d.%d", &whole, &frac)
	if err != nil {
		return 0
	}
	return whole*10000000 + frac
}

// formatBigNumericStroops converts a string representation of stroops to decimal format
// Handles values larger than int64 max by using string manipulation
func formatBigNumericStroops(stroopsStr string) string {
	if stroopsStr == "" {
		return "0.0000000"
	}

	// Remove any leading zeros
	stroopsStr = strings.TrimLeft(stroopsStr, "0")
	if stroopsStr == "" {
		return "0.0000000"
	}

	// Stellar uses 7 decimal places (10^7 stroops = 1 unit)
	const decimals = 7

	if len(stroopsStr) <= decimals {
		// Value is less than 1 unit, pad with leading zeros
		padding := strings.Repeat("0", decimals-len(stroopsStr))
		return "0." + padding + stroopsStr
	}

	// Split into whole and fractional parts
	splitPoint := len(stroopsStr) - decimals
	wholePart := stroopsStr[:splitPoint]
	fracPart := stroopsStr[splitPoint:]

	return wholePart + "." + fracPart
}

// GetAssetCount returns the total count of assets matching the given filters (ignoring pagination)
func (r *SilverHotReader) GetAssetCount(ctx context.Context, filters AssetListFilters) (int, error) {
	// Build count query with same filters as main query, but no pagination/sorting
	query := `
		WITH asset_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				asset_type,
				COUNT(*) FILTER (WHERE balance > 0) as holder_count
			FROM trustlines_current
			WHERE asset_code IS NOT NULL AND asset_code != ''
			GROUP BY asset_code, asset_issuer, asset_type
		),
		transfer_stats AS (
			SELECT
				asset_code,
				asset_issuer,
				COALESCE(SUM(amount), 0) as volume_24h
			FROM token_transfers_raw
			WHERE timestamp >= NOW() - INTERVAL '24 hours'
			  AND transaction_successful = true
			GROUP BY asset_code, asset_issuer
		)
		SELECT COUNT(*)
		FROM asset_stats a
		LEFT JOIN transfer_stats t
			ON a.asset_code = t.asset_code
			AND COALESCE(a.asset_issuer, '') = COALESCE(t.asset_issuer, '')
		WHERE a.holder_count > 0
	`

	args := []interface{}{}
	argIndex := 1

	// Apply same filters as main query (excluding pagination/cursor)
	if filters.MinHolders != nil && *filters.MinHolders > 0 {
		query += fmt.Sprintf(" AND a.holder_count >= $%d", argIndex)
		args = append(args, *filters.MinHolders)
		argIndex++
	}

	if filters.MinVolume24h != nil && *filters.MinVolume24h > 0 {
		query += fmt.Sprintf(" AND COALESCE(t.volume_24h, 0) >= $%d", argIndex)
		args = append(args, *filters.MinVolume24h)
		argIndex++
	}

	if filters.AssetType != "" {
		query += fmt.Sprintf(" AND a.asset_type = $%d", argIndex)
		args = append(args, filters.AssetType)
		argIndex++
	}

	if filters.Search != "" {
		query += fmt.Sprintf(" AND UPPER(a.asset_code) LIKE UPPER($%d)", argIndex)
		args = append(args, filters.Search+"%")
		argIndex++
	}

	var count int
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count assets: %w", err)
	}

	return count, nil
}
