package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (h *SilverHotReader) GetDefiProtocols(ctx context.Context, filters DefiProtocolFilters) ([]DefiProtocol, error) {
	query := `SELECT protocol_id, network, display_name, slug, version, category, status,
		adapter_name, pricing_model, health_model,
		supports_history, supports_rewards, supports_health_factor,
		website_url, docs_url, icon_url, verified, config_json
		FROM serving.sv_defi_protocols WHERE 1=1`
	args := []any{}
	argIdx := 1

	if filters.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, filters.Status)
		argIdx++
	}
	if filters.Category != "" {
		query += fmt.Sprintf(" AND category = $%d", argIdx)
		args = append(args, filters.Category)
		argIdx++
	}
	if filters.Network != "" {
		query += fmt.Sprintf(" AND network = $%d", argIdx)
		args = append(args, filters.Network)
		argIdx++
	}

	query += " ORDER BY display_name ASC"
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []DefiProtocol{}
	for rows.Next() {
		var p DefiProtocol
		var version, healthModel, websiteURL, docsURL, iconURL sql.NullString
		var configRaw []byte
		if err := rows.Scan(
			&p.ProtocolID, &p.Network, &p.DisplayName, &p.Slug, &version, &p.Category, &p.Status,
			&p.AdapterName, &p.PricingModel, &healthModel,
			&p.SupportsHistory, &p.SupportsRewards, &p.SupportsHealthFactor,
			&websiteURL, &docsURL, &iconURL, &p.Verified, &configRaw,
		); err != nil {
			return nil, err
		}
		if version.Valid {
			p.Version = &version.String
		}
		if healthModel.Valid {
			p.HealthModel = &healthModel.String
		}
		if websiteURL.Valid {
			p.WebsiteURL = &websiteURL.String
		}
		if docsURL.Valid {
			p.DocsURL = &docsURL.String
		}
		if iconURL.Valid {
			p.IconURL = &iconURL.String
		}
		p.ConfigJSON = jsonObject(configRaw)
		results = append(results, p)
	}
	return results, rows.Err()
}

func (h *SilverHotReader) GetDefiMarkets(ctx context.Context, filters DefiMarketFilters) ([]DefiMarket, string, bool, error) {
	requestLimit := filters.Limit + 1
	query := `SELECT market_id, protocol_id, market_type, market_address, pool_address, router_address,
		oracle_contract_id,
		input_asset_1_type, input_asset_1_code, input_asset_1_issuer, input_asset_1_contract_id, input_asset_1_symbol, input_asset_1_decimals,
		input_asset_2_type, input_asset_2_code, input_asset_2_issuer, input_asset_2_contract_id, input_asset_2_symbol, input_asset_2_decimals,
		tvl_value_usd::TEXT, total_deposit_value_usd::TEXT, total_borrowed_value_usd::TEXT,
		apr_deposit::TEXT, apr_borrow::TEXT, apr_rewards::TEXT,
		is_active, metadata_json, as_of_ledger, as_of_time
		FROM serving.sv_defi_markets_current WHERE 1=1`
	args := []any{}
	argIdx := 1

	if filters.Protocol != "" {
		query += fmt.Sprintf(" AND protocol_id = $%d", argIdx)
		args = append(args, filters.Protocol)
		argIdx++
	}
	if filters.MarketType != "" {
		query += fmt.Sprintf(" AND market_type = $%d", argIdx)
		args = append(args, filters.MarketType)
		argIdx++
	}
	if filters.ActiveOnly {
		query += " AND is_active = true"
	}
	if filters.Cursor != nil {
		query += fmt.Sprintf(" AND market_id > $%d", argIdx)
		args = append(args, filters.Cursor.ID)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY market_id ASC LIMIT $%d", argIdx)
	args = append(args, requestLimit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	results := []DefiMarket{}
	for rows.Next() {
		var m DefiMarket
		var marketAddress, poolAddress, routerAddress, oracleContractID sql.NullString
		var a1Type, a1Code, a1Issuer, a1ContractID, a1Symbol sql.NullString
		var a2Type, a2Code, a2Issuer, a2ContractID, a2Symbol sql.NullString
		var a1Decimals, a2Decimals sql.NullInt32
		var tvl, totalDeposit, totalBorrowed, aprDeposit, aprBorrow, aprRewards sql.NullString
		var metadataRaw []byte
		var asOfTime time.Time
		if err := rows.Scan(
			&m.MarketID, &m.ProtocolID, &m.MarketType, &marketAddress, &poolAddress, &routerAddress,
			&oracleContractID,
			&a1Type, &a1Code, &a1Issuer, &a1ContractID, &a1Symbol, &a1Decimals,
			&a2Type, &a2Code, &a2Issuer, &a2ContractID, &a2Symbol, &a2Decimals,
			&tvl, &totalDeposit, &totalBorrowed,
			&aprDeposit, &aprBorrow, &aprRewards,
			&m.IsActive, &metadataRaw, &m.AsOfLedger, &asOfTime,
		); err != nil {
			return nil, "", false, err
		}
		m.MarketAddress = nullableString(marketAddress.String)
		m.PoolAddress = nullableString(poolAddress.String)
		m.RouterAddress = nullableString(routerAddress.String)
		m.OracleContractID = nullableString(oracleContractID.String)
		m.InputAsset1 = buildDefiAsset(a1Type, a1Code, a1Issuer, a1ContractID, a1Symbol, a1Decimals)
		m.InputAsset2 = buildDefiAsset(a2Type, a2Code, a2Issuer, a2ContractID, a2Symbol, a2Decimals)
		m.TVLValueUSD = nullableString(tvl.String)
		m.TotalDepositValueUSD = nullableString(totalDeposit.String)
		m.TotalBorrowedValueUSD = nullableString(totalBorrowed.String)
		m.APRDeposit = nullableString(aprDeposit.String)
		m.APRBorrow = nullableString(aprBorrow.String)
		m.APRRewards = nullableString(aprRewards.String)
		m.MetadataJSON = jsonObject(metadataRaw)
		m.AsOfTime = asOfTime.UTC().Format(time.RFC3339)
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}

	hasMore := len(results) > filters.Limit
	if hasMore {
		results = results[:filters.Limit]
	}
	var nextCursor string
	if hasMore && len(results) > 0 {
		nextCursor = (DefiCursor{ID: results[len(results)-1].MarketID}).Encode()
	}
	return results, nextCursor, hasMore, nil
}

func (h *SilverHotReader) GetDefiStatus(ctx context.Context) (*DefiStatusResponse, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT p.protocol_id,
			COALESCE(s.status, CASE WHEN p.status IN ('active', 'degraded') THEN 'ok' ELSE p.status END) as api_status,
			s.reason,
			s.last_successful_ledger,
			s.last_successful_time,
			s.freshness_seconds,
			COALESCE(s.source_divergence, false)
		FROM serving.sv_defi_protocols p
		LEFT JOIN serving.sv_defi_protocol_status s ON s.protocol_id = p.protocol_id
		ORDER BY p.protocol_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resp := &DefiStatusResponse{Status: "ok", Protocols: []DefiStatusProtocol{}}
	var maxLedger sql.NullInt64
	var maxFreshness sql.NullInt32

	for rows.Next() {
		var p DefiStatusProtocol
		var reason sql.NullString
		var lastLedger sql.NullInt64
		var lastTime sql.NullTime
		var fresh sql.NullInt32
		if err := rows.Scan(&p.ProtocolID, &p.Status, &reason, &lastLedger, &lastTime, &fresh, &p.SourceDivergence); err != nil {
			return nil, err
		}
		if reason.Valid {
			p.Reason = &reason.String
		}
		if lastLedger.Valid {
			v := lastLedger.Int64
			p.LastSuccessfulLedger = &v
			if !maxLedger.Valid || v > maxLedger.Int64 {
				maxLedger = lastLedger
			}
		}
		if lastTime.Valid {
			formatted := lastTime.Time.UTC().Format(time.RFC3339)
			p.LastSuccessfulTime = &formatted
		}
		if fresh.Valid {
			v := int(fresh.Int32)
			p.FreshnessSeconds = &v
			if !maxFreshness.Valid || fresh.Int32 > maxFreshness.Int32 {
				maxFreshness = fresh
			}
		}
		resp.Status = aggregateDefiOverallStatus(resp.Status, p.Status)
		resp.Protocols = append(resp.Protocols, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if maxLedger.Valid {
		v := maxLedger.Int64
		resp.AsOfLedger = &v
	}
	if maxFreshness.Valid {
		v := int(maxFreshness.Int32)
		resp.FreshnessSeconds = &v
	}
	return resp, nil
}

func aggregateDefiOverallStatus(current, candidate string) string {
	rank := func(v string) int {
		switch strings.ToLower(v) {
		case "halted", "paused", "retired":
			return 4
		case "degraded", "backfill":
			return 3
		case "stale_prices", "partial_protocol_coverage", "backfill_in_progress", "protocol_unavailable":
			return 2
		case "ok", "active":
			return 1
		default:
			return 1
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

func (h *SilverHotReader) GetDefiExposure(ctx context.Context, address, quote, protocol string) (*DefiExposureResponse, error) {
	if protocol != "" {
		return h.getDefiExposureFromPositions(ctx, address, quote, protocol)
	}

	var resp DefiExposureResponse
	var byProtocolRaw []byte
	var asOfTime time.Time
	var dataStatus sql.NullString
	var lowestHealth sql.NullString
	row := h.db.QueryRowContext(ctx, `
		SELECT owner_address, quote_currency, as_of_ledger, as_of_time,
			COALESCE(source_json->>'data_status', 'ok') as data_status,
			COALESCE(total_value, 0)::TEXT,
			COALESCE(total_deposit_value, 0)::TEXT,
			COALESCE(total_borrowed_value, 0)::TEXT,
			COALESCE(net_value, 0)::TEXT,
			COALESCE(total_claimable_rewards_value, 0)::TEXT,
			open_position_count,
			protocol_count,
			lowest_health_factor::TEXT,
			positions_at_risk,
			by_protocol_json
		FROM serving.sv_defi_user_totals_current
		WHERE owner_address = $1 AND quote_currency = $2
	`, address, quote)
	if err := row.Scan(
		&resp.Address, &resp.Quote, &resp.AsOfLedger, &asOfTime,
		&dataStatus,
		&resp.Totals.TotalValue,
		&resp.Totals.TotalDepositValue,
		&resp.Totals.TotalBorrowedValue,
		&resp.Totals.NetValue,
		&resp.Totals.TotalClaimableRewardsValue,
		&resp.Totals.OpenPositionCount,
		&resp.Totals.ProtocolCount,
		&lowestHealth,
		&resp.Totals.PositionsAtRisk,
		&byProtocolRaw,
	); err != nil {
		return nil, err
	}
	if dataStatus.Valid {
		resp.DataStatus = dataStatus.String
	} else {
		resp.DataStatus = "ok"
	}
	if lowestHealth.Valid {
		resp.Totals.LowestHealthFactor = &lowestHealth.String
	}
	resp.AsOfTime = asOfTime.UTC().Format(time.RFC3339)
	resp.FreshnessSeconds = freshnessSeconds(asOfTime)
	resp.ByProtocol = jsonByProtocol(byProtocolRaw)
	return &resp, nil
}

func (h *SilverHotReader) getDefiExposureFromPositions(ctx context.Context, address, quote, protocol string) (*DefiExposureResponse, error) {
	row := h.db.QueryRowContext(ctx, `
		SELECT owner_address,
			quote_currency,
			COALESCE(MAX(as_of_ledger), 0),
			MAX(as_of_time),
			COALESCE(SUM(current_value), 0)::TEXT,
			COALESCE(SUM(deposit_value), 0)::TEXT,
			COALESCE(SUM(borrowed_value), 0)::TEXT,
			COALESCE(SUM(net_value), 0)::TEXT,
			COALESCE(SUM(claimable_rewards_value), 0)::TEXT,
			COUNT(*) FILTER (WHERE status = 'open'),
			COUNT(DISTINCT protocol_id),
			MIN(health_factor)::TEXT,
			COUNT(*) FILTER (WHERE risk_status IS NOT NULL AND risk_status NOT IN ('ok', 'healthy'))
		FROM serving.sv_defi_positions_current
		WHERE owner_address = $1 AND quote_currency = $2 AND protocol_id = $3
		GROUP BY owner_address, quote_currency
	`, address, quote, protocol)

	var resp DefiExposureResponse
	var asOfTime time.Time
	var lowestHealth sql.NullString
	if err := row.Scan(
		&resp.Address, &resp.Quote, &resp.AsOfLedger, &asOfTime,
		&resp.Totals.TotalValue, &resp.Totals.TotalDepositValue,
		&resp.Totals.TotalBorrowedValue, &resp.Totals.NetValue,
		&resp.Totals.TotalClaimableRewardsValue, &resp.Totals.OpenPositionCount,
		&resp.Totals.ProtocolCount, &lowestHealth, &resp.Totals.PositionsAtRisk,
	); err != nil {
		return nil, err
	}
	if lowestHealth.Valid {
		resp.Totals.LowestHealthFactor = &lowestHealth.String
	}
	resp.DataStatus = "ok"
	resp.AsOfTime = asOfTime.UTC().Format(time.RFC3339)
	resp.FreshnessSeconds = freshnessSeconds(asOfTime)
	resp.ByProtocol = []DefiExposureByProtocol{{
		ProtocolID:         protocol,
		TotalValue:         resp.Totals.TotalValue,
		TotalDepositValue:  resp.Totals.TotalDepositValue,
		TotalBorrowedValue: resp.Totals.TotalBorrowedValue,
		NetValue:           resp.Totals.NetValue,
		PositionCount:      resp.Totals.OpenPositionCount,
	}}
	return &resp, nil
}

func (h *SilverHotReader) GetDefiPositions(ctx context.Context, filters DefiPositionsFilters) (*DefiPositionsResponse, error) {
	summary, err := h.GetDefiExposure(ctx, filters.Address, filters.Quote, filters.Protocol)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == sql.ErrNoRows {
		summary = &DefiExposureResponse{
			Address:    filters.Address,
			Quote:      filters.Quote,
			DataStatus: "ok",
			ByProtocol: []DefiExposureByProtocol{},
			Totals: DefiExposureSummary{
				TotalValue:                 "0",
				TotalDepositValue:          "0",
				TotalBorrowedValue:         "0",
				NetValue:                   "0",
				TotalClaimableRewardsValue: "0",
			},
		}
	}

	requestLimit := filters.Limit + 1
	query := `SELECT position_id, protocol_id, protocol_version, position_type, status,
		owner_address, account_address, related_address, market_id, market_address, position_key_hash,
		underlying_asset_type, underlying_asset_code, underlying_asset_issuer, underlying_asset_contract_id, underlying_symbol, underlying_decimals,
		quote_currency,
		deposit_amount::TEXT, borrow_amount::TEXT, share_amount::TEXT, claimable_reward_amount::TEXT,
		deposit_value::TEXT, borrowed_value::TEXT, current_value::TEXT, net_value::TEXT,
		current_return_value::TEXT, current_return_percent::TEXT, claimable_rewards_value::TEXT,
		health_factor::TEXT, ltv::TEXT, collateral_ratio::TEXT, liquidation_threshold::TEXT, risk_status,
		protocol_state_json, valuation_json, source_json,
		as_of_ledger, as_of_time, last_updated_ledger, last_updated_at
		FROM serving.sv_defi_positions_current
		WHERE owner_address = $1 AND quote_currency = $2`
	args := []any{filters.Address, filters.Quote}
	argIdx := 3
	if filters.Protocol != "" {
		query += fmt.Sprintf(" AND protocol_id = $%d", argIdx)
		args = append(args, filters.Protocol)
		argIdx++
	}
	if !filters.IncludeClosed {
		query += " AND status = 'open'"
	}
	if filters.Cursor != nil {
		query += fmt.Sprintf(" AND position_id > $%d", argIdx)
		args = append(args, filters.Cursor.ID)
		argIdx++
	}
	query += fmt.Sprintf(" ORDER BY position_id ASC LIMIT $%d", argIdx)
	args = append(args, requestLimit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	positions := []DefiPosition{}
	for rows.Next() {
		position, err := scanDefiPositionRow(rows)
		if err != nil {
			return nil, err
		}
		positions = append(positions, position)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(positions) > filters.Limit
	if hasMore {
		positions = positions[:filters.Limit]
	}
	var nextCursor *string
	if hasMore && len(positions) > 0 {
		encoded := (DefiCursor{ID: positions[len(positions)-1].PositionID}).Encode()
		nextCursor = &encoded
	}

	resp := &DefiPositionsResponse{
		Address:          filters.Address,
		Quote:            filters.Quote,
		AsOfLedger:       summary.AsOfLedger,
		AsOfTime:         summary.AsOfTime,
		FreshnessSeconds: summary.FreshnessSeconds,
		DataStatus:       summary.DataStatus,
		Summary:          summary.Totals,
		Positions:        positions,
		HasMore:          hasMore,
		NextCursor:       nextCursor,
	}
	return resp, nil
}

func (h *SilverHotReader) GetDefiPosition(ctx context.Context, positionID, quote string) (*DefiPosition, []DefiPositionComponent, error) {
	row := h.db.QueryRowContext(ctx, `SELECT position_id, protocol_id, protocol_version, position_type, status,
		owner_address, account_address, related_address, market_id, market_address, position_key_hash,
		underlying_asset_type, underlying_asset_code, underlying_asset_issuer, underlying_asset_contract_id, underlying_symbol, underlying_decimals,
		quote_currency,
		deposit_amount::TEXT, borrow_amount::TEXT, share_amount::TEXT, claimable_reward_amount::TEXT,
		deposit_value::TEXT, borrowed_value::TEXT, current_value::TEXT, net_value::TEXT,
		current_return_value::TEXT, current_return_percent::TEXT, claimable_rewards_value::TEXT,
		health_factor::TEXT, ltv::TEXT, collateral_ratio::TEXT, liquidation_threshold::TEXT, risk_status,
		protocol_state_json, valuation_json, source_json,
		as_of_ledger, as_of_time, last_updated_ledger, last_updated_at
		FROM serving.sv_defi_positions_current
		WHERE position_id = $1 AND quote_currency = $2`, positionID, quote)
	position, err := scanDefiPositionSingleRow(row)
	if err != nil {
		return nil, nil, err
	}

	rows, err := h.db.QueryContext(ctx, `SELECT component_id, component_type,
		asset_type, asset_code, asset_issuer, asset_contract_id, symbol, decimals,
		amount::TEXT, value::TEXT, price::TEXT, price_source, metadata_json
		FROM serving.sv_defi_position_components_current
		WHERE position_id = $1
		ORDER BY component_id ASC`, positionID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	components := []DefiPositionComponent{}
	for rows.Next() {
		var c DefiPositionComponent
		var assetType, assetCode, assetIssuer, assetContractID, symbol, amount, value, price, priceSource sql.NullString
		var decimals sql.NullInt32
		var metadataRaw []byte
		if err := rows.Scan(&c.ComponentID, &c.ComponentType, &assetType, &assetCode, &assetIssuer, &assetContractID, &symbol, &decimals, &amount, &value, &price, &priceSource, &metadataRaw); err != nil {
			return nil, nil, err
		}
		c.Asset = buildDefiAsset(assetType, assetCode, assetIssuer, assetContractID, symbol, decimals)
		c.Amount = nullableString(amount.String)
		c.Value = nullableString(value.String)
		c.Price = nullableString(price.String)
		c.PriceSource = nullableString(priceSource.String)
		c.MetadataJSON = jsonObject(metadataRaw)
		components = append(components, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return position, components, nil
}

func scanDefiPositionSingleRow(row *sql.Row) (*DefiPosition, error) {
	var position DefiPosition
	var protocolVersion, accountAddress, relatedAddress, marketID, marketAddress, positionKeyHash sql.NullString
	var assetType, assetCode, assetIssuer, assetContractID, symbol sql.NullString
	var decimals sql.NullInt32
	var depositAmount, borrowAmount, shareAmount, claimableRewardAmount sql.NullString
	var depositValue, borrowedValue, currentValue, netValue sql.NullString
	var currentReturnValue, currentReturnPercent, claimableRewardsValue sql.NullString
	var healthFactor, ltv, collateralRatio, liquidationThreshold, riskStatus sql.NullString
	var protocolStateRaw, valuationRaw, sourceRaw []byte
	var asOfTime, lastUpdatedAt time.Time
	if err := row.Scan(
		&position.PositionID, &position.ProtocolID, &protocolVersion, &position.PositionType, &position.Status,
		&position.OwnerAddress, &accountAddress, &relatedAddress, &marketID, &marketAddress, &positionKeyHash,
		&assetType, &assetCode, &assetIssuer, &assetContractID, &symbol, &decimals,
		&position.QuoteCurrency,
		&depositAmount, &borrowAmount, &shareAmount, &claimableRewardAmount,
		&depositValue, &borrowedValue, &currentValue, &netValue,
		&currentReturnValue, &currentReturnPercent, &claimableRewardsValue,
		&healthFactor, &ltv, &collateralRatio, &liquidationThreshold, &riskStatus,
		&protocolStateRaw, &valuationRaw, &sourceRaw,
		&position.AsOfLedger, &asOfTime, &position.LastUpdatedLedger, &lastUpdatedAt,
	); err != nil {
		return nil, err
	}
	position.ProtocolVersion = nullableString(protocolVersion.String)
	position.AccountAddress = nullableString(accountAddress.String)
	position.RelatedAddress = nullableString(relatedAddress.String)
	position.MarketID = nullableString(marketID.String)
	position.MarketAddress = nullableString(marketAddress.String)
	position.PositionKeyHash = nullableString(positionKeyHash.String)
	position.UnderlyingAsset = buildDefiAsset(assetType, assetCode, assetIssuer, assetContractID, symbol, decimals)
	position.DepositAmount = nullableString(depositAmount.String)
	position.BorrowAmount = nullableString(borrowAmount.String)
	position.ShareAmount = nullableString(shareAmount.String)
	position.ClaimableRewardAmount = nullableString(claimableRewardAmount.String)
	position.DepositValue = nullableString(depositValue.String)
	position.BorrowedValue = nullableString(borrowedValue.String)
	position.CurrentValue = nullableString(currentValue.String)
	position.NetValue = nullableString(netValue.String)
	position.CurrentReturnValue = nullableString(currentReturnValue.String)
	position.CurrentReturnPercent = nullableString(currentReturnPercent.String)
	position.ClaimableRewardsValue = nullableString(claimableRewardsValue.String)
	position.HealthFactor = nullableString(healthFactor.String)
	position.LTV = nullableString(ltv.String)
	position.CollateralRatio = nullableString(collateralRatio.String)
	position.LiquidationThreshold = nullableString(liquidationThreshold.String)
	position.RiskStatus = nullableString(riskStatus.String)
	position.ProtocolStateJSON = jsonObject(protocolStateRaw)
	position.ValuationJSON = jsonObject(valuationRaw)
	position.SourceJSON = jsonObject(sourceRaw)
	position.AsOfTime = asOfTime.UTC().Format(time.RFC3339)
	position.LastUpdatedAt = lastUpdatedAt.UTC().Format(time.RFC3339)
	return &position, nil
}

func scanDefiPositionRow(rows *sql.Rows) (DefiPosition, error) {
	return scanDefiPositionCommon(func(dest ...any) error {
		return rows.Scan(dest...)
	})
}

func scanDefiPositionCommon(scan func(dest ...any) error) (DefiPosition, error) {
	var position DefiPosition
	var protocolVersion, accountAddress, relatedAddress, marketID, marketAddress, positionKeyHash sql.NullString
	var assetType, assetCode, assetIssuer, assetContractID, symbol sql.NullString
	var decimals sql.NullInt32
	var depositAmount, borrowAmount, shareAmount, claimableRewardAmount sql.NullString
	var depositValue, borrowedValue, currentValue, netValue sql.NullString
	var currentReturnValue, currentReturnPercent, claimableRewardsValue sql.NullString
	var healthFactor, ltv, collateralRatio, liquidationThreshold, riskStatus sql.NullString
	var protocolStateRaw, valuationRaw, sourceRaw []byte
	var asOfTime, lastUpdatedAt time.Time
	if err := scan(
		&position.PositionID, &position.ProtocolID, &protocolVersion, &position.PositionType, &position.Status,
		&position.OwnerAddress, &accountAddress, &relatedAddress, &marketID, &marketAddress, &positionKeyHash,
		&assetType, &assetCode, &assetIssuer, &assetContractID, &symbol, &decimals,
		&position.QuoteCurrency,
		&depositAmount, &borrowAmount, &shareAmount, &claimableRewardAmount,
		&depositValue, &borrowedValue, &currentValue, &netValue,
		&currentReturnValue, &currentReturnPercent, &claimableRewardsValue,
		&healthFactor, &ltv, &collateralRatio, &liquidationThreshold, &riskStatus,
		&protocolStateRaw, &valuationRaw, &sourceRaw,
		&position.AsOfLedger, &asOfTime, &position.LastUpdatedLedger, &lastUpdatedAt,
	); err != nil {
		return DefiPosition{}, err
	}
	position.ProtocolVersion = nullableString(protocolVersion.String)
	position.AccountAddress = nullableString(accountAddress.String)
	position.RelatedAddress = nullableString(relatedAddress.String)
	position.MarketID = nullableString(marketID.String)
	position.MarketAddress = nullableString(marketAddress.String)
	position.PositionKeyHash = nullableString(positionKeyHash.String)
	position.UnderlyingAsset = buildDefiAsset(assetType, assetCode, assetIssuer, assetContractID, symbol, decimals)
	position.DepositAmount = nullableString(depositAmount.String)
	position.BorrowAmount = nullableString(borrowAmount.String)
	position.ShareAmount = nullableString(shareAmount.String)
	position.ClaimableRewardAmount = nullableString(claimableRewardAmount.String)
	position.DepositValue = nullableString(depositValue.String)
	position.BorrowedValue = nullableString(borrowedValue.String)
	position.CurrentValue = nullableString(currentValue.String)
	position.NetValue = nullableString(netValue.String)
	position.CurrentReturnValue = nullableString(currentReturnValue.String)
	position.CurrentReturnPercent = nullableString(currentReturnPercent.String)
	position.ClaimableRewardsValue = nullableString(claimableRewardsValue.String)
	position.HealthFactor = nullableString(healthFactor.String)
	position.LTV = nullableString(ltv.String)
	position.CollateralRatio = nullableString(collateralRatio.String)
	position.LiquidationThreshold = nullableString(liquidationThreshold.String)
	position.RiskStatus = nullableString(riskStatus.String)
	position.ProtocolStateJSON = jsonObject(protocolStateRaw)
	position.ValuationJSON = jsonObject(valuationRaw)
	position.SourceJSON = jsonObject(sourceRaw)
	position.AsOfTime = asOfTime.UTC().Format(time.RFC3339)
	position.LastUpdatedAt = lastUpdatedAt.UTC().Format(time.RFC3339)
	return position, nil
}
