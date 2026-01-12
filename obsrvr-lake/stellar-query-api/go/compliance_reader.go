package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// ============================================
// COMPLIANCE ARCHIVE API - Reader Methods
// ============================================

// formatSupplyTimestamp formats a date string to RFC3339 timestamp
// Handles both "2026-01-12" and "2026-01-12T00:00:00Z" formats
func formatSupplyTimestamp(dateStr string) string {
	// If already has timestamp portion, return as is
	if strings.Contains(dateStr, "T") {
		return dateStr
	}
	// Otherwise, add midnight UTC
	return dateStr + "T00:00:00Z"
}

// GetAssetTransactions returns all transactions for an asset within a date range
// Used for compliance transaction lineage archives
func (r *UnifiedSilverReader) GetAssetTransactions(ctx context.Context, assetCode, assetIssuer string, startDate, endDate time.Time, includeFailed bool, limit int) (*ComplianceTransactionsResponse, error) {
	// Build query for enriched_history_operations
	query := `
		SELECT
			ledger_sequence,
			ledger_closed_at,
			transaction_hash,
			COALESCE(operation_index, 0) as operation_index,
			type,
			source_account,
			destination,
			amount,
			tx_successful
		FROM enriched_history_operations
		WHERE is_payment_op = true
		  AND ledger_closed_at >= $1
		  AND ledger_closed_at <= $2
	`

	args := []interface{}{startDate, endDate}
	argIdx := 3

	// Handle native XLM vs issued assets
	if assetCode == "XLM" || assetCode == "native" {
		query += fmt.Sprintf(" AND (asset_code IS NULL OR asset_code = '' OR asset_code = 'XLM')")
	} else {
		query += fmt.Sprintf(" AND asset_code = $%d", argIdx)
		args = append(args, assetCode)
		argIdx++

		if assetIssuer != "" {
			query += fmt.Sprintf(" AND asset_issuer = $%d", argIdx)
			args = append(args, assetIssuer)
			argIdx++
		}
	}

	if !includeFailed {
		query += " AND tx_successful = true"
	}

	query += fmt.Sprintf(" ORDER BY ledger_sequence, operation_index LIMIT $%d", argIdx)
	args = append(args, limit+1) // +1 to check if more results exist

	rows, err := r.hot.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []ComplianceTransaction
	var minLedger, maxLedger int64
	uniqueAccounts := make(map[string]bool)
	var totalVolume big.Float
	successCount, failCount := 0, 0

	for rows.Next() {
		var ledgerSeq int64
		var closedAt string
		var txHash string
		var opIndex int
		var opType int32
		var sourceAccount string
		var destination *string
		var amount *string
		var successful bool

		if err := rows.Scan(&ledgerSeq, &closedAt, &txHash, &opIndex, &opType,
			&sourceAccount, &destination, &amount, &successful); err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}

		// Track ledger range
		if minLedger == 0 || ledgerSeq < minLedger {
			minLedger = ledgerSeq
		}
		if ledgerSeq > maxLedger {
			maxLedger = ledgerSeq
		}

		// Track unique accounts
		uniqueAccounts[sourceAccount] = true
		if destination != nil {
			uniqueAccounts[*destination] = true
		}

		// Track volume
		if amount != nil {
			var amtFloat big.Float
			amtFloat.SetString(*amount)
			totalVolume.Add(&totalVolume, &amtFloat)
		}

		// Track success/failure
		if successful {
			successCount++
		} else {
			failCount++
		}

		// Only include up to limit (we fetched limit+1)
		if len(transactions) < limit {
			toAccount := ""
			if destination != nil {
				toAccount = *destination
			}
			amountStr := "0"
			if amount != nil {
				amountStr = *amount
			}

			transactions = append(transactions, ComplianceTransaction{
				LedgerSequence:  ledgerSeq,
				ClosedAt:        closedAt,
				TransactionHash: txHash,
				OperationIndex:  opIndex,
				OperationType:   operationTypeName(opType),
				FromAccount:     sourceAccount,
				ToAccount:       toAccount,
				Amount:          amountStr,
				Successful:      successful,
			})
		}
	}

	// Build asset info
	asset := AssetInfo{
		Code: assetCode,
	}
	if assetCode == "XLM" || assetCode == "native" {
		asset.Type = "native"
	} else {
		asset.Type = "credit_alphanum4"
		if len(assetCode) > 4 {
			asset.Type = "credit_alphanum12"
		}
		if assetIssuer != "" {
			asset.Issuer = &assetIssuer
		}
	}

	// Build period info
	period := PeriodInfo{
		Start:       startDate.Format(time.RFC3339),
		End:         endDate.Format(time.RFC3339),
		StartLedger: minLedger,
		EndLedger:   maxLedger,
	}

	// Build summary
	summary := TransactionSummary{
		TotalTransactions:      len(transactions),
		TotalVolume:            totalVolume.Text('f', 7),
		UniqueAccounts:         len(uniqueAccounts),
		SuccessfulTransactions: successCount,
		FailedTransactions:     failCount,
	}

	// Generate archive ID
	archiveID := GenerateArchiveID("txn", assetCode, time.Now())

	// Generate checksum (over deterministic data only)
	checksum, err := GenerateTransactionsChecksum(asset, period, transactions)
	if err != nil {
		checksum = "error_generating_checksum"
	}

	return &ComplianceTransactionsResponse{
		ArchiveID:          archiveID,
		Asset:              asset,
		Period:             period,
		Summary:            summary,
		Transactions:       transactions,
		Checksum:           checksum,
		MethodologyVersion: MethodologyTransactionsV1,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetComplianceBalances returns all holders of an asset at a specific timestamp
// with supply statistics for compliance reporting
func (r *UnifiedSilverReader) GetComplianceBalances(ctx context.Context, assetCode, assetIssuer string, timestamp time.Time, minBalance string, limit int) (*ComplianceBalancesResponse, error) {
	// For native XLM, query accounts_snapshot
	// For issued assets, query trustlines_snapshot
	var holders []ComplianceHolder
	var totalSupply big.Float
	var issuerBalance big.Float
	var maxLedger int64
	var totalHolderCount int

	if assetCode == "XLM" || assetCode == "native" {
		// First, get aggregate totals (total supply and holder count) from ALL accounts
		aggQuery := `
			SELECT
				COALESCE(SUM(CAST(balance AS NUMERIC)), 0) as total_supply,
				COUNT(DISTINCT account_id) as holder_count,
				COALESCE(MAX(ledger_sequence), 0) as max_ledger
			FROM accounts_snapshot
			WHERE closed_at <= $1
			  AND (valid_to IS NULL OR valid_to > $1)
			  AND CAST(balance AS NUMERIC) > 0
		`
		var totalSupplyStr string
		err := r.hot.db.QueryRowContext(ctx, aggQuery, timestamp).Scan(&totalSupplyStr, &totalHolderCount, &maxLedger)
		if err != nil {
			return nil, fmt.Errorf("failed to query XLM supply totals: %w", err)
		}
		totalSupply.SetString(totalSupplyStr)

		// Now query individual holders with limit
		query := `
			SELECT
				account_id,
				balance,
				ledger_sequence
			FROM accounts_snapshot
			WHERE closed_at <= $1
			  AND (valid_to IS NULL OR valid_to > $1)
			  AND CAST(balance AS NUMERIC) > 0
		`
		args := []interface{}{timestamp}
		argIdx := 2

		if minBalance != "" {
			query += fmt.Sprintf(" AND CAST(balance AS NUMERIC) >= $%d", argIdx)
			args = append(args, minBalance)
			argIdx++
		}

		query += fmt.Sprintf(" ORDER BY CAST(balance AS NUMERIC) DESC LIMIT $%d", argIdx)
		args = append(args, limit)

		rows, err := r.hot.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query XLM balances: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var accountID, balance string
			var ledgerSeq int64
			if err := rows.Scan(&accountID, &balance, &ledgerSeq); err != nil {
				continue
			}

			holders = append(holders, ComplianceHolder{
				AccountID: accountID,
				Balance:   balance,
			})
		}
	} else {
		// First, get aggregate totals for issued assets
		aggQuery := `
			SELECT
				COALESCE(SUM(CAST(balance AS NUMERIC)), 0) as total_supply,
				COUNT(DISTINCT account_id) as holder_count,
				COALESCE(MAX(ledger_sequence), 0) as max_ledger
			FROM trustlines_snapshot
			WHERE asset_code = $1
			  AND asset_issuer = $2
			  AND created_at <= $3
			  AND (valid_to IS NULL OR valid_to > $3)
			  AND CAST(balance AS NUMERIC) > 0
		`
		var totalSupplyStr string
		err := r.hot.db.QueryRowContext(ctx, aggQuery, assetCode, assetIssuer, timestamp).Scan(&totalSupplyStr, &totalHolderCount, &maxLedger)
		if err != nil {
			return nil, fmt.Errorf("failed to query asset supply totals: %w", err)
		}
		totalSupply.SetString(totalSupplyStr)

		// Now query individual holders with limit
		query := `
			SELECT
				account_id,
				balance,
				ledger_sequence
			FROM trustlines_snapshot
			WHERE asset_code = $1
			  AND asset_issuer = $2
			  AND created_at <= $3
			  AND (valid_to IS NULL OR valid_to > $3)
			  AND CAST(balance AS NUMERIC) > 0
		`
		args := []interface{}{assetCode, assetIssuer, timestamp}
		argIdx := 4

		if minBalance != "" {
			query += fmt.Sprintf(" AND CAST(balance AS NUMERIC) >= $%d", argIdx)
			args = append(args, minBalance)
			argIdx++
		}

		query += fmt.Sprintf(" ORDER BY CAST(balance AS NUMERIC) DESC LIMIT $%d", argIdx)
		args = append(args, limit)

		rows, err := r.hot.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query trustline balances: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var accountID, balance string
			var ledgerSeq int64
			if err := rows.Scan(&accountID, &balance, &ledgerSeq); err != nil {
				continue
			}

			holders = append(holders, ComplianceHolder{
				AccountID: accountID,
				Balance:   balance,
			})
		}

		// Get issuer balance from accounts_snapshot
		issuerQuery := `
			SELECT balance
			FROM accounts_snapshot
			WHERE account_id = $1
			  AND closed_at <= $2
			  AND (valid_to IS NULL OR valid_to > $2)
			ORDER BY closed_at DESC
			LIMIT 1
		`
		var issuerBalStr sql.NullString
		err = r.hot.db.QueryRowContext(ctx, issuerQuery, assetIssuer, timestamp).Scan(&issuerBalStr)
		if err == nil && issuerBalStr.Valid {
			issuerBalance.SetString(issuerBalStr.String)
		}
	}

	// Calculate percent of supply for each holder
	if totalSupply.Cmp(big.NewFloat(0)) > 0 {
		for i := range holders {
			var holderBal big.Float
			holderBal.SetString(holders[i].Balance)
			var percent big.Float
			percent.Quo(&holderBal, &totalSupply)
			percent.Mul(&percent, big.NewFloat(100))
			holders[i].PercentOfSupply = percent.Text('f', 2)
		}
	}

	// Calculate circulating supply
	var circulatingSupply big.Float
	circulatingSupply.Sub(&totalSupply, &issuerBalance)

	// Build asset info
	asset := AssetInfo{
		Code: assetCode,
	}
	if assetCode == "XLM" || assetCode == "native" {
		asset.Type = "native"
	} else {
		asset.Type = "credit_alphanum4"
		if len(assetCode) > 4 {
			asset.Type = "credit_alphanum12"
		}
		if assetIssuer != "" {
			asset.Issuer = &assetIssuer
		}
	}

	// Build summary - use totalHolderCount from aggregate query, not len(holders) which is limited
	summary := BalanceSummary{
		TotalHolders:      totalHolderCount,
		TotalSupply:       totalSupply.Text('f', 7),
		IssuerBalance:     issuerBalance.Text('f', 7),
		CirculatingSupply: circulatingSupply.Text('f', 7),
	}

	// Generate archive ID
	archiveID := GenerateArchiveID("bal", assetCode, timestamp)

	// Generate checksum
	snapshotAt := timestamp.Format(time.RFC3339)
	checksum, err := GenerateBalancesChecksum(asset, snapshotAt, holders)
	if err != nil {
		checksum = "error_generating_checksum"
	}

	return &ComplianceBalancesResponse{
		ArchiveID:          archiveID,
		Asset:              asset,
		SnapshotAt:         snapshotAt,
		SnapshotLedger:     maxLedger,
		Summary:            summary,
		Holders:            holders,
		Checksum:           checksum,
		MethodologyVersion: MethodologyBalancesV1,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetSupplyTimeline returns daily supply totals for an asset over a date range
func (r *UnifiedSilverReader) GetSupplyTimeline(ctx context.Context, assetCode, assetIssuer string, startDate, endDate time.Time, interval string) (*ComplianceSupplyResponse, error) {
	// For each day, calculate point-in-time supply using SCD2 logic
	var timeline []SupplyDataPoint

	if assetCode == "XLM" || assetCode == "native" {
		// For XLM, use accounts_snapshot with proper SCD2 point-in-time queries
		// Generate list of dates to query
		var prevSupply *big.Float
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
			// End of day timestamp for this date
			endOfDay := time.Date(d.Year(), d.Month(), d.Day(), 23, 59, 59, 0, time.UTC)

			// Query point-in-time supply using SCD2 logic
			query := `
				SELECT
					COALESCE(SUM(CAST(balance AS NUMERIC)), 0) as total_supply,
					COUNT(DISTINCT account_id) as holder_count,
					COALESCE(MAX(ledger_sequence), 0) as max_ledger
				FROM accounts_snapshot
				WHERE closed_at <= $1
				  AND (valid_to IS NULL OR valid_to > $1)
				  AND CAST(balance AS NUMERIC) > 0
			`

			var totalSupplyStr string
			var holderCount int
			var maxLedger int64

			err := r.hot.db.QueryRowContext(ctx, query, endOfDay).Scan(&totalSupplyStr, &holderCount, &maxLedger)
			if err != nil {
				continue // Skip this day if query fails
			}

			var totalSupply big.Float
			totalSupply.SetString(totalSupplyStr)

			dataPoint := SupplyDataPoint{
				Timestamp:         d.Format("2006-01-02") + "T00:00:00Z",
				LedgerSequence:    maxLedger,
				TotalSupply:       totalSupply.Text('f', 7),
				CirculatingSupply: totalSupply.Text('f', 7), // For XLM, all is circulating
				IssuerBalance:     "0",
				HolderCount:       holderCount,
			}

			// Calculate supply change
			if prevSupply != nil {
				var change big.Float
				change.Sub(&totalSupply, prevSupply)
				changeStr := change.Text('f', 7)
				dataPoint.SupplyChange = &changeStr

				if prevSupply.Cmp(big.NewFloat(0)) > 0 {
					var pctChange big.Float
					pctChange.Quo(&change, prevSupply)
					pctChange.Mul(&pctChange, big.NewFloat(100))
					pctStr := pctChange.Text('f', 2)
					dataPoint.SupplyChangePercent = &pctStr
				}
			}

			timeline = append(timeline, dataPoint)
			prevSupply = new(big.Float).Copy(&totalSupply)
		}
	} else {
		// For issued assets, use trustlines_snapshot with proper SCD2 point-in-time queries
		var prevSupply *big.Float
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
			// End of day timestamp for this date
			endOfDay := time.Date(d.Year(), d.Month(), d.Day(), 23, 59, 59, 0, time.UTC)

			// Query point-in-time supply using SCD2 logic
			query := `
				SELECT
					COALESCE(SUM(CAST(balance AS NUMERIC)), 0) as total_supply,
					COUNT(DISTINCT account_id) as holder_count,
					COALESCE(MAX(ledger_sequence), 0) as max_ledger
				FROM trustlines_snapshot
				WHERE asset_code = $1
				  AND asset_issuer = $2
				  AND created_at <= $3
				  AND (valid_to IS NULL OR valid_to > $3)
				  AND CAST(balance AS NUMERIC) > 0
			`

			var totalSupplyStr string
			var holderCount int
			var maxLedger int64

			err := r.hot.db.QueryRowContext(ctx, query, assetCode, assetIssuer, endOfDay).Scan(&totalSupplyStr, &holderCount, &maxLedger)
			if err != nil {
				continue // Skip this day if query fails
			}

			var totalSupply big.Float
			totalSupply.SetString(totalSupplyStr)

			dataPoint := SupplyDataPoint{
				Timestamp:         d.Format("2006-01-02") + "T00:00:00Z",
				LedgerSequence:    maxLedger,
				TotalSupply:       totalSupply.Text('f', 7),
				CirculatingSupply: totalSupply.Text('f', 7), // TODO: subtract issuer balance
				IssuerBalance:     "0",                      // TODO: get from accounts_snapshot
				HolderCount:       holderCount,
			}

			// Calculate supply change
			if prevSupply != nil {
				var change big.Float
				change.Sub(&totalSupply, prevSupply)
				changeStr := change.Text('f', 7)
				dataPoint.SupplyChange = &changeStr

				if prevSupply.Cmp(big.NewFloat(0)) > 0 {
					var pctChange big.Float
					pctChange.Quo(&change, prevSupply)
					pctChange.Mul(&pctChange, big.NewFloat(100))
					pctStr := pctChange.Text('f', 2)
					dataPoint.SupplyChangePercent = &pctStr
				}
			}

			timeline = append(timeline, dataPoint)
			prevSupply = new(big.Float).Copy(&totalSupply)
		}
	}

	// Build asset info
	asset := AssetInfo{
		Code: assetCode,
	}
	if assetCode == "XLM" || assetCode == "native" {
		asset.Type = "native"
	} else {
		asset.Type = "credit_alphanum4"
		if len(assetCode) > 4 {
			asset.Type = "credit_alphanum12"
		}
		if assetIssuer != "" {
			asset.Issuer = &assetIssuer
		}
	}

	// Build period info
	period := PeriodInfo{
		Start: startDate.Format(time.RFC3339),
		End:   endDate.Format(time.RFC3339),
	}
	if len(timeline) > 0 {
		period.StartLedger = timeline[0].LedgerSequence
		period.EndLedger = timeline[len(timeline)-1].LedgerSequence
	}

	// Build summary
	summary := SupplySummary{
		DataPoints: len(timeline),
	}
	if len(timeline) > 0 {
		summary.StartSupply = timeline[0].TotalSupply
		summary.EndSupply = timeline[len(timeline)-1].TotalSupply

		// Calculate net minted
		var startSup, endSup big.Float
		startSup.SetString(summary.StartSupply)
		endSup.SetString(summary.EndSupply)
		var netMinted big.Float
		netMinted.Sub(&endSup, &startSup)
		summary.NetMinted = netMinted.Text('f', 7)

		// Find peak and lowest
		var peakSupply, lowestSupply big.Float
		peakSupply.SetString(timeline[0].TotalSupply)
		lowestSupply.SetString(timeline[0].TotalSupply)
		summary.PeakDate = timeline[0].Timestamp[:10]
		summary.LowestDate = timeline[0].Timestamp[:10]

		for _, dp := range timeline {
			var sup big.Float
			sup.SetString(dp.TotalSupply)
			if sup.Cmp(&peakSupply) > 0 {
				peakSupply.Copy(&sup)
				summary.PeakDate = dp.Timestamp[:10]
			}
			if sup.Cmp(&lowestSupply) < 0 {
				lowestSupply.Copy(&sup)
				summary.LowestDate = dp.Timestamp[:10]
			}
		}
		summary.PeakSupply = peakSupply.Text('f', 7)
		summary.LowestSupply = lowestSupply.Text('f', 7)
	}

	// Generate archive ID
	archiveID := GenerateArchiveID("supply", assetCode, time.Now())

	// Generate checksum
	checksum, err := GenerateSupplyChecksum(asset, period, interval, timeline)
	if err != nil {
		checksum = "error_generating_checksum"
	}

	return &ComplianceSupplyResponse{
		ArchiveID:          archiveID,
		Asset:              asset,
		Period:             period,
		Interval:           interval,
		Timeline:           timeline,
		Summary:            summary,
		Checksum:           checksum,
		MethodologyVersion: MethodologySupplyV1,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
	}, nil
}
