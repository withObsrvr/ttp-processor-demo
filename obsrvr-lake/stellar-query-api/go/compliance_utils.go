package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// ============================================
// COMPLIANCE ARCHIVE API - Utility Functions
// ============================================

// GenerateChecksum creates a SHA-256 checksum of a data payload
// The checksum is computed over the JSON representation of the data
// (excluding the checksum field itself)
func GenerateChecksum(data interface{}) (string, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data for checksum: %w", err)
	}
	hash := sha256.Sum256(jsonBytes)
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

// GenerateArchiveID creates a unique archive identifier
// Format: {prefix}_{asset}_{date}_{hash}
func GenerateArchiveID(prefix string, assetCode string, timestamp time.Time) string {
	// Create a short hash from timestamp for uniqueness
	hashInput := fmt.Sprintf("%s_%s_%d", prefix, assetCode, timestamp.UnixNano())
	hash := sha256.Sum256([]byte(hashInput))
	shortHash := hex.EncodeToString(hash[:])[:8]

	dateStr := timestamp.Format("20060102")
	return fmt.Sprintf("%s_%s_%s_%s", prefix, assetCode, dateStr, shortHash)
}

// parseDateRange extracts start_date and end_date from request parameters
func parseDateRange(r *http.Request) (start, end time.Time, err error) {
	startStr := r.URL.Query().Get("start_date")
	endStr := r.URL.Query().Get("end_date")

	if startStr == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("start_date required")
	}
	if endStr == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("end_date required")
	}

	// Try RFC3339 first
	start, err = time.Parse(time.RFC3339, startStr)
	if err != nil {
		// Try date-only format (interpret as start of day)
		start, err = time.Parse("2006-01-02", startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start_date format, use RFC3339 or YYYY-MM-DD")
		}
	}

	// Try RFC3339 first for end
	end, err = time.Parse(time.RFC3339, endStr)
	if err != nil {
		// Try date-only format (interpret as end of day)
		end, err = time.Parse("2006-01-02", endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end_date format, use RFC3339 or YYYY-MM-DD")
		}
		// Set to end of day
		end = end.Add(23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond)
	}

	// Validate that start is before end
	if start.After(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("start_date must be before end_date")
	}

	return start, end, nil
}

// parseInterval extracts the interval parameter (1h, 1d, 1w)
func parseInterval(r *http.Request) string {
	interval := r.URL.Query().Get("interval")
	switch interval {
	case "1h", "1d", "1w":
		return interval
	default:
		return "1d" // default to daily
	}
}

// parseIncludeFailed extracts the include_failed boolean parameter
func parseIncludeFailed(r *http.Request) bool {
	return r.URL.Query().Get("include_failed") == "true"
}

// checksumDataForTransactions creates a deterministic representation for checksum
// This excludes fields that vary between requests (checksum, generated_at)
type checksumTransactionsData struct {
	Asset        AssetInfo               `json:"asset"`
	Period       PeriodInfo              `json:"period"`
	Transactions []ComplianceTransaction `json:"transactions"`
}

// checksumDataForBalances creates a deterministic representation for checksum
type checksumBalancesData struct {
	Asset      AssetInfo          `json:"asset"`
	SnapshotAt string             `json:"snapshot_at"`
	Holders    []ComplianceHolder `json:"holders"`
}

// checksumDataForSupply creates a deterministic representation for checksum
type checksumSupplyData struct {
	Asset    AssetInfo         `json:"asset"`
	Period   PeriodInfo        `json:"period"`
	Interval string            `json:"interval"`
	Timeline []SupplyDataPoint `json:"timeline"`
}

// GenerateTransactionsChecksum creates checksum for transactions response
func GenerateTransactionsChecksum(asset AssetInfo, period PeriodInfo, transactions []ComplianceTransaction) (string, error) {
	data := checksumTransactionsData{
		Asset:        asset,
		Period:       period,
		Transactions: transactions,
	}
	return GenerateChecksum(data)
}

// GenerateBalancesChecksum creates checksum for balances response
func GenerateBalancesChecksum(asset AssetInfo, snapshotAt string, holders []ComplianceHolder) (string, error) {
	data := checksumBalancesData{
		Asset:      asset,
		SnapshotAt: snapshotAt,
		Holders:    holders,
	}
	return GenerateChecksum(data)
}

// GenerateSupplyChecksum creates checksum for supply response
func GenerateSupplyChecksum(asset AssetInfo, period PeriodInfo, interval string, timeline []SupplyDataPoint) (string, error) {
	data := checksumSupplyData{
		Asset:    asset,
		Period:   period,
		Interval: interval,
		Timeline: timeline,
	}
	return GenerateChecksum(data)
}

// formatDecimalPercent formats a decimal value as a percentage string
func formatDecimalPercent(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

// ============================================
// CSV EXPORT UTILITIES
// ============================================

// TransactionsToCSV converts transaction data to CSV format
func TransactionsToCSV(transactions []ComplianceTransaction) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Write header
	header := []string{
		"ledger_sequence",
		"closed_at",
		"transaction_hash",
		"operation_index",
		"operation_type",
		"from_account",
		"to_account",
		"amount",
		"successful",
	}
	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write rows
	for _, tx := range transactions {
		row := []string{
			strconv.FormatInt(tx.LedgerSequence, 10),
			tx.ClosedAt,
			tx.TransactionHash,
			strconv.Itoa(tx.OperationIndex),
			tx.OperationType,
			tx.FromAccount,
			tx.ToAccount,
			tx.Amount,
			strconv.FormatBool(tx.Successful),
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("CSV write error: %w", err)
	}

	return buf.Bytes(), nil
}

// BalancesToCSV converts balance holder data to CSV format
func BalancesToCSV(holders []ComplianceHolder) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Write header
	header := []string{
		"account_id",
		"balance",
		"percent_of_supply",
	}
	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write rows
	for _, h := range holders {
		row := []string{
			h.AccountID,
			h.Balance,
			h.PercentOfSupply,
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("CSV write error: %w", err)
	}

	return buf.Bytes(), nil
}

// SupplyTimelineToCSV converts supply timeline data to CSV format
func SupplyTimelineToCSV(timeline []SupplyDataPoint) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Write header
	header := []string{
		"timestamp",
		"ledger_sequence",
		"total_supply",
		"circulating_supply",
		"issuer_balance",
		"holder_count",
		"supply_change",
		"supply_change_percent",
	}
	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write rows
	for _, dp := range timeline {
		supplyChange := ""
		if dp.SupplyChange != nil {
			supplyChange = *dp.SupplyChange
		}
		supplyChangePercent := ""
		if dp.SupplyChangePercent != nil {
			supplyChangePercent = *dp.SupplyChangePercent
		}

		row := []string{
			dp.Timestamp,
			strconv.FormatInt(dp.LedgerSequence, 10),
			dp.TotalSupply,
			dp.CirculatingSupply,
			dp.IssuerBalance,
			strconv.Itoa(dp.HolderCount),
			supplyChange,
			supplyChangePercent,
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("CSV write error: %w", err)
	}

	return buf.Bytes(), nil
}
