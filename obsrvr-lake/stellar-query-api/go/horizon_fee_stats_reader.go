package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"

	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
)

type HorizonFeeStatsReader struct {
	hot  *HotReader
	cold *ColdReader
}

type horizonFeeLedger struct {
	sequence         int64
	baseFee          int64
	transactionCount int64
	maxTxSetSize     int64
}

func NewHorizonFeeStatsReader(hot *HotReader, cold *ColdReader) *HorizonFeeStatsReader {
	if hot == nil && cold == nil {
		return nil
	}
	return &HorizonFeeStatsReader{hot: hot, cold: cold}
}

func (r *HorizonFeeStatsReader) GetFeeStats(ctx context.Context) (*protocol.FeeStats, error) {
	if r == nil {
		return nil, fmt.Errorf("horizon fee stats reader unavailable")
	}

	if r.hot != nil {
		stats, err := r.getFeeStatsFromTables(ctx, r.hot.db, "ledgers_row_v2", "transactions_row_v2")
		if err == nil {
			return stats, nil
		}
	}
	if r.cold != nil {
		ledgerTable := fmt.Sprintf("%s.%s.ledgers_row_v2", r.cold.config.CatalogName, r.cold.config.SchemaName)
		txTable := fmt.Sprintf("%s.%s.transactions_row_v2", r.cold.config.CatalogName, r.cold.config.SchemaName)
		return r.getFeeStatsFromTables(ctx, r.cold.db, ledgerTable, txTable)
	}
	return nil, fmt.Errorf("horizon fee stats reader has no bronze readers")
}

func (r *HorizonFeeStatsReader) getFeeStatsFromTables(ctx context.Context, db *sql.DB, ledgerTable, txTable string) (*protocol.FeeStats, error) {
	ledgers, err := queryFeeStatLedgers(ctx, db, ledgerTable)
	if err != nil {
		return nil, err
	}
	if len(ledgers) == 0 {
		return nil, sql.ErrNoRows
	}
	oldest, latest := ledgers[len(ledgers)-1].sequence, ledgers[0].sequence
	feeCharged, maxFee, err := queryFeeValues(ctx, db, txTable, oldest, latest)
	if err != nil {
		return nil, err
	}

	var txCapacityNumerator, txCapacityDenominator int64
	for _, ledger := range ledgers {
		txCapacityNumerator += ledger.transactionCount
		txCapacityDenominator += ledger.maxTxSetSize
	}
	capacityUsage := 0.0
	if txCapacityDenominator > 0 {
		capacityUsage = float64(txCapacityNumerator) / float64(txCapacityDenominator)
	}

	return &protocol.FeeStats{
		LastLedger:          uint32(latest),
		LastLedgerBaseFee:   ledgers[0].baseFee,
		LedgerCapacityUsage: capacityUsage,
		FeeCharged:          horizonFeeDistribution(feeCharged),
		MaxFee:              horizonFeeDistribution(maxFee),
	}, nil
}

func queryFeeStatLedgers(ctx context.Context, db *sql.DB, table string) ([]horizonFeeLedger, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT sequence, base_fee, transaction_count, max_tx_set_size
		FROM %s
		ORDER BY sequence DESC
		LIMIT 5
	`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []horizonFeeLedger
	for rows.Next() {
		var ledger horizonFeeLedger
		if err := rows.Scan(&ledger.sequence, &ledger.baseFee, &ledger.transactionCount, &ledger.maxTxSetSize); err != nil {
			return nil, err
		}
		out = append(out, ledger)
	}
	return out, rows.Err()
}

func queryFeeValues(ctx context.Context, db *sql.DB, table string, start, end int64) ([]int64, []int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT fee_charged, max_fee
		FROM %s
		WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
	`, table), start, end)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var feeCharged, maxFee []int64
	for rows.Next() {
		var charged, max sql.NullInt64
		if err := rows.Scan(&charged, &max); err != nil {
			return nil, nil, err
		}
		if charged.Valid {
			feeCharged = append(feeCharged, charged.Int64)
		}
		if max.Valid {
			maxFee = append(maxFee, max.Int64)
		}
	}
	return feeCharged, maxFee, rows.Err()
}

func horizonFeeDistribution(values []int64) protocol.FeeDistribution {
	if len(values) == 0 {
		return protocol.FeeDistribution{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return protocol.FeeDistribution{
		Max:  values[len(values)-1],
		Min:  values[0],
		Mode: horizonFeeMode(values),
		P10:  horizonFeePercentile(values, 10),
		P20:  horizonFeePercentile(values, 20),
		P30:  horizonFeePercentile(values, 30),
		P40:  horizonFeePercentile(values, 40),
		P50:  horizonFeePercentile(values, 50),
		P60:  horizonFeePercentile(values, 60),
		P70:  horizonFeePercentile(values, 70),
		P80:  horizonFeePercentile(values, 80),
		P90:  horizonFeePercentile(values, 90),
		P95:  horizonFeePercentile(values, 95),
		P99:  horizonFeePercentile(values, 99),
	}
}

func horizonFeePercentile(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(pct)/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func horizonFeeMode(sorted []int64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	bestValue, bestCount := sorted[0], 1
	currentValue, currentCount := sorted[0], 1
	for _, value := range sorted[1:] {
		if value == currentValue {
			currentCount++
		} else {
			currentValue, currentCount = value, 1
		}
		if currentCount > bestCount {
			bestValue, bestCount = currentValue, currentCount
		}
	}
	return bestValue
}
