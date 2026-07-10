package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	"github.com/stellar/go-stellar-sdk/toid"
)

var errHorizonLedgerNotFound = errors.New("horizon ledger not found")

type HorizonLedgerReader struct {
	queryService *QueryService
	unified      *UnifiedDuckDBReader
}

func NewHorizonLedgerReader(queryService *QueryService, unified *UnifiedDuckDBReader) *HorizonLedgerReader {
	if queryService == nil {
		return nil
	}
	return &HorizonLedgerReader{queryService: queryService, unified: unified}
}

func (r *HorizonLedgerReader) GetLedger(ctx context.Context, sequence int64) (*protocol.Ledger, error) {
	if r == nil || r.queryService == nil {
		return nil, fmt.Errorf("horizon ledger reader unavailable")
	}
	rows, err := r.queryLedgerExact(ctx, sequence)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errHorizonLedgerNotFound
	}
	ledger := horizonLedgerFromMap(rows[0])
	return &ledger, nil
}

func (r *HorizonLedgerReader) queryLedgerExact(ctx context.Context, sequence int64) ([]map[string]interface{}, error) {
	qs := r.queryService
	if qs.hot == nil && qs.cold == nil {
		return nil, fmt.Errorf("horizon ledger reader has no bronze readers")
	}
	if qs.hot == nil {
		rows, err := qs.cold.QueryLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanLedgers(rows)
	}
	if qs.cold == nil {
		rows, err := qs.hot.QueryLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanLedgers(rows)
	}

	queryHot, queryCold, _, _, _, _ := qs.determineSource(sequence, sequence)
	if queryHot {
		rows, err := qs.hot.QueryLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		scanned, err := scanLedgers(rows)
		if err != nil {
			return nil, err
		}
		if len(scanned) > 0 {
			return scanned, nil
		}
	}
	if queryCold {
		rows, err := qs.cold.QueryLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanLedgers(rows)
	}
	return []map[string]interface{}{}, nil
}

func (r *HorizonLedgerReader) GetLedgers(ctx context.Context, page horizonPageQuery) ([]protocol.Ledger, error) {
	if r == nil || r.queryService == nil {
		return nil, fmt.Errorf("horizon ledger reader unavailable")
	}
	bounds, err := r.ledgerBounds(ctx)
	if err != nil {
		return nil, err
	}
	if bounds == nil || bounds.Oldest <= 0 || bounds.Latest <= 0 || bounds.Oldest > bounds.Latest {
		return []protocol.Ledger{}, nil
	}

	cursorSeq, hasCursor, err := horizonLedgerSequenceFromCursor(page.Cursor)
	if err != nil {
		return nil, err
	}

	start, end := bounds.Oldest, bounds.Latest
	if page.Order == "asc" {
		if hasCursor {
			start = cursorSeq + 1
		}
	} else {
		if hasCursor {
			end = cursorSeq - 1
		}
	}
	if start > end {
		return []protocol.Ledger{}, nil
	}

	sort := "sequence_asc"
	if page.Order == "desc" {
		sort = "sequence_desc"
	}
	rows, err := r.queryLedgerRange(ctx, start, end, int(page.Limit), sort)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.Ledger, 0, len(rows))
	for _, row := range rows {
		out = append(out, horizonLedgerFromMap(row))
	}
	return out, nil
}

func (r *HorizonLedgerReader) ledgerBounds(ctx context.Context) (*LedgerRange, error) {
	if r.unified != nil {
		return r.unified.GetAvailableLedgers(ctx)
	}
	if r.queryService == nil || r.queryService.hot == nil {
		return nil, fmt.Errorf("horizon ledger bounds require unified reader or hot watermark reader")
	}
	low, err := r.queryService.hot.GetLowWatermark()
	if err != nil {
		return nil, err
	}
	high, err := r.queryService.hot.GetHighWatermark()
	if err != nil {
		return nil, err
	}
	return &LedgerRange{Oldest: low, Latest: high}, nil
}

func (r *HorizonLedgerReader) queryLedgerRange(ctx context.Context, start, end int64, limit int, sort string) ([]map[string]interface{}, error) {
	if limit <= 0 {
		limit = int(defaultHorizonLimit)
	}
	qs := r.queryService
	if qs.hot == nil && qs.cold == nil {
		return nil, fmt.Errorf("horizon ledger reader has no bronze readers")
	}
	if qs.hot == nil {
		rows, err := qs.cold.QueryLedgers(ctx, start, end, limit, sort)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanLedgers(rows)
	}
	if qs.cold == nil {
		rows, err := qs.hot.QueryLedgers(ctx, start, end, limit, sort)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanLedgers(rows)
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)
	desc := sort == "sequence_desc" || sort == "closed_at_desc" || sort == "tx_count_desc"
	results := make([]map[string]interface{}, 0, limit)

	appendHot := func() error {
		if !queryHot || len(results) >= limit {
			return nil
		}
		rows, err := qs.hot.QueryLedgers(ctx, hotStart, hotEnd, limit-len(results), sort)
		if err != nil {
			return err
		}
		defer rows.Close()
		scanned, err := scanLedgers(rows)
		if err != nil {
			return err
		}
		results = append(results, scanned...)
		return nil
	}
	appendCold := func() error {
		if !queryCold || len(results) >= limit {
			return nil
		}
		rows, err := qs.cold.QueryLedgers(ctx, coldStart, coldEnd, limit-len(results), sort)
		if err != nil {
			return err
		}
		defer rows.Close()
		scanned, err := scanLedgers(rows)
		if err != nil {
			return err
		}
		results = append(results, scanned...)
		return nil
	}
	if desc {
		if err := appendHot(); err != nil {
			return nil, err
		}
		if err := appendCold(); err != nil {
			return nil, err
		}
	} else {
		if err := appendCold(); err != nil {
			return nil, err
		}
		if err := appendHot(); err != nil {
			return nil, err
		}
	}
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func horizonLedgerFromMap(row map[string]interface{}) protocol.Ledger {
	sequence := int64FromMap(row, "sequence")
	failed := int32(int64FromMap(row, "failed_tx_count"))
	txSetOps := int32(int64FromMap(row, "tx_set_operation_count"))
	return protocol.Ledger{
		ID:                         stringFromMap(row, "ledger_hash"),
		PT:                         horizonLedgerPagingToken(sequence),
		Hash:                       stringFromMap(row, "ledger_hash"),
		PrevHash:                   stringFromMap(row, "previous_ledger_hash"),
		Sequence:                   int32(sequence),
		SuccessfulTransactionCount: int32(int64FromMap(row, "successful_tx_count")),
		FailedTransactionCount:     &failed,
		OperationCount:             int32(int64FromMap(row, "operation_count")),
		TxSetOperationCount:        &txSetOps,
		ClosedAt:                   timeFromMap(row, "closed_at"),
		TotalCoins:                 horizonBalanceAmount(strconv.FormatInt(int64FromMap(row, "total_coins"), 10)),
		FeePool:                    horizonBalanceAmount(strconv.FormatInt(int64FromMap(row, "fee_pool"), 10)),
		BaseFee:                    int32(int64FromMap(row, "base_fee")),
		BaseReserve:                int32(int64FromMap(row, "base_reserve")),
		MaxTxSetSize:               int32(int64FromMap(row, "max_tx_set_size")),
		ProtocolVersion:            int32(int64FromMap(row, "protocol_version")),
		HeaderXDR:                  stringFromMap(row, "ledger_header"),
	}
}

func horizonLedgerPagingToken(sequence int64) string {
	const maxInt32 = int64(1<<31 - 1)
	if sequence > 0 && sequence <= maxInt32 {
		return toid.New(int32(sequence), 0, 0).String()
	}
	return strconv.FormatInt(sequence, 10)
}

func horizonLedgerSequenceFromCursor(raw string) (int64, bool, error) {
	if raw == "" || raw == "now" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, false, fmt.Errorf("invalid ledger cursor")
	}
	if n >= 1<<32 {
		return int64(toid.Parse(n).LedgerSequence), true, nil
	}
	return n, true, nil
}

func int64FromMap(row map[string]interface{}, key string) int64 {
	switch v := row[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(v)
	case float64:
		return int64(v)
	case nil:
		return 0
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func stringFromMap(row map[string]interface{}, key string) string {
	if row[key] == nil {
		return ""
	}
	return fmt.Sprint(row[key])
}

func timeFromMap(row map[string]interface{}, key string) time.Time {
	switch v := row[key].(type) {
	case time.Time:
		return v.UTC()
	case string:
		return parseHorizonTimestamp(v)
	default:
		return time.Time{}
	}
}
