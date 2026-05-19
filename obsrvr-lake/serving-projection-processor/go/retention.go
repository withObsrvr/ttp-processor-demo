package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func applyRecentRetention(ctx context.Context, tx pgx.Tx, tableName, timeColumn, intervalLiteral string) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s < NOW() - INTERVAL '%s'", tableName, timeColumn, intervalLiteral)
	ct, err := tx.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("apply retention %s: %w", tableName, err)
	}
	return ct.RowsAffected(), nil
}

// applyRecentRetentionWithReference is the data-time-anchored counterpart to
// applyRecentRetention. It deletes rows whose timeColumn is older than
// referenceTime - intervalLiteral, instead of NOW() - intervalLiteral.
//
// This is the right semantics when the column being compared holds the data's
// own clock (e.g. ledger closed_at) rather than insert wall-clock — during a
// historical backfill, NOW() is years ahead of any indexed closed_at, so the
// wall-clock variant deletes every freshly-inserted row.
func applyRecentRetentionWithReference(ctx context.Context, tx pgx.Tx, tableName, timeColumn, intervalLiteral string, referenceTime time.Time) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s < $1::timestamp - INTERVAL '%s'", tableName, timeColumn, intervalLiteral)
	ct, err := tx.Exec(ctx, query, referenceTime)
	if err != nil {
		return 0, fmt.Errorf("apply retention %s: %w", tableName, err)
	}
	return ct.RowsAffected(), nil
}

// resolveDataTime returns the timestamp of the latest row in the source table.
// It anchors "recent" semantics to the data's own clock (the most recent indexed
// closed_at) instead of wall-clock NOW(), so projectors stay correct during
// historical backfills where the data is years behind real time.
//
// The query orders by an indexed ledger column DESC LIMIT 1 to avoid a heap
// scan. On error or empty source, falls back to wall-clock NOW() so live
// operation is unaffected.
func resolveDataTime(ctx context.Context, pool *pgxpool.Pool, sourceTable, timeColumn string) time.Time {
	return resolveDataTimeBy(ctx, pool, sourceTable, timeColumn, "ledger_sequence")
}

// resolveDataTimeBy is like resolveDataTime but lets the caller specify which
// indexed column to ORDER BY (e.g., ledgers_row_v2 uses "sequence" instead of
// "ledger_sequence").
func resolveDataTimeBy(ctx context.Context, pool *pgxpool.Pool, sourceTable, timeColumn, orderColumn string) time.Time {
	var t time.Time
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", timeColumn, sourceTable, orderColumn)
	if err := pool.QueryRow(ctx, query).Scan(&t); err != nil {
		return time.Now().UTC()
	}
	return t
}
