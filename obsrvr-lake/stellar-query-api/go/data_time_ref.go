package main

import (
	"context"
	"database/sql"
	"time"
)

// resolveDataTime returns the latest data-time from a source table by running
// the supplied query (which must return a single timestamp, e.g.
// `SELECT MAX(closed_at) FROM ...`).
//
// "Recent" filters in this codebase historically used wall-clock NOW(); during
// a historical backfill (e.g. mainnet 2020 era), wall-clock is years ahead of
// any indexed row, so every "last 24h" filter excludes everything and forces
// full table / cold-parquet scans. Anchoring the recency window to the data's
// own clock keeps these endpoints fast and meaningful during backfill, and is
// equivalent to NOW() once the pipeline catches up to live.
//
// On error or empty source, returns time.Now().UTC() so behavior matches the
// previous wall-clock semantics.
func resolveDataTime(ctx context.Context, db *sql.DB, query string, args ...interface{}) time.Time {
	if db == nil {
		return time.Now().UTC()
	}
	var t sql.NullTime
	if err := db.QueryRowContext(ctx, query, args...).Scan(&t); err != nil {
		return time.Now().UTC()
	}
	if !t.Valid {
		return time.Now().UTC()
	}
	return t.Time.UTC()
}

// dataTimeQueryEnrichedHistoryOps fetches MAX(ledger_closed_at) using the
// ledger_sequence index (avoids a heap scan on the large operations table).
const dataTimeQueryEnrichedHistoryOps = `SELECT ledger_closed_at FROM enriched_history_operations ORDER BY ledger_sequence DESC LIMIT 1`

// dataTimeQueryContractInvocations / dataTimeQueryContractInvocationCalls /
// dataTimeQueryTokenTransfers / dataTimeQueryTradesRaw — same pattern,
// indexed-access reference-time lookups for the other commonly filtered
// silver-hot sources.
const dataTimeQueryContractInvocations = `SELECT closed_at FROM contract_invocations_raw ORDER BY ledger_sequence DESC LIMIT 1`
const dataTimeQueryContractInvocationCalls = `SELECT closed_at FROM contract_invocation_calls ORDER BY ledger_sequence DESC LIMIT 1`
const dataTimeQueryTokenTransfers = `SELECT timestamp FROM token_transfers_raw ORDER BY ledger_sequence DESC LIMIT 1`

// dataTimeQueryBronzeTransactions / dataTimeQueryBronzeLedgers — bronze
// reference-time lookups (different schema/column conventions than silver).
const dataTimeQueryBronzeTransactions = `SELECT created_at FROM transactions_row_v2 ORDER BY ledger_sequence DESC LIMIT 1`
const dataTimeQueryBronzeLedgers = `SELECT closed_at FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 1`
