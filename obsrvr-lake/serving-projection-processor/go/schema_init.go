package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/serving_schema.sql
var servingSchemaSQL string

func EnsureServingSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, servingSchemaSQL); err != nil {
		return fmt.Errorf("apply serving schema: %w", err)
	}
	return nil
}

// sourceIndex is a transaction_hash index the tx_receipts projector's batched
// loaders need on a SOURCE table (silver_hot / bronze stellar_hot). Without a
// transaction_hash-LEADING index, `WHERE transaction_hash = ANY(...)` seq-scans the
// table, which is why tx_receipts ran ~15x too slow to keep up with streaming.
// (token_transfers_raw and enriched_history_operations already lead with
// transaction_hash, so they are intentionally absent here.)
// See docs/tx-receipts-projector-throughput-shaped-fix.md.
type sourceIndex struct {
	pool  *pgxpool.Pool
	db    string
	table string
	name  string
	col   string
}

// EnsureSourceIndexes creates, idempotently and CONCURRENTLY, the transaction_hash
// indexes tx_receipts needs on its silver/bronze source tables. It is best-effort:
// every failure is logged and skipped — a missing source index only makes tx_receipts
// slow, it must never crash the processor or block the other projectors. CONCURRENTLY
// builds on large tables can take minutes, so callers should run this in the background.
func EnsureSourceIndexes(ctx context.Context, silverPool, bronzePool *pgxpool.Pool) {
	want := []sourceIndex{
		{silverPool, "silver_hot", "effects", "effects_txhash_idx", "transaction_hash"},
		{silverPool, "silver_hot", "semantic_activities", "semantic_activities_txhash_idx", "transaction_hash"},
		{bronzePool, "stellar_hot", "transactions_row_v2", "transactions_row_v2_txhash_idx", "transaction_hash"},
	}
	for _, x := range want {
		// A CONCURRENTLY build interrupted (e.g. by a restart) leaves an INVALID index;
		// CREATE ... IF NOT EXISTS would then skip the unusable index forever. Drop it first.
		var invalid bool
		if err := x.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
			   WHERE c.relname = $1 AND NOT i.indisvalid)`,
			pgx.QueryExecModeSimpleProtocol, x.name,
		).Scan(&invalid); err == nil && invalid {
			log.Printf("source-index: dropping invalid leftover %s on %s.%s", x.name, x.db, x.table)
			if _, err := x.pool.Exec(ctx,
				fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", x.name),
				pgx.QueryExecModeSimpleProtocol); err != nil {
				log.Printf("source-index: drop invalid %s failed (non-fatal): %v", x.name, err)
			}
		}

		start := time.Now()
		stmt := fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", x.name, x.table, x.col)
		if _, err := x.pool.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
			log.Printf("source-index: ensure %s on %s.%s failed (non-fatal — tx_receipts stays slow until present): %v",
				x.name, x.db, x.table, err)
			continue
		}
		log.Printf("source-index: %s on %s.%s ready in %s", x.name, x.db, x.table, time.Since(start).Round(time.Second))
	}
}
