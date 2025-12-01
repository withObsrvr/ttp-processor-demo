package main

import (
	"fmt"
	"log"
	"strings"
)

// setupPartitioning configures DuckLake partitioning for all tables
// This only affects NEW data written after this call
// Existing data retains its original partition scheme
func (ing *Ingester) setupPartitioning() error {
	log.Println("Setting up DuckLake partitioning...")

	// Get partition size from config (default: 64000 ledgers, matching Stellar Galexie)
	partitionSize := ing.config.DuckLake.PartitionSize
	if partitionSize <= 0 {
		partitionSize = 64000 // Default: Galexie-compatible partitioning
	}

	log.Printf("Using partition size: %d ledgers per partition", partitionSize)

	// Partition strategy: configurable ledger chunks
	// Examples:
	// - 64,000 ledgers (Galexie default): 60M ledgers = ~938 partitions
	// - 100,000 ledgers: 60M ledgers = 600 partitions
	// - 1,000 ledgers: 60M ledgers = 60,000 partitions (too many!)
	partitionExpr := fmt.Sprintf("identity(ledger_sequence / %d)", partitionSize)

	// Tables to partition
	ledgersPartitionExpr := fmt.Sprintf("identity(sequence / %d)", partitionSize)

	tables := []struct {
		name           string
		partitionKey   string // Column name for partitioning
		partitionExpr  string // Partition expression
	}{
		// Core tables (use ledger_sequence or sequence)
		{"ledgers_row_v2", "sequence", ledgersPartitionExpr},
		{"transactions_row_v2", "ledger_sequence", partitionExpr},
		{"operations_row_v2", "ledger_sequence", partitionExpr},
		{"effects_row_v1", "ledger_sequence", partitionExpr},
		{"trades_row_v1", "ledger_sequence", partitionExpr},

		// Snapshot tables (use ledger_sequence)
		{"accounts_snapshot_v1", "ledger_sequence", partitionExpr},
		{"trustlines_snapshot_v1", "ledger_sequence", partitionExpr},
		{"native_balances_snapshot_v1", "ledger_sequence", partitionExpr},
		{"account_signers_snapshot_v1", "ledger_sequence", partitionExpr},

		// Soroban tables
		{"contract_data_snapshot_v1", "ledger_sequence", partitionExpr},
		{"contract_code_snapshot_v1", "ledger_sequence", partitionExpr},
		{"contract_events_row_v1", "ledger_sequence", partitionExpr},
		{"config_settings_snapshot_v1", "ledger_sequence", partitionExpr},
		{"ttl_snapshot_v1", "ledger_sequence", partitionExpr},

		// Soroban archival
		{"restored_keys_state_v1", "ledger_sequence", partitionExpr},
		{"evicted_keys_state_v1", "ledger_sequence", partitionExpr},

		// DEX state
		{"offers_snapshot_v1", "ledger_sequence", partitionExpr},
		{"claimable_balances_snapshot_v1", "ledger_sequence", partitionExpr},
		{"liquidity_pools_snapshot_v1", "ledger_sequence", partitionExpr},
	}

	successCount := 0
	for _, table := range tables {
		tableName := fmt.Sprintf("%s.%s.%s",
			ing.config.DuckLake.CatalogName,
			ing.config.DuckLake.SchemaName,
			table.name)

		alterSQL := fmt.Sprintf("ALTER TABLE %s SET PARTITIONED BY (%s)",
			tableName, table.partitionExpr)

		log.Printf("Partitioning %s by %s...", table.name, table.partitionExpr)

		_, err := ing.db.Exec(alterSQL)
		if err != nil {
			errMsg := strings.ToLower(err.Error())

			// Check if table doesn't exist yet (not an error)
			if strings.Contains(errMsg, "does not exist") ||
				strings.Contains(errMsg, "catalog error") {
				log.Printf("  ⚠️  Table %s does not exist yet, will partition on creation", table.name)
				continue
			}

			// Check if already partitioned
			if strings.Contains(errMsg, "already partitioned") {
				log.Printf("  ✓ Table %s already partitioned", table.name)
				successCount++
				continue
			}

			// Real error
			log.Printf("  ❌ Failed to partition %s: %v", table.name, err)
			continue
		}

		log.Printf("  ✓ Partitioned %s", table.name)
		successCount++
	}

	log.Printf("✅ Partitioning setup complete: %d/%d tables configured", successCount, len(tables))
	return nil
}

// flushInlinedData flushes any inlined data to Parquet files
// Only needed if DATA_INLINING_ROW_LIMIT was set during ATTACH
// (Not recommended for bulk ingestion use case)
func (ing *Ingester) flushInlinedData() error {
	log.Println("Flushing inlined data to Parquet...")

	flushSQL := "CALL ducklake_flush_inlined_data()"
	_, err := ing.db.Exec(flushSQL)
	if err != nil {
		// Inlining may not be enabled, not an error
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "does not exist") {
			log.Println("  ℹ️  Inlining not enabled, skipping flush")
			return nil
		}
		return fmt.Errorf("failed to flush inlined data: %w", err)
	}

	log.Println("  ✓ Flushed inlined data")
	return nil
}
