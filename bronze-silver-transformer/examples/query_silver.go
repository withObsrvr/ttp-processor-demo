package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Install extensions
	if _, err := db.Exec("INSTALL ducklake"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("LOAD ducklake"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		log.Fatal(err)
	}

	// Configure S3 credentials
	secretSQL := `CREATE SECRET (
		TYPE S3,
		KEY_ID 'YOUR_KEY',
		SECRET 'YOUR_SECRET',
		REGION 'us-west-004',
		ENDPOINT 's3.us-west-004.backblazeb2.com',
		URL_STYLE 'path'
	)`
	if _, err := db.Exec(secretSQL); err != nil {
		log.Fatal(err)
	}

	// Attach DuckLake catalog
	attachSQL := `ATTACH 'ducklake:postgres:postgresql://user:pass@host:port/catalog_db?sslmode=require'
	  AS catalog
	  (DATA_PATH 's3://bucket/network_silver/', METADATA_SCHEMA 'bronze_meta')`
	if _, err := db.Exec(attachSQL); err != nil {
		log.Fatal(err)
	}

	// Example 1: Query current accounts
	fmt.Println("=== Current Accounts (Top 10 by Balance) ===")
	rows, err := db.Query(`
		SELECT
			account_id,
			balance,
			ledger_sequence,
			closed_at
		FROM catalog.silver.accounts_current
		ORDER BY CAST(balance AS BIGINT) DESC
		LIMIT 10
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var accountID string
		var balance string
		var ledgerSeq int64
		var closedAt string
		if err := rows.Scan(&accountID, &balance, &ledgerSeq, &closedAt); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %s (ledger %d)\n", accountID, balance, ledgerSeq)
	}

	// Example 2: Historical account changes (SCD Type 2)
	fmt.Println("\n=== Account History (with valid_from/valid_to) ===")
	rows2, err := db.Query(`
		SELECT
			account_id,
			balance,
			ledger_sequence,
			valid_from,
			valid_to
		FROM catalog.silver.accounts_snapshot
		WHERE account_id = 'SPECIFIC_ACCOUNT_ID'
		ORDER BY ledger_sequence DESC
		LIMIT 5
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var accountID, balance, validFrom string
		var ledgerSeq int64
		var validTo sql.NullString
		if err := rows2.Scan(&accountID, &balance, &ledgerSeq, &validFrom, &validTo); err != nil {
			log.Fatal(err)
		}
		validToStr := "current"
		if validTo.Valid {
			validToStr = validTo.String
		}
		fmt.Printf("Ledger %d: %s (valid %s → %s)\n", ledgerSeq, balance, validFrom, validToStr)
	}

	// Example 3: Enriched operations with transaction context
	fmt.Println("\n=== Recent Payment Operations (Enriched) ===")
	rows3, err := db.Query(`
		SELECT
			transaction_hash,
			ledger_sequence,
			ledger_closed_at,
			source_account,
			destination,
			asset_code,
			amount,
			tx_successful,
			tx_fee_charged
		FROM catalog.silver.enriched_history_operations
		WHERE is_payment_op = true
		ORDER BY ledger_sequence DESC
		LIMIT 10
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows3.Close()

	for rows3.Next() {
		var txHash, sourceAccount, destination, assetCode, amount string
		var ledgerSeq, feeCharged int64
		var closedAt string
		var successful bool
		if err := rows3.Scan(&txHash, &ledgerSeq, &closedAt, &sourceAccount, &destination, &assetCode, &amount, &successful, &feeCharged); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %s → %s (%s %s) [success=%v]\n",
			txHash[:8], sourceAccount[:8], destination[:8], amount, assetCode, successful)
	}

	// Example 4: Token transfers analytics
	fmt.Println("\n=== Token Transfers (Classic + Soroban) ===")
	rows4, err := db.Query(`
		SELECT
			source_type,
			COUNT(*) as transfer_count,
			asset_code,
			SUM(CAST(amount AS DOUBLE)) as total_volume
		FROM catalog.silver.token_transfers_raw
		WHERE transaction_successful = true
			AND timestamp >= NOW() - INTERVAL '24 hours'
		GROUP BY source_type, asset_code
		ORDER BY total_volume DESC
		LIMIT 10
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows4.Close()

	for rows4.Next() {
		var sourceType, assetCode string
		var count int64
		var volume float64
		if err := rows4.Scan(&sourceType, &count, &assetCode, &volume); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s (%s): %d transfers, %.2f total\n", assetCode, sourceType, count, volume)
	}
}
