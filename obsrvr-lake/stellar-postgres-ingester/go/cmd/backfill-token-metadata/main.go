// backfill-token-metadata reads existing contract_data_snapshot_v1 rows
// where contract_key_type = 'ScValTypeScvLedgerKeyContractInstance'
// and re-parses the contract_data_xdr to extract token metadata
// (name, symbol, decimals) from the METADATA key in instance storage.
//
// Usage:
//
//	go run ./cmd/backfill-token-metadata \
//	  -dsn "postgres://stellar:stellar@localhost:5432/stellar_hot?sslmode=disable" \
//	  -batch-size 1000
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/ttp-processor-demo/stellar-postgres-ingester/go/internal/processors/contract"
)

func main() {
	dsn := flag.String("dsn", "", "PostgreSQL connection string for stellar_hot")
	batchSize := flag.Int("batch-size", 1000, "Number of rows to process per batch")
	dryRun := flag.Bool("dry-run", false, "Print what would be updated without writing")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("--dsn is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	log.Println("Starting token metadata backfill...")
	start := time.Now()

	// Count total rows to process
	var total int64
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM contract_data_snapshot_v1
		WHERE contract_key_type = 'ScValTypeScvLedgerKeyContractInstance'
		  AND (token_name IS NULL OR token_symbol IS NULL OR token_decimals IS NULL)
		  AND contract_data_xdr IS NOT NULL
		  AND contract_data_xdr != ''
	`).Scan(&total)
	if err != nil {
		log.Fatalf("Failed to count rows: %v", err)
	}
	log.Printf("Found %d instance entries to process", total)

	updated := 0
	skipped := 0
	processed := 0
	lastContractID := ""
	lastLedgerSequence := int64(0)

	for {
		rows, err := pool.Query(ctx, `
			SELECT contract_id, ledger_key_hash, ledger_sequence, contract_data_xdr
			FROM contract_data_snapshot_v1
			WHERE contract_key_type = 'ScValTypeScvLedgerKeyContractInstance'
			  AND (token_name IS NULL OR token_symbol IS NULL OR token_decimals IS NULL)
			  AND contract_data_xdr IS NOT NULL
			  AND contract_data_xdr != ''
			  AND (contract_id, ledger_sequence) > ($2, $3)
			ORDER BY contract_id, ledger_sequence
			LIMIT $1
		`, *batchSize, lastContractID, lastLedgerSequence)
		if err != nil {
			log.Fatalf("Failed to query batch after (%s, %d): %v", lastContractID, lastLedgerSequence, err)
		}

		batchCount := 0
		for rows.Next() {
			var contractID, ledgerKeyHash, contractDataXDR string
			var ledgerSequence int64

			if err := rows.Scan(&contractID, &ledgerKeyHash, &ledgerSequence, &contractDataXDR); err != nil {
				log.Printf("Failed to scan row: %v", err)
				continue
			}
			batchCount++
			lastContractID = contractID
			lastLedgerSequence = ledgerSequence

			// Decode base64 XDR
			xdrBytes, err := base64.StdEncoding.DecodeString(contractDataXDR)
			if err != nil {
				skipped++
				continue
			}

			// Unmarshal into ContractData
			var contractData xdr.ContractDataEntry
			if err := contractData.UnmarshalBinary(xdrBytes); err != nil {
				skipped++
				continue
			}

			// Build a minimal LedgerEntry for TokenMetadataFromContractData
			ledgerEntry := xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type:         xdr.LedgerEntryTypeContractData,
					ContractData: &contractData,
				},
			}

			meta := contract.TokenMetadataFromContractData(ledgerEntry)
			if meta == nil {
				skipped++
				continue
			}

			if *dryRun {
				log.Printf("[DRY RUN] %s: name=%q symbol=%q decimals=%d",
					contractID, meta.Name, meta.Symbol, meta.Decimals)
				updated++
				continue
			}

			// Update the row
			var tokenName, tokenSymbol *string
			if meta.Name != "" {
				tokenName = &meta.Name
			}
			if meta.Symbol != "" {
				tokenSymbol = &meta.Symbol
			}

			_, err = pool.Exec(ctx, `
				UPDATE contract_data_snapshot_v1
				SET token_name = $1, token_symbol = $2, token_decimals = $3
				WHERE contract_id = $4 AND ledger_key_hash = $5 AND ledger_sequence = $6
			`, tokenName, tokenSymbol, meta.Decimals, contractID, ledgerKeyHash, ledgerSequence)
			if err != nil {
				log.Printf("Failed to update %s/%s/%d: %v", contractID, ledgerKeyHash, ledgerSequence, err)
				continue
			}

			updated++
		}
		rows.Close()

		if batchCount == 0 {
			break
		}

		processed += batchCount
		log.Printf("Progress: processed %d/%d rows (%d updated, %d skipped)",
			processed, total, updated, skipped)
	}

	elapsed := time.Since(start)
	log.Printf("Backfill complete in %v: %d updated, %d skipped out of %d total",
		elapsed, updated, skipped, total)

	if !*dryRun && updated > 0 {
		fmt.Println("\nTo populate the Silver token_registry, re-run the silver-realtime-transformer")
		fmt.Println("or run a targeted query against the updated Bronze data.")
	}
}
