// backfill-balance-holder re-decodes contract_data_snapshot_v1 rows whose
// Balance(Address) entries were not populated by earlier ingester builds
// (notably the native XLM SAC, which the upstream stellar-sdk decoder
// intentionally excluded). Running this once after deploying the updated
// ingester populates historical balance_holder / balance columns so the
// silver state-based balance path has complete coverage without waiting
// for every wallet to have new on-chain activity.
//
// Usage:
//
//	go run ./cmd/backfill-balance-holder \
//	  -dsn "postgres://doadmin:...@bronze-hot:25060/stellar_hot?sslmode=require" \
//	  -batch-size 1000
//
// MUST be kept in sync with extractors_soroban.go:contractStorageBalanceFromData.
// Duplicated here because the production decoder lives in package main.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"log"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

var balanceMetadataSym = xdr.ScSymbol("Balance")

// decodeBalance mirrors contractStorageBalanceFromData in the ingester.
// Returns (holder C... strkey, balance decimal string, ok).
func decodeBalance(contractDataXDR string) (string, string, bool) {
	raw, err := base64.StdEncoding.DecodeString(contractDataXDR)
	if err != nil {
		return "", "", false
	}
	var cd xdr.ContractDataEntry
	if err := cd.UnmarshalBinary(raw); err != nil {
		return "", "", false
	}
	if cd.Durability != xdr.ContractDataDurabilityPersistent {
		return "", "", false
	}
	if cd.Contract.ContractId == nil {
		return "", "", false
	}
	keyVecPtr, ok := cd.Key.GetVec()
	if !ok || keyVecPtr == nil {
		return "", "", false
	}
	keyVec := *keyVecPtr
	if len(keyVec) != 2 || !keyVec[0].Equals(
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &balanceMetadataSym},
	) {
		return "", "", false
	}
	scAddr, ok := keyVec[1].GetAddress()
	if !ok {
		return "", "", false
	}
	holderBytes, ok := scAddr.GetContractId()
	if !ok {
		return "", "", false
	}
	mPtr, ok := cd.Val.GetMap()
	if !ok || mPtr == nil {
		return "", "", false
	}
	m := *mPtr
	if len(m) != 3 {
		return "", "", false
	}
	if s, ok := m[0].Key.GetSym(); !ok || s != "amount" {
		return "", "", false
	}
	if s, ok := m[1].Key.GetSym(); !ok || s != "authorized" || !m[1].Val.IsBool() {
		return "", "", false
	}
	if s, ok := m[2].Key.GetSym(); !ok || s != "clawback" || !m[2].Val.IsBool() {
		return "", "", false
	}
	amt, ok := m[0].Val.GetI128()
	if !ok {
		return "", "", false
	}
	if int64(amt.Hi) < 0 {
		return "", "", false
	}
	value := new(big.Int).Lsh(new(big.Int).SetInt64(int64(amt.Hi)), 64)
	value.Add(value, new(big.Int).SetUint64(uint64(amt.Lo)))
	holder, err := strkey.Encode(strkey.VersionByteContract, holderBytes[:])
	if err != nil {
		return "", "", false
	}
	return holder, value.String(), true
}

func main() {
	dsn := flag.String("dsn", "", "PostgreSQL connection string for stellar_hot (bronze)")
	batchSize := flag.Int("batch-size", 1000, "Rows per batch")
	dryRun := flag.Bool("dry-run", false, "Print what would change without writing")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("--dsn is required")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	log.Println("Starting balance_holder backfill...")
	start := time.Now()

	var total int64
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM contract_data_snapshot_v1
		WHERE contract_key_type = 'ScValTypeScvVec'
		  AND balance_holder IS NULL
		  AND contract_data_xdr IS NOT NULL
		  AND contract_data_xdr != ''
		  AND deleted = false
	`).Scan(&total)
	if err != nil {
		log.Fatalf("count: %v", err)
	}
	log.Printf("Candidate rows: %d", total)

	var (
		processed int64
		updated   int64
		skipped   int64
		// Keyset-pagination cursor — tuple (contract_id, ledger_key_hash, ledger_sequence).
		lastContractID, lastKeyHash string
		lastLedgerSeq               int64
	)

	for {
		rows, err := pool.Query(ctx, `
			SELECT contract_id, ledger_key_hash, ledger_sequence, contract_data_xdr
			FROM contract_data_snapshot_v1
			WHERE contract_key_type = 'ScValTypeScvVec'
			  AND balance_holder IS NULL
			  AND contract_data_xdr IS NOT NULL
			  AND contract_data_xdr != ''
			  AND deleted = false
			  AND (contract_id, ledger_key_hash, ledger_sequence) > ($2::text, $3::text, $4::bigint)
			ORDER BY contract_id, ledger_key_hash, ledger_sequence
			LIMIT $1
		`, *batchSize, lastContractID, lastKeyHash, lastLedgerSeq)
		if err != nil {
			log.Fatalf("batch query: %v", err)
		}

		batchCount := 0
		for rows.Next() {
			var (
				contractID, ledgerKeyHash, contractDataXDR string
				ledgerSequence                             int64
			)
			if err := rows.Scan(&contractID, &ledgerKeyHash, &ledgerSequence, &contractDataXDR); err != nil {
				log.Printf("scan: %v", err)
				continue
			}
			batchCount++
			lastContractID = contractID
			lastKeyHash = ledgerKeyHash
			lastLedgerSeq = ledgerSequence

			holder, balance, ok := decodeBalance(contractDataXDR)
			if !ok {
				skipped++
				continue
			}
			if *dryRun {
				log.Printf("[DRY RUN] contract=%s holder=%s balance=%s", contractID, holder, balance)
				updated++
				continue
			}
			_, err := pool.Exec(ctx, `
				UPDATE contract_data_snapshot_v1
				SET balance_holder = $1, balance = $2
				WHERE contract_id = $3 AND ledger_key_hash = $4 AND ledger_sequence = $5
			`, holder, balance, contractID, ledgerKeyHash, ledgerSequence)
			if err != nil {
				log.Printf("update %s/%s/%d: %v", contractID, ledgerKeyHash, ledgerSequence, err)
				continue
			}
			updated++
		}
		rows.Close()
		if batchCount == 0 {
			break
		}
		processed += int64(batchCount)
		log.Printf("progress: %d/%d processed, %d updated, %d skipped",
			processed, total, updated, skipped)
	}

	log.Printf("done in %v: %d updated, %d skipped of %d candidates",
		time.Since(start), updated, skipped, total)
}
