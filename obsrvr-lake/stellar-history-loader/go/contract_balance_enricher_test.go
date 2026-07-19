package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

const historicalNativeBalanceXDR = "AAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAABAAAAABAAAAAgAAAA8AAAAHQmFsYW5jZQAAAAASAAAAAaAYIAbRNL61yGL1IwE4Zd6B7YCDUI7STCNQPchp4CbqAAAAAQAAABEAAAABAAAAAwAAAA8AAAAGYW1vdW50AAAAAAAKAAAAAAAAAAAAAAAXRXv3gAAAAA8AAAAKYXV0aG9yaXplZAAAAAAAAAAAAAEAAAAPAAAACGNsYXdiYWNrAAAAAAAAAAA="

func TestDecodeContractBalanceDataXDRHistoricalNativeBalance(t *testing.T) {
	holder, balance, ok, err := decodeContractBalanceDataXDR(historicalNativeBalanceXDR)
	if err != nil {
		t.Fatalf("decode historical contract data XDR: %v", err)
	}
	if !ok {
		t.Fatal("historical native balance entry was not recognized")
	}
	if holder != "CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG" {
		t.Fatalf("holder = %q", holder)
	}
	if balance != "99950000000" {
		t.Fatalf("balance = %q, want raw stroops 99950000000", balance)
	}
}

func TestEnrichContractBalanceChunkIsVerifiedAndIdempotent(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
		CREATE TABLE contract_data_snapshot_v1 (
			contract_id VARCHAR,
			ledger_sequence BIGINT,
			ledger_key_hash VARCHAR,
			contract_key_type VARCHAR,
			contract_data_xdr VARCHAR,
			balance_holder VARCHAR,
			balance VARCHAR
		);
		INSERT INTO contract_data_snapshot_v1 VALUES
			('CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC', 2996166, 'd2ccddb78a822eb99301d8ddf17861a387392655834cf957b708aba18eee4241', 'ScValTypeScvVec', ?, NULL, NULL);
	`, historicalNativeBalanceXDR)
	if err != nil {
		t.Fatalf("seed contract data: %v", err)
	}

	result, err := enrichContractBalanceChunk(ctx, db, "contract_data_snapshot_v1", 2_900_000, 3_000_000, false)
	if err != nil {
		t.Fatalf("enrich chunk: %v", err)
	}
	if result.CandidateRows != 1 || result.DecodedRows != 1 || result.UpdatedRows != 1 || result.SkippedRows != 0 {
		t.Fatalf("unexpected first result: %+v", result)
	}

	var holder, balance string
	if err := db.QueryRowContext(ctx, `SELECT balance_holder, balance FROM contract_data_snapshot_v1`).Scan(&holder, &balance); err != nil {
		t.Fatalf("read repaired row: %v", err)
	}
	if holder != "CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG" || balance != "99950000000" {
		t.Fatalf("repaired row = holder %q, balance %q", holder, balance)
	}

	second, err := enrichContractBalanceChunk(ctx, db, "contract_data_snapshot_v1", 2_900_000, 3_000_000, false)
	if err != nil {
		t.Fatalf("rerun chunk: %v", err)
	}
	if second.CandidateRows != 0 || second.UpdatedRows != 0 {
		t.Fatalf("rerun was not idempotent: %+v", second)
	}
}
