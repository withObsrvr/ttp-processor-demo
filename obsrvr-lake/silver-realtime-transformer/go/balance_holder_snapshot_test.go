package main

import (
	"context"
	"database/sql"
	"testing"
)

func TestQueryBalanceHolderSnapshotsEnrichesMetadataOutsideChangeWindow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *sql.DB)
		query func(context.Context, *sql.DB) (*sql.Rows, error)
	}{
		{
			name: "hot",
			setup: func(t *testing.T, db *sql.DB) {
				createBalanceHolderSnapshotTable(t, db, "contract_data_snapshot_v1")
				insertBalanceHolderFixtures(t, db, "contract_data_snapshot_v1")
			},
			query: func(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
				return (&BronzeReader{db: db}).QueryBalanceHolderSnapshots(ctx, 100, 200)
			},
		},
		{
			name: "cold",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE SCHEMA bronze`); err != nil {
					t.Fatal(err)
				}
				createBalanceHolderSnapshotTable(t, db, "memory.bronze.contract_data_snapshot_v1")
				insertBalanceHolderFixtures(t, db, "memory.bronze.contract_data_snapshot_v1")
			},
			query: func(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
				return (&BronzeColdReader{db: db, catalogName: "memory", schemaName: "bronze"}).QueryBalanceHolderSnapshots(ctx, 100, 200)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.setup(t, db)

			rows, err := tc.query(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			var contractID, holder, balance, keyHash string
			var assetType, assetCode, assetIssuer, symbol sql.NullString
			var decimals sql.NullInt32
			var ledger int64
			var closedAt sql.NullTime
			var deleted bool
			if !rows.Next() {
				t.Fatal("expected balance row")
			}
			if err := rows.Scan(&contractID, &holder, &balance, &assetType, &assetCode, &assetIssuer, &symbol, &decimals, &keyHash, &ledger, &closedAt, &deleted); err != nil {
				t.Fatal(err)
			}
			if contractID != "CXLM" || holder != "CWALLET" || balance != "1230000000" ||
				!assetType.Valid || assetType.String != "native" || !assetCode.Valid || assetCode.String != "XLM" ||
				assetIssuer.Valid || !symbol.Valid || symbol.String != "XLM" || !decimals.Valid || decimals.Int32 != 7 ||
				keyHash != "BX" || ledger != 110 || !closedAt.Valid || !deleted {
				t.Fatalf("unexpected enriched balance row: %s/%s/%s %v/%v/%v/%v/%v %s/%d/%v/%t",
					contractID, holder, balance, assetType, assetCode, assetIssuer, symbol, decimals, keyHash, ledger, closedAt, deleted)
			}
			if rows.Next() {
				t.Fatal("expected one latest balance row")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryTokenMetadataEntriesNormalizesNativeSAC(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *sql.DB)
		query func(context.Context, *sql.DB) (*sql.Rows, error)
	}{
		{
			name: "hot",
			setup: func(t *testing.T, db *sql.DB) {
				createBalanceHolderSnapshotTable(t, db, "contract_data_snapshot_v1")
				insertBalanceHolderFixtures(t, db, "contract_data_snapshot_v1")
			},
			query: func(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
				return (&BronzeReader{db: db}).QueryTokenMetadataEntries(ctx, 1, 100)
			},
		},
		{
			name: "cold",
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE SCHEMA bronze`); err != nil {
					t.Fatal(err)
				}
				createBalanceHolderSnapshotTable(t, db, "memory.bronze.contract_data_snapshot_v1")
				insertBalanceHolderFixtures(t, db, "memory.bronze.contract_data_snapshot_v1")
			},
			query: func(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
				return (&BronzeColdReader{db: db, catalogName: "memory", schemaName: "bronze"}).QueryTokenMetadataEntries(ctx, 1, 100)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.setup(t, db)

			rows, err := tc.query(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal("expected token metadata row")
			}
			var contractID string
			var name, symbol, assetCode, issuer sql.NullString
			var decimals sql.NullInt32
			var ledger int64
			if err := rows.Scan(&contractID, &name, &symbol, &decimals, &assetCode, &issuer, &ledger); err != nil {
				t.Fatal(err)
			}
			if contractID != "CXLM" || !assetCode.Valid || assetCode.String != "XLM" || issuer.Valid {
				t.Fatalf("native token metadata = %s/%v/%v", contractID, assetCode, issuer)
			}
		})
	}
}

func createBalanceHolderSnapshotTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE ` + table + ` (
		contract_id VARCHAR, ledger_sequence BIGINT, ledger_key_hash VARCHAR,
		contract_key_type VARCHAR, contract_durability VARCHAR,
		asset_code VARCHAR, asset_issuer VARCHAR, asset_type VARCHAR,
		balance_holder VARCHAR, balance VARCHAR, last_modified_ledger BIGINT,
		ledger_entry_change INTEGER, deleted BOOLEAN, closed_at TIMESTAMP,
		contract_data_xdr VARCHAR, created_at TIMESTAMP, ledger_range BIGINT,
		token_name VARCHAR, token_symbol VARCHAR, token_decimals INTEGER,
		era_id VARCHAR, version_label VARCHAR
	)`)
	if err != nil {
		t.Fatalf("create balance snapshot table: %v", err)
	}
}

func insertBalanceHolderFixtures(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO ` + table + ` VALUES
		('CXLM',90,'MX','ScValTypeScvLedgerKeyContractInstance','persistent','',NULL,'AssetTypeAssetTypeNative',NULL,NULL,90,0,false,TIMESTAMP '2026-01-01 00:00:09',NULL,NULL,1,'Stellar Lumens','XLM',7,NULL,NULL),
		('CXLM',100,'BX','balance','persistent',NULL,NULL,NULL,'CWALLET','1200000000',100,0,false,TIMESTAMP '2026-01-01 00:00:10',NULL,NULL,1,NULL,NULL,NULL,NULL,NULL),
		('CXLM',110,'BX','balance','persistent',NULL,NULL,NULL,'CWALLET','1230000000',110,0,true,TIMESTAMP '2026-01-01 00:00:11',NULL,NULL,1,NULL,NULL,NULL,NULL,NULL)`)
	if err != nil {
		t.Fatalf("insert balance snapshot fixtures: %v", err)
	}
}
