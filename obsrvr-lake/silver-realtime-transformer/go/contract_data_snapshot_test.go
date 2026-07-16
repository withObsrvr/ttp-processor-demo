package main

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestTransformContractDataCurrentDeletesLatestDeletedRows(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	createContractDataSnapshotTable(t, db, "contract_data_snapshot_v1")
	if _, err := db.Exec(`CREATE TABLE contract_data_current (
		contract_id VARCHAR,
		key_hash VARCHAR,
		durability VARCHAR,
		asset_type VARCHAR,
		asset_code VARCHAR,
		asset_issuer VARCHAR,
		data_value VARCHAR,
		last_modified_ledger BIGINT,
		ledger_sequence BIGINT,
		closed_at TIMESTAMP,
		created_at TIMESTAMP,
		ledger_range BIGINT,
		updated_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY (contract_id, key_hash)
	)`); err != nil {
		t.Fatalf("create current table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE contract_data_deletions (
		contract_id VARCHAR,
		key_hash VARCHAR,
		ledger_sequence BIGINT,
		closed_at TIMESTAMP,
		inserted_at TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY (contract_id, key_hash, ledger_sequence)
	)`); err != nil {
		t.Fatalf("create deletion table: %v", err)
	}
	insertContractDataSnapshot(t, db, "C1", "k-deleted", 10, false)
	insertContractDataSnapshot(t, db, "C1", "k-deleted", 11, true)
	insertContractDataSnapshot(t, db, "C1", "k-live", 12, false)
	if _, err := db.Exec(`INSERT INTO contract_data_current (
		contract_id, key_hash, durability, asset_type, asset_code, asset_issuer,
		data_value, last_modified_ledger, ledger_sequence, closed_at, created_at, ledger_range
	) VALUES ('C1', 'k-deleted', 'persistent', NULL, NULL, NULL, 'old-xdr', 10, 10, NOW(), NOW(), 0)`); err != nil {
		t.Fatalf("seed current table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	rt := &RealtimeTransformer{
		config:        &Config{},
		sourceManager: NewSourceManager(&BronzeReader{db: db}, nil, false),
	}
	if _, err := rt.transformContractDataCurrent(context.Background(), tx, 1, 20); err != nil {
		_ = tx.Rollback()
		t.Fatalf("transformContractDataCurrent: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM contract_data_current WHERE contract_id='C1' AND key_hash='k-deleted'`).Scan(&count); err != nil {
		t.Fatalf("count deleted key: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected deleted key removed from current table, count=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM contract_data_current WHERE contract_id='C1' AND key_hash='k-live'`).Scan(&count); err != nil {
		t.Fatalf("count live key: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected live key upserted into current table, count=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM contract_data_deletions WHERE contract_id='C1' AND key_hash='k-deleted' AND ledger_sequence=11`).Scan(&count); err != nil {
		t.Fatalf("count deletion tombstone: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected deletion tombstone, count=%d", count)
	}
}

func TestQueryContractDataSnapshotLatestDeletedIsExcludedOnHotAndCold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query func(context.Context, *sql.DB, int64, int64) (*sql.Rows, error)
		setup func(*testing.T, *sql.DB)
	}{
		{
			name: "hot",
			query: func(ctx context.Context, db *sql.DB, start, end int64) (*sql.Rows, error) {
				return (&BronzeReader{db: db}).QueryContractDataSnapshot(ctx, start, end)
			},
			setup: func(t *testing.T, db *sql.DB) { createContractDataSnapshotTable(t, db, "contract_data_snapshot_v1") },
		},
		{
			name: "cold",
			query: func(ctx context.Context, db *sql.DB, start, end int64) (*sql.Rows, error) {
				return (&BronzeColdReader{db: db, catalogName: "memory", schemaName: "bronze"}).QueryContractDataSnapshot(ctx, start, end)
			},
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE SCHEMA bronze`); err != nil {
					t.Fatalf("create schema: %v", err)
				}
				createContractDataSnapshotTable(t, db, "memory.bronze.contract_data_snapshot_v1")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			tc.setup(t, db)
			insertContractDataSnapshot(t, db, "C1", "k-deleted", 10, false)
			insertContractDataSnapshot(t, db, "C1", "k-deleted", 11, true)
			insertContractDataSnapshot(t, db, "C1", "k-live", 12, false)

			rows, err := tc.query(context.Background(), db, 1, 20)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer rows.Close()

			var keys []string
			for rows.Next() {
				var contractID, keyHash, durability string
				var assetType, assetCode, assetIssuer, dataValue sql.NullString
				var lastModified, ledgerSequence, ledgerRange int64
				var closedAt, createdAt sql.NullTime
				if err := rows.Scan(&contractID, &keyHash, &durability, &assetType, &assetCode, &assetIssuer, &dataValue, &lastModified, &ledgerSequence, &closedAt, &createdAt, &ledgerRange); err != nil {
					t.Fatalf("scan: %v", err)
				}
				keys = append(keys, keyHash)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			if len(keys) != 1 || keys[0] != "k-live" {
				t.Fatalf("expected only k-live, got %#v", keys)
			}
		})
	}
}

func TestQueryDeletedContractDataSnapshotLatestDeletedOnHotAndCold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query func(context.Context, *sql.DB, int64, int64) (*sql.Rows, error)
		setup func(*testing.T, *sql.DB)
	}{
		{
			name: "hot",
			query: func(ctx context.Context, db *sql.DB, start, end int64) (*sql.Rows, error) {
				return (&BronzeReader{db: db}).QueryDeletedContractDataSnapshot(ctx, start, end)
			},
			setup: func(t *testing.T, db *sql.DB) { createContractDataSnapshotTable(t, db, "contract_data_snapshot_v1") },
		},
		{
			name: "cold",
			query: func(ctx context.Context, db *sql.DB, start, end int64) (*sql.Rows, error) {
				return (&BronzeColdReader{db: db, catalogName: "memory", schemaName: "bronze"}).QueryDeletedContractDataSnapshot(ctx, start, end)
			},
			setup: func(t *testing.T, db *sql.DB) {
				if _, err := db.Exec(`CREATE SCHEMA bronze`); err != nil {
					t.Fatalf("create schema: %v", err)
				}
				createContractDataSnapshotTable(t, db, "memory.bronze.contract_data_snapshot_v1")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			tc.setup(t, db)
			insertContractDataSnapshot(t, db, "C1", "k-deleted", 10, false)
			insertContractDataSnapshot(t, db, "C1", "k-deleted", 11, true)
			insertContractDataSnapshot(t, db, "C1", "k-restored", 12, true)
			insertContractDataSnapshot(t, db, "C1", "k-restored", 13, false)

			rows, err := tc.query(context.Background(), db, 1, 20)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer rows.Close()

			var got []string
			for rows.Next() {
				var contractID, keyHash string
				var ledgerSequence int64
				var closedAt sql.NullTime
				if err := rows.Scan(&contractID, &keyHash, &ledgerSequence, &closedAt); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if ledgerSequence != 11 || !closedAt.Valid {
					t.Fatalf("deletion version = %d/%v, want 11/valid", ledgerSequence, closedAt.Valid)
				}
				got = append(got, keyHash)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			if len(got) != 1 || got[0] != "k-deleted" {
				t.Fatalf("expected only k-deleted, got %#v", got)
			}
		})
	}
}

func createContractDataSnapshotTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE ` + table + ` (
		contract_id VARCHAR,
		ledger_key_hash VARCHAR,
		contract_durability VARCHAR,
		asset_type VARCHAR,
		asset_code VARCHAR,
		asset_issuer VARCHAR,
		contract_data_xdr VARCHAR,
		last_modified_ledger BIGINT,
		ledger_sequence BIGINT,
		closed_at TIMESTAMP,
		created_at TIMESTAMP,
		ledger_range BIGINT,
		deleted BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
}

func insertContractDataSnapshot(t *testing.T, db *sql.DB, contractID, keyHash string, ledger int64, deleted bool) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO contract_data_snapshot_v1 VALUES (?, ?, 'persistent', NULL, NULL, NULL, 'xdr', ?, ?, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00', ?, ?)`, contractID, keyHash, ledger, ledger, ledger/64, deleted)
	if err != nil {
		// Cold tests create the table in memory.bronze; retry there.
		_, err = db.Exec(`INSERT INTO memory.bronze.contract_data_snapshot_v1 VALUES (?, ?, 'persistent', NULL, NULL, NULL, 'xdr', ?, ?, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00', ?, ?)`, contractID, keyHash, ledger, ledger, ledger/64, deleted)
	}
	if err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
}
