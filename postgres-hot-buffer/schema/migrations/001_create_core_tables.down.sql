-- Rollback migration 001: Drop core transaction tables

DROP TABLE IF EXISTS operations_row_v2;
DROP TABLE IF EXISTS transactions_row_v2;
DROP TABLE IF EXISTS ledgers_row_v2;
