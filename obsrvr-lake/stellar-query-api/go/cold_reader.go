package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

type ColdReader struct {
	db     *sql.DB
	config DuckLakeConfig
}

func NewColdReader(config DuckLakeConfig) (*ColdReader, error) {
	// Open DuckDB connection (in-memory)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	reader := &ColdReader{
		db:     db,
		config: config,
	}

	// Initialize DuckLake connection
	if err := reader.initialize(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize DuckLake: %w", err)
	}

	return reader, nil
}

func (c *ColdReader) initialize(ctx context.Context) error {
	// Cap DuckDB memory to prevent OOM kills (container has 3.5-5 GiB total)
	if _, err := c.db.ExecContext(ctx, "SET memory_limit='1GB';"); err != nil {
		return fmt.Errorf("failed to set memory limit: %w", err)
	}

	// Install required extensions
	if _, err := c.db.ExecContext(ctx, "INSTALL ducklake FROM core_nightly;"); err != nil {
		return fmt.Errorf("failed to install ducklake extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD ducklake;"); err != nil {
		return fmt.Errorf("failed to load ducklake extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "INSTALL httpfs;"); err != nil {
		return fmt.Errorf("failed to install httpfs extension: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
		return fmt.Errorf("failed to load httpfs extension: %w", err)
	}

	// Remove https:// prefix from endpoint (DuckDB adds it automatically)
	endpoint := c.config.AWSEndpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	// Configure S3 credentials for Backblaze B2
	s3ConfigSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE 'path',
		URL_COMPATIBILITY_MODE true
		);
	`, c.config.AWSAccessKeyID, c.config.AWSSecretAccessKey, c.config.AWSRegion, endpoint)

	if _, err := c.db.ExecContext(ctx, s3ConfigSQL); err != nil {
		return fmt.Errorf("failed to configure S3 credentials: %w", err)
	}

	// Attach DuckLake catalog
	attachSQL := fmt.Sprintf(`
		ATTACH '%s'
		AS %s
		(DATA_PATH '%s', METADATA_SCHEMA '%s', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
	`, c.config.CatalogPath, c.config.CatalogName, c.config.DataPath, c.config.MetadataSchema)

	if _, err := c.db.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	return nil
}

func (c *ColdReader) QueryLedgers(ctx context.Context, start, end int64, limit int, sort string) (*sql.Rows, error) {
	// Map sort parameter to ORDER BY clause
	orderBy := "sequence ASC" // default
	switch sort {
	case "sequence_desc":
		orderBy = "sequence DESC"
	case "closed_at_asc":
		orderBy = "closed_at ASC"
	case "closed_at_desc":
		orderBy = "closed_at DESC"
	case "tx_count_desc":
		orderBy = "transaction_count DESC, sequence DESC"
	}

	query := fmt.Sprintf(`
		SELECT
			sequence,
			ledger_hash,
			previous_ledger_hash,
			transaction_count,
			operation_count,
			successful_tx_count,
			failed_tx_count,
			tx_set_operation_count,
			closed_at,
			total_coins,
			fee_pool,
			base_fee,
			base_reserve,
			max_tx_set_size,
			protocol_version,
			ledger_header,
			soroban_fee_write1kb as soroban_fee_write_1kb,
			node_id,
			signature,
			ledger_range,
			era_id,
			version_label,
			soroban_op_count,
			total_fee_charged,
			contract_events_count,
			COALESCE(ingestion_timestamp, closed_at) as created_at
		FROM %s.%s.ledgers_row_v2
		WHERE sequence >= ? AND sequence <= ?
		ORDER BY %s
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName, orderBy)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query ledgers from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTransactions(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			source_account,
			source_account_muxed,
			account_sequence,
			max_fee,
			operation_count,
			created_at,
			ledger_range,
			era_id,
			version_label,
			successful
		FROM %s.%s.transactions_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryOperations(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			type,
			source_account,
			source_account_muxed,
			created_at,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.operations_row_v2
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query operations from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryEffects(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			effect_index,
			effect_type,
			effect_type_string,
			account_id,
			amount,
			asset_code,
			asset_issuer,
			asset_type,
			trustline_limit,
			authorize_flag,
			clawback_flag,
			signer_account,
			signer_weight,
			offer_id,
			seller_account,
			created_at,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.effects_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, effect_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTrades(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			trade_index,
			seller_account,
			selling_asset_code,
			selling_asset_issuer,
			selling_amount,
			buyer_account,
			buying_asset_code,
			buying_asset_issuer,
			buying_amount,
			price,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.trades_row_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryAccounts(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			flags,
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			ledger_sequence,
			num_sponsoring,
			num_sponsored,
			sponsor_account,
			signers,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.accounts_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryTrustlines(ctx context.Context, accountID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			asset_code,
			asset_issuer,
			asset_type,
			balance,
			trust_limit,
			buying_liabilities,
			selling_liabilities,
			ledger_sequence,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.trustlines_snapshot_v1
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, accountID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryOffers(ctx context.Context, sellerID string, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			seller_account,
			offer_id,
			selling_asset_code,
			selling_asset_issuer,
			selling_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			buying_asset_type,
			amount,
			price,
			flags,
			ledger_sequence,
			ledger_range,
			era_id,
			version_label,
			created_at
		FROM %s.%s.offers_snapshot_v1
		WHERE seller_account = ?
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, sellerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers from DuckLake: %w", err)
	}

	return rows, nil
}

func (c *ColdReader) QueryContractEvents(ctx context.Context, start, end int64, limit int) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			event_id,
			contract_id,
			ledger_sequence,
			transaction_hash,
			operation_index,
			event_index,
			event_type,
			topics_json,
			data_decoded,
			closed_at,
			ledger_range,
			era_id,
			version_label
		FROM %s.%s.contract_events_stream_v1
		WHERE ledger_sequence >= ? AND ledger_sequence <= ?
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, event_index ASC
		LIMIT ?
	`, c.config.CatalogName, c.config.SchemaName)

	rows, err := c.db.QueryContext(ctx, query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract events from DuckLake: %w", err)
	}

	return rows, nil
}

// GetDistinctAccountCount returns the count of unique accounts in Bronze storage
func (c *ColdReader) GetDistinctAccountCount(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT account_id)
		FROM %s.%s.accounts_snapshot_v1
	`, c.config.CatalogName, c.config.SchemaName)

	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count distinct accounts: %w", err)
	}

	return count, nil
}

// DB returns the underlying DuckDB handle. Callers can use it to run
// ad-hoc DuckLake queries directly, bypassing the per-method wrappers.
// Unlike the UnifiedDuckDBReader, this handle ONLY has DuckLake attached
// (no ATTACH POSTGRES), so queries go straight to cold Parquet without
// federation overhead.
func (c *ColdReader) DB() *sql.DB {
	return c.db
}

// CatalogName returns the configured DuckLake catalog name (e.g. "lake")
// so callers can construct fully-qualified table references like
// `<catalog>.<schema>.<table>`.
func (c *ColdReader) CatalogName() string {
	return c.config.CatalogName
}

// SchemaName returns the DuckLake schema name (e.g. "bronze").
func (c *ColdReader) SchemaName() string {
	return c.config.SchemaName
}

func (c *ColdReader) Close() error {
	return c.db.Close()
}

// GetGenericEvents returns generic contract events from bronze cold storage only.
func (c *ColdReader) GetGenericEvents(ctx context.Context, filters GenericEventFilters) ([]GenericEvent, string, bool, error) {
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1

	order := "DESC"
	if filters.Order == "asc" {
		order = "ASC"
	}

	var conditions []string
	var args []any
	argIdx := 1

	if filters.ContractID != nil && *filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argIdx))
		args = append(args, *filters.ContractID)
		argIdx++
	}
	if filters.TxHash != nil && *filters.TxHash != "" {
		conditions = append(conditions, fmt.Sprintf("transaction_hash = $%d", argIdx))
		args = append(args, *filters.TxHash)
		argIdx++
	}
	if filters.EventType != nil && *filters.EventType != "" {
		conditions = append(conditions, fmt.Sprintf("event_type = $%d", argIdx))
		args = append(args, *filters.EventType)
		argIdx++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		conditions = append(conditions, fmt.Sprintf("topics_decoded ILIKE '%%' || $%d || '%%'", argIdx))
		args = append(args, *filters.TopicMatch)
		argIdx++
	}
	if filters.Topic0 != nil && *filters.Topic0 != "" {
		conditions = append(conditions, fmt.Sprintf("topic0_decoded = $%d", argIdx))
		args = append(args, *filters.Topic0)
		argIdx++
	}
	if filters.Topic1 != nil && *filters.Topic1 != "" {
		conditions = append(conditions, fmt.Sprintf("topic1_decoded = $%d", argIdx))
		args = append(args, *filters.Topic1)
		argIdx++
	}
	if filters.Topic2 != nil && *filters.Topic2 != "" {
		conditions = append(conditions, fmt.Sprintf("topic2_decoded = $%d", argIdx))
		args = append(args, *filters.Topic2)
		argIdx++
	}
	if filters.Topic3 != nil && *filters.Topic3 != "" {
		conditions = append(conditions, fmt.Sprintf("topic3_decoded = $%d", argIdx))
		args = append(args, *filters.Topic3)
		argIdx++
	}
	if filters.StartLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence >= $%d", argIdx))
		args = append(args, *filters.StartLedger)
		argIdx++
	}
	if filters.EndLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence <= $%d", argIdx))
		args = append(args, *filters.EndLedger)
		argIdx++
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) == 2 {
			cursorLedger, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor ledger value %q: %w", parts[0], err)
			}
			cursorEvent, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor event_index value %q: %w", parts[1], err)
			}
			if order == "DESC" {
				conditions = append(conditions, fmt.Sprintf("(ledger_sequence < $%d OR (ledger_sequence = $%d AND event_index < $%d))", argIdx, argIdx, argIdx+1))
			} else {
				conditions = append(conditions, fmt.Sprintf("(ledger_sequence > $%d OR (ledger_sequence = $%d AND event_index > $%d))", argIdx, argIdx, argIdx+1))
			}
			args = append(args, cursorLedger, cursorEvent)
			argIdx += 2
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
		       event_type, in_successful_contract_call, topics_json, topics_decoded, data_decoded,
		       topic_count, operation_index, event_index,
		       topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded
		FROM %s.%s.contract_events_stream_v1
		%s
		ORDER BY ledger_sequence %s, event_index %s
		LIMIT $%d
	`, c.config.CatalogName, c.config.SchemaName, whereClause, order, order, argIdx)
	args = append(args, requestLimit)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetGenericEvents: %w", err)
	}
	defer rows.Close()

	var events []GenericEvent
	for rows.Next() {
		var e GenericEvent
		if err := rows.Scan(&e.EventID, &e.ContractID, &e.LedgerSeq, &e.TxHash, &e.ClosedAt,
			&e.EventType, &e.Successful, &e.TopicsJSON, &e.TopicsDecoded, &e.DataDecoded,
			&e.TopicCount, &e.OpIndex, &e.EventIndex,
			&e.Topic0Decoded, &e.Topic1Decoded, &e.Topic2Decoded, &e.Topic3Decoded); err != nil {
			return nil, "", false, err
		}
		events = append(events, e)
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}

	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSeq, last.EventIndex)
	}

	return events, nextCursor, hasMore, nil
}

func (c *ColdReader) GetContractGenericEvents(ctx context.Context, contractID string, filters GenericEventFilters) ([]GenericEvent, string, bool, error) {
	filters.ContractID = &contractID
	return c.GetGenericEvents(ctx, filters)
}

// GetExplorerEvents returns enriched explorer events from bronze cold storage only.
func (c *ColdReader) GetTransactionDiffs(ctx context.Context, txHash string) (*TxDiffs, error) {
	query := fmt.Sprintf(`
		SELECT ledger_sequence, tx_meta
		FROM %s.%s.transactions_row_v2
		WHERE transaction_hash = ?
		LIMIT 1
	`, c.config.CatalogName, c.config.SchemaName)

	var ledgerSeq int64
	var txMetaXDR sql.NullString
	err := c.db.QueryRowContext(ctx, query, txHash).Scan(&ledgerSeq, &txMetaXDR)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	if err != nil {
		return nil, fmt.Errorf("GetTransactionDiffs query: %w", err)
	}

	diffs := &TxDiffs{TxHash: txHash, LedgerSequence: ledgerSeq, BalanceChanges: []TxBalanceChange{}, StateChanges: []TxStateChange{}}
	if !txMetaXDR.Valid || txMetaXDR.String == "" {
		return diffs, nil
	}
	balanceChanges, stateChanges, err := decodeTxMetaXDR(txMetaXDR.String)
	if err != nil {
		return diffs, nil
	}
	diffs.BalanceChanges = balanceChanges
	diffs.StateChanges = stateChanges
	return diffs, nil
}

// GetTransactionDiffsWithLedgerHint looks up tx_meta using the index plane's
// ledger_sequence to enable partition pruning. Without the hint, DuckLake scans
// ALL Parquet files (30s+). With it, one partition (~100ms).
func (c *ColdReader) GetTransactionDiffsWithLedgerHint(ctx context.Context, txHash string, indexReader *IndexReader) (*TxDiffs, error) {
	// Resolve hash → ledger_sequence via the index plane (fast DuckDB lookup)
	var ledgerSeq int64
	if indexReader != nil {
		if txLoc, err := indexReader.LookupTransactionHash(ctx, txHash); err == nil && txLoc != nil {
			ledgerSeq = txLoc.LedgerSequence
		}
	}

	var query string
	var args []interface{}
	if ledgerSeq > 0 {
		// Partition-pruned: ledger_sequence narrows to one ledger_range bucket
		query = fmt.Sprintf(`
			SELECT ledger_sequence, tx_meta
			FROM %s.%s.transactions_row_v2
			WHERE ledger_sequence = ? AND transaction_hash = ?
			LIMIT 1
		`, c.config.CatalogName, c.config.SchemaName)
		args = []interface{}{ledgerSeq, txHash}
	} else {
		// Fallback: full scan (slow but correct)
		query = fmt.Sprintf(`
			SELECT ledger_sequence, tx_meta
			FROM %s.%s.transactions_row_v2
			WHERE transaction_hash = ?
			LIMIT 1
		`, c.config.CatalogName, c.config.SchemaName)
		args = []interface{}{txHash}
	}

	var seq int64
	var txMetaXDR sql.NullString
	err := c.db.QueryRowContext(ctx, query, args...).Scan(&seq, &txMetaXDR)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: %s", ErrTxNotFound, txHash)
	}
	if err != nil {
		return nil, fmt.Errorf("GetTransactionDiffsWithLedgerHint: %w", err)
	}

	diffs := &TxDiffs{TxHash: txHash, LedgerSequence: seq, BalanceChanges: []TxBalanceChange{}, StateChanges: []TxStateChange{}}
	if !txMetaXDR.Valid || txMetaXDR.String == "" {
		return diffs, nil
	}
	balanceChanges, stateChanges, err := decodeTxMetaXDR(txMetaXDR.String)
	if err != nil {
		return diffs, nil
	}
	diffs.BalanceChanges = balanceChanges
	diffs.StateChanges = stateChanges
	return diffs, nil
}

func (c *ColdReader) GetExplorerEvents(ctx context.Context, filters ExplorerEventFilters, hotReader *SilverHotReader, classifier *EventClassifier) ([]ExplorerEvent, *ExplorerEventMeta, string, bool, error) {
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1
	order := "DESC"
	if filters.Order == "asc" {
		order = "ASC"
	}

	var conditions []string
	var args []any
	argIdx := 1

	if len(filters.Types) > 0 {
		topic0Values, includesCatchAll := classifier.Topic0ValuesForTypes(filters.Types)
		if includesCatchAll && len(topic0Values) > 0 {
			allKnown := classifier.AllKnownTopic0Values()
			placeholders := make([]string, len(topic0Values))
			for i, t := range topic0Values {
				placeholders[i] = fmt.Sprintf("$%d", argIdx)
				args = append(args, t)
				argIdx++
			}
			excludePlaceholders := make([]string, len(allKnown))
			for i, t := range allKnown {
				excludePlaceholders[i] = fmt.Sprintf("$%d", argIdx)
				args = append(args, t)
				argIdx++
			}
			conditions = append(conditions, fmt.Sprintf("(topic0_decoded IN (%s) OR topic0_decoded IS NULL OR topic0_decoded NOT IN (%s))", strings.Join(placeholders, ","), strings.Join(excludePlaceholders, ",")))
		} else if includesCatchAll {
			allKnown := classifier.AllKnownTopic0Values()
			if len(allKnown) > 0 {
				excludePlaceholders := make([]string, len(allKnown))
				for i, t := range allKnown {
					excludePlaceholders[i] = fmt.Sprintf("$%d", argIdx)
					args = append(args, t)
					argIdx++
				}
				conditions = append(conditions, fmt.Sprintf("(topic0_decoded IS NULL OR topic0_decoded NOT IN (%s))", strings.Join(excludePlaceholders, ",")))
			}
		} else if len(topic0Values) > 0 {
			placeholders := make([]string, len(topic0Values))
			for i, t := range topic0Values {
				placeholders[i] = fmt.Sprintf("$%d", argIdx)
				args = append(args, t)
				argIdx++
			}
			conditions = append(conditions, fmt.Sprintf("topic0_decoded IN (%s)", strings.Join(placeholders, ",")))
		}
	}
	if filters.ContractID != nil && *filters.ContractID != "" {
		hexID, err := normalizeContractID(*filters.ContractID)
		if err != nil {
			return nil, nil, "", false, fmt.Errorf("invalid contract_id %q: must be C... address or 64-char hex hash", *filters.ContractID)
		}
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argIdx))
		args = append(args, hexID)
		argIdx++
	}
	if filters.ContractName != nil && *filters.ContractName != "" {
		if hotReader == nil {
			conditions = append(conditions, "1 = 0")
		} else {
			hexIDs, err := hotReader.resolveContractNameToHexHot(ctx, *filters.ContractName)
			if err != nil {
				return nil, nil, "", false, err
			}
			if len(hexIDs) == 0 {
				conditions = append(conditions, "1 = 0")
			} else {
				placeholders := make([]string, len(hexIDs))
				for i, hexID := range hexIDs {
					placeholders[i] = fmt.Sprintf("$%d", argIdx)
					args = append(args, hexID)
					argIdx++
				}
				conditions = append(conditions, fmt.Sprintf("contract_id IN (%s)", strings.Join(placeholders, ",")))
			}
		}
	}
	if filters.TxHash != nil && *filters.TxHash != "" {
		conditions = append(conditions, fmt.Sprintf("transaction_hash = $%d", argIdx))
		args = append(args, *filters.TxHash)
		argIdx++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		conditions = append(conditions, fmt.Sprintf("topics_decoded ILIKE '%%' || $%d || '%%'", argIdx))
		args = append(args, *filters.TopicMatch)
		argIdx++
	}
	if filters.Topic0 != nil && *filters.Topic0 != "" {
		conditions = append(conditions, fmt.Sprintf("topic0_decoded = $%d", argIdx))
		args = append(args, *filters.Topic0)
		argIdx++
	}
	if filters.Topic1 != nil && *filters.Topic1 != "" {
		conditions = append(conditions, fmt.Sprintf("topic1_decoded = $%d", argIdx))
		args = append(args, *filters.Topic1)
		argIdx++
	}
	if filters.Topic2 != nil && *filters.Topic2 != "" {
		conditions = append(conditions, fmt.Sprintf("topic2_decoded = $%d", argIdx))
		args = append(args, *filters.Topic2)
		argIdx++
	}
	if filters.Topic3 != nil && *filters.Topic3 != "" {
		conditions = append(conditions, fmt.Sprintf("topic3_decoded = $%d", argIdx))
		args = append(args, *filters.Topic3)
		argIdx++
	}
	if filters.StartLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence >= $%d", argIdx))
		args = append(args, *filters.StartLedger)
		argIdx++
	}
	if filters.EndLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence <= $%d", argIdx))
		args = append(args, *filters.EndLedger)
		argIdx++
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) != 2 {
			return nil, nil, "", false, fmt.Errorf("invalid cursor format")
		}
		cursorLedger, err1 := strconv.ParseInt(parts[0], 10, 64)
		cursorEvent, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, nil, "", false, fmt.Errorf("invalid cursor values")
		}
		if order == "DESC" {
			conditions = append(conditions, fmt.Sprintf("(ledger_sequence < $%d OR (ledger_sequence = $%d AND event_index < $%d))", argIdx, argIdx, argIdx+1))
		} else {
			conditions = append(conditions, fmt.Sprintf("(ledger_sequence > $%d OR (ledger_sequence = $%d AND event_index > $%d))", argIdx, argIdx, argIdx+1))
		}
		args = append(args, cursorLedger, cursorEvent)
		argIdx += 2
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT event_id, contract_id, ledger_sequence, transaction_hash, closed_at,
		       in_successful_contract_call, topic0_decoded, topic1_decoded, topic2_decoded, topic3_decoded,
		       topics_decoded, data_decoded, event_index, operation_index
		FROM %s.%s.contract_events_stream_v1
		%s
		ORDER BY ledger_sequence %s, event_index %s
		LIMIT $%d
	`, c.config.CatalogName, c.config.SchemaName, whereClause, order, order, argIdx)
	queryArgs := append(append([]any{}, args...), requestLimit)
	rows, err := c.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("GetExplorerEvents: %w", err)
	}
	defer rows.Close()

	var events []ExplorerEvent
	var contractIDs []string
	contractSet := make(map[string]bool)
	typeSet := make(map[string]bool)
	for _, t := range filters.Types {
		typeSet[t] = true
	}
	for rows.Next() {
		var e ExplorerEvent
		var successful sql.NullBool
		if err := rows.Scan(&e.EventID, &e.ContractID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt,
			&successful, &e.Topic0, &e.Topic1, &e.Topic2, &e.Topic3,
			&e.TopicsDecoded, &e.DataDecoded, &e.EventIndex, &e.OpIndex); err != nil {
			return nil, nil, "", false, fmt.Errorf("GetExplorerEvents scan: %w", err)
		}
		e.Successful = true
		e.Data = e.DataDecoded
		if e.ContractID != nil {
			if strKeyID, err := hexToStrKey(*e.ContractID); err == nil {
				e.ContractID = &strKeyID
			}
		}
		classification := classifier.Classify(e.ContractID, e.Topic0, e.TopicsDecoded)
		e.Type = classification.EventType
		e.Protocol = classification.Protocol
		if len(typeSet) > 0 && !typeSet[e.Type] {
			continue
		}
		events = append(events, e)
		if e.ContractID != nil && !contractSet[*e.ContractID] {
			contractIDs = append(contractIDs, *e.ContractID)
			contractSet[*e.ContractID] = true
		}
	}

	if hotReader != nil && len(contractIDs) > 0 {
		nameMap := hotReader.batchResolveContractNames(ctx, contractIDs)
		for i := range events {
			if events[i].ContractID != nil {
				if info, ok := nameMap[*events[i].ContractID]; ok {
					events[i].ContractName = info.name
					events[i].ContractSymbol = info.symbol
					events[i].ContractCategory = info.category
				}
			}
		}
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSequence, last.EventIndex)
	}

	metaQuery := fmt.Sprintf(`
		WITH capped AS (
			SELECT ledger_sequence FROM %s.%s.contract_events_stream_v1 %s LIMIT 10001
		)
		SELECT COUNT(*), MIN(ledger_sequence), MAX(ledger_sequence) FROM capped
	`, c.config.CatalogName, c.config.SchemaName, whereClause)
	var count int64
	var minLedger, maxLedger sql.NullInt64
	meta := &ExplorerEventMeta{}
	if err := c.db.QueryRowContext(ctx, metaQuery, args...).Scan(&count, &minLedger, &maxLedger); err == nil {
		meta.MatchedCount = count
		meta.CountCapped = count > 10000
		if meta.CountCapped {
			meta.MatchedCount = 10000
		}
		if minLedger.Valid {
			meta.LedgerRange.Min = minLedger.Int64
		}
		if maxLedger.Valid {
			meta.LedgerRange.Max = maxLedger.Int64
		}
	}

	return events, meta, nextCursor, hasMore, nil
}
