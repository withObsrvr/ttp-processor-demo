package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const defaultInsertBatchSize = 500

// BatchInserter accumulates rows and flushes multi-row INSERT statements.
// Works for both DO NOTHING and DO UPDATE conflict resolution.
type BatchInserter struct {
	table       string
	columns     []string
	conflictSQL string // e.g. "ON CONFLICT (tx_hash, op_index) DO NOTHING" or full DO UPDATE SET ...
	rows        [][]interface{}
	maxRows     int
	flushed     int64
}

// NewBatchInserter creates a new batch inserter for the given table.
func NewBatchInserter(table string, columns []string, conflictSQL string, maxRows int) *BatchInserter {
	if maxRows <= 0 {
		maxRows = defaultInsertBatchSize
	}
	return &BatchInserter{
		table:       table,
		columns:     columns,
		conflictSQL: conflictSQL,
		maxRows:     maxRows,
	}
}

// Add appends a row to the batch. Returns an error if the number of values
// does not match the number of columns.
func (bi *BatchInserter) Add(values ...interface{}) error {
	if len(values) != len(bi.columns) {
		return fmt.Errorf("batch %s: got %d values, expected %d columns", bi.table, len(values), len(bi.columns))
	}
	bi.rows = append(bi.rows, values)
	return nil
}

// Len returns the number of pending rows.
func (bi *BatchInserter) Len() int {
	return len(bi.rows)
}

// Flush writes all pending rows as a single multi-row INSERT statement.
func (bi *BatchInserter) Flush(ctx context.Context, tx *sql.Tx) error {
	if len(bi.rows) == 0 {
		return nil
	}

	numCols := len(bi.columns)
	numRows := len(bi.rows)

	// Pre-allocate builder capacity: rough estimate
	var b strings.Builder
	b.Grow(128 + numRows*numCols*5)

	b.WriteString("INSERT INTO ")
	b.WriteString(bi.table)
	b.WriteString(" (")
	for i, col := range bi.columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(") VALUES ")

	args := make([]interface{}, 0, numRows*numCols)
	paramIdx := 1

	for i, row := range bi.rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for j := 0; j < numCols; j++ {
			if j > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "$%d", paramIdx)
			paramIdx++
			args = append(args, row[j])
		}
		b.WriteByte(')')
	}

	b.WriteByte(' ')
	b.WriteString(bi.conflictSQL)

	_, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return fmt.Errorf("batch insert into %s (%d rows): %w", bi.table, numRows, err)
	}

	bi.flushed += int64(numRows)
	bi.rows = bi.rows[:0]
	return nil
}

// FlushIfNeeded flushes if the batch has reached maxRows.
func (bi *BatchInserter) FlushIfNeeded(ctx context.Context, tx *sql.Tx) error {
	if len(bi.rows) >= bi.maxRows {
		return bi.Flush(ctx, tx)
	}
	return nil
}

// TotalFlushed returns the total number of rows flushed across all flushes.
func (bi *BatchInserter) TotalFlushed() int64 {
	return bi.flushed
}

// =============================================================================
// Table column definitions and conflict SQL
// =============================================================================

// --- Phase 1: DO NOTHING tables ---

var enrichedOpColumns = []string{
	"transaction_hash", "operation_index", "ledger_sequence", "source_account",
	"type", "type_string", "created_at", "transaction_successful",
	"operation_result_code", "operation_trace_code", "ledger_range",
	"source_account_muxed", "asset", "asset_type", "asset_code", "asset_issuer",
	"source_asset", "source_asset_type", "source_asset_code", "source_asset_issuer",
	"destination", "destination_muxed", "amount", "source_amount",
	"from_account", "from_muxed", "to_address", "to_muxed",
	"limit_amount", "offer_id",
	"selling_asset", "selling_asset_type", "selling_asset_code", "selling_asset_issuer",
	"buying_asset", "buying_asset_type", "buying_asset_code", "buying_asset_issuer",
	"price_n", "price_d", "price",
	"starting_balance", "home_domain", "inflation_dest",
	"set_flags", "set_flags_s", "clear_flags", "clear_flags_s",
	"master_key_weight", "low_threshold", "med_threshold", "high_threshold",
	"signer_account_id", "signer_key", "signer_weight",
	"data_name", "data_value",
	"host_function_type", "parameters", "address", "contract_id", "function_name",
	"balance_id", "claimant", "claimant_muxed", "predicate",
	"liquidity_pool_id", "reserve_a_asset", "reserve_a_amount",
	"reserve_b_asset", "reserve_b_amount", "shares", "shares_received",
	"into_account", "into_muxed",
	"sponsor", "sponsored_id", "begin_sponsor",
	"tx_successful", "tx_fee_charged", "tx_max_fee", "tx_operation_count",
	"tx_memo_type", "tx_memo",
	"ledger_closed_at", "ledger_total_coins", "ledger_fee_pool",
	"ledger_base_fee", "ledger_base_reserve",
	"ledger_transaction_count", "ledger_operation_count",
	"ledger_successful_tx_count", "ledger_failed_tx_count",
	"is_payment_op", "is_soroban_op",
}

const enrichedOpConflict = `ON CONFLICT (transaction_hash, operation_index) DO NOTHING`
const enrichedOpSorobanConflict = `ON CONFLICT (transaction_hash, operation_index) DO NOTHING`

var tokenTransferColumns = []string{
	"timestamp", "transaction_hash", "ledger_sequence", "source_type",
	"from_account", "to_account", "asset_code", "asset_issuer", "amount",
	"token_contract_id", "operation_type", "transaction_successful", "event_index",
}

const tokenTransferConflict = `ON CONFLICT DO NOTHING`

var unmatchedContractEventColumns = []string{
	"timestamp", "transaction_hash", "ledger_sequence", "contract_id",
	"event_index", "event_name", "topics_decoded", "data_decoded",
	"successful", "parse_reason",
}

const unmatchedContractEventConflict = `ON CONFLICT DO NOTHING`

var accountSnapshotColumns = []string{
	"account_id", "ledger_sequence", "closed_at", "balance", "sequence_number",
	"num_subentries", "num_sponsoring", "num_sponsored", "home_domain",
	"master_weight", "low_threshold", "med_threshold", "high_threshold",
	"flags", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
	"signers", "sponsor_account", "created_at", "updated_at",
	"ledger_range", "era_id", "version_label", "valid_to",
}

const accountSnapshotConflict = `ON CONFLICT (account_id, ledger_sequence) DO NOTHING`

var trustlineSnapshotColumns = []string{
	"account_id", "asset_code", "asset_issuer", "asset_type", "balance", "trust_limit",
	"buying_liabilities", "selling_liabilities", "authorized",
	"authorized_to_maintain_liabilities", "clawback_enabled",
	"ledger_sequence", "created_at", "ledger_range", "era_id", "version_label", "valid_to",
}

const trustlineSnapshotConflict = `ON CONFLICT (account_id, asset_code, asset_issuer, asset_type, ledger_sequence) DO NOTHING`

var offerSnapshotColumns = []string{
	"offer_id", "seller_account", "ledger_sequence", "closed_at",
	"selling_asset_type", "selling_asset_code", "selling_asset_issuer",
	"buying_asset_type", "buying_asset_code", "buying_asset_issuer",
	"amount", "price", "flags", "created_at", "ledger_range", "era_id", "version_label", "valid_to",
}

const offerSnapshotConflict = `ON CONFLICT (offer_id, ledger_sequence) DO NOTHING`

var accountSignerSnapshotColumns = []string{
	"account_id", "signer", "ledger_sequence", "closed_at", "weight", "sponsor",
	"ledger_range", "era_id", "version_label", "valid_to",
}

const accountSignerSnapshotConflict = `ON CONFLICT (account_id, signer, ledger_sequence) DO NOTHING`

var contractCallColumns = []string{
	"ledger_sequence", "transaction_index", "operation_index", "transaction_hash",
	"from_contract", "to_contract", "function_name", "call_depth", "execution_order",
	"successful", "closed_at", "ledger_range",
}

const contractCallConflict = `ON CONFLICT (ledger_sequence, transaction_hash, operation_index, execution_order) DO NOTHING`

var contractHierarchyColumns = []string{
	"transaction_hash", "root_contract", "child_contract", "path_depth", "full_path", "ledger_range",
}

const contractHierarchyConflict = `ON CONFLICT (transaction_hash, root_contract, child_contract) DO NOTHING`

var tradeColumns = []string{
	"ledger_sequence", "transaction_hash", "operation_index", "trade_index",
	"trade_type", "trade_timestamp", "seller_account",
	"selling_asset_code", "selling_asset_issuer", "selling_amount",
	"buyer_account", "buying_asset_code", "buying_asset_issuer", "buying_amount",
	"price", "created_at", "ledger_range",
}

const tradeConflict = `ON CONFLICT (ledger_sequence, transaction_hash, operation_index, trade_index) DO NOTHING`

var effectColumns = []string{
	"ledger_sequence", "transaction_hash", "operation_index", "effect_index",
	"operation_id", "effect_type", "effect_type_string", "account_id",
	"amount", "asset_code", "asset_issuer", "asset_type",
	"details_json",
	"trustline_limit", "authorize_flag", "clawback_flag",
	"signer_account", "signer_weight", "offer_id", "seller_account",
	"created_at", "ledger_range",
}

const effectConflict = `ON CONFLICT (ledger_sequence, transaction_hash, operation_index, effect_index) DO NOTHING`

var evictedKeyColumns = []string{
	"contract_id", "key_hash", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const evictedKeyConflict = `ON CONFLICT (contract_id, key_hash, ledger_sequence) DO NOTHING`

var restoredKeyColumns = []string{
	"contract_id", "key_hash", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const restoredKeyConflict = `ON CONFLICT (contract_id, key_hash, ledger_sequence) DO NOTHING`

// --- Phase 2: DO UPDATE tables ---

var accountCurrentColumns = []string{
	"account_id", "balance", "sequence_number", "num_subentries",
	"num_sponsoring", "num_sponsored", "home_domain",
	"master_weight", "low_threshold", "med_threshold", "high_threshold",
	"flags", "auth_required", "auth_revocable", "auth_immutable", "auth_clawback_enabled",
	"signers", "sponsor_account", "created_at", "updated_at",
	"last_modified_ledger", "ledger_range", "era_id", "version_label",
}

const accountCurrentConflict = `ON CONFLICT (account_id) DO UPDATE SET
	balance = EXCLUDED.balance,
	sequence_number = EXCLUDED.sequence_number,
	num_subentries = EXCLUDED.num_subentries,
	num_sponsoring = EXCLUDED.num_sponsoring,
	num_sponsored = EXCLUDED.num_sponsored,
	home_domain = EXCLUDED.home_domain,
	master_weight = EXCLUDED.master_weight,
	low_threshold = EXCLUDED.low_threshold,
	med_threshold = EXCLUDED.med_threshold,
	high_threshold = EXCLUDED.high_threshold,
	flags = EXCLUDED.flags,
	auth_required = EXCLUDED.auth_required,
	auth_revocable = EXCLUDED.auth_revocable,
	auth_immutable = EXCLUDED.auth_immutable,
	auth_clawback_enabled = EXCLUDED.auth_clawback_enabled,
	signers = EXCLUDED.signers,
	sponsor_account = EXCLUDED.sponsor_account,
	updated_at = EXCLUDED.updated_at,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_range = EXCLUDED.ledger_range`

var trustlineCurrentColumns = []string{
	"account_id", "asset_type", "asset_issuer", "asset_code", "liquidity_pool_id",
	"balance", "trust_line_limit", "buying_liabilities", "selling_liabilities",
	"flags", "last_modified_ledger", "ledger_sequence", "created_at", "sponsor", "ledger_range", "era_id", "version_label",
}

const trustlineCurrentConflict = `ON CONFLICT (account_id, asset_type, COALESCE(asset_code, ''), COALESCE(asset_issuer, ''), COALESCE(liquidity_pool_id, '')) DO UPDATE SET
	balance = EXCLUDED.balance,
	trust_line_limit = EXCLUDED.trust_line_limit,
	buying_liabilities = EXCLUDED.buying_liabilities,
	selling_liabilities = EXCLUDED.selling_liabilities,
	flags = EXCLUDED.flags,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	sponsor = EXCLUDED.sponsor,
	ledger_range = EXCLUDED.ledger_range,
	era_id = EXCLUDED.era_id,
	version_label = EXCLUDED.version_label,
	updated_at = NOW()`

var offerCurrentColumns = []string{
	"offer_id", "seller_id", "selling_asset_type", "selling_asset_code", "selling_asset_issuer",
	"buying_asset_type", "buying_asset_code", "buying_asset_issuer",
	"amount", "price_n", "price_d", "price", "flags",
	"last_modified_ledger", "ledger_sequence", "created_at", "sponsor", "ledger_range", "era_id", "version_label",
}

const offerCurrentConflict = `ON CONFLICT (offer_id) DO UPDATE SET
	seller_id = EXCLUDED.seller_id,
	selling_asset_type = EXCLUDED.selling_asset_type,
	selling_asset_code = EXCLUDED.selling_asset_code,
	selling_asset_issuer = EXCLUDED.selling_asset_issuer,
	buying_asset_type = EXCLUDED.buying_asset_type,
	buying_asset_code = EXCLUDED.buying_asset_code,
	buying_asset_issuer = EXCLUDED.buying_asset_issuer,
	amount = EXCLUDED.amount,
	price_n = EXCLUDED.price_n,
	price_d = EXCLUDED.price_d,
	price = EXCLUDED.price,
	flags = EXCLUDED.flags,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	sponsor = EXCLUDED.sponsor,
	ledger_range = EXCLUDED.ledger_range,
	era_id = EXCLUDED.era_id,
	version_label = EXCLUDED.version_label,
	updated_at = NOW()`

var contractInvocationColumns = []string{
	"ledger_sequence", "transaction_index", "operation_index",
	"transaction_hash", "source_account", "contract_id", "function_name",
	"arguments_json", "successful", "closed_at", "ledger_range", "era_id", "version_label",
}

const contractInvocationConflict = `ON CONFLICT (ledger_sequence, transaction_index, operation_index) DO UPDATE SET
	contract_id = EXCLUDED.contract_id,
	function_name = EXCLUDED.function_name,
	arguments_json = EXCLUDED.arguments_json,
	successful = EXCLUDED.successful,
	era_id = EXCLUDED.era_id,
	version_label = EXCLUDED.version_label`

var contractMetadataColumns = []string{
	"contract_id", "creator_address", "wasm_hash", "created_ledger", "created_at", "era_id", "version_label",
}

const contractMetadataConflict = `ON CONFLICT (contract_id) DO UPDATE SET
	creator_address = EXCLUDED.creator_address,
	wasm_hash = COALESCE(EXCLUDED.wasm_hash, contract_metadata.wasm_hash),
	created_ledger = EXCLUDED.created_ledger,
	created_at = EXCLUDED.created_at,
	era_id = EXCLUDED.era_id,
	version_label = EXCLUDED.version_label`

var liquidityPoolCurrentColumns = []string{
	"liquidity_pool_id", "pool_type", "fee", "trustline_count", "total_pool_shares",
	"asset_a_type", "asset_a_code", "asset_a_issuer", "asset_a_amount",
	"asset_b_type", "asset_b_code", "asset_b_issuer", "asset_b_amount",
	"last_modified_ledger", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const liquidityPoolCurrentConflict = `ON CONFLICT (liquidity_pool_id) DO UPDATE SET
	pool_type = EXCLUDED.pool_type,
	fee = EXCLUDED.fee,
	trustline_count = EXCLUDED.trustline_count,
	total_pool_shares = EXCLUDED.total_pool_shares,
	asset_a_type = EXCLUDED.asset_a_type,
	asset_a_code = EXCLUDED.asset_a_code,
	asset_a_issuer = EXCLUDED.asset_a_issuer,
	asset_a_amount = EXCLUDED.asset_a_amount,
	asset_b_type = EXCLUDED.asset_b_type,
	asset_b_code = EXCLUDED.asset_b_code,
	asset_b_issuer = EXCLUDED.asset_b_issuer,
	asset_b_amount = EXCLUDED.asset_b_amount,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var claimableBalanceCurrentColumns = []string{
	"balance_id", "sponsor", "asset_type", "asset_code", "asset_issuer", "amount",
	"claimants_count", "flags", "last_modified_ledger", "ledger_sequence",
	"closed_at", "created_at", "ledger_range",
}

const claimableBalanceCurrentConflict = `ON CONFLICT (balance_id) DO UPDATE SET
	sponsor = EXCLUDED.sponsor,
	asset_type = EXCLUDED.asset_type,
	asset_code = EXCLUDED.asset_code,
	asset_issuer = EXCLUDED.asset_issuer,
	amount = EXCLUDED.amount,
	claimants_count = EXCLUDED.claimants_count,
	flags = EXCLUDED.flags,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var nativeBalanceCurrentColumns = []string{
	"account_id", "balance", "buying_liabilities", "selling_liabilities",
	"num_subentries", "num_sponsoring", "num_sponsored", "sequence_number",
	"last_modified_ledger", "ledger_sequence", "ledger_range",
}

const nativeBalanceCurrentConflict = `ON CONFLICT (account_id) DO UPDATE SET
	balance = EXCLUDED.balance,
	buying_liabilities = EXCLUDED.buying_liabilities,
	selling_liabilities = EXCLUDED.selling_liabilities,
	num_subentries = EXCLUDED.num_subentries,
	num_sponsoring = EXCLUDED.num_sponsoring,
	num_sponsored = EXCLUDED.num_sponsored,
	sequence_number = EXCLUDED.sequence_number,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var contractDataCurrentColumns = []string{
	"contract_id", "key_hash", "durability", "asset_type", "asset_code", "asset_issuer",
	"data_value", "last_modified_ledger", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const contractDataCurrentConflict = `ON CONFLICT (contract_id, key_hash) DO UPDATE SET
	durability = EXCLUDED.durability,
	asset_type = EXCLUDED.asset_type,
	asset_code = EXCLUDED.asset_code,
	asset_issuer = EXCLUDED.asset_issuer,
	data_value = EXCLUDED.data_value,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var contractCodeCurrentColumns = []string{
	"contract_code_hash", "contract_code_ext_v",
	"n_data_segment_bytes", "n_data_segments", "n_elem_segments", "n_exports",
	"n_functions", "n_globals", "n_imports", "n_instructions", "n_table_entries", "n_types",
	"last_modified_ledger", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const contractCodeCurrentConflict = `ON CONFLICT (contract_code_hash) DO UPDATE SET
	contract_code_ext_v = EXCLUDED.contract_code_ext_v,
	n_data_segment_bytes = EXCLUDED.n_data_segment_bytes,
	n_data_segments = EXCLUDED.n_data_segments,
	n_elem_segments = EXCLUDED.n_elem_segments,
	n_exports = EXCLUDED.n_exports,
	n_functions = EXCLUDED.n_functions,
	n_globals = EXCLUDED.n_globals,
	n_imports = EXCLUDED.n_imports,
	n_instructions = EXCLUDED.n_instructions,
	n_table_entries = EXCLUDED.n_table_entries,
	n_types = EXCLUDED.n_types,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var ttlCurrentColumns = []string{
	"key_hash", "live_until_ledger_seq", "ttl_remaining", "expired",
	"last_modified_ledger", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const ttlCurrentConflict = `ON CONFLICT (key_hash) DO UPDATE SET
	live_until_ledger_seq = EXCLUDED.live_until_ledger_seq,
	ttl_remaining = EXCLUDED.ttl_remaining,
	expired = EXCLUDED.expired,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`

var tokenRegistryColumns = []string{
	"contract_id", "token_name", "token_symbol", "token_decimals",
	"asset_code", "asset_issuer", "token_type",
	"first_seen_ledger", "last_updated_ledger",
}

const tokenRegistryConflict = `ON CONFLICT (contract_id) DO UPDATE SET
	token_name = COALESCE(EXCLUDED.token_name, token_registry.token_name),
	token_symbol = COALESCE(EXCLUDED.token_symbol, token_registry.token_symbol),
	token_decimals = EXCLUDED.token_decimals,
	asset_code = COALESCE(EXCLUDED.asset_code, token_registry.asset_code),
	asset_issuer = COALESCE(EXCLUDED.asset_issuer, token_registry.asset_issuer),
	token_type = EXCLUDED.token_type,
	last_updated_ledger = EXCLUDED.last_updated_ledger,
	updated_at = NOW()`

var configSettingsCurrentColumns = []string{
	"config_setting_id",
	"ledger_max_instructions", "tx_max_instructions",
	"fee_rate_per_instructions_increment", "tx_memory_limit",
	"ledger_max_read_ledger_entries", "ledger_max_read_bytes",
	"ledger_max_write_ledger_entries", "ledger_max_write_bytes",
	"tx_max_read_ledger_entries", "tx_max_read_bytes",
	"tx_max_write_ledger_entries", "tx_max_write_bytes",
	"contract_max_size_bytes", "config_setting_xdr",
	"last_modified_ledger", "ledger_sequence", "closed_at", "created_at", "ledger_range",
}

const configSettingsCurrentConflict = `ON CONFLICT (config_setting_id) DO UPDATE SET
	ledger_max_instructions = EXCLUDED.ledger_max_instructions,
	tx_max_instructions = EXCLUDED.tx_max_instructions,
	fee_rate_per_instructions_increment = EXCLUDED.fee_rate_per_instructions_increment,
	tx_memory_limit = EXCLUDED.tx_memory_limit,
	ledger_max_read_ledger_entries = EXCLUDED.ledger_max_read_ledger_entries,
	ledger_max_read_bytes = EXCLUDED.ledger_max_read_bytes,
	ledger_max_write_ledger_entries = EXCLUDED.ledger_max_write_ledger_entries,
	ledger_max_write_bytes = EXCLUDED.ledger_max_write_bytes,
	tx_max_read_ledger_entries = EXCLUDED.tx_max_read_ledger_entries,
	tx_max_read_bytes = EXCLUDED.tx_max_read_bytes,
	tx_max_write_ledger_entries = EXCLUDED.tx_max_write_ledger_entries,
	tx_max_write_bytes = EXCLUDED.tx_max_write_bytes,
	contract_max_size_bytes = EXCLUDED.contract_max_size_bytes,
	config_setting_xdr = EXCLUDED.config_setting_xdr,
	last_modified_ledger = EXCLUDED.last_modified_ledger,
	ledger_sequence = EXCLUDED.ledger_sequence,
	closed_at = EXCLUDED.closed_at,
	ledger_range = EXCLUDED.ledger_range,
	updated_at = NOW()`
