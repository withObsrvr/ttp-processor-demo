package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OperationCursor represents a cursor for paginating operations
// Encodes ledger_sequence, operation_index, and order for stable pagination
type OperationCursor struct {
	LedgerSequence int64
	OperationIndex int64
	Order          string // "asc" or "desc" - preserves order across pagination
}

// TransferCursor represents a cursor for paginating transfers
// Encodes ledger_sequence, timestamp, and order for stable pagination
type TransferCursor struct {
	LedgerSequence int64
	Timestamp      time.Time
	Order          string // "asc" or "desc" - preserves order across pagination
}

// AccountCursor represents a cursor for paginating account history
// Encodes ledger_sequence for stable pagination
type AccountCursor struct {
	LedgerSequence int64
}

// AccountListCursor represents a cursor for paginating account lists
// Encodes balance, last_modified_ledger, and account_id for stable pagination
// The cursor adapts to the sort order being used
type AccountListCursor struct {
	Balance            int64  // Balance in stroops for comparison (used when sorting by balance)
	LastModifiedLedger int64  // Last modified ledger (used when sorting by last_modified)
	AccountID          string // Account ID for tie-breaking (always used)
	SortBy             string // The sort field used when this cursor was generated
	SortOrder          string // The sort order used when this cursor was generated (asc/desc)
}

// TokenHoldersCursor represents a cursor for paginating token holders
// Encodes balance and account_id for stable pagination
type TokenHoldersCursor struct {
	Balance   int64
	AccountID string
}

// Encode encodes an operation cursor to an opaque base64 string
// Format: "ledger:op_index:order" (3 parts) or legacy "ledger:op_index" (2 parts)
func (c OperationCursor) Encode() string {
	var raw string
	if c.Order == "" {
		raw = fmt.Sprintf("%d:%d", c.LedgerSequence, c.OperationIndex)
	} else {
		raw = fmt.Sprintf("%d:%d:%s", c.LedgerSequence, c.OperationIndex, c.Order)
	}
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeOperationCursor decodes a base64 cursor string into an OperationCursor
// Returns nil if the cursor string is empty
// Supports both legacy format (2 parts: ledger:op_index) and new format (3 parts: ledger:op_index:order)
func DecodeOperationCursor(cursor string) (*OperationCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.Split(string(decoded), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:op_index or ledger:op_index:order")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	opIndex, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid operation index in cursor: %w", err)
	}

	// Default order for legacy cursors (operations default to DESC)
	order := "desc"
	if len(parts) == 3 {
		order = parts[2]
	}

	return &OperationCursor{
		LedgerSequence: ledger,
		OperationIndex: opIndex,
		Order:          order,
	}, nil
}

// Encode encodes a transfer cursor to an opaque base64 string
// Format: "ledger:timestamp:order" (3 parts) or legacy "ledger:timestamp" (2 parts)
func (c TransferCursor) Encode() string {
	ts := c.Timestamp.Format(time.RFC3339Nano)
	var raw string
	if c.Order == "" {
		raw = fmt.Sprintf("%d:%s", c.LedgerSequence, ts)
	} else {
		raw = fmt.Sprintf("%d:%s:%s", c.LedgerSequence, ts, c.Order)
	}
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeTransferCursor decodes a base64 cursor string into a TransferCursor
// Returns nil if the cursor string is empty
// Supports both legacy format (2 parts: ledger:timestamp) and new format (3 parts: ledger:timestamp:order)
func DecodeTransferCursor(cursor string) (*TransferCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	// Split into at most 3 parts: ledger:timestamp:order
	// But timestamp contains colons, so we need careful parsing
	decodedStr := string(decoded)

	// Find the first colon (after ledger)
	firstColon := strings.Index(decodedStr, ":")
	if firstColon == -1 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:timestamp or ledger:timestamp:order")
	}

	ledgerStr := decodedStr[:firstColon]
	rest := decodedStr[firstColon+1:]

	ledger, err := strconv.ParseInt(ledgerStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	// Check if there's an order suffix (format: timestamp:order where order is asc/desc)
	var timestampStr, order string
	if strings.HasSuffix(rest, ":asc") {
		order = "asc"
		timestampStr = rest[:len(rest)-4]
	} else if strings.HasSuffix(rest, ":desc") {
		order = "desc"
		timestampStr = rest[:len(rest)-5]
	} else {
		// Legacy format without order (transfers default to DESC)
		timestampStr = rest
		order = "desc"
	}

	ts, err := time.Parse(time.RFC3339Nano, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in cursor: %w", err)
	}

	return &TransferCursor{
		LedgerSequence: ledger,
		Timestamp:      ts,
		Order:          order,
	}, nil
}

// Encode encodes an account cursor to an opaque base64 string
func (c AccountCursor) Encode() string {
	raw := fmt.Sprintf("%d", c.LedgerSequence)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeAccountCursor decodes a base64 cursor string into an AccountCursor
// Returns nil if the cursor string is empty
func DecodeAccountCursor(cursor string) (*AccountCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	ledger, err := strconv.ParseInt(string(decoded), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	return &AccountCursor{
		LedgerSequence: ledger,
	}, nil
}

// Encode encodes an account list cursor to an opaque base64 string
// Format: balance:last_modified_ledger:sort_by:sort_order:account_id
func (c AccountListCursor) Encode() string {
	raw := fmt.Sprintf("%d:%d:%s:%s:%s", c.Balance, c.LastModifiedLedger, c.SortBy, c.SortOrder, c.AccountID)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeAccountListCursor decodes a base64 cursor string into an AccountListCursor
// Returns nil if the cursor string is empty
func DecodeAccountListCursor(cursor string) (*AccountListCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 5)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid cursor format")
	}

	// Handle legacy cursor format (balance:account_id) for backward compatibility
	if len(parts) == 2 {
		balance, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid balance in cursor: %w", err)
		}
		return &AccountListCursor{
			Balance:   balance,
			AccountID: parts[1],
			SortBy:    "balance", // Legacy cursors were for balance sort
			SortOrder: "desc",
		}, nil
	}

	// New format: balance:last_modified_ledger:sort_by:sort_order:account_id
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid cursor format: expected balance:last_modified_ledger:sort_by:sort_order:account_id")
	}

	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid balance in cursor: %w", err)
	}

	lastModifiedLedger, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid last_modified_ledger in cursor: %w", err)
	}

	return &AccountListCursor{
		Balance:            balance,
		LastModifiedLedger: lastModifiedLedger,
		SortBy:             parts[2],
		SortOrder:          parts[3],
		AccountID:          parts[4],
	}, nil
}

// Encode encodes a token holders cursor to an opaque base64 string
// Format: balance:account_id
func (c TokenHoldersCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s", c.Balance, c.AccountID)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeTokenHoldersCursor decodes a base64 cursor string into a TokenHoldersCursor
// Returns nil if the cursor string is empty
func DecodeTokenHoldersCursor(cursor string) (*TokenHoldersCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected balance:account_id")
	}

	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid balance in cursor: %w", err)
	}

	return &TokenHoldersCursor{
		Balance:   balance,
		AccountID: parts[1],
	}, nil
}

// ============================================
// PHASE 6: STATE TABLE CURSORS
// ============================================

// OfferCursor represents a cursor for paginating offers
// Encodes offer_id for stable pagination
type OfferCursor struct {
	OfferID int64
}

// Encode encodes an offer cursor to an opaque base64 string
func (c OfferCursor) Encode() string {
	raw := fmt.Sprintf("%d", c.OfferID)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeOfferCursor decodes a base64 cursor string into an OfferCursor
// Returns nil if the cursor string is empty
func DecodeOfferCursor(cursor string) (*OfferCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	offerID, err := strconv.ParseInt(string(decoded), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid offer_id in cursor: %w", err)
	}

	return &OfferCursor{
		OfferID: offerID,
	}, nil
}

// LiquidityPoolCursor represents a cursor for paginating liquidity pools
// Encodes pool_id for stable pagination
type LiquidityPoolCursor struct {
	PoolID string
}

// Encode encodes a liquidity pool cursor to an opaque base64 string
func (c LiquidityPoolCursor) Encode() string {
	return base64.URLEncoding.EncodeToString([]byte(c.PoolID))
}

// DecodeLiquidityPoolCursor decodes a base64 cursor string into a LiquidityPoolCursor
// Returns nil if the cursor string is empty
func DecodeLiquidityPoolCursor(cursor string) (*LiquidityPoolCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	return &LiquidityPoolCursor{
		PoolID: string(decoded),
	}, nil
}

// ClaimableBalanceCursor represents a cursor for paginating claimable balances
// Encodes balance_id for stable pagination
type ClaimableBalanceCursor struct {
	BalanceID string
}

// Encode encodes a claimable balance cursor to an opaque base64 string
func (c ClaimableBalanceCursor) Encode() string {
	return base64.URLEncoding.EncodeToString([]byte(c.BalanceID))
}

// DecodeClaimableBalanceCursor decodes a base64 cursor string into a ClaimableBalanceCursor
// Returns nil if the cursor string is empty
func DecodeClaimableBalanceCursor(cursor string) (*ClaimableBalanceCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	return &ClaimableBalanceCursor{
		BalanceID: string(decoded),
	}, nil
}

// ============================================
// PHASE 7: EVENT TABLE CURSORS
// ============================================

// TradeCursor represents a cursor for paginating trades
// Encodes composite key: ledger_sequence, transaction_hash, operation_index, trade_index, order
type TradeCursor struct {
	LedgerSequence  int64
	TransactionHash string
	OperationIndex  int
	TradeIndex      int
	Order           string // "asc" or "desc" - preserves order across pagination
}

// Encode encodes a trade cursor to an opaque base64 string
// Format: ledger:tx_hash:op_index:trade_index:order (5 parts) or legacy (4 parts)
func (c TradeCursor) Encode() string {
	var raw string
	if c.Order == "" {
		raw = fmt.Sprintf("%d:%s:%d:%d", c.LedgerSequence, c.TransactionHash, c.OperationIndex, c.TradeIndex)
	} else {
		raw = fmt.Sprintf("%d:%s:%d:%d:%s", c.LedgerSequence, c.TransactionHash, c.OperationIndex, c.TradeIndex, c.Order)
	}
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeTradeCursor decodes a base64 cursor string into a TradeCursor
// Returns nil if the cursor string is empty
// Supports both legacy format (4 parts) and new format (5 parts with order)
func DecodeTradeCursor(cursor string) (*TradeCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 5)
	if len(parts) < 4 || len(parts) > 5 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:tx_hash:op_index:trade_index or ledger:tx_hash:op_index:trade_index:order")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	opIndex, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid operation_index in cursor: %w", err)
	}

	tradeIndex, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid trade_index in cursor: %w", err)
	}

	// Default order for legacy cursors (trades default to ASC)
	order := "asc"
	if len(parts) == 5 {
		order = parts[4]
	}

	return &TradeCursor{
		LedgerSequence:  ledger,
		TransactionHash: parts[1],
		OperationIndex:  opIndex,
		TradeIndex:      tradeIndex,
		Order:           order,
	}, nil
}

// EffectCursor represents a cursor for paginating effects
// Encodes composite key: ledger_sequence, transaction_hash, operation_index, effect_index, order
type EffectCursor struct {
	LedgerSequence  int64
	TransactionHash string
	OperationIndex  int
	EffectIndex     int
	Order           string // "asc" or "desc" - preserves order across pagination
}

// Encode encodes an effect cursor to an opaque base64 string
// Format: ledger:tx_hash:op_index:effect_index:order (5 parts) or legacy (4 parts)
func (c EffectCursor) Encode() string {
	var raw string
	if c.Order == "" {
		raw = fmt.Sprintf("%d:%s:%d:%d", c.LedgerSequence, c.TransactionHash, c.OperationIndex, c.EffectIndex)
	} else {
		raw = fmt.Sprintf("%d:%s:%d:%d:%s", c.LedgerSequence, c.TransactionHash, c.OperationIndex, c.EffectIndex, c.Order)
	}
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeEffectCursor decodes a base64 cursor string into an EffectCursor
// Returns nil if the cursor string is empty
// Supports both legacy format (4 parts) and new format (5 parts with order)
func DecodeEffectCursor(cursor string) (*EffectCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 5)
	if len(parts) < 4 || len(parts) > 5 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:tx_hash:op_index:effect_index or ledger:tx_hash:op_index:effect_index:order")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	opIndex, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid operation_index in cursor: %w", err)
	}

	effectIndex, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid effect_index in cursor: %w", err)
	}

	// Default order for legacy cursors (effects default to ASC)
	order := "asc"
	if len(parts) == 5 {
		order = parts[4]
	}

	return &EffectCursor{
		LedgerSequence:  ledger,
		TransactionHash: parts[1],
		OperationIndex:  opIndex,
		EffectIndex:     effectIndex,
		Order:           order,
	}, nil
}

// ============================================
// CAP-67 UNIFIED EVENT CURSORS
// ============================================

// UnifiedEventCursor represents a cursor for paginating unified CAP-67 events
// Encodes ledger_sequence, tx_hash, and event_index for stable pagination
type UnifiedEventCursor struct {
	LedgerSequence int64
	TxHash         string
	EventIndex     int
	Order          string // "asc" or "desc"
}

// Encode encodes a unified event cursor to an opaque base64 string
// Format: "ledger:tx_hash:event_index:order"
func (c UnifiedEventCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s:%d:%s", c.LedgerSequence, c.TxHash, c.EventIndex, c.Order)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeUnifiedEventCursor decodes a base64 cursor string into a UnifiedEventCursor
// Returns nil if the cursor string is empty
func DecodeUnifiedEventCursor(cursor string) (*UnifiedEventCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:tx_hash:event_index:order")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	eventIndex, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid event_index in cursor: %w", err)
	}

	return &UnifiedEventCursor{
		LedgerSequence: ledger,
		TxHash:         parts[1],
		EventIndex:     eventIndex,
		Order:          parts[3],
	}, nil
}

// ============================================
// SEP-41 TOKEN CURSORS
// ============================================

// SEP41BalanceCursor represents a cursor for paginating SEP-41 token holder balances
// Encodes balance (for ordering) and address (for tie-breaking)
type SEP41BalanceCursor struct {
	Balance int64
	Address string
}

// Encode encodes a SEP-41 balance cursor to an opaque base64 string
func (c SEP41BalanceCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s", c.Balance, c.Address)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeSEP41BalanceCursor decodes a base64 cursor string into a SEP41BalanceCursor
// Returns nil if the cursor string is empty
func DecodeSEP41BalanceCursor(cursor string) (*SEP41BalanceCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected balance:address")
	}

	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid balance in cursor: %w", err)
	}

	return &SEP41BalanceCursor{
		Balance: balance,
		Address: parts[1],
	}, nil
}

// ============================================
// PHASE 8: SOROBAN TABLE CURSORS
// ============================================

// TTLCursor represents a cursor for paginating TTL entries
// Encodes live_until_ledger and key_hash for stable pagination
type TTLCursor struct {
	LiveUntilLedger int64
	KeyHash         string
}

// Encode encodes a TTL cursor to an opaque base64 string
func (c TTLCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s", c.LiveUntilLedger, c.KeyHash)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeTTLCursor decodes a base64 cursor string into a TTLCursor
// Returns nil if the cursor string is empty
func DecodeTTLCursor(cursor string) (*TTLCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected live_until:key_hash")
	}

	liveUntil, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid live_until_ledger in cursor: %w", err)
	}

	return &TTLCursor{
		LiveUntilLedger: liveUntil,
		KeyHash:         parts[1],
	}, nil
}

// EvictionCursor represents a cursor for paginating eviction/restoration events
// Encodes contract_id, key_hash, and ledger_sequence for stable pagination
type EvictionCursor struct {
	ContractID     string
	KeyHash        string
	LedgerSequence int64
}

// Encode encodes an eviction cursor to an opaque base64 string
func (c EvictionCursor) Encode() string {
	raw := fmt.Sprintf("%s:%s:%d", c.ContractID, c.KeyHash, c.LedgerSequence)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeEvictionCursor decodes a base64 cursor string into an EvictionCursor
// Returns nil if the cursor string is empty
func DecodeEvictionCursor(cursor string) (*EvictionCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid cursor format: expected contract_id:key_hash:ledger_sequence")
	}

	ledger, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger_sequence in cursor: %w", err)
	}

	return &EvictionCursor{
		ContractID:     parts[0],
		KeyHash:        parts[1],
		LedgerSequence: ledger,
	}, nil
}

// ContractDataCursor represents a cursor for paginating contract data entries
// Encodes contract_id and key_hash for stable pagination
type ContractDataCursor struct {
	ContractID string
	KeyHash    string
}

// Encode encodes a contract data cursor to an opaque base64 string
func (c ContractDataCursor) Encode() string {
	raw := fmt.Sprintf("%s:%s", c.ContractID, c.KeyHash)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeContractDataCursor decodes a base64 cursor string into a ContractDataCursor
// Returns nil if the cursor string is empty
func DecodeContractDataCursor(cursor string) (*ContractDataCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected contract_id:key_hash")
	}

	return &ContractDataCursor{
		ContractID: parts[0],
		KeyHash:    parts[1],
	}, nil
}

// ============================================
// TOKEN LIST CURSORS
// ============================================

// Encode encodes a token list cursor to an opaque base64 string
// Format: holder_count:contract_id:sort_by:sort_order
func (c SEP41TokenListCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s:%s:%s", c.HolderCount, c.ContractID, c.SortBy, c.SortOrder)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeSEP41TokenListCursor decodes a base64 cursor string into a SEP41TokenListCursor
// Returns nil if the cursor string is empty
func DecodeSEP41TokenListCursor(cursor string) (*SEP41TokenListCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid cursor format: expected holder_count:contract_id:sort_by:sort_order")
	}

	holderCount, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid holder_count in cursor: %w", err)
	}

	return &SEP41TokenListCursor{
		HolderCount: holderCount,
		ContractID:  parts[1],
		SortBy:      parts[2],
		SortOrder:   parts[3],
	}, nil
}
