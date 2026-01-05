package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OperationCursor represents a cursor for paginating operations
// Encodes ledger_sequence and operation_index for stable pagination
type OperationCursor struct {
	LedgerSequence int64
	OperationIndex int64
}

// TransferCursor represents a cursor for paginating transfers
// Encodes ledger_sequence and timestamp for stable pagination
type TransferCursor struct {
	LedgerSequence int64
	Timestamp      time.Time
}

// AccountCursor represents a cursor for paginating account history
// Encodes ledger_sequence for stable pagination
type AccountCursor struct {
	LedgerSequence int64
}

// AccountListCursor represents a cursor for paginating account lists
// Encodes balance and account_id for stable pagination (ordered by balance DESC)
type AccountListCursor struct {
	Balance   int64  // Balance in stroops for comparison
	AccountID string // Account ID for tie-breaking
}

// Encode encodes an operation cursor to an opaque base64 string
func (c OperationCursor) Encode() string {
	raw := fmt.Sprintf("%d:%d", c.LedgerSequence, c.OperationIndex)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeOperationCursor decodes a base64 cursor string into an OperationCursor
// Returns nil if the cursor string is empty
func DecodeOperationCursor(cursor string) (*OperationCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.Split(string(decoded), ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:op_index")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	opIndex, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid operation index in cursor: %w", err)
	}

	return &OperationCursor{
		LedgerSequence: ledger,
		OperationIndex: opIndex,
	}, nil
}

// Encode encodes a transfer cursor to an opaque base64 string
func (c TransferCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s", c.LedgerSequence, c.Timestamp.Format(time.RFC3339Nano))
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeTransferCursor decodes a base64 cursor string into a TransferCursor
// Returns nil if the cursor string is empty
func DecodeTransferCursor(cursor string) (*TransferCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected ledger:timestamp")
	}

	ledger, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ledger in cursor: %w", err)
	}

	ts, err := time.Parse(time.RFC3339Nano, parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in cursor: %w", err)
	}

	return &TransferCursor{
		LedgerSequence: ledger,
		Timestamp:      ts,
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
func (c AccountListCursor) Encode() string {
	raw := fmt.Sprintf("%d:%s", c.Balance, c.AccountID)
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

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected balance:account_id")
	}

	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid balance in cursor: %w", err)
	}

	return &AccountListCursor{
		Balance:   balance,
		AccountID: parts[1],
	}, nil
}
