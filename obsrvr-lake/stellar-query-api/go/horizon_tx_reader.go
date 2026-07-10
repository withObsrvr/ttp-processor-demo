package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	"github.com/stellar/go-stellar-sdk/xdr"
)

var (
	errHorizonTransactionReaderUnavailable = errors.New("horizon transaction reader unavailable")
	errHorizonTransactionNotFound          = errors.New("horizon transaction not found")
	errHorizonTransactionXDRUnavailable    = errors.New("horizon transaction xdr unavailable")
)

type HorizonTransactionReader struct {
	hot        *sql.DB
	cold       *sql.DB
	serving    *sql.DB
	coldTable  string
	index      transactionLocationLookup
	hotEnabled bool
}

type transactionLocationLookup interface {
	LookupTransactionHash(context.Context, string) (*TxLocation, error)
}

func NewHorizonTransactionReader(hot *HotReader, cold *ColdReader, index transactionLocationLookup, serving *SilverHotReader) *HorizonTransactionReader {
	reader := &HorizonTransactionReader{index: index}
	if hot != nil {
		reader.hot = hot.DB()
		reader.hotEnabled = reader.hot != nil
	}
	if serving != nil {
		reader.serving = serving.DB()
	}
	if cold != nil {
		reader.cold = cold.DB()
		reader.coldTable = fmt.Sprintf("%s.%s.transactions_row_v2", cold.CatalogName(), cold.SchemaName())
	}
	return reader
}

func (r *HorizonTransactionReader) Available() bool {
	return r != nil && (r.serving != nil || r.hot != nil || r.cold != nil)
}

func (r *HorizonTransactionReader) GetTransactionByHash(ctx context.Context, hash string) (*protocol.Transaction, error) {
	return r.getTransactionByHash(ctx, hash, 0)
}

func (r *HorizonTransactionReader) GetTransactionByHashAtLedger(ctx context.Context, hash string, ledgerSeq int64) (*protocol.Transaction, error) {
	return r.getTransactionByHash(ctx, hash, ledgerSeq)
}

func (r *HorizonTransactionReader) GetTransactionByIDAtLedger(ctx context.Context, transactionID, ledgerSeq int64) (*protocol.Transaction, error) {
	if !r.Available() {
		return nil, errHorizonTransactionReaderUnavailable
	}
	if transactionID <= 0 {
		return nil, errHorizonTransactionNotFound
	}
	var servingResourceErr error
	if r.serving != nil {
		query := horizonServingTransactionIDQuery
		args := []interface{}{transactionID}
		if ledgerSeq > 0 {
			query = horizonServingTransactionIDQueryWithLedger
			args = []interface{}{ledgerSeq, transactionID}
		}
		tx, err := r.query(ctx, r.serving, query, args...)
		if err == nil {
			return tx, nil
		}
		if errors.Is(err, errHorizonTransactionXDRUnavailable) {
			servingResourceErr = err
		}
		if err != sql.ErrNoRows && !errors.Is(err, errHorizonTransactionXDRUnavailable) && !isMissingServingTransactionResource(err) {
			return nil, err
		}
	}
	if r.hot != nil {
		query := horizonHotTransactionIDQuery
		args := []interface{}{transactionID}
		if ledgerSeq > 0 {
			query = horizonHotTransactionIDQueryWithLedger
			args = []interface{}{ledgerSeq, transactionID}
		}
		tx, err := r.query(ctx, r.hot, query, args...)
		if err == nil {
			return tx, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	if r.cold != nil {
		query := fmt.Sprintf(horizonColdTransactionIDQuery, r.coldTable)
		args := []interface{}{transactionID}
		if ledgerSeq > 0 {
			query = fmt.Sprintf(horizonColdTransactionIDQueryWithLedger, r.coldTable)
			args = []interface{}{ledgerSeq, transactionID}
		}
		tx, err := r.query(ctx, r.cold, query, args...)
		if err == nil {
			return tx, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	if servingResourceErr != nil {
		return nil, servingResourceErr
	}
	return nil, errHorizonTransactionNotFound
}

func (r *HorizonTransactionReader) getTransactionByHash(ctx context.Context, hash string, ledgerSeq int64) (*protocol.Transaction, error) {
	if !r.Available() {
		return nil, errHorizonTransactionReaderUnavailable
	}

	var servingResourceErr error
	if r.serving != nil {
		query := horizonServingTransactionHashQuery
		args := []interface{}{hash}
		if ledgerSeq > 0 {
			query = horizonServingTransactionHashQueryWithLedger
			args = []interface{}{ledgerSeq, hash}
		}
		tx, err := r.query(ctx, r.serving, query, args...)
		if err == nil {
			return tx, nil
		}
		if errors.Is(err, errHorizonTransactionXDRUnavailable) {
			servingResourceErr = err
		}
		if err != sql.ErrNoRows && !errors.Is(err, errHorizonTransactionXDRUnavailable) && !isMissingServingTransactionResource(err) {
			return nil, err
		}
	}

	if r.hot != nil {
		query := horizonHotTransactionQuery
		args := []interface{}{hash}
		if ledgerSeq > 0 {
			query = horizonHotTransactionQueryWithLedger
			args = []interface{}{ledgerSeq, hash}
		}
		tx, err := r.query(ctx, r.hot, query, args...)
		if err == nil {
			return tx, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}

	if r.cold != nil {
		query := fmt.Sprintf(horizonColdTransactionQuery, r.coldTable)
		args := []interface{}{hash}
		if ledgerSeq <= 0 {
			ledgerSeq = r.lookupLedgerHint(ctx, hash)
		}
		if ledgerSeq > 0 {
			query = fmt.Sprintf(horizonColdTransactionQueryWithLedger, r.coldTable)
			args = []interface{}{ledgerSeq, hash}
		}
		tx, err := r.query(ctx, r.cold, query, args...)
		if err == nil {
			return tx, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}

	if servingResourceErr != nil {
		return nil, servingResourceErr
	}
	return nil, errHorizonTransactionNotFound
}

func (r *HorizonTransactionReader) lookupLedgerHint(ctx context.Context, hash string) int64 {
	if r.index == nil {
		return 0
	}
	indexCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	loc, err := r.index.LookupTransactionHash(indexCtx, hash)
	if err != nil {
		log.Printf("horizon_tx path=index_lookup_error tx=%s err=%v", hash, err)
		return 0
	}
	if loc == nil {
		log.Printf("horizon_tx path=index_lookup_miss tx=%s", hash)
		return 0
	}
	return loc.LedgerSequence
}

func isMissingServingTransactionResource(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "sv_transactions_recent") ||
		strings.Contains(msg, "transaction_id") ||
		strings.Contains(msg, "tx_envelope") ||
		strings.Contains(msg, "tx_result") ||
		strings.Contains(msg, "tx_meta") ||
		strings.Contains(msg, "tx_fee_meta") ||
		strings.Contains(msg, "tx_signers")
}

const horizonTransactionSelect = `
	SELECT ledger_sequence, transaction_hash, transaction_id, source_account,
	       source_account_muxed, account_sequence, fee_charged, max_fee,
	       successful, operation_count, memo_type, memo, created_at,
	       tx_envelope, tx_result, tx_meta, tx_fee_meta, tx_signers,
	       fee_account_muxed, inner_transaction_hash
`

const horizonServingTransactionSelect = `
	SELECT ledger_sequence, tx_hash AS transaction_hash, transaction_id, source_account,
	       NULL::text AS source_account_muxed, account_sequence,
	       fee_charged_stroops AS fee_charged, max_fee_stroops AS max_fee,
	       successful, operation_count, memo_type, memo_value AS memo, created_at,
	       tx_envelope, tx_result, tx_meta, tx_fee_meta, tx_signers,
	       NULL::text AS fee_account_muxed, NULL::text AS inner_transaction_hash
`

const horizonServingTransactionHashQuery = horizonServingTransactionSelect + `
	FROM serving.sv_transactions_recent
	WHERE tx_hash = $1
	LIMIT 1
`

const horizonServingTransactionHashQueryWithLedger = horizonServingTransactionSelect + `
	FROM serving.sv_transactions_recent
	WHERE ledger_sequence = $1 AND tx_hash = $2
	LIMIT 1
`

const horizonServingTransactionIDQuery = horizonServingTransactionSelect + `
	FROM serving.sv_transactions_recent
	WHERE transaction_id = $1
	LIMIT 1
`

const horizonServingTransactionIDQueryWithLedger = horizonServingTransactionSelect + `
	FROM serving.sv_transactions_recent
	WHERE ledger_sequence = $1 AND transaction_id = $2
	LIMIT 1
`

const horizonHotTransactionQuery = horizonTransactionSelect + `
	FROM transactions_row_v2
	WHERE transaction_hash = $1
	LIMIT 1
`

const horizonHotTransactionQueryWithLedger = horizonTransactionSelect + `
	FROM transactions_row_v2
	WHERE ledger_sequence = $1 AND transaction_hash = $2
	LIMIT 1
`

const horizonHotTransactionIDQuery = horizonTransactionSelect + `
	FROM transactions_row_v2
	WHERE transaction_id = $1
	LIMIT 1
`

const horizonHotTransactionIDQueryWithLedger = horizonTransactionSelect + `
	FROM transactions_row_v2
	WHERE ledger_sequence = $1 AND transaction_id = $2
	LIMIT 1
`

const horizonColdTransactionQuery = horizonTransactionSelect + `
	FROM %s
	WHERE transaction_hash = ?
	LIMIT 1
`

const horizonColdTransactionQueryWithLedger = horizonTransactionSelect + `
	FROM %s
	WHERE ledger_sequence = ? AND transaction_hash = ?
	LIMIT 1
`

const horizonColdTransactionIDQuery = horizonTransactionSelect + `
	FROM %s
	WHERE transaction_id = ?
	LIMIT 1
`

const horizonColdTransactionIDQueryWithLedger = horizonTransactionSelect + `
	FROM %s
	WHERE ledger_sequence = ? AND transaction_id = ?
	LIMIT 1
`

type horizonTransactionScanner interface {
	Scan(dest ...interface{}) error
}

type horizonTransactionRow struct {
	LedgerSequence       int64
	TransactionHash      string
	TransactionID        sql.NullInt64
	SourceAccount        sql.NullString
	SourceAccountMuxed   sql.NullString
	AccountSequence      sql.NullInt64
	FeeCharged           sql.NullInt64
	MaxFee               sql.NullInt64
	Successful           sql.NullBool
	OperationCount       sql.NullInt64
	MemoType             sql.NullString
	Memo                 sql.NullString
	CreatedAt            sql.NullTime
	EnvelopeXDR          sql.NullString
	ResultXDR            sql.NullString
	ResultMetaXDR        sql.NullString
	FeeMetaXDR           sql.NullString
	RawSignatures        sql.NullString
	FeeAccountMuxed      sql.NullString
	InnerTransactionHash sql.NullString
}

func (r *HorizonTransactionReader) query(ctx context.Context, db *sql.DB, query string, args ...interface{}) (*protocol.Transaction, error) {
	row, err := scanHorizonTransactionRow(db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return nil, err
	}
	return row.toProtocol()
}

func scanHorizonTransactionRow(scanner horizonTransactionScanner) (*horizonTransactionRow, error) {
	var row horizonTransactionRow
	err := scanner.Scan(
		&row.LedgerSequence,
		&row.TransactionHash,
		&row.TransactionID,
		&row.SourceAccount,
		&row.SourceAccountMuxed,
		&row.AccountSequence,
		&row.FeeCharged,
		&row.MaxFee,
		&row.Successful,
		&row.OperationCount,
		&row.MemoType,
		&row.Memo,
		&row.CreatedAt,
		&row.EnvelopeXDR,
		&row.ResultXDR,
		&row.ResultMetaXDR,
		&row.FeeMetaXDR,
		&row.RawSignatures,
		&row.FeeAccountMuxed,
		&row.InnerTransactionHash,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (row *horizonTransactionRow) toProtocol() (*protocol.Transaction, error) {
	if missing := row.missingMandatoryFields(); len(missing) > 0 {
		return nil, fmt.Errorf("%w: missing %s", errHorizonTransactionXDRUnavailable, strings.Join(missing, ", "))
	}

	signatures := parseHorizonSignatures(row.RawSignatures)
	tx := &protocol.Transaction{
		ID:              row.TransactionHash,
		PT:              strconv.FormatInt(row.TransactionID.Int64, 10),
		Successful:      row.Successful.Bool,
		Hash:            row.TransactionHash,
		Ledger:          int32(row.LedgerSequence),
		LedgerCloseTime: row.CreatedAt.Time.UTC(),
		Account:         row.SourceAccount.String,
		AccountSequence: row.AccountSequence.Int64,
		FeeAccount:      row.SourceAccount.String,
		FeeCharged:      row.FeeCharged.Int64,
		MaxFee:          row.MaxFee.Int64,
		OperationCount:  int32(row.OperationCount.Int64),
		EnvelopeXdr:     row.EnvelopeXDR.String,
		ResultXdr:       row.ResultXDR.String,
		ResultMetaXdr:   row.ResultMetaXDR.String,
		FeeMetaXdr:      row.FeeMetaXDR.String,
		MemoType:        normalizeHorizonMemoType(row.MemoType.String),
		Signatures:      signatures,
	}
	if row.SourceAccountMuxed.Valid {
		tx.AccountMuxed = row.SourceAccountMuxed.String
		tx.FeeAccountMuxed = row.SourceAccountMuxed.String
	}
	if row.Memo.Valid {
		tx.Memo = row.Memo.String
	}
	tx.Preconditions = horizonTransactionPreconditions(row.EnvelopeXDR.String)
	return tx, nil
}

func horizonTransactionPreconditions(envelopeXDR string) *protocol.TransactionPreconditions {
	var envelope xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(envelopeXDR, &envelope); err != nil {
		return nil
	}

	cond := envelope.Preconditions()
	out := &protocol.TransactionPreconditions{}
	hasFields := false
	switch cond.Type {
	case xdr.PreconditionTypePrecondTime:
		if cond.TimeBounds != nil {
			out.TimeBounds = horizonTransactionTimeBounds(cond.TimeBounds)
			hasFields = true
		}
	case xdr.PreconditionTypePrecondV2:
		if cond.V2 == nil {
			break
		}
		if cond.V2.TimeBounds != nil {
			out.TimeBounds = horizonTransactionTimeBounds(cond.V2.TimeBounds)
			hasFields = true
		}
		if cond.V2.LedgerBounds != nil {
			out.LedgerBounds = &protocol.TransactionPreconditionsLedgerbounds{
				MinLedger: uint32(cond.V2.LedgerBounds.MinLedger),
				MaxLedger: uint32(cond.V2.LedgerBounds.MaxLedger),
			}
			hasFields = true
		}
		if cond.V2.MinSeqNum != nil {
			out.MinAccountSequence = strconv.FormatInt(int64(*cond.V2.MinSeqNum), 10)
			hasFields = true
		}
		if cond.V2.MinSeqAge > 0 {
			out.MinAccountSequenceAge = strconv.FormatUint(uint64(cond.V2.MinSeqAge), 10)
			hasFields = true
		}
		if cond.V2.MinSeqLedgerGap > 0 {
			out.MinAccountSequenceLedgerGap = uint32(cond.V2.MinSeqLedgerGap)
			hasFields = true
		}
		for _, signer := range cond.V2.ExtraSigners {
			if address, err := signer.GetAddress(); err == nil && address != "" {
				out.ExtraSigners = append(out.ExtraSigners, address)
			}
		}
		if len(out.ExtraSigners) > 0 {
			hasFields = true
		}
	}
	if !hasFields {
		return nil
	}
	return out
}

func horizonTransactionTimeBounds(bounds *xdr.TimeBounds) *protocol.TransactionPreconditionsTimebounds {
	return &protocol.TransactionPreconditionsTimebounds{
		MinTime: strconv.FormatUint(uint64(bounds.MinTime), 10),
		MaxTime: strconv.FormatUint(uint64(bounds.MaxTime), 10),
	}
}

func (row *horizonTransactionRow) missingMandatoryFields() []string {
	var missing []string
	if !row.TransactionID.Valid {
		missing = append(missing, "transaction_id")
	}
	if !row.SourceAccount.Valid || strings.TrimSpace(row.SourceAccount.String) == "" {
		missing = append(missing, "source_account")
	}
	if !row.AccountSequence.Valid {
		missing = append(missing, "account_sequence")
	}
	if !row.CreatedAt.Valid || row.CreatedAt.Time.IsZero() {
		missing = append(missing, "created_at")
	}
	if strings.TrimSpace(row.EnvelopeXDR.String) == "" {
		missing = append(missing, "tx_envelope")
	}
	if strings.TrimSpace(row.ResultXDR.String) == "" {
		missing = append(missing, "tx_result")
	}
	if strings.TrimSpace(row.FeeMetaXDR.String) == "" {
		missing = append(missing, "tx_fee_meta")
	}
	if len(parseHorizonSignatures(row.RawSignatures)) == 0 {
		missing = append(missing, "tx_signers")
	}
	return missing
}

func parseHorizonSignatures(raw sql.NullString) []string {
	if !raw.Valid {
		return nil
	}
	value := strings.TrimSpace(raw.String)
	if value == "" || value == "[]" || value == "{}" {
		return nil
	}

	var jsonValues []string
	if strings.HasPrefix(value, "[") && json.Unmarshal([]byte(value), &jsonValues) == nil {
		return compactStrings(jsonValues)
	}

	value = strings.TrimPrefix(value, "{")
	value = strings.TrimSuffix(value, "}")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"`)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func normalizeHorizonMemoType(memoType string) string {
	if memoType == "" {
		return "none"
	}
	if memoType == "text_base64" {
		return "text"
	}
	return memoType
}
