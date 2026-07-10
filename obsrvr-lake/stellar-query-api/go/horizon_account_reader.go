package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	hbase "github.com/stellar/go-stellar-sdk/protocols/horizon/base"
)

var errHorizonAccountNotFound = errors.New("horizon account not found")

type HorizonAccountReader struct {
	hot     *SilverHotReader
	unified *UnifiedDuckDBReader
}

func NewHorizonAccountReader(hot *SilverHotReader, unified *UnifiedDuckDBReader) *HorizonAccountReader {
	if hot == nil && unified == nil {
		return nil
	}
	return &HorizonAccountReader{hot: hot, unified: unified}
}

func (r *HorizonAccountReader) GetHorizonAccount(ctx context.Context, accountID string) (*protocol.Account, error) {
	if r == nil {
		return nil, fmt.Errorf("horizon account reader unavailable")
	}

	acc, err := r.currentAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if acc == nil {
		return nil, errHorizonAccountNotFound
	}

	sequence, _ := strconv.ParseInt(acc.SequenceNumber, 10, 64)
	out := &protocol.Account{
		ID:                 acc.AccountID,
		AccountID:          acc.AccountID,
		Sequence:           sequence,
		SubentryCount:      int32(acc.NumSubentries),
		LastModifiedLedger: uint32(acc.LastModifiedLedger),
		Data:               map[string]string{},
		PT:                 acc.AccountID,
	}
	if acc.HomeDomain != nil {
		out.HomeDomain = *acc.HomeDomain
	}
	if ts := parseOptionalHorizonTime(acc.UpdatedAt); ts != nil {
		out.LastModifiedTime = ts
	}

	if r.hot != nil {
		if balances, err := r.hot.GetServingAccountBalances(ctx, accountID); err == nil && balances != nil {
			out.Balances = horizonBalances(balances)
		} else if r.unified == nil {
			return nil, fmt.Errorf("horizon account balances: %w", err)
		}
	}
	if len(out.Balances) == 0 && r.unified != nil {
		balances, err := r.unified.GetAccountBalances(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("horizon account balances: %w", err)
		}
		out.Balances = horizonBalances(balances)
	}
	if r.unified != nil {
		signers, err := r.unified.GetAccountSigners(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("horizon account signers: %w", err)
		}
		applyHorizonSigners(out, signers)
	} else {
		out.Balances = []protocol.Balance{{
			Balance: horizonBalanceAmount(acc.Balance),
			Asset:   hbase.Asset{Type: "native"},
		}}
		out.Signers = []protocol.Signer{{
			Key:    accountID,
			Type:   "ed25519_public_key",
			Weight: 1,
		}}
	}

	return out, nil
}

func (r *HorizonAccountReader) currentAccount(ctx context.Context, accountID string) (*AccountCurrent, error) {
	var servingErr error
	if r.hot != nil {
		acc, err := r.hot.GetServingAccountCurrent(ctx, accountID)
		if err == nil && acc != nil {
			return acc, nil
		}
		servingErr = err
	}
	if r.unified != nil {
		acc, err := r.unified.GetAccountCurrent(ctx, accountID)
		if err != nil {
			return nil, err
		}
		if acc != nil {
			return acc, nil
		}
	}
	if r.hot != nil {
		acc, err := r.hot.GetAccountCurrent(ctx, accountID)
		if err != nil {
			return nil, err
		}
		return acc, nil
	}
	if servingErr != nil && !errors.Is(servingErr, sql.ErrNoRows) {
		return nil, servingErr
	}
	return nil, nil
}

func horizonBalances(resp *AccountBalancesResponse) []protocol.Balance {
	if resp == nil {
		return nil
	}
	out := make([]protocol.Balance, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		bal := protocol.Balance{
			Balance:      horizonBalanceAmount(b.Balance),
			Limit:        derefString(b.Limit),
			IsAuthorized: b.IsAuthorized,
			Asset: hbase.Asset{
				Type:   b.AssetType,
				Code:   b.AssetCode,
				Issuer: derefString(b.AssetIssuer),
			},
		}
		if bal.Asset.Type == "" || bal.Asset.Type == "native" || bal.Asset.Code == "XLM" {
			bal.Asset = hbase.Asset{Type: "native"}
			bal.Limit = ""
		}
		out = append(out, bal)
	}
	return out
}

func applyHorizonSigners(account *protocol.Account, resp *AccountSignersResponse) {
	if resp == nil {
		return
	}
	account.Thresholds.LowThreshold = byte(resp.Thresholds.LowThreshold)
	account.Thresholds.MedThreshold = byte(resp.Thresholds.MedThreshold)
	account.Thresholds.HighThreshold = byte(resp.Thresholds.HighThreshold)
	account.Signers = make([]protocol.Signer, 0, len(resp.Signers))
	for _, signer := range resp.Signers {
		account.Signers = append(account.Signers, protocol.Signer{
			Key:     signer.Key,
			Type:    signer.Type,
			Weight:  int32(signer.Weight),
			Sponsor: signer.Sponsor,
		})
	}
}

func horizonBalanceAmount(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "0.0000000"
	}
	if strings.Contains(raw, ".") {
		return raw
	}
	stroops, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return raw
	}
	return formatStroopsToDecimal(stroops)
}

func parseOptionalHorizonTime(raw string) *time.Time {
	if raw == "" {
		return nil
	}
	ts := parseHorizonTimestamp(raw)
	if ts.IsZero() {
		return nil
	}
	return &ts
}
