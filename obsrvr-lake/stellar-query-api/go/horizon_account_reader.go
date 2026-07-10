package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
		NumSponsoring:      uint32(acc.NumSponsoring),
		NumSponsored:       uint32(acc.NumSponsored),
		PT:                 acc.AccountID,
	}
	if acc.SequenceLedger > 0 {
		out.SequenceLedger = uint32(acc.SequenceLedger)
	}
	if acc.SequenceTime > 0 {
		out.SequenceTime = strconv.FormatInt(acc.SequenceTime, 10)
	}
	if acc.HomeDomain != nil {
		out.HomeDomain = *acc.HomeDomain
	}
	if acc.Sponsor != nil {
		out.Sponsor = *acc.Sponsor
	}
	if acc.AuthRequired != nil {
		out.Flags.AuthRequired = *acc.AuthRequired
	}
	if acc.AuthRevocable != nil {
		out.Flags.AuthRevocable = *acc.AuthRevocable
	}
	if acc.AuthImmutable != nil {
		out.Flags.AuthImmutable = *acc.AuthImmutable
	}
	if acc.AuthClawbackEnabled != nil {
		out.Flags.AuthClawbackEnabled = *acc.AuthClawbackEnabled
	}
	if ts := parseOptionalHorizonTime(acc.UpdatedAt); ts != nil {
		out.LastModifiedTime = ts
	}

	var balances *AccountBalancesResponse
	if r.hot != nil {
		if servingBalances, err := r.hot.GetServingAccountBalances(ctx, accountID); err == nil && servingBalances != nil {
			balances = servingBalances
		} else if r.unified == nil {
			return nil, fmt.Errorf("horizon account balances: %w", err)
		}
	}
	if balances == nil && r.unified != nil {
		unifiedBalances, err := r.unified.GetAccountBalances(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("horizon account balances: %w", err)
		}
		balances = unifiedBalances
	}
	out.Balances = horizonBalances(balances)
	if r.hot != nil {
		if signers, err := r.hotAccountSigners(ctx, accountID); err == nil && signers != nil {
			applyHorizonSigners(out, signers)
		} else if r.unified == nil {
			return nil, fmt.Errorf("horizon account signers: %w", err)
		}
	}
	if len(out.Signers) == 0 && r.unified != nil {
		signers, err := r.unified.GetAccountSigners(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("horizon account signers: %w", err)
		}
		applyHorizonSigners(out, signers)
	}
	if len(out.Balances) == 0 {
		out.Balances = []protocol.Balance{{
			Balance: horizonBalanceAmount(acc.Balance),
			Asset:   hbase.Asset{Type: "native"},
		}}
	}
	if len(out.Signers) == 0 {
		out.Signers = []protocol.Signer{{
			Key:    accountID,
			Type:   "ed25519_public_key",
			Weight: 1,
		}}
	}

	return out, nil
}

func (r *HorizonAccountReader) currentAccount(ctx context.Context, accountID string) (*AccountCurrent, error) {
	var servingAcc *AccountCurrent
	var servingErr error
	if r.hot != nil {
		acc, err := r.hot.GetServingAccountCurrent(ctx, accountID)
		if err == nil && acc != nil {
			if !horizonAccountCurrentNeedsUnifiedFill(acc) {
				return acc, nil
			}
			servingAcc = acc
		}
		servingErr = err
	}
	if r.unified != nil {
		acc, err := r.unified.GetAccountCurrent(ctx, accountID)
		if err != nil {
			if !(servingAcc != nil && isUnifiedAccountCurrentSequenceSchemaGap(err)) {
				return nil, err
			}
			// Keep going: hot silver may have the sequence metadata even when the
			// cold/unified schema is still missing those newer columns.
		} else {
			if merged := mergeAccountCurrent(servingAcc, acc); merged != nil {
				return merged, nil
			}
		}
	}
	if r.hot != nil {
		acc, err := r.hot.GetAccountCurrent(ctx, accountID)
		if err != nil {
			return nil, err
		}
		if merged := mergeAccountCurrent(servingAcc, acc); merged != nil {
			return merged, nil
		}
		return acc, nil
	}
	if servingAcc != nil {
		return servingAcc, nil
	}
	if servingErr != nil && !errors.Is(servingErr, sql.ErrNoRows) {
		return nil, servingErr
	}
	return nil, nil
}

func isUnifiedAccountCurrentSequenceSchemaGap(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "sequence_ledger") || strings.Contains(msg, "sequence_time")
}

func horizonAccountCurrentNeedsUnifiedFill(acc *AccountCurrent) bool {
	if acc == nil {
		return true
	}
	return acc.SequenceNumber != "" && (acc.SequenceLedger == 0 || acc.SequenceTime == 0)
}

func mergeAccountCurrent(primary, fallback *AccountCurrent) *AccountCurrent {
	if primary == nil {
		return fallback
	}
	if fallback == nil {
		return primary
	}

	merged := *primary
	if fallback.LastModifiedLedger > primary.LastModifiedLedger {
		merged = *fallback
		if merged.CreatedAt == nil {
			merged.CreatedAt = primary.CreatedAt
		}
	}

	if merged.SequenceNumber == fallback.SequenceNumber {
		if merged.SequenceLedger == 0 {
			merged.SequenceLedger = fallback.SequenceLedger
		}
		if merged.SequenceTime == 0 {
			merged.SequenceTime = fallback.SequenceTime
		}
	}
	if merged.HomeDomain == nil {
		merged.HomeDomain = fallback.HomeDomain
	}
	if merged.Sponsor == nil {
		merged.Sponsor = fallback.Sponsor
	}
	if merged.AuthRequired == nil {
		merged.AuthRequired = fallback.AuthRequired
	}
	if merged.AuthRevocable == nil {
		merged.AuthRevocable = fallback.AuthRevocable
	}
	if merged.AuthImmutable == nil {
		merged.AuthImmutable = fallback.AuthImmutable
	}
	if merged.AuthClawbackEnabled == nil {
		merged.AuthClawbackEnabled = fallback.AuthClawbackEnabled
	}
	return &merged
}

func (r *HorizonAccountReader) hotAccountSigners(ctx context.Context, accountID string) (*AccountSignersResponse, error) {
	if r.hot == nil || r.hot.db == nil {
		return nil, nil
	}
	var accID string
	var signersJSON string
	var masterWeight, lowThreshold, medThreshold, highThreshold int
	err := r.hot.db.QueryRowContext(ctx, `
		SELECT
			account_id,
			COALESCE(signers, '[]') as signers,
			COALESCE(master_weight, 1) as master_weight,
			COALESCE(low_threshold, 0) as low_threshold,
			COALESCE(med_threshold, 0) as med_threshold,
			COALESCE(high_threshold, 0) as high_threshold
		FROM accounts_current
		WHERE account_id = $1
	`, accountID).Scan(&accID, &signersJSON, &masterWeight, &lowThreshold, &medThreshold, &highThreshold)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var signers []AccountSigner
	if signersJSON != "" && signersJSON != "[]" {
		_ = json.Unmarshal([]byte(signersJSON), &signers)
	}
	if masterWeight > 0 {
		hasMaster := false
		for _, signer := range signers {
			if signer.Key == accountID {
				hasMaster = true
				break
			}
		}
		if !hasMaster {
			signers = append([]AccountSigner{{
				Key:    accountID,
				Weight: masterWeight,
				Type:   "ed25519_public_key",
			}}, signers...)
		}
	}

	resp := &AccountSignersResponse{AccountID: accID, Signers: signers}
	resp.Thresholds.LowThreshold = lowThreshold
	resp.Thresholds.MedThreshold = medThreshold
	resp.Thresholds.HighThreshold = highThreshold
	return resp, nil
}

func horizonBalances(resp *AccountBalancesResponse) []protocol.Balance {
	if resp == nil {
		return nil
	}
	out := make([]protocol.Balance, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		bal := protocol.Balance{
			Balance:                           horizonBalanceAmount(b.Balance),
			Limit:                             derefString(b.Limit),
			BuyingLiabilities:                 derefString(b.BuyingLiabilities),
			SellingLiabilities:                derefString(b.SellingLiabilities),
			IsAuthorized:                      b.IsAuthorized,
			IsAuthorizedToMaintainLiabilities: b.IsAuthorizedToMaintainLiabilities,
			IsClawbackEnabled:                 b.IsClawbackEnabled,
			Sponsor:                           derefString(b.Sponsor),
			Asset: hbase.Asset{
				Type:   b.AssetType,
				Code:   b.AssetCode,
				Issuer: derefString(b.AssetIssuer),
			},
		}
		if b.LastModifiedLedger != nil && *b.LastModifiedLedger > 0 {
			bal.LastModifiedLedger = uint32(*b.LastModifiedLedger)
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
