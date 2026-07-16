package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/horizon"
	hbase "github.com/stellar/go-stellar-sdk/protocols/horizon/base"
)

var (
	errHorizonAccountNotFound = errors.New("horizon account not found")
	// errHorizonAccountDataUnavailable marks an account whose row exists but whose
	// security-relevant fields (sequence, signers) could not be read faithfully.
	// Handlers map it to 503 rather than serving fabricated defaults.
	errHorizonAccountDataUnavailable = errors.New("horizon account data unavailable")
)

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

	out, err := buildHorizonAccountShell(acc, accountID)
	if err != nil {
		return nil, err
	}
	if r.hot != nil && out.SequenceLedger > 0 {
		if closedAt, err := r.hot.GetServingLedgerClosedAt(ctx, int64(out.SequenceLedger)); err == nil && !closedAt.IsZero() {
			out.SequenceTime = strconv.FormatInt(closedAt.Unix(), 10)
		}
	}

	var balances *AccountBalancesResponse
	if r.hot != nil {
		servingBalances, err := r.hot.GetServingAccountBalances(ctx, accountID)
		if err != nil {
			if r.unified == nil {
				return nil, fmt.Errorf("horizon account balances: %w", err)
			}
			log.Printf("horizon_account path=serving_balances_error account=%s err=%v; falling back to unified", accountID, err)
		} else if servingBalances != nil {
			balances = servingBalances
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
	if len(out.Balances) == 0 {
		// The native balance lives on the account row itself — the same synthesis
		// Horizon performs from the account entry. An account with no trustline
		// rows still has its XLM balance; the account row was read successfully.
		out.Balances = []protocol.Balance{{
			Balance: horizonBalanceAmount(acc.Balance),
			Asset:   hbase.Asset{Type: "native"},
		}}
	}

	signersResolved := false
	if r.hot != nil {
		signers, err := r.hotAccountSigners(ctx, accountID)
		if err != nil {
			if r.unified == nil {
				return nil, fmt.Errorf("horizon account signers: %w", err)
			}
			log.Printf("horizon_account path=hot_signers_error account=%s err=%v; falling back to unified", accountID, err)
		} else if signers != nil {
			applyHorizonSigners(out, signers)
			signersResolved = true
		}
	}
	if !signersResolved && r.unified != nil {
		signers, err := r.unified.GetAccountSigners(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("horizon account signers: %w", err)
		}
		if signers != nil {
			applyHorizonSigners(out, signers)
			signersResolved = true
		}
	}
	if !signersResolved {
		// Never fabricate a default signer: reporting a weight-1 master key for an
		// account whose signers we could not read misstates spendability — a master
		// weight of 0 is exactly how multisig accounts get locked. An empty-but-read
		// signer list (signersResolved) is served as-is; an unreadable one is refused.
		return nil, fmt.Errorf("%w: signers unreadable for account %s", errHorizonAccountDataUnavailable, accountID)
	}

	return out, nil
}

// buildHorizonAccountShell maps the current-account row onto the Horizon account
// resource, minus balances and signers (which come from their own sources).
func buildHorizonAccountShell(acc *AccountCurrent, accountID string) (*protocol.Account, error) {
	// Clients build their next transaction from this sequence; a parse failure
	// silently coerced to 0 would produce unsubmittable transactions.
	sequence, err := strconv.ParseInt(acc.SequenceNumber, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: account %s sequence %q unparseable: %v", errHorizonAccountDataUnavailable, accountID, acc.SequenceNumber, err)
	}
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
	} else if acc.LastModifiedLedger > 0 {
		out.SequenceLedger = uint32(acc.LastModifiedLedger)
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
		if out.SequenceTime == "" {
			out.SequenceTime = strconv.FormatInt(ts.Unix(), 10)
		}
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
		// Bound the unified (hot+cold) lookup so a slow cold scan — including the
		// null-cast retry after a cold schema gap — cannot consume the handler's
		// whole interactive budget and starve the hot-silver fallback below.
		unifiedCtx, cancel := context.WithTimeout(ctx, horizonAccountUnifiedTimeout())
		acc, err := r.unified.GetAccountCurrent(unifiedCtx, accountID)
		cancel()
		if err != nil {
			fallbackable := isUnifiedAccountCurrentSequenceSchemaGap(err) || isQueryTimeout(err)
			if !(servingAcc != nil && fallbackable) {
				return nil, err
			}
			logTierFallback("horizon_account currentAccount", "unified", "hot-silver", err)
			// Keep going: hot silver may have the sequence metadata even when the
			// cold/unified schema is missing the newer columns or the cold scan
			// cannot answer within the interactive deadline.
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
	return isSchemaGapError(err)
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
	return r.hot.GetAccountSigners(ctx, accountID)
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

func horizonAccountUnifiedTimeout() time.Duration {
	return 1500 * time.Millisecond
}
