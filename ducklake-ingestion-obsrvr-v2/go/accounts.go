package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractAccounts extracts account snapshots from LedgerCloseMeta
// Captures complete account state including flags, thresholds, signers, and sponsorship
// Obsrvr playbook: core.accounts_snapshot_v1
func (ing *Ingester) extractAccounts(lcm *xdr.LedgerCloseMeta) []AccountData {
	var accounts []AccountData

	// Get ledger sequence and closed time
	var ledgerSeq uint32
	var closedAt time.Time
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	}

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create change reader for accounts: %v", err)
		return accounts
	}
	defer reader.Close()

	// Track unique accounts (to avoid duplicates within a ledger)
	accountMap := make(map[string]*AccountData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for accounts: %v", err)
			continue
		}

		// We only care about AccountEntry changes
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		// Get the account entry after the change (post state)
		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		// Extract account data
		accountData := extractAccountData(accountEntry, ledgerSeq, closedAt)

		// Store in map (overwrites if account appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		accountCopy := accountData
		accountMap[accountData.AccountID] = &accountCopy
	}

	// Convert map to slice
	for _, account := range accountMap {
		accounts = append(accounts, *account)
	}

	return accounts
}

// extractAccountData extracts all fields from an AccountEntry
func extractAccountData(entry *xdr.AccountEntry, ledgerSeq uint32, closedAt time.Time) AccountData {
	// Identity
	accountID := entry.AccountId.Address()

	// Balance
	balance := amount.String(entry.Balance)

	// Account Settings
	sequenceNumber := uint64(entry.SeqNum)
	numSubentries := uint32(entry.NumSubEntries)
	numSponsoring := uint32(0)
	numSponsored := uint32(0)

	var homeDomain *string
	if entry.HomeDomain != "" {
		hd := string(entry.HomeDomain)
		homeDomain = &hd
	}

	// Thresholds (stored as [4]byte)
	masterWeight := uint32(entry.Thresholds[0])
	lowThreshold := uint32(entry.Thresholds[1])
	medThreshold := uint32(entry.Thresholds[2])
	highThreshold := uint32(entry.Thresholds[3])

	// Flags (bitmask)
	flags := uint32(entry.Flags)
	authRequired := (flags & uint32(xdr.AccountFlagsAuthRequiredFlag)) != 0
	authRevocable := (flags & uint32(xdr.AccountFlagsAuthRevocableFlag)) != 0
	authImmutable := (flags & uint32(xdr.AccountFlagsAuthImmutableFlag)) != 0
	authClawbackEnabled := (flags & uint32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0

	// Signers (extract as JSON array)
	var signersJSON *string
	if len(entry.Signers) > 0 {
		type SignerData struct {
			Key    string `json:"key"`
			Weight uint32 `json:"weight"`
		}
		var signers []SignerData
		for _, signer := range entry.Signers {
			signers = append(signers, SignerData{
				Key:    signer.Key.Address(),
				Weight: uint32(signer.Weight),
			})
		}
		if jsonBytes, err := json.Marshal(signers); err == nil {
			jsonStr := string(jsonBytes)
			signersJSON = &jsonStr
		}
	}

	// Sponsorship (from extensions)
	var sponsorAccount *string

	// Extract liabilities and sponsorship counts (Protocol 10+)
	if ext, ok := entry.Ext.GetV1(); ok {
		// Extract sponsorship counts if available (Protocol 14+)
		if ext2, ok := ext.Ext.GetV2(); ok {
			numSponsoring = uint32(ext2.NumSponsoring)
			numSponsored = uint32(ext2.NumSponsored)

			// Extract sponsor account if available (Protocol 15+)
			if ext2.SignerSponsoringIDs != nil {
				// Note: Account sponsorship is more complex
				// For now, we'll leave this as nil and implement if needed
				// Full sponsorship tracking would require tracking ledger entry sponsors
			}
		}
	}

	// Metadata
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	return AccountData{
		// Identity
		AccountID:      accountID,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,

		// Balance
		Balance: balance,

		// Account Settings
		SequenceNumber: sequenceNumber,
		NumSubentries:  numSubentries,
		NumSponsoring:  numSponsoring,
		NumSponsored:   numSponsored,
		HomeDomain:     homeDomain,

		// Thresholds
		MasterWeight:  masterWeight,
		LowThreshold:  lowThreshold,
		MedThreshold:  medThreshold,
		HighThreshold: highThreshold,

		// Flags
		Flags:               flags,
		AuthRequired:        authRequired,
		AuthRevocable:       authRevocable,
		AuthImmutable:       authImmutable,
		AuthClawbackEnabled: authClawbackEnabled,

		// Signers
		Signers: signersJSON,

		// Sponsorship
		SponsorAccount: sponsorAccount,

		// Metadata
		CreatedAt:   now,
		UpdatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// extractTrustlines extracts trustline snapshots from LedgerCloseMeta
// Captures non-native asset holdings and trust relationships
// Obsrvr playbook: core.trustlines_snapshot_v1
func (ing *Ingester) extractTrustlines(lcm *xdr.LedgerCloseMeta) []TrustlineData {
	var trustlines []TrustlineData

	// Get ledger sequence
	var ledgerSeq uint32
	var closedAt time.Time
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV0().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV1().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
		closedAt = time.Unix(int64(lcm.MustV2().LedgerHeader.Header.ScpValue.CloseTime), 0).UTC()
	}

	// Use ingest package to read ledger changes
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create change reader for trustlines: %v", err)
		return trustlines
	}
	defer reader.Close()

	// Track unique trustlines (to avoid duplicates within a ledger)
	// Key: accountID:assetCode:assetIssuer
	trustlineMap := make(map[string]*TrustlineData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for trustlines: %v", err)
			continue
		}

		// We only care about TrustLine changes
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		// Get the trustline entry after the change (post state)
		var trustlineEntry *xdr.TrustLineEntry
		if change.Post != nil {
			if tl, ok := change.Post.Data.GetTrustLine(); ok {
				trustlineEntry = &tl
			}
		}

		if trustlineEntry == nil {
			continue
		}

		// Extract trustline data
		trustlineData := extractTrustlineData(trustlineEntry, ledgerSeq, closedAt)

		// Create unique key
		key := fmt.Sprintf("%s:%s:%s", trustlineData.AccountID, trustlineData.AssetCode, trustlineData.AssetIssuer)

		// Store in map (overwrites if trustline appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		trustlineCopy := trustlineData
		trustlineMap[key] = &trustlineCopy
	}

	// Convert map to slice
	for _, trustline := range trustlineMap {
		trustlines = append(trustlines, *trustline)
	}

	return trustlines
}

// extractTrustlineData extracts all fields from a TrustLineEntry
func extractTrustlineData(entry *xdr.TrustLineEntry, ledgerSeq uint32, closedAt time.Time) TrustlineData {
	// Identity
	accountID := entry.AccountId.Address()

	// Extract asset details
	var assetCode, assetIssuer, assetType string
	switch entry.Asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := entry.Asset.MustAlphaNum4()
		assetCode = trimNullBytes(string(a4.AssetCode[:]))
		assetIssuer = a4.Issuer.Address()
		assetType = "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := entry.Asset.MustAlphaNum12()
		assetCode = trimNullBytes(string(a12.AssetCode[:]))
		assetIssuer = a12.Issuer.Address()
		assetType = "credit_alphanum12"
	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+) - deferred to future cycle
		assetCode = "POOL_SHARE"
		assetIssuer = "pool"
		assetType = "liquidity_pool_shares"
	}

	// Trust & Balance
	balance := amount.String(entry.Balance)
	trustLimit := amount.String(entry.Limit)
	buyingLiabilities := "0"
	sellingLiabilities := "0"

	// Extract liabilities (Protocol 10+)
	if ext, ok := entry.Ext.GetV1(); ok {
		buyingLiabilities = amount.String(ext.Liabilities.Buying)
		sellingLiabilities = amount.String(ext.Liabilities.Selling)
	}

	// Authorization flags
	flags := uint32(entry.Flags)
	authorized := (flags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
	authorizedToMaintainLiabilities := (flags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
	clawbackEnabled := (flags & uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)) != 0

	// Metadata
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	return TrustlineData{
		// Identity
		AccountID:   accountID,
		AssetCode:   assetCode,
		AssetIssuer: assetIssuer,
		AssetType:   assetType,

		// Trust & Balance
		Balance:            balance,
		TrustLimit:         trustLimit,
		BuyingLiabilities:  buyingLiabilities,
		SellingLiabilities: sellingLiabilities,

		// Authorization
		Authorized:                      authorized,
		AuthorizedToMaintainLiabilities: authorizedToMaintainLiabilities,
		ClawbackEnabled:                 clawbackEnabled,

		// Metadata
		LedgerSequence: ledgerSeq,
		CreatedAt:      now,
		LedgerRange:    ledgerRange,
	}
}
