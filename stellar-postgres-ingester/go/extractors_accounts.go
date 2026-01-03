package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// extractAccounts extracts account snapshots from raw ledger
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeAccount + Map deduplication
// Reference: ducklake-ingestion-obsrvr-v3/go/accounts.go lines 18-90
func (w *Writer) extractAccounts(rawLedger *pb.RawLedger) ([]AccountData, error) {
	var accounts []AccountData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for accounts: %v", err)
		return accounts, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique accounts (to avoid duplicates within a ledger)
	// Map-based deduplication: same account can change multiple times per ledger
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
		accountData := w.extractAccountData(accountEntry, ledgerSeq, closedAt)

		// Store in map (overwrites if account appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		accountCopy := accountData
		accountMap[accountData.AccountID] = &accountCopy
	}

	// Convert map to slice
	for _, account := range accountMap {
		accounts = append(accounts, *account)
	}

	return accounts, nil
}

// extractAccountData extracts all fields from an AccountEntry
func (w *Writer) extractAccountData(entry *xdr.AccountEntry, ledgerSeq uint32, closedAt time.Time) AccountData {
	// Identity
	accountID := entry.AccountId.Address()

	// Balance (convert from stroops to XLM string)
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

			// Note: Account sponsorship is more complex
			// For now, we'll leave this as nil and implement if needed
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

// extractTrustlines extracts trustline snapshots from raw ledger
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeTrustline + Map deduplication
// Reference: ducklake-ingestion-obsrvr-v3/go/accounts.go lines 210-289
func (w *Writer) extractTrustlines(rawLedger *pb.RawLedger) ([]TrustlineData, error) {
	var trustlines []TrustlineData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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
	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for trustlines: %v", err)
		return trustlines, nil // Return empty slice, don't fail the whole batch
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
		trustlineData := w.extractTrustlineData(trustlineEntry, ledgerSeq, closedAt)

		// Create unique key
		key := fmt.Sprintf("%s:%s:%s", trustlineData.AccountID, trustlineData.AssetCode, trustlineData.AssetIssuer)

		// Store in map (overwrites if trustline appears multiple times)
		trustlineCopy := trustlineData
		trustlineMap[key] = &trustlineCopy
	}

	// Convert map to slice
	for _, trustline := range trustlineMap {
		trustlines = append(trustlines, *trustline)
	}

	// Debug logging
	if len(trustlines) > 0 {
		log.Printf("DEBUG: extractTrustlines found %d trustlines for ledger %d", len(trustlines), ledgerSeq)
	}

	return trustlines, nil
}

// extractTrustlineData extracts all fields from a TrustLineEntry
func (w *Writer) extractTrustlineData(entry *xdr.TrustLineEntry, ledgerSeq uint32, closedAt time.Time) TrustlineData {
	// Identity
	accountID := entry.AccountId.Address()

	// Extract asset details
	var assetCode, assetIssuer, assetType string
	switch entry.Asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := entry.Asset.MustAlphaNum4()
		assetCode = strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		assetIssuer = a4.Issuer.Address()
		assetType = "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := entry.Asset.MustAlphaNum12()
		assetCode = strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		assetIssuer = a12.Issuer.Address()
		assetType = "credit_alphanum12"
	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+)
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

// extractAccountSigners extracts account signers from account ledger entries
// Tracks multi-sig configurations and signer changes
// Reference: ducklake-ingestion-obsrvr-v3/go/account_signers.go lines 15-126
func (w *Writer) extractAccountSigners(rawLedger *pb.RawLedger) ([]AccountSignerData, error) {
	var signersList []AccountSignerData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

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

	// Create change reader for ledger entries
	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(
		w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for account signers: %v", err)
		return signersList, nil // Return empty slice, don't fail the whole batch
	}
	defer changeReader.Close()

	// Process all changes
	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for account signers: %v", err)
			continue
		}

		// We only care about account entries (current state)
		var accountEntry *xdr.AccountEntry
		var deleted bool

		switch change.Type {
		case xdr.LedgerEntryTypeAccount:
			if change.Post != nil {
				// Account exists (created or updated)
				account := change.Post.Data.MustAccount()
				accountEntry = &account
				deleted = false
			} else if change.Pre != nil {
				// Account deleted (mark all signers as deleted)
				account := change.Pre.Data.MustAccount()
				accountEntry = &account
				deleted = true
			}
		default:
			continue
		}

		if accountEntry == nil {
			continue
		}

		// Extract account ID
		accountID := accountEntry.AccountId.Address()

		// Get signer sponsoring IDs if available (Protocol 14+)
		var sponsorIDs []xdr.SponsorshipDescriptor
		if accountEntry.Ext.V == 1 {
			v1 := accountEntry.Ext.MustV1()
			if v1.Ext.V == 2 {
				v2 := v1.Ext.MustV2()
				sponsorIDs = v2.SignerSponsoringIDs
			}
		}

		// Process each signer
		for i, signer := range accountEntry.Signers {
			// Get sponsor if available (matches index in signers array)
			var sponsor string
			if i < len(sponsorIDs) && sponsorIDs[i] != nil {
				sponsor = sponsorIDs[i].Address()
			}

			// Extract signer key as string
			signerKey := signer.Key.Address()

			signerData := AccountSignerData{
				// Identity (3 fields)
				AccountID:      accountID,
				Signer:         signerKey,
				LedgerSequence: ledgerSeq,

				// Signer details (2 fields)
				Weight:  uint32(signer.Weight),
				Sponsor: sponsor,

				// Status (1 field)
				Deleted: deleted,

				// Metadata (3 fields)
				ClosedAt:    closedAt,
				LedgerRange: (ledgerSeq / 10000) * 10000,
				CreatedAt:   time.Now().UTC(),
			}

			signersList = append(signersList, signerData)
		}
	}

	return signersList, nil
}

// extractNativeBalances extracts XLM-only balances from raw ledger
// Native (XLM) balance tracking
func (w *Writer) extractNativeBalances(rawLedger *pb.RawLedger) ([]NativeBalanceData, error) {
	var nativeBalancesList []NativeBalanceData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Get ledger sequence
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		ledgerSeq = uint32(lcm.MustV0().LedgerHeader.Header.LedgerSeq)
	case 1:
		ledgerSeq = uint32(lcm.MustV1().LedgerHeader.Header.LedgerSeq)
	case 2:
		ledgerSeq = uint32(lcm.MustV2().LedgerHeader.Header.LedgerSeq)
	}

	// Create change reader
	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		log.Printf("Failed to create change reader for native balances: %v", err)
		return nativeBalancesList, nil
	}
	defer changeReader.Close()

	// Track unique native balances by account_id (deduplicate within ledger)
	nativeBalancesMap := make(map[string]*NativeBalanceData)

	// Process each change
	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for native balances: %v", err)
			continue
		}

		// Only process account entries
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		// Extract account entry
		var accountEntry *xdr.AccountEntry
		var lastModifiedLedger uint32

		if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Post.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Post.LastModifiedLedgerSeq)
		} else if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeAccount {
			entry, _ := change.Pre.Data.GetAccount()
			accountEntry = &entry
			lastModifiedLedger = uint32(change.Pre.LastModifiedLedgerSeq)
		}

		if accountEntry == nil {
			continue
		}

		// Extract native balance data
		accountID := accountEntry.AccountId.Address()

		// Balance and liabilities (Protocol 10+ fields)
		balance := int64(accountEntry.Balance)
		buyingLiabilities := int64(0)
		sellingLiabilities := int64(0)

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			buyingLiabilities = int64(ext.Liabilities.Buying)
			sellingLiabilities = int64(ext.Liabilities.Selling)
		}

		// Account metadata
		numSubentries := int32(accountEntry.NumSubEntries)
		numSponsoring := int32(0)
		numSponsored := int32(0)

		// Extract sponsoring/sponsored counts (Protocol 14+)
		if ext, ok := accountEntry.Ext.GetV1(); ok {
			if ext2, ok := ext.Ext.GetV2(); ok {
				numSponsoring = int32(ext2.NumSponsoring)
				numSponsored = int32(ext2.NumSponsored)
			}
		}

		// Sequence number (nullable)
		seqNum := int64(accountEntry.SeqNum)
		var sequenceNumber *int64
		if seqNum > 0 {
			sequenceNumber = &seqNum
		}

		// Create native balance entry
		ledgerRange := (ledgerSeq / 10000) * 10000

		data := NativeBalanceData{
			// Identity (1 field)
			AccountID: accountID,

			// Balance details (7 fields)
			Balance:            balance,
			BuyingLiabilities:  buyingLiabilities,
			SellingLiabilities: sellingLiabilities,
			NumSubentries:      numSubentries,
			NumSponsoring:      numSponsoring,
			NumSponsored:       numSponsored,
			SequenceNumber:     sequenceNumber,

			// Ledger tracking (2 fields)
			LastModifiedLedger: int64(lastModifiedLedger),
			LedgerSequence:     int64(ledgerSeq),

			// Partition key (1 field)
			LedgerRange: int64(ledgerRange),
		}

		// Deduplicate by account_id
		nativeBalancesMap[accountID] = &data
	}

	// Convert map to slice
	for _, data := range nativeBalancesMap {
		nativeBalancesList = append(nativeBalancesList, *data)
	}

	return nativeBalancesList, nil
}
