package main

import (
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractClaimableBalances extracts claimable balance state from LedgerCloseMeta
// Protocol 14+ feature for deferred payments and multi-party asset distribution
// Obsrvr playbook: core.claimable_balances_snapshot_v1
func (ing *Ingester) extractClaimableBalances(lcm *xdr.LedgerCloseMeta) []ClaimableBalanceData {
	var balances []ClaimableBalanceData

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
		log.Printf("Failed to create change reader for claimable balances: %v", err)
		return balances
	}
	defer reader.Close()

	// Track unique balances (to avoid duplicates within a ledger)
	balanceMap := make(map[string]*ClaimableBalanceData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for claimable balances: %v", err)
			continue
		}

		// We only care about ClaimableBalance changes
		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		// Get the claimable balance entry after the change (post state)
		var balanceEntry *xdr.ClaimableBalanceEntry
		var ledgerEntry *xdr.LedgerEntry
		if change.Post != nil {
			ledgerEntry = change.Post
			if cb, ok := change.Post.Data.GetClaimableBalance(); ok {
				balanceEntry = &cb
			}
		}

		// Skip if no post-state (balance was removed)
		if balanceEntry == nil {
			continue
		}

		// Extract balance ID (32-byte hash as hex string)
		var balanceID string
		switch balanceEntry.BalanceId.Type {
		case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
			hashBytes := balanceEntry.BalanceId.MustV0()
			balanceID = hex.EncodeToString(hashBytes[:])
		default:
			log.Printf("Unknown ClaimableBalanceId type: %v", balanceEntry.BalanceId.Type)
			continue
		}

		// Extract sponsor (account funding the reserve) from LedgerEntry extension
		var sponsor string
		sponsorDesc := ledgerEntry.SponsoringID()
		if sponsorDesc != nil {
			sponsor = sponsorDesc.Address()
		} else {
			// No sponsor - use empty string
			sponsor = ""
		}

		// Parse asset using existing parseAsset function from offers.go
		assetType, assetCode, assetIssuer := parseAsset(balanceEntry.Asset)

		// Extract amount (in stroops)
		amount := int64(balanceEntry.Amount)

		// Count claimants
		claimantsCount := int32(len(balanceEntry.Claimants))

		// Metadata
		now := time.Now().UTC()
		ledgerRange := (ledgerSeq / 10000) * 10000

		// Create balance data
		balance := ClaimableBalanceData{
			// Identity
			BalanceID:      balanceID,
			Sponsor:        sponsor,
			LedgerSequence: ledgerSeq,
			ClosedAt:       closedAt,

			// Asset & Amount
			AssetType:   assetType,
			AssetCode:   assetCode,
			AssetIssuer: assetIssuer,
			Amount:      amount,

			// Claimants
			ClaimantsCount: claimantsCount,

			// Flags (for future use, currently always 0)
			Flags: 0,

			// Metadata
			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		// Deduplicate by balance_id
		balanceMap[balanceID] = &balance
	}

	// Convert map to slice
	for _, balance := range balanceMap {
		balances = append(balances, *balance)
	}

	return balances
}
