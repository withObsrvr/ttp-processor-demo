package main

import (
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractAccountSigners extracts account signers from account ledger entries
// Tracks multi-sig configurations and signer changes
// Obsrvr playbook: core.account_signers_snapshot_v1
func (ing *Ingester) extractAccountSigners(lcm *xdr.LedgerCloseMeta) []AccountSignerData {
	var signersList []AccountSignerData

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
		ing.config.Source.NetworkPassphrase, *lcm)
	if err != nil {
		log.Printf("Failed to create change reader for account signers: %v", err)
		return signersList
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

	return signersList
}
