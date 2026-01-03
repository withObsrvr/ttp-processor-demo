package main

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// extractOffers extracts offer snapshots from raw ledger
// Pattern: LedgerChangeReader + Filter by LedgerEntryTypeOffer + Map deduplication
// Reference: ducklake-ingestion-obsrvr-v3/go/offers.go lines 18-90
func (w *Writer) extractOffers(rawLedger *pb.RawLedger) ([]OfferData, error) {
	var offers []OfferData

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
		log.Printf("Failed to create change reader for offers: %v", err)
		return offers, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique offers (to avoid duplicates within a ledger)
	// Map-based deduplication: same offer can change multiple times per ledger
	offerMap := make(map[int64]*OfferData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for offers: %v", err)
			continue
		}

		// We only care about Offer changes
		if change.Type != xdr.LedgerEntryTypeOffer {
			continue
		}

		// Get the offer entry after the change (post state)
		var offerEntry *xdr.OfferEntry
		if change.Post != nil {
			if oe, ok := change.Post.Data.GetOffer(); ok {
				offerEntry = &oe
			}
		}

		if offerEntry == nil {
			continue
		}

		// Extract offer data
		offerData := w.extractOfferData(offerEntry, ledgerSeq, closedAt)

		// Store in map (overwrites if offer appears multiple times)
		// Create a copy to avoid pointer reuse bug (loop variable is reused)
		offerCopy := offerData
		offerMap[offerData.OfferID] = &offerCopy
	}

	// Convert map to slice
	for _, offer := range offerMap {
		offers = append(offers, *offer)
	}

	return offers, nil
}

// extractOfferData extracts all fields from an OfferEntry
func (w *Writer) extractOfferData(entry *xdr.OfferEntry, ledgerSeq uint32, closedAt time.Time) OfferData {
	// Identity
	offerID := int64(entry.OfferId)
	sellerAccount := entry.SellerId.Address()

	// Parse selling asset
	sellingType, sellingCode, sellingIssuer := parseAsset(entry.Selling)

	// Parse buying asset
	buyingType, buyingCode, buyingIssuer := parseAsset(entry.Buying)

	// Amount (in stroops)
	amount := strconv.FormatInt(int64(entry.Amount), 10)

	// Price as N/D ratio string (preserves precision)
	priceStr := fmt.Sprintf("%d/%d", entry.Price.N, entry.Price.D)

	// Flags
	flags := uint32(entry.Flags)

	// Metadata
	now := time.Now().UTC()
	ledgerRange := (ledgerSeq / 10000) * 10000

	return OfferData{
		// Identity
		OfferID:        offerID,
		SellerAccount:  sellerAccount,
		LedgerSequence: ledgerSeq,
		ClosedAt:       closedAt,

		// Selling Asset
		SellingAssetType:   sellingType,
		SellingAssetCode:   sellingCode,
		SellingAssetIssuer: sellingIssuer,

		// Buying Asset
		BuyingAssetType:   buyingType,
		BuyingAssetCode:   buyingCode,
		BuyingAssetIssuer: buyingIssuer,

		// Offer Details
		Amount: amount,
		Price:  priceStr,
		Flags:  flags,

		// Metadata
		CreatedAt:   now,
		LedgerRange: ledgerRange,
	}
}

// parseAsset extracts asset type, code, and issuer from xdr.Asset
// Returns (type, code, issuer) where code and issuer are nullable
func parseAsset(asset xdr.Asset) (assetType string, code *string, issuer *string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", nil, nil

	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		c := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
		i := a4.Issuer.Address()
		return "credit_alphanum4", &c, &i

	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		c := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
		i := a12.Issuer.Address()
		return "credit_alphanum12", &c, &i

	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+) - rare in offers
		poolType := "liquidity_pool_shares"
		poolCode := "POOL_SHARE"
		poolIssuer := "pool"
		return poolType, &poolCode, &poolIssuer

	default:
		return "unknown", nil, nil
	}
}
