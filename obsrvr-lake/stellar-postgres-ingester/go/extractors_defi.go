package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// extractClaimableBalances extracts claimable balance state from raw ledger
// Protocol 14+ feature for deferred payments and multi-party asset distribution
// Reference: ducklake-ingestion-obsrvr-v3/go/claimable_balances.go lines 16-145
func (w *Writer) extractClaimableBalances(rawLedger *pb.RawLedger) ([]ClaimableBalanceData, error) {
	var balances []ClaimableBalanceData

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
		log.Printf("Failed to create change reader for claimable balances: %v", err)
		return balances, nil // Return empty slice, don't fail the whole batch
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

		// Parse asset using parseAsset from extractors_market.go
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

	return balances, nil
}

// extractLiquidityPools extracts liquidity pool state from raw ledger
// Protocol 18+ feature for AMM (Automated Market Maker) pools
// Reference: ducklake-ingestion-obsrvr-v3/go/liquidity_pools.go lines 16-147
func (w *Writer) extractLiquidityPools(rawLedger *pb.RawLedger) ([]LiquidityPoolData, error) {
	var pools []LiquidityPoolData

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
		log.Printf("Failed to create change reader for liquidity pools: %v", err)
		return pools, nil // Return empty slice, don't fail the whole batch
	}
	defer reader.Close()

	// Track unique pools (to avoid duplicates within a ledger)
	poolMap := make(map[string]*LiquidityPoolData)

	// Read all changes
	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading change for liquidity pools: %v", err)
			continue
		}

		// We only care about LiquidityPool changes
		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		// Get the liquidity pool entry after the change (post state)
		var poolEntry *xdr.LiquidityPoolEntry
		if change.Post != nil {
			if lp, ok := change.Post.Data.GetLiquidityPool(); ok {
				poolEntry = &lp
			}
		}

		// Skip if no post-state (pool was removed)
		if poolEntry == nil {
			continue
		}

		// Extract pool ID (32-byte hash as hex string)
		poolID := hex.EncodeToString(poolEntry.LiquidityPoolId[:])

		// Currently only LIQUIDITY_POOL_CONSTANT_PRODUCT is supported
		if poolEntry.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
			log.Printf("Unknown liquidity pool type: %v", poolEntry.Body.Type)
			continue
		}

		// Extract constant product pool data
		cp := poolEntry.Body.MustConstantProduct()

		// Extract assets using parseAsset from extractors_market.go
		assetAType, assetACode, assetAIssuer := parseAsset(cp.Params.AssetA)
		assetBType, assetBCode, assetBIssuer := parseAsset(cp.Params.AssetB)

		// Extract amounts and shares (in stroops)
		assetAAmount := int64(cp.ReserveA)
		assetBAmount := int64(cp.ReserveB)
		totalPoolShares := int64(cp.TotalPoolShares)
		trustlineCount := int32(cp.PoolSharesTrustLineCount)

		// Fee is in basis points (30 = 0.3%)
		fee := int32(cp.Params.Fee)

		// Metadata
		now := time.Now().UTC()
		ledgerRange := (ledgerSeq / 10000) * 10000

		// Create pool data
		pool := LiquidityPoolData{
			// Identity (3 fields)
			LiquidityPoolID: poolID,
			LedgerSequence:  ledgerSeq,
			ClosedAt:        closedAt,

			// Pool Type (1 field)
			PoolType: "constant_product",

			// Fee (1 field)
			Fee: fee,

			// Pool Shares (2 fields)
			TrustlineCount:  trustlineCount,
			TotalPoolShares: totalPoolShares,

			// Asset A (4 fields)
			AssetAType:   assetAType,
			AssetACode:   assetACode,
			AssetAIssuer: assetAIssuer,
			AssetAAmount: assetAAmount,

			// Asset B (4 fields)
			AssetBType:   assetBType,
			AssetBCode:   assetBCode,
			AssetBIssuer: assetBIssuer,
			AssetBAmount: assetBAmount,

			// Metadata (2 fields)
			CreatedAt:   now,
			LedgerRange: ledgerRange,
		}

		// Deduplicate by pool_id
		poolMap[poolID] = &pool
	}

	// Convert map to slice
	for _, pool := range poolMap {
		pools = append(pools, *pool)
	}

	return pools, nil
}
