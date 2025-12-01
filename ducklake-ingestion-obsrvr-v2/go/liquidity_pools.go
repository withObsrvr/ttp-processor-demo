package main

import (
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractLiquidityPools extracts liquidity pool state from LedgerCloseMeta
// Protocol 18+ feature for AMM (Automated Market Maker) pools
// Obsrvr playbook: core.liquidity_pools_snapshot_v1
func (ing *Ingester) extractLiquidityPools(lcm *xdr.LedgerCloseMeta) []LiquidityPoolData {
	var pools []LiquidityPoolData

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
		log.Printf("Failed to create change reader for liquidity pools: %v", err)
		return pools
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

		// Extract assets using existing parseAsset function
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
			// Identity (1 field)
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

	return pools
}
