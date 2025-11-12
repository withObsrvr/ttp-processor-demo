package main

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// extractTrades extracts trade executions from transaction meta
// Trades are found in the ClaimAtom (V0/V1) or ClaimOfferAtom (V2+) in operation results
func extractTrades(
	tx *ingest.LedgerTransaction,
	ledgerSequence uint32,
	ledgerClosedAt time.Time,
	txHash string,
) ([]TradeData, error) {
	var trades []TradeData

	// Get operation results from transaction result
	if !tx.Result.Successful() {
		// Failed transactions don't have trades
		return trades, nil
	}

	// Safely get operation results
	opResults, ok := tx.Result.Result.Result.GetResults()
	if !ok {
		// No results available (shouldn't happen for successful transactions)
		return trades, nil
	}

	// Process each operation result for trade effects
	for opIndex, opResult := range opResults {
		tradeIndex := int32(0)

		// Extract trades based on operation type
		switch opResult.Code {
		case xdr.OperationResultCodeOpInner:
			inner := opResult.MustTr()

			switch inner.Type {
			case xdr.OperationTypeManageSellOffer:
				// ManageSellOffer can generate trades
				result := inner.MustManageSellOfferResult()
				if result.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
					success := result.MustSuccess()
					for _, claimAtom := range success.OffersClaimed {
						trade, err := extractTradeFromClaimAtom(
							claimAtom,
							ledgerSequence,
							ledgerClosedAt,
							txHash,
							int32(opIndex),
							tradeIndex,
						)
						if err != nil {
							return nil, fmt.Errorf("failed to extract trade from ManageSellOffer op %d: %w", opIndex, err)
						}
						trades = append(trades, trade)
						tradeIndex++
					}
				}

			case xdr.OperationTypeManageBuyOffer:
				// ManageBuyOffer can generate trades
				result := inner.MustManageBuyOfferResult()
				if result.Code == xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess {
					success := result.MustSuccess()
					for _, claimAtom := range success.OffersClaimed {
						trade, err := extractTradeFromClaimAtom(
							claimAtom,
							ledgerSequence,
							ledgerClosedAt,
							txHash,
							int32(opIndex),
							tradeIndex,
						)
						if err != nil {
							return nil, fmt.Errorf("failed to extract trade from ManageBuyOffer op %d: %w", opIndex, err)
						}
						trades = append(trades, trade)
						tradeIndex++
					}
				}

			case xdr.OperationTypeCreatePassiveSellOffer:
				// CreatePassiveSellOffer can generate trades
				result := inner.MustCreatePassiveSellOfferResult()
				if result.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
					success := result.MustSuccess()
					for _, claimAtom := range success.OffersClaimed {
						trade, err := extractTradeFromClaimAtom(
							claimAtom,
							ledgerSequence,
							ledgerClosedAt,
							txHash,
							int32(opIndex),
							tradeIndex,
						)
						if err != nil {
							return nil, fmt.Errorf("failed to extract trade from CreatePassiveSellOffer op %d: %w", opIndex, err)
						}
						trades = append(trades, trade)
						tradeIndex++
					}
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				// PathPayment can generate multiple trades
				result := inner.MustPathPaymentStrictReceiveResult()
				if result.Code == xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess {
					success := result.MustSuccess()
					for _, claimAtom := range success.Offers {
						trade, err := extractTradeFromClaimAtom(
							claimAtom,
							ledgerSequence,
							ledgerClosedAt,
							txHash,
							int32(opIndex),
							tradeIndex,
						)
						if err != nil {
							return nil, fmt.Errorf("failed to extract trade from PathPaymentStrictReceive op %d: %w", opIndex, err)
						}
						trades = append(trades, trade)
						tradeIndex++
					}
				}

			case xdr.OperationTypePathPaymentStrictSend:
				// PathPaymentStrictSend can generate multiple trades
				result := inner.MustPathPaymentStrictSendResult()
				if result.Code == xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess {
					success := result.MustSuccess()
					for _, claimAtom := range success.Offers {
						trade, err := extractTradeFromClaimAtom(
							claimAtom,
							ledgerSequence,
							ledgerClosedAt,
							txHash,
							int32(opIndex),
							tradeIndex,
						)
						if err != nil {
							return nil, fmt.Errorf("failed to extract trade from PathPaymentStrictSend op %d: %w", opIndex, err)
						}
						trades = append(trades, trade)
						tradeIndex++
					}
				}
			}
		}
	}

	return trades, nil
}

// extractTradeFromClaimAtom extracts a single trade from a ClaimAtom
func extractTradeFromClaimAtom(
	claimAtom xdr.ClaimAtom,
	ledgerSequence uint32,
	ledgerClosedAt time.Time,
	txHash string,
	opIndex int32,
	tradeIndex int32,
) (TradeData, error) {
	var trade TradeData

	trade.LedgerSequence = ledgerSequence
	trade.TransactionHash = txHash
	trade.OperationIndex = opIndex
	trade.TradeIndex = tradeIndex
	trade.TradeType = "orderbook"
	trade.TradeTimestamp = ledgerClosedAt
	trade.CreatedAt = ledgerClosedAt
	trade.LedgerRange = ledgerSequence / 10000 * 10000

	switch claimAtom.Type {
	case xdr.ClaimAtomTypeClaimAtomTypeV0:
		// V0 claim atom (older format)
		v0 := claimAtom.MustV0()

		// Seller (SellerEd25519 is a Uint256, need to convert to account ID)
		// For now, use a placeholder - V0 atoms are old and rare
		trade.SellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)

		// Selling asset
		sellingCode, sellingIssuer := extractAssetFromAsset(v0.AssetSold)
		if sellingCode != "" {
			trade.SellingAssetCode = &sellingCode
		}
		if sellingIssuer != "" {
			trade.SellingAssetIssuer = &sellingIssuer
		}
		trade.SellingAmount = amount.String(v0.AmountSold)

		// Buying asset
		buyingCode, buyingIssuer := extractAssetFromAsset(v0.AssetBought)
		if buyingCode != "" {
			trade.BuyingAssetCode = &buyingCode
		}
		if buyingIssuer != "" {
			trade.BuyingAssetIssuer = &buyingIssuer
		}
		trade.BuyingAmount = amount.String(v0.AmountBought)

		// Buyer is implicit from the operation (would need to look at op source)
		// For now, use seller as placeholder
		trade.BuyerAccount = "unknown"

		// Calculate price (selling / buying)
		trade.Price = calculatePrice(v0.AmountSold, v0.AmountBought)

	case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
		// OrderBook claim atom (newer format)
		ob := claimAtom.MustOrderBook()

		// Seller
		trade.SellerAccount = ob.SellerId.Address()

		// Selling asset
		sellingCode, sellingIssuer := extractAssetFromAsset(ob.AssetSold)
		if sellingCode != "" {
			trade.SellingAssetCode = &sellingCode
		}
		if sellingIssuer != "" {
			trade.SellingAssetIssuer = &sellingIssuer
		}
		trade.SellingAmount = amount.String(ob.AmountSold)

		// Buying asset
		buyingCode, buyingIssuer := extractAssetFromAsset(ob.AssetBought)
		if buyingCode != "" {
			trade.BuyingAssetCode = &buyingCode
		}
		if buyingIssuer != "" {
			trade.BuyingAssetIssuer = &buyingIssuer
		}
		trade.BuyingAmount = amount.String(ob.AmountBought)

		// Buyer is the operation source (would need context from operation)
		// For now, use placeholder
		trade.BuyerAccount = "unknown"

		// Calculate price
		trade.Price = calculatePrice(ob.AmountSold, ob.AmountBought)

	case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
		// Liquidity pool trade (Protocol 18+)
		// V2 Cycle 13: Extract LP trades
		lp := claimAtom.MustLiquidityPool()

		// Set trade type to liquidity_pool
		trade.TradeType = "liquidity_pool"

		// Seller account is not available in ClaimAtom context
		// Would need operation source - use placeholder for now
		trade.SellerAccount = "unknown"

		// From pool's perspective:
		// - assetBought/amountBought: sent TO pool (user sold this)
		// - assetSold/amountSold: taken FROM pool (user bought this)

		// Selling asset (what user sent to pool)
		sellingCode, sellingIssuer := extractAssetFromAsset(lp.AssetBought)
		if sellingCode != "" {
			trade.SellingAssetCode = &sellingCode
		}
		if sellingIssuer != "" {
			trade.SellingAssetIssuer = &sellingIssuer
		}
		trade.SellingAmount = amount.String(lp.AmountBought)

		// Buying asset (what user received from pool)
		buyingCode, buyingIssuer := extractAssetFromAsset(lp.AssetSold)
		if buyingCode != "" {
			trade.BuyingAssetCode = &buyingCode
		}
		if buyingIssuer != "" {
			trade.BuyingAssetIssuer = &buyingIssuer
		}
		trade.BuyingAmount = amount.String(lp.AmountSold)

		// Buyer is the liquidity pool ID
		trade.BuyerAccount = hex.EncodeToString(lp.LiquidityPoolId[:])

		// Calculate price (selling / buying)
		trade.Price = calculatePrice(lp.AmountBought, lp.AmountSold)
	}

	return trade, nil
}

// extractAssetFromAsset extracts asset code and issuer from xdr.Asset
func extractAssetFromAsset(asset xdr.Asset) (code string, issuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "", "" // Native XLM has no code/issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		code = trimNullBytes(string(a4.AssetCode[:]))
		issuer = a4.Issuer.Address()
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		code = trimNullBytes(string(a12.AssetCode[:]))
		issuer = a12.Issuer.Address()
	}
	return
}

// calculatePrice calculates the price as selling_amount / buying_amount
func calculatePrice(amountSold xdr.Int64, amountBought xdr.Int64) string {
	if amountBought == 0 {
		return "0"
	}

	// Use big.Rat for precise decimal division
	sold := big.NewRat(int64(amountSold), 1e7) // Stellar amounts are in stroops (7 decimals)
	bought := big.NewRat(int64(amountBought), 1e7)

	price := new(big.Rat).Quo(sold, bought)

	// Format as decimal string with 7 decimal places
	return price.FloatString(7)
}
