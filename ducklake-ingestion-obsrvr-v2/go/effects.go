package main

import (
	"fmt"
	"log"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// Effect type constants (stellar-go effect types)
const (
	EffectAccountCreated                           = 0
	EffectAccountRemoved                           = 1
	EffectAccountCredited                          = 2
	EffectAccountDebited                           = 3
	EffectAccountThresholdsUpdated                 = 4
	EffectAccountHomeDomainUpdated                 = 5
	EffectAccountFlagsUpdated                      = 6
	EffectSignerCreated                            = 10
	EffectSignerRemoved                            = 11
	EffectSignerUpdated                            = 12
	EffectTrustlineCreated                         = 20
	EffectTrustlineRemoved                         = 21
	EffectTrustlineUpdated                         = 22
	EffectTrustlineAuthorized                      = 23
	EffectTrustlineDeauthorized                    = 24
	EffectTrustlineAuthorizedToMaintainLiabilities = 25
	EffectTrustlineFlagsUpdated                    = 26
	EffectOfferCreated                             = 30
	EffectOfferRemoved                             = 31
	EffectOfferUpdated                             = 32
	EffectTrade                                    = 33
	EffectDataCreated                              = 40
	EffectDataRemoved                              = 41
	EffectDataUpdated                              = 42
	EffectClaimableBalanceCreated                  = 50
	EffectClaimableBalanceClaimed                  = 51
	EffectClaimableBalanceClaimantCreated          = 52
)

// extractEffects extracts effects from transaction meta
// Effects represent state changes from operations (the "what actually happened" layer)
// This function uses the ingest.LedgerTransaction API which abstracts away meta version differences (V0-V4)
func extractEffects(
	tx *ingest.LedgerTransaction,
	ledgerSequence uint32,
	ledgerClosedAt time.Time,
	txHash string,
) ([]EffectData, error) {
	var effects []EffectData

	// Iterate through each operation in the transaction
	// GetOperationChanges handles all meta versions including V4 (Soroban)
	numOps := tx.OperationCount()
	for opIndex := uint32(0); opIndex < numOps; opIndex++ {
		// Get changes for this operation (abstracts meta version differences)
		changes, err := tx.GetOperationChanges(opIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get operation changes for op %d: %w", opIndex, err)
		}

		// Extract effects from the changes
		opEffects, err := extractOperationEffects(
			changes,
			ledgerSequence,
			txHash,
			int32(opIndex),
			ledgerClosedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to extract effects for op %d: %w", opIndex, err)
		}
		effects = append(effects, opEffects...)
	}

	return effects, nil
}

// extractOperationEffects extracts effects from a single operation's ledger entry changes
// Uses []ingest.Change which abstracts away meta version differences
func extractOperationEffects(
	changes []ingest.Change,
	ledgerSequence uint32,
	txHash string,
	opIndex int32,
	ledgerClosedAt time.Time,
) ([]EffectData, error) {
	var effects []EffectData
	effectIndex := int32(0)

	// Track account states for credit/debit detection
	accountBalances := make(map[string]struct {
		before *int64
		after  *int64
	})

	// First pass: collect before/after states for accounts
	// Change.Pre is the before state, Change.Post is the after state
	for _, change := range changes {
		// Only interested in account changes for balance tracking
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		// Get account ID from either Pre or Post
		var accountID string
		if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeAccount {
			account := change.Pre.Data.MustAccount()
			accountID = account.AccountId.Address()
			balance := int64(account.Balance)
			state := accountBalances[accountID]
			state.before = &balance
			accountBalances[accountID] = state
		}

		if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeAccount {
			account := change.Post.Data.MustAccount()
			accountID = account.AccountId.Address()
			balance := int64(account.Balance)
			state := accountBalances[accountID]
			state.after = &balance
			accountBalances[accountID] = state
		}
	}

	// Second pass: extract effects from changes
	// ingest.Change uses Pre/Post to indicate change type:
	//   - Created: Pre == nil, Post != nil
	//   - Updated: Pre != nil, Post != nil
	//   - Removed: Pre != nil, Post == nil
	for _, change := range changes {
		if change.Pre == nil && change.Post != nil {
			// Entity created
			effectsFromCreation := extractCreationEffects(
				*change.Post,
				ledgerSequence,
				txHash,
				opIndex,
				&effectIndex,
				ledgerClosedAt,
			)
			effects = append(effects, effectsFromCreation...)

		} else if change.Pre != nil && change.Post == nil {
			// Entity removed
			// Derive the ledger key from Pre
			key, err := change.Pre.LedgerKey()
			if err != nil {
				log.Printf("Failed to get ledger key for removed entry: %v", err)
				continue
			}
			effectsFromRemoval := extractRemovalEffects(
				key,
				ledgerSequence,
				txHash,
				opIndex,
				&effectIndex,
				ledgerClosedAt,
			)
			effects = append(effects, effectsFromRemoval...)

		} else if change.Pre != nil && change.Post != nil {
			// Entity updated - generate credit/debit effects for accounts
			if change.Type == xdr.LedgerEntryTypeAccount {
				account := change.Post.Data.MustAccount()
				accountID := account.AccountId.Address()

				// Check if balance changed
				state := accountBalances[accountID]
				if state.before != nil && state.after != nil {
					before := *state.before
					after := *state.after

					if after > before {
						// Account credited
						amountChanged := after - before
						effect := createCreditEffect(
							accountID,
							amountChanged,
							ledgerSequence,
							txHash,
							opIndex,
							effectIndex,
							ledgerClosedAt,
						)
						effects = append(effects, effect)
						effectIndex++
					} else if after < before {
						// Account debited
						amountChanged := before - after
						effect := createDebitEffect(
							accountID,
							amountChanged,
							ledgerSequence,
							txHash,
							opIndex,
							effectIndex,
							ledgerClosedAt,
						)
						effects = append(effects, effect)
						effectIndex++
					}
				}
			}
		}
	}

	return effects, nil
}

// extractCreationEffects extracts effects from ledger entry creation
func extractCreationEffects(
	entry xdr.LedgerEntry,
	ledgerSequence uint32,
	txHash string,
	opIndex int32,
	effectIndex *int32,
	ledgerClosedAt time.Time,
) []EffectData {
	var effects []EffectData

	switch entry.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		// Account created
		account := entry.Data.MustAccount()
		accountID := account.AccountId.Address()
		startingBalance := int64(account.Balance)

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectAccountCreated,
			EffectTypeString: "account_created",
			AccountID:        &accountID,
			Amount:           stringPtr(amount.String(xdr.Int64(startingBalance))),
			AssetType:        stringPtr("native"),
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++

	case xdr.LedgerEntryTypeTrustline:
		// Trustline created
		trustline := entry.Data.MustTrustLine()
		accountID := trustline.AccountId.Address()
		assetCode, assetIssuer := extractAssetInfo(trustline.Asset)
		limitStr := amount.String(xdr.Int64(trustline.Limit))

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectTrustlineCreated,
			EffectTypeString: "trustline_created",
			AccountID:        &accountID,
			AssetCode:        &assetCode,
			AssetIssuer:      &assetIssuer,
			AssetType:        stringPtr(getAssetType(trustline.Asset)),
			TrustlineLimit:   &limitStr,
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++

	case xdr.LedgerEntryTypeOffer:
		// Offer created
		offer := entry.Data.MustOffer()
		sellerID := offer.SellerId.Address()
		offerID := int64(offer.OfferId)

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectOfferCreated,
			EffectTypeString: "offer_created",
			SellerAccount:    &sellerID,
			OfferID:          &offerID,
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++

	case xdr.LedgerEntryTypeClaimableBalance:
		// Claimable balance created
		cb := entry.Data.MustClaimableBalance()
		balanceID := cb.BalanceId
		var balanceIDHex string
		switch balanceID.Type {
		case xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0:
			balanceIDHex = fmt.Sprintf("%x", balanceID.MustV0())
		}

		// Extract asset info
		var assetCode, assetIssuer *string
		var assetType *string
		switch cb.Asset.Type {
		case xdr.AssetTypeAssetTypeNative:
			assetType = stringPtr("native")
		case xdr.AssetTypeAssetTypeCreditAlphanum4:
			a4 := cb.Asset.MustAlphaNum4()
			codeBytes := a4.AssetCode[:]
			code := trimNullBytes(string(codeBytes))
			issuer := a4.Issuer.Address()
			assetCode = &code
			assetIssuer = &issuer
			assetType = stringPtr("credit_alphanum4")
		case xdr.AssetTypeAssetTypeCreditAlphanum12:
			a12 := cb.Asset.MustAlphaNum12()
			codeBytes := a12.AssetCode[:]
			code := trimNullBytes(string(codeBytes))
			issuer := a12.Issuer.Address()
			assetCode = &code
			assetIssuer = &issuer
			assetType = stringPtr("credit_alphanum12")
		}

		amountStr := amount.String(xdr.Int64(cb.Amount))

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectClaimableBalanceCreated,
			EffectTypeString: "claimable_balance_created",
			AccountID:        &balanceIDHex,
			Amount:           &amountStr,
			AssetCode:        assetCode,
			AssetIssuer:      assetIssuer,
			AssetType:        assetType,
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++
	}

	return effects
}

// extractRemovalEffects extracts effects from ledger entry removal
func extractRemovalEffects(
	key xdr.LedgerKey,
	ledgerSequence uint32,
	txHash string,
	opIndex int32,
	effectIndex *int32,
	ledgerClosedAt time.Time,
) []EffectData {
	var effects []EffectData

	switch key.Type {
	case xdr.LedgerEntryTypeAccount:
		// Account removed (merged)
		accountID := key.MustAccount().AccountId.Address()

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectAccountRemoved,
			EffectTypeString: "account_removed",
			AccountID:        &accountID,
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++

	case xdr.LedgerEntryTypeTrustline:
		// Trustline removed
		tl := key.MustTrustLine()
		accountID := tl.AccountId.Address()
		assetCode, assetIssuer := extractAssetInfo(tl.Asset)

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectTrustlineRemoved,
			EffectTypeString: "trustline_removed",
			AccountID:        &accountID,
			AssetCode:        &assetCode,
			AssetIssuer:      &assetIssuer,
			AssetType:        stringPtr(getAssetType(tl.Asset)),
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++

	case xdr.LedgerEntryTypeOffer:
		// Offer removed
		offer := key.MustOffer()
		sellerID := offer.SellerId.Address()
		offerID := int64(offer.OfferId)

		effect := EffectData{
			LedgerSequence:   ledgerSequence,
			TransactionHash:  txHash,
			OperationIndex:   opIndex,
			EffectIndex:      *effectIndex,
			EffectType:       EffectOfferRemoved,
			EffectTypeString: "offer_removed",
			SellerAccount:    &sellerID,
			OfferID:          &offerID,
			CreatedAt:        ledgerClosedAt,
			LedgerRange:      ledgerSequence / 10000 * 10000,
		}
		effects = append(effects, effect)
		(*effectIndex)++
	}

	return effects
}

// createCreditEffect creates an account_credited effect
func createCreditEffect(
	accountID string,
	amountChanged int64,
	ledgerSequence uint32,
	txHash string,
	opIndex int32,
	effectIndex int32,
	ledgerClosedAt time.Time,
) EffectData {
	amountStr := amount.String(xdr.Int64(amountChanged))

	return EffectData{
		LedgerSequence:   ledgerSequence,
		TransactionHash:  txHash,
		OperationIndex:   opIndex,
		EffectIndex:      effectIndex,
		EffectType:       EffectAccountCredited,
		EffectTypeString: "account_credited",
		AccountID:        &accountID,
		Amount:           &amountStr,
		AssetType:        stringPtr("native"),
		CreatedAt:        ledgerClosedAt,
		LedgerRange:      ledgerSequence / 10000 * 10000,
	}
}

// createDebitEffect creates an account_debited effect
func createDebitEffect(
	accountID string,
	amountChanged int64,
	ledgerSequence uint32,
	txHash string,
	opIndex int32,
	effectIndex int32,
	ledgerClosedAt time.Time,
) EffectData {
	amountStr := amount.String(xdr.Int64(amountChanged))

	return EffectData{
		LedgerSequence:   ledgerSequence,
		TransactionHash:  txHash,
		OperationIndex:   opIndex,
		EffectIndex:      effectIndex,
		EffectType:       EffectAccountDebited,
		EffectTypeString: "account_debited",
		AccountID:        &accountID,
		Amount:           &amountStr,
		AssetType:        stringPtr("native"),
		CreatedAt:        ledgerClosedAt,
		LedgerRange:      ledgerSequence / 10000 * 10000,
	}
}

// extractAssetInfo extracts asset code and issuer from TrustLineAsset
func extractAssetInfo(asset xdr.TrustLineAsset) (code string, issuer string) {
	switch asset.Type {
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

// getAssetType returns the asset type string
func getAssetType(asset xdr.TrustLineAsset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return "credit_alphanum12"
	case xdr.AssetTypeAssetTypePoolShare:
		return "liquidity_pool_shares"
	default:
		return "unknown"
	}
}

// stringPtr returns a pointer to a string
func stringPtr(s string) *string {
	return &s
}

// trimNullBytes removes null bytes from strings
func trimNullBytes(s string) string {
	for i, c := range s {
		if c == 0 {
			return s[:i]
		}
	}
	return s
}
