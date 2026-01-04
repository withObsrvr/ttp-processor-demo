package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	pb "github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go/gen/raw_ledger_service"
)

// extractTransactions extracts transaction data from raw ledger
func (w *Writer) extractTransactions(rawLedger *pb.RawLedger) ([]TransactionData, error) {
	var transactions []TransactionData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Get closed_at timestamp
	var closedAt time.Time
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		header := lcm.MustV0().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 1:
		header := lcm.MustV1().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 2:
		header := lcm.MustV2().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	}

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	// Read all transactions
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
			continue
		}

		// Extract core transaction data
		txData := TransactionData{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        len(tx.Envelope.Operations()),
			CreatedAt:             closedAt,
			AccountSequence:       int64(tx.Envelope.SeqNum()),
			LedgerRange:           (ledgerSeq / 10000) * 10000,
			SignaturesCount:       len(tx.Envelope.Signatures()),
			NewAccount:            false,
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			memoType := "none"
			txData.MemoType = &memoType
		case xdr.MemoTypeMemoText:
			memoType := "text"
			txData.MemoType = &memoType
			if text, ok := memo.GetText(); ok {
				txData.Memo = &text
			}
		case xdr.MemoTypeMemoId:
			memoType := "id"
			txData.MemoType = &memoType
			if id, ok := memo.GetId(); ok {
				memoStr := fmt.Sprintf("%d", id)
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoHash:
			memoType := "hash"
			txData.MemoType = &memoType
			if hash, ok := memo.GetHash(); ok {
				memoStr := hex.EncodeToString(hash[:])
				txData.Memo = &memoStr
			}
		case xdr.MemoTypeMemoReturn:
			memoType := "return"
			txData.MemoType = &memoType
			if ret, ok := memo.GetRetHash(); ok {
				memoStr := hex.EncodeToString(ret[:])
				txData.Memo = &memoStr
			}
		}

		// Check for CREATE_ACCOUNT operation
		for _, op := range tx.Envelope.Operations() {
			if op.Body.Type == xdr.OperationTypeCreateAccount {
				txData.NewAccount = true
				break
			}
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// extractOperations extracts operation data from raw ledger
func (w *Writer) extractOperations(rawLedger *pb.RawLedger) ([]OperationData, error) {
	var operations []OperationData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Get closed_at timestamp
	var closedAt time.Time
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		header := lcm.MustV0().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 1:
		header := lcm.MustV1().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 2:
		header := lcm.MustV2().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	}

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	// Read all transactions and extract operations
	txIndex := 0 // Track transaction index for TOID generation
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for operations in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		// Extract each operation
		for i, op := range tx.Envelope.Operations() {
			// Get source account (defaults to transaction source if not specified)
			sourceAccount := tx.Envelope.SourceAccount().ToAccountId().Address()
			if op.SourceAccount != nil {
				sourceAccount = op.SourceAccount.ToAccountId().Address()
			}

			opData := OperationData{
				TransactionHash:       txHash,
				TransactionIndex:      txIndex,
				OperationIndex:        i,
				LedgerSequence:        ledgerSeq,
				SourceAccount:         sourceAccount,
				OpType:                int(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           (ledgerSeq / 10000) * 10000,
			}

			// Extract operation-specific fields (simplified - just key fields)
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				if payment, ok := op.Body.GetPaymentOp(); ok {
					amount := int64(payment.Amount)
					opData.Amount = &amount

					asset := payment.Asset.StringCanonical()
					opData.Asset = &asset

					dest := payment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypeCreateAccount:
				if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
					amount := int64(createAcct.StartingBalance)
					opData.Amount = &amount

					dest := createAcct.Destination.Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictReceive:
				if pathPayment, ok := op.Body.GetPathPaymentStrictReceiveOp(); ok {
					amount := int64(pathPayment.DestAmount)
					opData.Amount = &amount

					asset := pathPayment.DestAsset.StringCanonical()
					opData.Asset = &asset

					dest := pathPayment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}

			case xdr.OperationTypePathPaymentStrictSend:
				if pathPayment, ok := op.Body.GetPathPaymentStrictSendOp(); ok {
					amount := int64(pathPayment.SendAmount)
					opData.Amount = &amount

					asset := pathPayment.SendAsset.StringCanonical()
					opData.Asset = &asset

					dest := pathPayment.Destination.ToAccountId().Address()
					opData.Destination = &dest
				}
			}

			// Get operation result code if available
			if txSuccessful {
				if opResults, ok := tx.Result.Result.OperationResults(); ok {
					if i < len(opResults) {
						resultCode := opResults[i].Code.String()
						opData.OperationResultCode = &resultCode
					}
				}
			}

			// Extract contract invocation details for InvokeHostFunction operations (type 24)
			if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
				contractID, functionName, argsJSON, err := extractContractInvocationDetails(op)
				if err != nil {
					log.Printf("Warning: Failed to extract contract invocation details for op %s:%d: %v", txHash, i, err)
				}
				opData.SorobanContractID = contractID
				opData.SorobanFunction = functionName
				opData.SorobanArgumentsJSON = argsJSON
			}

			operations = append(operations, opData)
		}

		// Increment transaction index
		txIndex++
	}

	return operations, nil
}

// Helper function to get asset canonical string
func getAssetString(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if a4, ok := asset.GetAlphaNum4(); ok {
			code := string(a4.AssetCode[:])
			issuer := a4.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if a12, ok := asset.GetAlphaNum12(); ok {
			code := string(a12.AssetCode[:])
			issuer := a12.Issuer.Address()
			return fmt.Sprintf("%s:%s", code, issuer)
		}
	}
	return ""
}

// Helper to extract base64-encoded XDR
func marshalToBase64(v interface{ MarshalBinary() ([]byte, error) }) *string {
	if bytes, err := v.MarshalBinary(); err == nil {
		encoded := base64.StdEncoding.EncodeToString(bytes)
		return &encoded
	}
	return nil
}

// extractEffects extracts basic effects from raw ledger (simplified version)
// NOTE: This is a simplified implementation covering only the most common effect types:
// - Account credited/debited (payment effects)
// - Trustline created/removed
// - Signer added/removed
// For full effect extraction including offers, claimable balances, and liquidity pools,
// see ducklake-ingestion-obsrvr-v3/go/effects.go (528 lines)
func (w *Writer) extractEffects(rawLedger *pb.RawLedger) ([]EffectData, error) {
	var effects []EffectData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Get closed_at timestamp and ledger sequence
	var closedAt time.Time
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		header := lcm.MustV0().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 1:
		header := lcm.MustV1().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 2:
		header := lcm.MustV2().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	}

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	// Read all transactions and extract basic effects
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for effects in ledger %d: %v", ledgerSeq, err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Extract effects from operations (simplified - only payment effects)
		for opIdx := uint32(0); opIdx < tx.OperationCount(); opIdx++ {
			changes, err := tx.GetOperationChanges(opIdx)
			if err != nil {
				log.Printf("Error getting operation changes: %v", err)
				continue
			}

			effectIdx := 0
			for _, change := range changes {
				// Only track account balance changes (most common effect)
				if change.Type == xdr.LedgerEntryTypeAccount {
					if change.Pre != nil && change.Post != nil {
						preAccount := change.Pre.Data.MustAccount()
						postAccount := change.Post.Data.MustAccount()
						preBal := int64(preAccount.Balance)
						postBal := int64(postAccount.Balance)

						if postBal > preBal {
							// Account credited
							amount := fmt.Sprintf("%d", postBal-preBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"

							effects = append(effects, EffectData{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       2, // EffectAccountCredited
								EffectTypeString: "account_credited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      (ledgerSeq / 10000) * 10000,
							})
							effectIdx++
						} else if postBal < preBal {
							// Account debited
							amount := fmt.Sprintf("%d", preBal-postBal)
							accountID := postAccount.AccountId.Address()
							assetType := "native"

							effects = append(effects, EffectData{
								LedgerSequence:   ledgerSeq,
								TransactionHash:  txHash,
								OperationIndex:   int(opIdx),
								EffectIndex:      effectIdx,
								EffectType:       3, // EffectAccountDebited
								EffectTypeString: "account_debited",
								AccountID:        &accountID,
								Amount:           &amount,
								AssetType:        &assetType,
								CreatedAt:        closedAt,
								LedgerRange:      (ledgerSeq / 10000) * 10000,
							})
							effectIdx++
						}
					}
				}
			}
		}
	}

	return effects, nil
}

// extractTrades extracts trades from raw ledger (simplified version)
// NOTE: This is a simplified implementation that extracts basic trade data from
// MANAGE_SELL_OFFER and MANAGE_BUY_OFFER operations when they result in trades.
// For full trade extraction including liquidity pool swaps and complex scenarios,
// see ducklake-ingestion-obsrvr-v3/go/trades.go (330 lines)
func (w *Writer) extractTrades(rawLedger *pb.RawLedger) ([]TradeData, error) {
	var trades []TradeData

	// Unmarshal XDR
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(rawLedger.LedgerCloseMetaXdr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
	}

	// Get closed_at timestamp and ledger sequence
	var closedAt time.Time
	var ledgerSeq uint32
	switch lcm.V {
	case 0:
		header := lcm.MustV0().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 1:
		header := lcm.MustV1().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	case 2:
		header := lcm.MustV2().LedgerHeader
		closedAt = time.Unix(int64(header.Header.ScpValue.CloseTime), 0).UTC()
		ledgerSeq = uint32(header.Header.LedgerSeq)
	}

	// Use ingest package to read transactions
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(w.config.Source.NetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer reader.Close()

	// Read all transactions and extract trade data
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading transaction for trades in ledger %d: %v", ledgerSeq, err)
			continue
		}

		if !tx.Result.Successful() {
			continue // Only process successful transactions
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Check each operation for trade-generating operations
		for opIdx, op := range tx.Envelope.Operations() {
			// Get operation result
			if opResults, ok := tx.Result.Result.OperationResults(); ok {
				if opIdx >= len(opResults) {
					continue
				}
				opResult := opResults[opIdx]

				// Extract trades from successful offer operations
				tradeIndex := 0
				switch op.Body.Type {
				case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer,
					xdr.OperationTypeCreatePassiveSellOffer:

					// Get offer result which contains trades
					var offerResult *xdr.ManageOfferSuccessResult
					switch opResult.Code {
					case xdr.OperationResultCodeOpInner:
						tr := opResult.MustTr()
						switch tr.Type {
						case xdr.OperationTypeManageSellOffer:
							if r, ok := tr.GetManageSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeManageBuyOffer:
							if r, ok := tr.GetManageBuyOfferResult(); ok && r.Code == xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						case xdr.OperationTypeCreatePassiveSellOffer:
							if r, ok := tr.GetCreatePassiveSellOfferResult(); ok && r.Code == xdr.ManageSellOfferResultCodeManageSellOfferSuccess {
								result := r.MustSuccess()
								offerResult = &result
							}
						}
					}

					if offerResult != nil {
						// Extract claimed offers (trades)
						for _, claimAtom := range offerResult.OffersClaimed {
							// Handle different ClaimAtom types
							var sellerAccount, sellingAmount, buyingAmount string
							var sellingCode, sellingIssuer, buyingCode, buyingIssuer *string

							switch claimAtom.Type {
							case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
								ob := claimAtom.MustOrderBook()
								sellerAccount = ob.SellerId.Address()
								sellingAmount = fmt.Sprintf("%d", ob.AmountSold)
								buyingAmount = fmt.Sprintf("%d", ob.AmountBought)

								// Parse selling asset
								if ob.AssetSold.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetSold.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetSold.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetSold.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										sellingCode = &code
										sellingIssuer = &issuer
									}
								}

								// Parse buying asset
								if ob.AssetBought.Type != xdr.AssetTypeAssetTypeNative {
									switch ob.AssetBought.Type {
									case xdr.AssetTypeAssetTypeCreditAlphanum4:
										a4 := ob.AssetBought.MustAlphaNum4()
										code := strings.TrimRight(string(a4.AssetCode[:]), "\x00")
										issuer := a4.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									case xdr.AssetTypeAssetTypeCreditAlphanum12:
										a12 := ob.AssetBought.MustAlphaNum12()
										code := strings.TrimRight(string(a12.AssetCode[:]), "\x00")
										issuer := a12.Issuer.Address()
										buyingCode = &code
										buyingIssuer = &issuer
									}
								}

							case xdr.ClaimAtomTypeClaimAtomTypeV0:
								// V0 atoms are old and rare - simplified handling
								v0 := claimAtom.MustV0()
								sellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
								sellingAmount = fmt.Sprintf("%d", v0.AmountSold)
								buyingAmount = fmt.Sprintf("%d", v0.AmountBought)
							}

							buyerAccount := tx.Envelope.SourceAccount().ToAccountId().Address()

							trades = append(trades, TradeData{
								LedgerSequence:     ledgerSeq,
								TransactionHash:    txHash,
								OperationIndex:     opIdx,
								TradeIndex:         tradeIndex,
								TradeType:          "orderbook",
								TradeTimestamp:     closedAt,
								SellerAccount:      sellerAccount,
								SellingAssetCode:   sellingCode,
								SellingAssetIssuer: sellingIssuer,
								SellingAmount:      sellingAmount,
								BuyerAccount:       buyerAccount,
								BuyingAssetCode:    buyingCode,
								BuyingAssetIssuer:  buyingIssuer,
								BuyingAmount:       buyingAmount,
								Price:              fmt.Sprintf("%s/%s", buyingAmount, sellingAmount),
								CreatedAt:          closedAt,
								LedgerRange:        (ledgerSeq / 10000) * 10000,
							})
							tradeIndex++
						}
					}
				}
			}
		}
	}

	return trades, nil
}
