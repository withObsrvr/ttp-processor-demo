package com.stellar.ttpmod.event;

/**
 * Represents a token transfer event in the Stellar network.
 * This is an internal representation that we'll use in our mod.
 */
public class TokenTransferEvent {
    // Event types
    public enum Type {
        TRANSFER,
        MINT,
        BURN,
        UNKNOWN
    }
    
    private final Type type;
    private final String assetCode;
    private final String assetIssuer;
    private final String fromAccount;
    private final String toAccount;
    private final long amount;
    private final long ledgerSequence;
    private final long transactionIndex;
    private final long operationIndex;
    
    /**
     * Creates a new token transfer event.
     */
    public TokenTransferEvent(
            Type type,
            String assetCode,
            String assetIssuer,
            String fromAccount,
            String toAccount,
            long amount,
            long ledgerSequence,
            long transactionIndex,
            long operationIndex) {
        this.type = type;
        this.assetCode = assetCode;
        this.assetIssuer = assetIssuer;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.ledgerSequence = ledgerSequence;
        this.transactionIndex = transactionIndex;
        this.operationIndex = operationIndex;
    }
    
    // Getters
    
    public Type getType() {
        return type;
    }
    
    public String getAssetCode() {
        return assetCode;
    }
    
    public String getAssetIssuer() {
        return assetIssuer;
    }
    
    public String getFromAccount() {
        return fromAccount;
    }
    
    public String getToAccount() {
        return toAccount;
    }
    
    public long getAmount() {
        return amount;
    }
    
    public long getLedgerSequence() {
        return ledgerSequence;
    }
    
    public long getTransactionIndex() {
        return transactionIndex;
    }
    
    public long getOperationIndex() {
        return operationIndex;
    }
    
    @Override
    public String toString() {
        return String.format(
            "TokenTransferEvent{type=%s, asset=%s:%s, from=%s, to=%s, amount=%d, ledger=%d}",
            type, assetCode, assetIssuer, fromAccount, toAccount, amount, ledgerSequence);
    }
} 