package com.stellar.ttpmod.grpc;

import com.stellar.ttpmod.StellarTTPMod;
import com.stellar.ttpmod.event.TokenTransferEvent;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.minecraft.util.math.BlockPos;
import net.minecraft.world.World;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Client for the TTP Processor gRPC service.
 * This class handles the communication with the TTP Processor service
 * and streams token transfer events.
 */
public class TTPClient {
    // Default endpoint for the TTP Processor service
    private static final String DEFAULT_ENDPOINT = "localhost:50052";
    
    // gRPC channel and stubs
    private ManagedChannel channel;
    private EventServiceGrpc.EventServiceBlockingStub blockingStub;
    private EventServiceGrpc.EventServiceStub asyncStub;
    
    // Streaming state
    private final AtomicBoolean isStreaming = new AtomicBoolean(false);
    private ExecutorService streamExecutor;
    private int startLedger;
    private int endLedger;
    
    // Event handler for token transfer events
    private Consumer<TokenTransferEvent> eventHandler;
    
    /**
     * Creates a new TTPClient with the default endpoint.
     */
    public TTPClient() {
        this(DEFAULT_ENDPOINT);
    }
    
    /**
     * Creates a new TTPClient with the specified endpoint.
     * 
     * @param endpoint the gRPC endpoint for the TTP Processor service
     */
    public TTPClient(String endpoint) {
        this.channel = ManagedChannelBuilder.forTarget(endpoint)
            .usePlaintext()  // For simplicity; use TLS in production
            .build();
        
        this.blockingStub = EventServiceGrpc.newBlockingStub(channel);
        this.asyncStub = EventServiceGrpc.newStub(channel);
        
        StellarTTPMod.LOGGER.info("TTPClient initialized with endpoint: {}", endpoint);
    }
    
    /**
     * Sets the event handler for token transfer events.
     * 
     * @param eventHandler the event handler to call when events are received
     */
    public void setEventHandler(Consumer<TokenTransferEvent> eventHandler) {
        this.eventHandler = eventHandler;
    }
    
    /**
     * Starts streaming TTP events from the specified ledger range.
     * 
     * @param startLedger the starting ledger sequence number
     * @param endLedger the ending ledger sequence number (0 for unbounded streaming)
     * @throws IllegalStateException if already streaming
     */
    public void startStreaming(int startLedger, int endLedger) {
        if (isStreaming.get()) {
            throw new IllegalStateException("Already streaming TTP events");
        }
        
        this.startLedger = startLedger;
        this.endLedger = endLedger;
        
        streamExecutor = Executors.newSingleThreadExecutor();
        isStreaming.set(true);
        
        streamExecutor.submit(() -> {
            try {
                // Create the request
                GetEventsRequest request = GetEventsRequest.newBuilder()
                    .setStartLedger(startLedger)
                    .setEndLedger(endLedger)
                    .build();
                
                StellarTTPMod.LOGGER.info("Starting to stream TTP events from ledger {} to {}",
                    startLedger, endLedger > 0 ? endLedger : "unbounded");
                
                // Start streaming events
                blockingStub.getTTPEvents(request).forEachRemaining(event -> {
                    if (!isStreaming.get()) {
                        return;  // Stop if streaming was canceled
                    }
                    
                    try {
                        // Process the event
                        StellarTTPMod.LOGGER.debug("Received TTP event: {}", event);
                        
                        // Convert the event to our internal format and notify the handler
                        TokenTransferEvent ttpEvent = convertEvent(event);
                        if (ttpEvent != null && eventHandler != null) {
                            eventHandler.accept(ttpEvent);
                        }
                    } catch (Exception e) {
                        StellarTTPMod.LOGGER.error("Error processing TTP event", e);
                    }
                });
                
                StellarTTPMod.LOGGER.info("TTP event stream completed normally");
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.CANCELLED) {
                    StellarTTPMod.LOGGER.info("TTP event stream was cancelled");
                } else {
                    StellarTTPMod.LOGGER.error("Error in TTP event stream", e);
                }
            } catch (Exception e) {
                StellarTTPMod.LOGGER.error("Unexpected error in TTP event stream", e);
            } finally {
                isStreaming.set(false);
            }
        });
    }
    
    /**
     * Stops the current streaming operation.
     */
    public void stopStreaming() {
        if (!isStreaming.get()) {
            return;  // Not streaming
        }
        
        StellarTTPMod.LOGGER.info("Stopping TTP event stream");
        
        isStreaming.set(false);
        
        if (streamExecutor != null) {
            streamExecutor.shutdownNow();
            try {
                // Wait for the streaming thread to terminate
                if (!streamExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    StellarTTPMod.LOGGER.warn("TTP event stream did not terminate in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                StellarTTPMod.LOGGER.error("Interrupted while waiting for TTP event stream to stop", e);
            }
            
            streamExecutor = null;
        }
    }
    
    /**
     * Closes the gRPC channel and releases resources.
     */
    public void close() {
        stopStreaming();
        
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                StellarTTPMod.LOGGER.error("Interrupted while closing gRPC channel", e);
            }
        }
    }
    
    /**
     * Checks if currently streaming TTP events.
     * 
     * @return true if streaming, false otherwise
     */
    public boolean isStreaming() {
        return isStreaming.get();
    }
    
    /**
     * Gets the starting ledger sequence number.
     * 
     * @return the starting ledger sequence number
     */
    public int getStartLedger() {
        return startLedger;
    }
    
    /**
     * Gets the ending ledger sequence number.
     * 
     * @return the ending ledger sequence number
     */
    public int getEndLedger() {
        return endLedger;
    }
    
    /**
     * Converts a gRPC token transfer event to our internal format.
     * 
     * @param grpcEvent the gRPC token transfer event
     * @return the converted event or null if conversion failed
     */
    private TokenTransferEvent convertEvent(com.stellar.ttpmod.grpc.TokenTransferEvent grpcEvent) {
        try {
            // Determine the event type
            TokenTransferEvent.Type type;
            switch (grpcEvent.getEventCase()) {
                case TRANSFER:
                    type = TokenTransferEvent.Type.TRANSFER;
                    break;
                case MINT:
                    type = TokenTransferEvent.Type.MINT;
                    break;
                case BURN:
                    type = TokenTransferEvent.Type.BURN;
                    break;
                default:
                    type = TokenTransferEvent.Type.UNKNOWN;
                    break;
            }
            
            // Extract common properties
            long ledgerSequence = grpcEvent.getLedgerSequence();
            long txIndex = grpcEvent.getTxIndex();
            long opIndex = grpcEvent.getOpIndex();
            
            String assetCode = "";
            String assetIssuer = "";
            String fromAccount = "";
            String toAccount = "";
            long amount = 0;
            
            // Extract event-specific properties
            switch (grpcEvent.getEventCase()) {
                case TRANSFER:
                    com.stellar.ttpmod.grpc.TransferEvent transfer = grpcEvent.getTransfer();
                    assetCode = transfer.getAsset().getCode();
                    assetIssuer = transfer.getAsset().getIssuer();
                    fromAccount = transfer.getFrom();
                    toAccount = transfer.getTo();
                    amount = transfer.getAmount();
                    break;
                case MINT:
                    com.stellar.ttpmod.grpc.MintEvent mint = grpcEvent.getMint();
                    assetCode = mint.getAsset().getCode();
                    assetIssuer = mint.getAsset().getIssuer();
                    fromAccount = mint.getAdmin();
                    toAccount = mint.getTo();
                    amount = mint.getAmount();
                    break;
                case BURN:
                    com.stellar.ttpmod.grpc.BurnEvent burn = grpcEvent.getBurn();
                    assetCode = burn.getAsset().getCode();
                    assetIssuer = burn.getAsset().getIssuer();
                    fromAccount = burn.getFrom();
                    toAccount = burn.getAdmin();
                    amount = burn.getAmount();
                    break;
            }
            
            // Create and return our internal event
            return new TokenTransferEvent(
                type,
                assetCode,
                assetIssuer,
                fromAccount,
                toAccount,
                amount,
                ledgerSequence,
                txIndex,
                opIndex
            );
        } catch (Exception e) {
            StellarTTPMod.LOGGER.error("Failed to convert gRPC event to TokenTransferEvent", e);
            return null;
        }
    }
} 