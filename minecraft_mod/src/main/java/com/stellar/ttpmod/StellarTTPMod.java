package com.stellar.ttpmod;

import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.stellar.ttpmod.event.TokenTransferEvent;
import com.stellar.ttpmod.grpc.TTPClient;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.server.command.CommandManager;
import net.minecraft.text.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class StellarTTPMod implements ModInitializer {
    // Logger for our mod
    public static final Logger LOGGER = LoggerFactory.getLogger("ttpmod");
    
    // TTP client instance
    private TTPClient ttpClient;
    
    // Event listeners for token transfer events
    private static final List<Consumer<TokenTransferEvent>> eventListeners = new CopyOnWriteArrayList<>();
    
    // Singleton instance
    private static StellarTTPMod instance;
    
    public static StellarTTPMod getInstance() {
        return instance;
    }

    @Override
    public void onInitialize() {
        LOGGER.info("Initializing Stellar TTP Mod");
        
        // Store the instance
        instance = this;
        
        // Create the TTP client
        ttpClient = new TTPClient();
        
        // Handle token transfer events
        ttpClient.setEventHandler(this::onTokenTransferEvent);
        
        // Register commands for controlling the mod
        registerCommands();
    }
    
    /**
     * Registers commands for the mod.
     */
    private void registerCommands() {
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) -> {
            // Command to start streaming TTP events
            dispatcher.register(
                CommandManager.literal("ttp")
                    .then(CommandManager.literal("start")
                        .then(CommandManager.argument("startLedger", IntegerArgumentType.integer(1))
                            .then(CommandManager.argument("endLedger", IntegerArgumentType.integer(0))
                                .executes(context -> {
                                    int startLedger = IntegerArgumentType.getInteger(context, "startLedger");
                                    int endLedger = IntegerArgumentType.getInteger(context, "endLedger");
                                    
                                    try {
                                        ttpClient.startStreaming(startLedger, endLedger);
                                        context.getSource().sendFeedback(() -> 
                                            Text.literal("Started streaming TTP events from ledger " + 
                                                        startLedger + " to " + endLedger), false);
                                    } catch (Exception e) {
                                        LOGGER.error("Error starting TTP stream", e);
                                        context.getSource().sendError(
                                            Text.literal("Error: " + e.getMessage()));
                                    }
                                    
                                    return 1;
                                })))
                    )
                    .then(CommandManager.literal("stop")
                        .executes(context -> {
                            ttpClient.stopStreaming();
                            context.getSource().sendFeedback(() -> 
                                Text.literal("Stopped streaming TTP events"), false);
                            return 1;
                        })
                    )
                    .then(CommandManager.literal("status")
                        .executes(context -> {
                            String status = ttpClient.isStreaming() ? 
                                "Active (Started at ledger " + ttpClient.getStartLedger() + ")" : 
                                "Inactive";
                            
                            context.getSource().sendFeedback(() -> 
                                Text.literal("TTP Stream Status: " + status), false);
                            return 1;
                        })
                    )
            );
        });
    }
    
    /**
     * Called when a token transfer event is received.
     */
    public void onTokenTransferEvent(TokenTransferEvent event) {
        // Log the event
        LOGGER.info("Received token transfer event: {}", event);
        
        // Notify all event listeners
        for (Consumer<TokenTransferEvent> listener : eventListeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                LOGGER.error("Error in event listener", e);
            }
        }
    }
    
    /**
     * Registers a listener for token transfer events.
     * 
     * @param listener the event listener to register
     */
    public static void registerEventListener(Consumer<TokenTransferEvent> listener) {
        eventListeners.add(listener);
    }
    
    /**
     * Unregisters a listener for token transfer events.
     * 
     * @param listener the event listener to unregister
     */
    public static void unregisterEventListener(Consumer<TokenTransferEvent> listener) {
        eventListeners.remove(listener);
    }
} 