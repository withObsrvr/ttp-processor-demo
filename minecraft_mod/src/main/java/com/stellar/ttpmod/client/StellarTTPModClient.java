package com.stellar.ttpmod.client;

import com.stellar.ttpmod.StellarTTPMod;
import com.stellar.ttpmod.event.TokenTransferEvent;
import com.stellar.ttpmod.render.EventRenderer;
import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.fabric.api.client.rendering.v1.WorldRenderEvents;
import net.minecraft.client.MinecraftClient;

/**
 * Client-side initialization for the Stellar TTP Mod.
 */
public class StellarTTPModClient implements ClientModInitializer {
    // Renderer for token transfer events
    private EventRenderer eventRenderer;

    @Override
    public void onInitializeClient() {
        StellarTTPMod.LOGGER.info("Initializing Stellar TTP Mod Client");
        
        // Create the event renderer
        eventRenderer = new EventRenderer();
        
        // Register rendering hooks
        registerRenderHooks();
        
        // Register client tick events
        registerTickEvents();
    }
    
    /**
     * Registers hooks for rendering events in the world.
     */
    private void registerRenderHooks() {
        // Register a hook for rendering events after translucent blocks are rendered
        WorldRenderEvents.AFTER_TRANSLUCENT.register(context -> {
            if (MinecraftClient.getInstance().world != null) {
                eventRenderer.render(context.matrixStack(), context.camera(), context.tickDelta());
            }
        });
    }
    
    /**
     * Registers events that should happen every client tick.
     */
    private void registerTickEvents() {
        ClientTickEvents.END_CLIENT_TICK.register(client -> {
            if (client.world != null && client.player != null) {
                eventRenderer.tick();
            }
        });
    }
    
    /**
     * Called by the main mod class when a token transfer event is received.
     * 
     * @param event the token transfer event to visualize
     */
    public void onTokenTransferEvent(TokenTransferEvent event) {
        if (eventRenderer != null) {
            eventRenderer.addEvent(event);
        }
    }
} 