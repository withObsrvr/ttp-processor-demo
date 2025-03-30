package com.stellar.ttpmod.render;

import com.mojang.blaze3d.systems.RenderSystem;
import com.stellar.ttpmod.StellarTTPMod;
import com.stellar.ttpmod.event.TokenTransferEvent;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.render.BufferBuilder;
import net.minecraft.client.render.Camera;
import net.minecraft.client.render.GameRenderer;
import net.minecraft.client.render.Tessellator;
import net.minecraft.client.render.VertexFormat;
import net.minecraft.client.render.VertexFormats;
import net.minecraft.client.util.math.MatrixStack;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.Vec3d;
import org.joml.Matrix4f;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Renderer for visualizing token transfer events in the Minecraft world.
 */
public class EventRenderer {
    // Minecraft client instance
    private final MinecraftClient client = MinecraftClient.getInstance();
    
    // List of active visualization effects
    private final List<VisualEffect> activeEffects = new ArrayList<>();
    
    // Random number generator for positioning effects
    private final Random random = new Random();
    
    // Maximum number of simultaneous effects
    private static final int MAX_EFFECTS = 50;
    
    /**
     * Creates a new event renderer.
     */
    public EventRenderer() {
        // Initialization code if needed
    }
    
    /**
     * Adds a token transfer event to be visualized.
     * 
     * @param event the token transfer event to visualize
     */
    public void addEvent(TokenTransferEvent event) {
        if (client.player == null) {
            return;
        }
        
        // Limit the number of active effects to prevent performance issues
        if (activeEffects.size() >= MAX_EFFECTS) {
            return;
        }
        
        // Get the player's position as a base
        BlockPos playerPos = client.player.getBlockPos();
        
        // Generate a random position around the player (within 20 blocks)
        int offsetX = random.nextInt(41) - 20;
        int offsetY = random.nextInt(11) - 5;
        int offsetZ = random.nextInt(41) - 20;
        
        BlockPos effectPos = playerPos.add(offsetX, offsetY, offsetZ);
        
        // Create different effects based on the event type
        VisualEffect effect;
        switch (event.getType()) {
            case TRANSFER:
                effect = new BeamEffect(effectPos, 0x00AAFF, 40); // Blue beam for transfers
                break;
            case MINT:
                effect = new ParticleEffect(effectPos, 0x00FF00, 60); // Green particles for mints
                break;
            case BURN:
                effect = new ExplosionEffect(effectPos, 0xFF0000, 30); // Red explosion for burns
                break;
            default:
                effect = new ParticleEffect(effectPos, 0xFFFFFF, 40); // White particles for unknown
                break;
        }
        
        // Add the effect to the active effects list
        activeEffects.add(effect);
        
        StellarTTPMod.LOGGER.debug("Added visual effect for event: {}", event);
    }
    
    /**
     * Updates all active visual effects.
     * Called every tick.
     */
    public void tick() {
        // Update each active effect and remove completed ones
        Iterator<VisualEffect> iterator = activeEffects.iterator();
        while (iterator.hasNext()) {
            VisualEffect effect = iterator.next();
            effect.tick();
            
            if (effect.isFinished()) {
                iterator.remove();
            }
        }
    }
    
    /**
     * Renders all active visual effects.
     * 
     * @param matrixStack the matrix stack for rendering
     * @param camera the camera for rendering
     * @param tickDelta the partial tick time
     */
    public void render(MatrixStack matrixStack, Camera camera, float tickDelta) {
        if (activeEffects.isEmpty()) {
            return; // Nothing to render
        }
        
        // Set up rendering state
        RenderSystem.enableBlend();
        RenderSystem.defaultBlendFunc();
        RenderSystem.disableCull();
        RenderSystem.setShader(GameRenderer::getPositionColorProgram);
        
        // Get camera position for translating render coordinates
        Vec3d cameraPos = camera.getPos();
        
        // Begin rendering
        Tessellator tessellator = Tessellator.getInstance();
        BufferBuilder buffer = tessellator.getBuffer();
        
        // Render each active effect
        for (VisualEffect effect : activeEffects) {
            matrixStack.push();
            
            // Translate to world coordinates relative to camera
            matrixStack.translate(
                effect.getPosition().getX() - cameraPos.x,
                effect.getPosition().getY() - cameraPos.y,
                effect.getPosition().getZ() - cameraPos.z
            );
            
            // Render the effect
            effect.render(matrixStack, buffer, tickDelta);
            
            matrixStack.pop();
        }
        
        // Clean up rendering state
        RenderSystem.enableCull();
        RenderSystem.disableBlend();
    }
    
    /**
     * Base class for visual effects that appear in the world.
     */
    private abstract static class VisualEffect {
        protected final Vec3d position;
        protected final int color;
        protected int lifetime;
        protected final int maxLifetime;
        
        protected VisualEffect(BlockPos blockPos, int color, int maxLifetime) {
            // Center of the block as position
            this.position = new Vec3d(
                blockPos.getX() + 0.5,
                blockPos.getY() + 0.5,
                blockPos.getZ() + 0.5
            );
            this.color = color;
            this.lifetime = 0;
            this.maxLifetime = maxLifetime;
        }
        
        /**
         * Updates the effect state.
         */
        public void tick() {
            lifetime++;
        }
        
        /**
         * Checks if the effect has completed its lifetime.
         * 
         * @return true if the effect is finished and should be removed
         */
        public boolean isFinished() {
            return lifetime >= maxLifetime;
        }
        
        /**
         * Gets the position of the effect in the world.
         * 
         * @return the effect position
         */
        public Vec3d getPosition() {
            return position;
        }
        
        /**
         * Renders the effect.
         * 
         * @param matrixStack the matrix stack for rendering
         * @param buffer the buffer for rendering vertices
         * @param tickDelta the partial tick time
         */
        public abstract void render(MatrixStack matrixStack, BufferBuilder buffer, float tickDelta);
        
        /**
         * Helper to get RGB components from a color integer.
         */
        protected float[] getRGBComponents() {
            float red = ((color >> 16) & 0xFF) / 255f;
            float green = ((color >> 8) & 0xFF) / 255f;
            float blue = (color & 0xFF) / 255f;
            return new float[]{red, green, blue};
        }
    }
    
    /**
     * A beam of light shooting upward.
     */
    private static class BeamEffect extends VisualEffect {
        public BeamEffect(BlockPos pos, int color, int maxLifetime) {
            super(pos, color, maxLifetime);
        }
        
        @Override
        public void render(MatrixStack matrixStack, BufferBuilder buffer, float tickDelta) {
            // Calculate alpha based on lifetime (fade in and out)
            float progress = (float) lifetime / maxLifetime;
            float alpha = progress < 0.2f ? (progress / 0.2f) : (progress > 0.8f ? (1 - (progress - 0.8f) / 0.2f) : 1f);
            
            // Get RGB components
            float[] rgb = getRGBComponents();
            
            // Calculate beam height based on lifetime
            float height = Math.min(20f, 20f * ((float) lifetime / (maxLifetime / 2)));
            
            // Render a beam
            buffer.begin(VertexFormat.DrawMode.QUADS, VertexFormats.POSITION_COLOR);
            
            Matrix4f matrix = matrixStack.peek().getPositionMatrix();
            float width = 0.5f;
            
            // Beam sides
            buffer.vertex(matrix, -width, 0, -width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            buffer.vertex(matrix, -width, height, -width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, width, height, -width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, width, 0, -width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            
            buffer.vertex(matrix, width, 0, -width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            buffer.vertex(matrix, width, height, -width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, width, height, width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, width, 0, width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            
            buffer.vertex(matrix, width, 0, width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            buffer.vertex(matrix, width, height, width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, -width, height, width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, -width, 0, width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            
            buffer.vertex(matrix, -width, 0, width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            buffer.vertex(matrix, -width, height, width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, -width, height, -width).color(rgb[0], rgb[1], rgb[2], alpha * 0.5f).next();
            buffer.vertex(matrix, -width, 0, -width).color(rgb[0], rgb[1], rgb[2], alpha).next();
            
            Tessellator.getInstance().draw();
        }
    }
    
    /**
     * An explosion-like effect.
     */
    private static class ExplosionEffect extends VisualEffect {
        private final Random random = new Random();
        
        public ExplosionEffect(BlockPos pos, int color, int maxLifetime) {
            super(pos, color, maxLifetime);
        }
        
        @Override
        public void render(MatrixStack matrixStack, BufferBuilder buffer, float tickDelta) {
            // Calculate alpha based on lifetime (fade out)
            float alpha = 1.0f - ((float) lifetime / maxLifetime);
            
            // Get RGB components
            float[] rgb = getRGBComponents();
            
            // Calculate explosion radius based on lifetime
            float radius = 1.0f + (4.0f * ((float) lifetime / maxLifetime));
            
            // Render explosion particles
            buffer.begin(VertexFormat.DrawMode.TRIANGLE_FAN, VertexFormats.POSITION_COLOR);
            
            Matrix4f matrix = matrixStack.peek().getPositionMatrix();
            
            // Center vertex
            buffer.vertex(matrix, 0, 0, 0).color(rgb[0], rgb[1], rgb[2], alpha).next();
            
            // Outer vertices in a circle
            int segments = 16;
            for (int i = 0; i <= segments; i++) {
                float angle = (float) (i * Math.PI * 2 / segments);
                float x = (float) Math.sin(angle) * radius;
                float z = (float) Math.cos(angle) * radius;
                float y = random.nextFloat() * radius * 0.5f;
                
                buffer.vertex(matrix, x, y, z).color(rgb[0], rgb[1], rgb[2], 0).next();
            }
            
            Tessellator.getInstance().draw();
        }
    }
    
    /**
     * A particle effect with points of light.
     */
    private static class ParticleEffect extends VisualEffect {
        private final Random random = new Random();
        private final Vec3d[] particles;
        
        public ParticleEffect(BlockPos pos, int color, int maxLifetime) {
            super(pos, color, maxLifetime);
            
            // Generate random particle positions
            int particleCount = 50;
            particles = new Vec3d[particleCount];
            
            for (int i = 0; i < particleCount; i++) {
                particles[i] = new Vec3d(
                    (random.nextFloat() - 0.5f) * 3,
                    (random.nextFloat() - 0.5f) * 3,
                    (random.nextFloat() - 0.5f) * 3
                );
            }
        }
        
        @Override
        public void render(MatrixStack matrixStack, BufferBuilder buffer, float tickDelta) {
            // Calculate alpha based on lifetime (fade in and out)
            float progress = (float) lifetime / maxLifetime;
            float alpha = progress < 0.2f ? (progress / 0.2f) : (progress > 0.8f ? (1 - (progress - 0.8f) / 0.2f) : 1f);
            
            // Get RGB components
            float[] rgb = getRGBComponents();
            
            // Scale particles based on lifetime
            float scale = 0.1f + 0.4f * (float) Math.sin(Math.PI * progress);
            
            // Render particles
            buffer.begin(VertexFormat.DrawMode.QUADS, VertexFormats.POSITION_COLOR);
            
            Matrix4f matrix = matrixStack.peek().getPositionMatrix();
            
            for (Vec3d particle : particles) {
                // Scale the particle position based on lifetime
                float x = (float) particle.x * (1 + progress * 2);
                float y = (float) particle.y * (1 + progress * 2);
                float z = (float) particle.z * (1 + progress * 2);
                
                // Render a small quad for each particle
                buffer.vertex(matrix, x - scale, y - scale, z).color(rgb[0], rgb[1], rgb[2], alpha).next();
                buffer.vertex(matrix, x - scale, y + scale, z).color(rgb[0], rgb[1], rgb[2], alpha).next();
                buffer.vertex(matrix, x + scale, y + scale, z).color(rgb[0], rgb[1], rgb[2], alpha).next();
                buffer.vertex(matrix, x + scale, y - scale, z).color(rgb[0], rgb[1], rgb[2], alpha).next();
            }
            
            Tessellator.getInstance().draw();
        }
    }
} 