// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library - Caching Layer
package resolver

import (
	"sync"
	"time"
)

// cache provides in-memory caching for era maps and coverage data.
type cache struct {
	mu       sync.RWMutex
	ttl      time.Duration
	eraMaps  map[string]*cacheEntry[[]Era]
	coverage map[string]*cacheEntry[*Coverage]
}

// cacheEntry wraps a cached value with expiration time.
type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
}

// newCache creates a new cache with the specified TTL.
func newCache(ttl time.Duration) *cache {
	return &cache{
		ttl:      ttl,
		eraMaps:  make(map[string]*cacheEntry[[]Era]),
		coverage: make(map[string]*cacheEntry[*Coverage]),
	}
}

// getEraMap retrieves a cached era map for a network.
func (c *cache) getEraMap(network string) ([]Era, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.eraMaps[network]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.value, true
}

// setEraMap caches an era map for a network.
func (c *cache) setEraMap(network string, eras []Era) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.eraMaps[network] = &cacheEntry[[]Era]{
		value:     eras,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// getCoverage retrieves cached coverage data.
func (c *cache) getCoverage(key string) (*Coverage, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.coverage[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.expiresAt) {
		return nil, false
	}

	return entry.value, true
}

// setCoverage caches coverage data.
func (c *cache) setCoverage(key string, cov *Coverage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.coverage[key] = &cacheEntry[*Coverage]{
		value:     cov,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// invalidate clears all cached data.
func (c *cache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.eraMaps = make(map[string]*cacheEntry[[]Era])
	c.coverage = make(map[string]*cacheEntry[*Coverage])
}

// invalidateNetwork clears cached data for a specific network.
func (c *cache) invalidateNetwork(network string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.eraMaps, network)

	// Clear coverage entries for this network
	for key := range c.coverage {
		// Coverage keys are formatted as "dataset:eraID:versionLabel"
		// We don't have network in the key, but we can invalidate all
		// This is a simple approach - a more sophisticated cache could track by network
		delete(c.coverage, key)
	}
}
