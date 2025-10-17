package server

import (
	"sync"
	"time"
)

// HistoricalCache provides caching for historical ledger data
type HistoricalCache struct {
	mu      sync.RWMutex
	entries map[uint32]*CacheEntry
	ttl     time.Duration
	maxSize int
	lru     []uint32 // Simple LRU tracking
}

// CacheEntry represents a cached ledger
type CacheEntry struct {
	Ledger    []byte // Raw XDR bytes
	Source    string // "local" or "historical"
	Timestamp time.Time
}

// NewHistoricalCache creates a new cache instance
func NewHistoricalCache(ttl time.Duration, maxSize int) *HistoricalCache {
	return &HistoricalCache{
		entries: make(map[uint32]*CacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
		lru:     make([]uint32, 0, maxSize),
	}
}

// Get retrieves a ledger from cache
func (c *HistoricalCache) Get(sequence uint32) ([]byte, string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[sequence]
	if !exists {
		return nil, "", false
	}

	// Check TTL
	if time.Since(entry.Timestamp) > c.ttl {
		return nil, "", false
	}

	// Update LRU on read
	c.updateLRU(sequence)

	return entry.Ledger, entry.Source, true
}

// Put stores a ledger in cache
func (c *HistoricalCache) Put(sequence uint32, ledgerXDR []byte, source string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict
	if len(c.entries) >= c.maxSize && c.entries[sequence] == nil {
		// Evict oldest entry
		if len(c.lru) > 0 {
			oldest := c.lru[0]
			delete(c.entries, oldest)
			c.lru = c.lru[1:]
		}
	}

	c.entries[sequence] = &CacheEntry{
		Ledger:    ledgerXDR,
		Source:    source,
		Timestamp: time.Now(),
	}

	c.updateLRU(sequence)
}

// updateLRU updates the LRU order (must be called with lock held)
func (c *HistoricalCache) updateLRU(sequence uint32) {
	// Remove from current position if exists
	for i, seq := range c.lru {
		if seq == sequence {
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	c.lru = append(c.lru, sequence)
}

// Clear removes all entries from cache
func (c *HistoricalCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[uint32]*CacheEntry)
	c.lru = c.lru[:0]
}

// Size returns the current number of cached entries
func (c *HistoricalCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// Stats returns cache statistics
func (c *HistoricalCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	localCount := 0
	historicalCount := 0
	totalBytes := 0

	for _, entry := range c.entries {
		if entry.Source == "local" {
			localCount++
		} else {
			historicalCount++
		}
		totalBytes += len(entry.Ledger)
	}

	return map[string]interface{}{
		"total_entries":     len(c.entries),
		"local_entries":     localCount,
		"historical_entries": historicalCount,
		"total_bytes":       totalBytes,
		"ttl_seconds":       c.ttl.Seconds(),
		"max_size":          c.maxSize,
	}
}

// PrefetchManager handles predictive prefetching of historical ledgers
type PrefetchManager struct {
	mu               sync.Mutex
	prefetchQueue    chan uint32
	activePrefetches map[uint32]bool
	maxConcurrent    int
	cache            *HistoricalCache
	fetchFunc        func(uint32) ([]byte, string, error)
}

// NewPrefetchManager creates a new prefetch manager
func NewPrefetchManager(cache *HistoricalCache, maxConcurrent int, fetchFunc func(uint32) ([]byte, string, error)) *PrefetchManager {
	return &PrefetchManager{
		prefetchQueue:    make(chan uint32, 1000),
		activePrefetches: make(map[uint32]bool),
		maxConcurrent:    maxConcurrent,
		cache:            cache,
		fetchFunc:        fetchFunc,
	}
}

// QueuePrefetch adds a ledger to the prefetch queue
func (pm *PrefetchManager) QueuePrefetch(sequence uint32) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Skip if already active or in cache
	if pm.activePrefetches[sequence] {
		return
	}

	_, _, exists := pm.cache.Get(sequence)
	if exists {
		return
	}

	select {
	case pm.prefetchQueue <- sequence:
		pm.activePrefetches[sequence] = true
	default:
		// Queue full, skip
	}
}

// Start begins the prefetch workers
func (pm *PrefetchManager) Start() {
	for i := 0; i < pm.maxConcurrent; i++ {
		go pm.worker()
	}
}

func (pm *PrefetchManager) worker() {
	for sequence := range pm.prefetchQueue {
		// Fetch the ledger
		ledgerXDR, source, err := pm.fetchFunc(sequence)
		if err == nil && ledgerXDR != nil {
			pm.cache.Put(sequence, ledgerXDR, source)
		}

		// Mark as no longer active
		pm.mu.Lock()
		delete(pm.activePrefetches, sequence)
		pm.mu.Unlock()
	}
}

// AccessPattern analyzes ledger access patterns for predictive prefetching
type AccessPattern struct {
	mu              sync.RWMutex
	recentAccesses  []uint32
	maxHistory      int
	sequentialCount int
	lastSequence    uint32
}

// NewAccessPattern creates a new access pattern analyzer
func NewAccessPattern(maxHistory int) *AccessPattern {
	return &AccessPattern{
		recentAccesses: make([]uint32, 0, maxHistory),
		maxHistory:     maxHistory,
	}
}

// Record adds a ledger access to the pattern history
func (ap *AccessPattern) Record(sequence uint32) {
	ap.mu.Lock()
	defer ap.mu.Unlock()

	// Check if sequential
	if ap.lastSequence > 0 && sequence == ap.lastSequence+1 {
		ap.sequentialCount++
	} else {
		ap.sequentialCount = 0
	}
	ap.lastSequence = sequence

	// Add to history
	ap.recentAccesses = append(ap.recentAccesses, sequence)
	if len(ap.recentAccesses) > ap.maxHistory {
		ap.recentAccesses = ap.recentAccesses[len(ap.recentAccesses)-ap.maxHistory:]
	}
}

// PredictNext predicts the next N ledgers likely to be accessed
func (ap *AccessPattern) PredictNext(n int) []uint32 {
	ap.mu.RLock()
	defer ap.mu.RUnlock()

	predictions := make([]uint32, 0, n)

	// If we've seen sequential access, predict continuation
	if ap.sequentialCount >= 3 && ap.lastSequence > 0 {
		for i := 1; i <= n; i++ {
			predictions = append(predictions, ap.lastSequence+uint32(i))
		}
		return predictions
	}

	// Otherwise, look for patterns in recent accesses
	// Simple approach: predict based on gaps
	if len(ap.recentAccesses) >= 2 {
		lastGap := ap.recentAccesses[len(ap.recentAccesses)-1] - ap.recentAccesses[len(ap.recentAccesses)-2]
		start := ap.lastSequence
		for i := 1; i <= n; i++ {
			predictions = append(predictions, start+lastGap*uint32(i))
		}
	}

	return predictions
}