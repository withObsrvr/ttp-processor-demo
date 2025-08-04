package memory

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// PooledAllocator implements a memory pool for Arrow allocations
type PooledAllocator struct {
	memory.Allocator
	logger        *logging.ComponentLogger
	maxMemory     int64
	currentMemory int64
	allocations   map[uintptr]*allocation
	mu            sync.Mutex
	metrics       *MemoryMetrics
}

// allocation tracks individual memory allocations
type allocation struct {
	size      int64
	timestamp time.Time
	stack     string
}

// MemoryMetrics tracks memory usage statistics
type MemoryMetrics struct {
	TotalAllocations   int64
	TotalDeallocations int64
	PeakMemory         int64
	CurrentMemory      int64
	AllocationErrors   int64
	LastGCTime         time.Time
}

// NewPooledAllocator creates a new pooled memory allocator
func NewPooledAllocator(maxMemory int64, logger *logging.ComponentLogger) *PooledAllocator {
	return &PooledAllocator{
		Allocator:   memory.NewGoAllocator(),
		logger:      logger,
		maxMemory:   maxMemory,
		allocations: make(map[uintptr]*allocation),
		metrics:     &MemoryMetrics{},
	}
}

// Allocate allocates memory with tracking
func (p *PooledAllocator) Allocate(size int) []byte {
	// Check if allocation would exceed limit
	if atomic.LoadInt64(&p.currentMemory)+int64(size) > p.maxMemory {
		atomic.AddInt64(&p.metrics.AllocationErrors, 1)
		p.logger.Error().
			Int64("requested", int64(size)).
			Int64("current", atomic.LoadInt64(&p.currentMemory)).
			Int64("max", p.maxMemory).
			Msg("Memory allocation would exceed limit")
		
		// Try garbage collection
		p.runGC()
		
		// Retry after GC
		if atomic.LoadInt64(&p.currentMemory)+int64(size) > p.maxMemory {
			return nil
		}
	}

	// Allocate memory
	buf := p.Allocator.Allocate(size)
	if buf == nil {
		atomic.AddInt64(&p.metrics.AllocationErrors, 1)
		return nil
	}

	// Track allocation
	p.mu.Lock()
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	p.allocations[ptr] = &allocation{
		size:      int64(size),
		timestamp: time.Now(),
		stack:     getStackTrace(),
	}
	p.mu.Unlock()

	// Update metrics
	atomic.AddInt64(&p.currentMemory, int64(size))
	atomic.AddInt64(&p.metrics.TotalAllocations, 1)
	
	// Update peak memory
	current := atomic.LoadInt64(&p.currentMemory)
	for {
		peak := atomic.LoadInt64(&p.metrics.PeakMemory)
		if current <= peak || atomic.CompareAndSwapInt64(&p.metrics.PeakMemory, peak, current) {
			break
		}
	}

	p.logger.Debug().
		Int64("size", int64(size)).
		Int64("current_memory", current).
		Float64("usage_percent", float64(current)/float64(p.maxMemory)*100).
		Msg("Memory allocated")

	return buf
}

// Reallocate reallocates memory with tracking
func (p *PooledAllocator) Reallocate(size int, buf []byte) []byte {
	if len(buf) == 0 {
		return p.Allocate(size)
	}

	oldSize := len(buf)
	newBuf := p.Allocator.Reallocate(size, buf)
	
	if newBuf != nil {
		// Update tracking
		p.mu.Lock()
		oldPtr := uintptr(unsafe.Pointer(&buf[0]))
		if alloc, exists := p.allocations[oldPtr]; exists {
			delete(p.allocations, oldPtr)
			newPtr := uintptr(unsafe.Pointer(&newBuf[0]))
			p.allocations[newPtr] = &allocation{
				size:      int64(size),
				timestamp: time.Now(),
				stack:     alloc.stack,
			}
		}
		p.mu.Unlock()

		// Update memory usage
		delta := int64(size - oldSize)
		atomic.AddInt64(&p.currentMemory, delta)
	}

	return newBuf
}

// Free frees memory with tracking
func (p *PooledAllocator) Free(buf []byte) {
	if len(buf) == 0 {
		return
	}

	// Remove from tracking
	p.mu.Lock()
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	if alloc, exists := p.allocations[ptr]; exists {
		delete(p.allocations, ptr)
		atomic.AddInt64(&p.currentMemory, -alloc.size)
		atomic.AddInt64(&p.metrics.TotalDeallocations, 1)
	}
	p.mu.Unlock()

	// Free memory
	p.Allocator.Free(buf)
}

// GetMetrics returns current memory metrics
func (p *PooledAllocator) GetMetrics() MemoryMetrics {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	metrics := *p.metrics
	metrics.CurrentMemory = atomic.LoadInt64(&p.currentMemory)
	return metrics
}

// runGC runs garbage collection and cleans up leaked allocations
func (p *PooledAllocator) runGC() {
	runtime.GC()
	runtime.Gosched()
	
	p.metrics.LastGCTime = time.Now()
	
	p.logger.Info().
		Int64("memory_before", atomic.LoadInt64(&p.currentMemory)).
		Msg("Running garbage collection")
	
	// Clean up old allocations that might be leaked
	p.mu.Lock()
	now := time.Now()
	for ptr, alloc := range p.allocations {
		if now.Sub(alloc.timestamp) > 5*time.Minute {
			p.logger.Warn().
				Int64("size", alloc.size).
				Dur("age", now.Sub(alloc.timestamp)).
				Str("stack", alloc.stack).
				Msg("Cleaning up potentially leaked allocation")
			delete(p.allocations, ptr)
			atomic.AddInt64(&p.currentMemory, -alloc.size)
		}
	}
	p.mu.Unlock()
}

// getStackTrace returns a simplified stack trace for debugging
func getStackTrace() string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	
	var trace string
	for i := 0; i < 3; i++ {
		frame, more := frames.Next()
		if !more {
			break
		}
		if i > 0 {
			trace += " <- "
		}
		trace += fmt.Sprintf("%s:%d", frame.Function, frame.Line)
	}
	return trace
}

// MonitorMemory starts a goroutine to monitor memory usage
func (p *PooledAllocator) MonitorMemory(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		
		for range ticker.C {
			current := atomic.LoadInt64(&p.currentMemory)
			usage := float64(current) / float64(p.maxMemory) * 100
			
			p.logger.Info().
				Int64("current_memory", current).
				Int64("max_memory", p.maxMemory).
				Float64("usage_percent", usage).
				Int64("total_allocations", atomic.LoadInt64(&p.metrics.TotalAllocations)).
				Int64("total_deallocations", atomic.LoadInt64(&p.metrics.TotalDeallocations)).
				Int64("peak_memory", atomic.LoadInt64(&p.metrics.PeakMemory)).
				Msg("Memory pool status")
			
			// Run GC if memory usage is high
			if usage > 80 {
				p.runGC()
			}
		}
	}()
}