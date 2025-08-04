package resilience

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxAttempts     int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	JitterFactor    float64
	RetryableErrors map[string]bool
}

// DefaultRetryPolicy returns a sensible default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:   5,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
		JitterFactor:  0.1,
		RetryableErrors: map[string]bool{
			"connection refused":     true,
			"connection reset":       true,
			"deadline exceeded":      true,
			"context deadline":       true,
			"temporary failure":      true,
			"resource exhausted":     true,
			"unavailable":           true,
		},
	}
}

// RetryManager handles retry logic with backoff
type RetryManager struct {
	policy  *RetryPolicy
	logger  *logging.ComponentLogger
	metrics *RetryMetrics
	mu      sync.RWMutex
}

// RetryMetrics tracks retry statistics
type RetryMetrics struct {
	TotalAttempts   int64
	SuccessfulRetries int64
	FailedRetries   int64
	TotalRetryTime  time.Duration
}

// NewRetryManager creates a new retry manager
func NewRetryManager(policy *RetryPolicy, logger *logging.ComponentLogger) *RetryManager {
	if policy == nil {
		policy = DefaultRetryPolicy()
	}
	
	return &RetryManager{
		policy:  policy,
		logger:  logger,
		metrics: &RetryMetrics{},
	}
}

// Execute executes a function with retry logic
func (rm *RetryManager) Execute(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	startTime := time.Now()
	
	for attempt := 1; attempt <= rm.policy.MaxAttempts; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// Execute the function
		err := fn()
		if err == nil {
			// Success
			if attempt > 1 {
				rm.recordSuccess(time.Since(startTime))
				rm.logger.Info().
					Str("operation", operation).
					Int("attempts", attempt).
					Dur("total_time", time.Since(startTime)).
					Msg("Operation succeeded after retry")
			}
			return nil
		}
		
		lastErr = err
		rm.recordAttempt()
		
		// Check if error is retryable
		if !rm.isRetryable(err) {
			rm.logger.Debug().
				Str("operation", operation).
				Err(err).
				Msg("Error is not retryable")
			return err
		}
		
		// Check if we've exhausted attempts
		if attempt >= rm.policy.MaxAttempts {
			rm.recordFailure(time.Since(startTime))
			rm.logger.Error().
				Str("operation", operation).
				Int("attempts", attempt).
				Err(err).
				Msg("Operation failed after max attempts")
			return fmt.Errorf("operation failed after %d attempts: %w", attempt, err)
		}
		
		// Calculate backoff delay
		delay := rm.calculateDelay(attempt)
		
		rm.logger.Warn().
			Str("operation", operation).
			Int("attempt", attempt).
			Dur("retry_in", delay).
			Err(err).
			Msg("Operation failed, retrying")
		
		// Wait before retry
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	
	return lastErr
}

// ExecuteWithResult executes a function that returns a value with retry logic
func (rm *RetryManager) ExecuteWithResult[T any](ctx context.Context, operation string, fn func() (T, error)) (T, error) {
	var result T
	err := rm.Execute(ctx, operation, func() error {
		var fnErr error
		result, fnErr = fn()
		return fnErr
	})
	return result, err
}

// isRetryable determines if an error should trigger a retry
func (rm *RetryManager) isRetryable(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	for pattern := range rm.policy.RetryableErrors {
		if containsString(errStr, pattern) {
			return true
		}
	}
	
	// Check for context errors
	if err == context.DeadlineExceeded || err == context.Canceled {
		return false
	}
	
	return false
}

// calculateDelay calculates the delay before the next retry
func (rm *RetryManager) calculateDelay(attempt int) time.Duration {
	// Exponential backoff
	delay := float64(rm.policy.InitialDelay) * math.Pow(rm.policy.BackoffFactor, float64(attempt-1))
	
	// Apply jitter
	if rm.policy.JitterFactor > 0 {
		jitter := delay * rm.policy.JitterFactor * (2*rand.Float64() - 1)
		delay += jitter
	}
	
	// Cap at max delay
	if delay > float64(rm.policy.MaxDelay) {
		delay = float64(rm.policy.MaxDelay)
	}
	
	return time.Duration(delay)
}

// recordAttempt records a retry attempt
func (rm *RetryManager) recordAttempt() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.metrics.TotalAttempts++
}

// recordSuccess records a successful retry
func (rm *RetryManager) recordSuccess(duration time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.metrics.SuccessfulRetries++
	rm.metrics.TotalRetryTime += duration
}

// recordFailure records a failed retry sequence
func (rm *RetryManager) recordFailure(duration time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.metrics.FailedRetries++
	rm.metrics.TotalRetryTime += duration
}

// GetMetrics returns retry metrics
func (rm *RetryManager) GetMetrics() RetryMetrics {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return *rm.metrics
}

// CircuitBreaker implements circuit breaker pattern for fault tolerance
type CircuitBreaker struct {
	name            string
	logger          *logging.ComponentLogger
	maxFailures     int
	resetTimeout    time.Duration
	halfOpenTimeout time.Duration
	
	mu              sync.RWMutex
	state           CircuitState
	failures        int
	lastFailureTime time.Time
	successCount    int
}

// CircuitState represents the state of the circuit breaker
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string, maxFailures int, resetTimeout time.Duration, logger *logging.ComponentLogger) *CircuitBreaker {
	return &CircuitBreaker{
		name:            name,
		logger:          logger,
		maxFailures:     maxFailures,
		resetTimeout:    resetTimeout,
		halfOpenTimeout: resetTimeout / 2,
		state:           StateClosed,
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.canExecute() {
		return fmt.Errorf("circuit breaker is open for %s", cb.name)
	}
	
	err := fn()
	cb.recordResult(err)
	return err
}

// canExecute checks if execution is allowed
func (cb *CircuitBreaker) canExecute() bool {
	cb.mu.RLock()
	state := cb.state
	lastFailure := cb.lastFailureTime
	cb.mu.RUnlock()
	
	switch state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(lastFailure) > cb.resetTimeout {
			cb.mu.Lock()
			cb.state = StateHalfOpen
			cb.successCount = 0
			cb.mu.Unlock()
			
			cb.logger.Info().
				Str("circuit", cb.name).
				Msg("Circuit breaker transitioning to half-open")
			
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return false
	}
}

// recordResult records the result of an execution
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if err == nil {
		// Success
		cb.failures = 0
		
		if cb.state == StateHalfOpen {
			cb.successCount++
			// Require multiple successes to close
			if cb.successCount >= 3 {
				cb.state = StateClosed
				cb.logger.Info().
					Str("circuit", cb.name).
					Msg("Circuit breaker closed after successful recovery")
			}
		}
	} else {
		// Failure
		cb.failures++
		cb.lastFailureTime = time.Now()
		
		if cb.state == StateHalfOpen {
			// Immediately open on failure in half-open state
			cb.state = StateOpen
			cb.logger.Warn().
				Str("circuit", cb.name).
				Err(err).
				Msg("Circuit breaker opened due to failure in half-open state")
		} else if cb.failures >= cb.maxFailures {
			// Open the circuit
			cb.state = StateOpen
			cb.logger.Error().
				Str("circuit", cb.name).
				Int("failures", cb.failures).
				Err(err).
				Msg("Circuit breaker opened due to excessive failures")
		}
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset manually resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.state = StateClosed
	cb.failures = 0
	cb.successCount = 0
	
	cb.logger.Info().
		Str("circuit", cb.name).
		Msg("Circuit breaker manually reset")
}

// containsString checks if a string contains a substring (case-insensitive)
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && containsSubstring(toLowerCase(s), toLowerCase(substr))
}

func containsSubstring(s, substr string) bool {
	return indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func toLowerCase(s string) string {
	// Simple lowercase conversion for ASCII
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}