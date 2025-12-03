package main

import (
	"fmt"
	"time"
)

// ErrorSeverity defines the severity level of errors
type ErrorSeverity string

const (
	// SeverityWarning indicates a non-critical issue that doesn't prevent processing
	SeverityWarning ErrorSeverity = "WARNING"

	// SeverityError indicates a failure that affects some events but allows partial success
	SeverityError ErrorSeverity = "ERROR"

	// SeverityFatal indicates a critical failure that prevents all processing
	SeverityFatal ErrorSeverity = "FATAL"
)

// ProcessingError represents an error that occurred during event processing
type ProcessingError struct {
	// Severity level
	Severity ErrorSeverity

	// Error message
	Message string

	// Context about where the error occurred
	Context map[string]string

	// Original error
	Err error

	// Timestamp when error occurred
	Timestamp time.Time
}

// Error implements the error interface
func (e *ProcessingError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Severity, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Severity, e.Message)
}

// NewProcessingError creates a new processing error
func NewProcessingError(severity ErrorSeverity, message string, err error, context map[string]string) *ProcessingError {
	return &ProcessingError{
		Severity:  severity,
		Message:   message,
		Err:       err,
		Context:   context,
		Timestamp: time.Now(),
	}
}

// ErrorCollector collects errors during processing
type ErrorCollector struct {
	errors []*ProcessingError
}

// NewErrorCollector creates a new error collector
func NewErrorCollector() *ErrorCollector {
	return &ErrorCollector{
		errors: make([]*ProcessingError, 0),
	}
}

// Add adds an error to the collector
func (ec *ErrorCollector) Add(err *ProcessingError) {
	ec.errors = append(ec.errors, err)
}

// AddError adds an error with severity and context
func (ec *ErrorCollector) AddError(severity ErrorSeverity, message string, err error, context map[string]string) {
	ec.Add(NewProcessingError(severity, message, err, context))
}

// HasErrors returns true if any errors were collected
func (ec *ErrorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}

// HasFatalErrors returns true if any fatal errors were collected
func (ec *ErrorCollector) HasFatalErrors() bool {
	for _, err := range ec.errors {
		if err.Severity == SeverityFatal {
			return true
		}
	}
	return false
}

// Errors returns all collected errors
func (ec *ErrorCollector) Errors() []*ProcessingError {
	return ec.errors
}

// ErrorsBySeverity returns errors filtered by severity
func (ec *ErrorCollector) ErrorsBySeverity(severity ErrorSeverity) []*ProcessingError {
	result := make([]*ProcessingError, 0)
	for _, err := range ec.errors {
		if err.Severity == severity {
			result = append(result, err)
		}
	}
	return result
}

// Count returns the total number of errors
func (ec *ErrorCollector) Count() int {
	return len(ec.errors)
}

// CountBySeverity returns the count of errors for a specific severity
func (ec *ErrorCollector) CountBySeverity(severity ErrorSeverity) int {
	count := 0
	for _, err := range ec.errors {
		if err.Severity == severity {
			count++
		}
	}
	return count
}

// Summary returns a summary of errors by severity
func (ec *ErrorCollector) Summary() map[string]int {
	return map[string]int{
		"warnings": ec.CountBySeverity(SeverityWarning),
		"errors":   ec.CountBySeverity(SeverityError),
		"fatal":    ec.CountBySeverity(SeverityFatal),
		"total":    ec.Count(),
	}
}

// Clear clears all collected errors
func (ec *ErrorCollector) Clear() {
	ec.errors = make([]*ProcessingError, 0)
}

// ProcessingResult represents the result of processing with partial success support
type ProcessingResult struct {
	// Number of items successfully processed
	SuccessCount int

	// Number of items that failed
	FailureCount int

	// Collected errors
	Errors *ErrorCollector

	// Additional context
	Context map[string]string
}

// NewProcessingResult creates a new processing result
func NewProcessingResult() *ProcessingResult {
	return &ProcessingResult{
		Errors:  NewErrorCollector(),
		Context: make(map[string]string),
	}
}

// IsPartialSuccess returns true if some items succeeded and some failed
func (pr *ProcessingResult) IsPartialSuccess() bool {
	return pr.SuccessCount > 0 && pr.FailureCount > 0
}

// IsCompleteSuccess returns true if all items succeeded
func (pr *ProcessingResult) IsCompleteSuccess() bool {
	return pr.SuccessCount > 0 && pr.FailureCount == 0 && !pr.Errors.HasErrors()
}

// IsCompleteFailure returns true if all items failed
func (pr *ProcessingResult) IsCompleteFailure() bool {
	return pr.SuccessCount == 0 && (pr.FailureCount > 0 || pr.Errors.HasFatalErrors())
}

// ShouldProceed returns true if processing should continue (no fatal errors)
func (pr *ProcessingResult) ShouldProceed() bool {
	return !pr.Errors.HasFatalErrors()
}

// Summary returns a summary of the processing result
func (pr *ProcessingResult) Summary() map[string]interface{} {
	return map[string]interface{}{
		"success_count":    pr.SuccessCount,
		"failure_count":    pr.FailureCount,
		"partial_success":  pr.IsPartialSuccess(),
		"complete_success": pr.IsCompleteSuccess(),
		"complete_failure": pr.IsCompleteFailure(),
		"errors":           pr.Errors.Summary(),
	}
}
