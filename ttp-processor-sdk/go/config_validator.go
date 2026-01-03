package main

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ValidationIssue represents a configuration validation issue
type ValidationIssue struct {
	Severity ErrorSeverity
	Field    string
	Message  string
	Hint     string
}

// ConfigValidator validates processor configuration
type ConfigValidator struct {
	issues []ValidationIssue
	logger *zap.Logger
}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator(logger *zap.Logger) *ConfigValidator {
	return &ConfigValidator{
		issues: make([]ValidationIssue, 0),
		logger: logger,
	}
}

// AddIssue adds a validation issue
func (cv *ConfigValidator) AddIssue(severity ErrorSeverity, field, message, hint string) {
	cv.issues = append(cv.issues, ValidationIssue{
		Severity: severity,
		Field:    field,
		Message:  message,
		Hint:     hint,
	})
}

// HasIssues returns true if any validation issues were found
func (cv *ConfigValidator) HasIssues() bool {
	return len(cv.issues) > 0
}

// HasFatalIssues returns true if any fatal validation issues were found
func (cv *ConfigValidator) HasFatalIssues() bool {
	for _, issue := range cv.issues {
		if issue.Severity == SeverityFatal {
			return true
		}
	}
	return false
}

// Issues returns all validation issues
func (cv *ConfigValidator) Issues() []ValidationIssue {
	return cv.issues
}

// ValidateNetworkPassphrase validates the network passphrase
func (cv *ConfigValidator) ValidateNetworkPassphrase(passphrase string) {
	if passphrase == "" {
		cv.AddIssue(
			SeverityFatal,
			"NETWORK_PASSPHRASE",
			"Network passphrase is required",
			"Set NETWORK_PASSPHRASE environment variable. Use 'Public Global Stellar Network ; September 2015' for mainnet or 'Test SDF Network ; September 2015' for testnet",
		)
		return
	}

	// Check for known passphrases
	knownPassphrases := []string{
		"Public Global Stellar Network ; September 2015",  // Mainnet
		"Test SDF Network ; September 2015",                // Testnet
		"Standalone Network ; February 2017",               // Local development
	}

	isKnown := false
	for _, known := range knownPassphrases {
		if passphrase == known {
			isKnown = true
			break
		}
	}

	if !isKnown {
		cv.AddIssue(
			SeverityWarning,
			"NETWORK_PASSPHRASE",
			"Unknown network passphrase",
			fmt.Sprintf("Using custom passphrase: '%s'. Verify this matches your network.", passphrase),
		)
	}
}

// ValidateFilterConfig validates filtering configuration
func (cv *ConfigValidator) ValidateFilterConfig(filterConfig *FilterConfig) {
	// Check for conflicting include/exclude
	if len(filterConfig.IncludeEventTypes) > 0 && len(filterConfig.ExcludeEventTypes) > 0 {
		// Check if any event types appear in both
		for _, included := range filterConfig.IncludeEventTypes {
			for _, excluded := range filterConfig.ExcludeEventTypes {
				if strings.EqualFold(included, excluded) {
					cv.AddIssue(
						SeverityWarning,
						"FILTER_EVENT_TYPES / EXCLUDE_EVENT_TYPES",
						fmt.Sprintf("Event type '%s' appears in both include and exclude lists", included),
						"EXCLUDE_EVENT_TYPES takes precedence. Remove from FILTER_EVENT_TYPES if you want to exclude it.",
					)
				}
			}
		}
	}

	// Check for valid event types
	validEventTypes := map[string]bool{
		"transfer": true,
		"mint":     true,
		"burn":     true,
		"clawback": true,
		"fee":      true,
	}

	for _, eventType := range filterConfig.IncludeEventTypes {
		if !validEventTypes[strings.ToLower(eventType)] {
			cv.AddIssue(
				SeverityWarning,
				"FILTER_EVENT_TYPES",
				fmt.Sprintf("Unknown event type: '%s'", eventType),
				"Valid event types are: transfer, mint, burn, clawback, fee",
			)
		}
	}

	for _, eventType := range filterConfig.ExcludeEventTypes {
		if !validEventTypes[strings.ToLower(eventType)] {
			cv.AddIssue(
				SeverityWarning,
				"EXCLUDE_EVENT_TYPES",
				fmt.Sprintf("Unknown event type: '%s'", eventType),
				"Valid event types are: transfer, mint, burn, clawback, fee",
			)
		}
	}

	// Check if all events are being filtered out
	if len(filterConfig.ExcludeEventTypes) == len(validEventTypes) {
		cv.AddIssue(
			SeverityError,
			"EXCLUDE_EVENT_TYPES",
			"All event types are excluded",
			"No events will be emitted. Remove some event types from EXCLUDE_EVENT_TYPES.",
		)
	}

	// Warn if filter might be too aggressive
	if filterConfig.MinAmount != nil && *filterConfig.MinAmount > 1000000000000 {
		cv.AddIssue(
			SeverityWarning,
			"FILTER_MIN_AMOUNT",
			"Very high minimum amount threshold",
			fmt.Sprintf("Filter set to %d stroops (%.2f XLM). Most events may be filtered out.", *filterConfig.MinAmount, float64(*filterConfig.MinAmount)/10000000.0),
		)
	}
}

// ValidateBatchConfig validates batch processing configuration
func (cv *ConfigValidator) ValidateBatchConfig(batchConfig *BatchConfig) {
	if !batchConfig.Enabled {
		return
	}

	// Check batch size
	if batchConfig.MaxBatchSize < 1 {
		cv.AddIssue(
			SeverityError,
			"BATCH_SIZE",
			"Batch size must be at least 1",
			"Set BATCH_SIZE to a positive integer (recommended: 10-100)",
		)
	}

	if batchConfig.MaxBatchSize > 1000 {
		cv.AddIssue(
			SeverityWarning,
			"BATCH_SIZE",
			"Very large batch size",
			fmt.Sprintf("Batch size of %d may cause high memory usage. Consider reducing to 10-100.", batchConfig.MaxBatchSize),
		)
	}

	// Check batch timeout
	if batchConfig.BatchTimeout.Seconds() < 1 {
		cv.AddIssue(
			SeverityWarning,
			"BATCH_TIMEOUT",
			"Very short batch timeout",
			fmt.Sprintf("Timeout of %v may cause frequent partial batches. Consider 5s or higher.", batchConfig.BatchTimeout),
		)
	}

	if batchConfig.BatchTimeout.Seconds() > 300 {
		cv.AddIssue(
			SeverityWarning,
			"BATCH_TIMEOUT",
			"Very long batch timeout",
			fmt.Sprintf("Timeout of %v may cause high latency. Consider 5-30s for most use cases.", batchConfig.BatchTimeout),
		)
	}

	// Check concurrency
	if batchConfig.ParallelProcessing {
		if batchConfig.MaxConcurrency < 1 {
			cv.AddIssue(
				SeverityError,
				"BATCH_CONCURRENCY",
				"Concurrency must be at least 1",
				"Set BATCH_CONCURRENCY to a positive integer (recommended: 2-8)",
			)
		}

		if batchConfig.MaxConcurrency > 32 {
			cv.AddIssue(
				SeverityWarning,
				"BATCH_CONCURRENCY",
				"Very high concurrency",
				fmt.Sprintf("Concurrency of %d may cause high CPU/memory usage. Consider 2-8 for most systems.", batchConfig.MaxConcurrency),
			)
		}
	}
}

// ValidateConfiguration validates all configuration and logs issues
func (cv *ConfigValidator) ValidateConfiguration(
	networkPassphrase string,
	filterConfig *FilterConfig,
	batchConfig *BatchConfig,
) bool {
	// Validate individual components
	cv.ValidateNetworkPassphrase(networkPassphrase)
	cv.ValidateFilterConfig(filterConfig)
	cv.ValidateBatchConfig(batchConfig)

	// Cross-component validation

	// Warn if batching is disabled but large filter is configured
	if !batchConfig.Enabled && filterConfig.Enabled {
		expectedReduction := 0
		if len(filterConfig.ExcludeEventTypes) > 0 {
			expectedReduction += 20 // Rough estimate
		}
		if filterConfig.MinAmount != nil {
			expectedReduction += 50
		}

		if expectedReduction > 50 {
			cv.AddIssue(
				SeverityWarning,
				"Configuration",
				"Filtering enabled but batching disabled",
				"Enable batching (ENABLE_BATCHING=true) to maximize performance benefits from filtering.",
			)
		}
	}

	// Log all issues
	if cv.HasIssues() {
		cv.logger.Info("Configuration validation complete",
			zap.Int("total_issues", len(cv.issues)),
			zap.Int("warnings", cv.CountBySeverity(SeverityWarning)),
			zap.Int("errors", cv.CountBySeverity(SeverityError)),
			zap.Int("fatal", cv.CountBySeverity(SeverityFatal)))

		for _, issue := range cv.issues {
			switch issue.Severity {
			case SeverityWarning:
				cv.logger.Warn("Configuration issue",
					zap.String("field", issue.Field),
					zap.String("message", issue.Message),
					zap.String("hint", issue.Hint))
			case SeverityError:
				cv.logger.Error("Configuration error",
					zap.String("field", issue.Field),
					zap.String("message", issue.Message),
					zap.String("hint", issue.Hint))
			case SeverityFatal:
				cv.logger.Fatal("Configuration fatal error",
					zap.String("field", issue.Field),
					zap.String("message", issue.Message),
					zap.String("hint", issue.Hint))
			}
		}
	} else {
		cv.logger.Info("Configuration validation passed - no issues found")
	}

	return !cv.HasFatalIssues()
}

// CountBySeverity returns the count of issues by severity
func (cv *ConfigValidator) CountBySeverity(severity ErrorSeverity) int {
	count := 0
	for _, issue := range cv.issues {
		if issue.Severity == severity {
			count++
		}
	}
	return count
}

// FormatIssues returns a formatted string of all issues
func (cv *ConfigValidator) FormatIssues() string {
	if len(cv.issues) == 0 {
		return "No configuration issues found"
	}

	output := fmt.Sprintf("Found %d configuration issue(s):\n\n", len(cv.issues))
	for i, issue := range cv.issues {
		output += fmt.Sprintf("%d. [%s] %s\n", i+1, issue.Severity, issue.Field)
		output += fmt.Sprintf("   %s\n", issue.Message)
		output += fmt.Sprintf("   💡 %s\n\n", issue.Hint)
	}
	return output
}
