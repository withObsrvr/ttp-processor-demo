package analytics

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/logging"
)

// AnalyticsEngine provides real-time analytics on Arrow data
type AnalyticsEngine struct {
	allocator memory.Allocator
	logger    *logging.ComponentLogger
	
	// Active aggregations
	aggregations map[string]*Aggregation
	mu           sync.RWMutex
}

// Aggregation represents an active analytics aggregation
type Aggregation struct {
	ID          string
	Type        AggregationType
	WindowSize  time.Duration
	GroupBy     []string
	Filters     []Filter
	LastUpdate  time.Time
	
	// Current state
	data        []arrow.Record
	results     *AggregationResult
	mu          sync.RWMutex
}

// AggregationType defines the type of aggregation
type AggregationType string

const (
	AggregationSum          AggregationType = "sum"
	AggregationAvg          AggregationType = "avg"
	AggregationCount        AggregationType = "count"
	AggregationMin          AggregationType = "min"
	AggregationMax          AggregationType = "max"
	AggregationDistinct     AggregationType = "distinct"
	AggregationPercentile   AggregationType = "percentile"
	AggregationStdDev       AggregationType = "stddev"
)

// Filter represents a data filter
type Filter struct {
	Field    string
	Operator FilterOperator
	Value    interface{}
}

// FilterOperator defines filter operations
type FilterOperator string

const (
	FilterEquals        FilterOperator = "="
	FilterNotEquals     FilterOperator = "!="
	FilterGreaterThan   FilterOperator = ">"
	FilterLessThan      FilterOperator = "<"
	FilterGreaterEqual  FilterOperator = ">="
	FilterLessEqual     FilterOperator = "<="
	FilterIn            FilterOperator = "in"
	FilterNotIn         FilterOperator = "not_in"
	FilterContains      FilterOperator = "contains"
)

// AggregationResult holds the results of an aggregation
type AggregationResult struct {
	Timestamp   time.Time
	WindowStart time.Time
	WindowEnd   time.Time
	Groups      map[string]*GroupResult
	TotalRows   int64
}

// GroupResult holds results for a specific group
type GroupResult struct {
	GroupKey string
	Values   map[string]interface{}
	Count    int64
}

// NewAnalyticsEngine creates a new analytics engine
func NewAnalyticsEngine(allocator memory.Allocator, logger *logging.ComponentLogger) *AnalyticsEngine {
	return &AnalyticsEngine{
		allocator:    allocator,
		logger:       logger,
		aggregations: make(map[string]*Aggregation),
	}
}

// CreateAggregation creates a new aggregation
func (ae *AnalyticsEngine) CreateAggregation(id string, aggType AggregationType, 
	windowSize time.Duration, groupBy []string, filters []Filter) (*Aggregation, error) {
	
	ae.mu.Lock()
	defer ae.mu.Unlock()
	
	if _, exists := ae.aggregations[id]; exists {
		return nil, fmt.Errorf("aggregation %s already exists", id)
	}
	
	agg := &Aggregation{
		ID:         id,
		Type:       aggType,
		WindowSize: windowSize,
		GroupBy:    groupBy,
		Filters:    filters,
		LastUpdate: time.Now(),
		data:       make([]arrow.Record, 0),
		results:    &AggregationResult{Groups: make(map[string]*GroupResult)},
	}
	
	ae.aggregations[id] = agg
	
	ae.logger.Info().
		Str("aggregation_id", id).
		Str("type", string(aggType)).
		Dur("window_size", windowSize).
		Strs("group_by", groupBy).
		Int("filters", len(filters)).
		Msg("Created aggregation")
	
	return agg, nil
}

// ProcessRecord processes an Arrow record through active aggregations
func (ae *AnalyticsEngine) ProcessRecord(record arrow.Record) error {
	ae.mu.RLock()
	aggregations := make([]*Aggregation, 0, len(ae.aggregations))
	for _, agg := range ae.aggregations {
		aggregations = append(aggregations, agg)
	}
	ae.mu.RUnlock()
	
	// Process each aggregation
	for _, agg := range aggregations {
		if err := ae.processAggregation(agg, record); err != nil {
			ae.logger.Error().
				Err(err).
				Str("aggregation_id", agg.ID).
				Msg("Failed to process aggregation")
		}
	}
	
	return nil
}

// processAggregation processes a record for a specific aggregation
func (ae *AnalyticsEngine) processAggregation(agg *Aggregation, record arrow.Record) error {
	agg.mu.Lock()
	defer agg.mu.Unlock()
	
	// Apply filters
	filteredRecord, err := ae.applyFilters(record, agg.Filters)
	if err != nil {
		return fmt.Errorf("failed to apply filters: %w", err)
	}
	if filteredRecord == nil || filteredRecord.NumRows() == 0 {
		return nil // No matching rows
	}
	defer filteredRecord.Release()
	
	// Add to window data
	agg.data = append(agg.data, filteredRecord)
	
	// Clean old data outside window
	ae.cleanOldData(agg)
	
	// Recompute aggregation
	return ae.computeAggregation(agg)
}

// applyFilters applies filters to a record (simplified implementation)
func (ae *AnalyticsEngine) applyFilters(record arrow.Record, filters []Filter) (arrow.Record, error) {
	if len(filters) == 0 {
		record.Retain()
		return record, nil
	}
	
	// For Phase 3, we'll implement a simple row-by-row filter
	// In production, you'd use Arrow compute kernels for better performance
	
	// Create a boolean array to track which rows pass filters
	passFilter := make([]bool, record.NumRows())
	for i := range passFilter {
		passFilter[i] = true
	}
	
	// Apply each filter
	for _, filter := range filters {
		// Get field index
		fieldIdx := -1
		for j, field := range record.Schema().Fields() {
			if field.Name == filter.Field {
				fieldIdx = j
				break
			}
		}
		if fieldIdx < 0 {
			continue // Skip unknown fields
		}
		
		col := record.Column(fieldIdx)
		
		// Apply filter based on type and operator
		switch filter.Operator {
		case FilterEquals:
			ae.applyEqualsFilter(col, filter.Value, passFilter)
		case FilterGreaterThan:
			ae.applyGreaterThanFilter(col, filter.Value, passFilter)
		// Add more operators as needed
		}
	}
	
	// Count passing rows
	passCount := 0
	for _, pass := range passFilter {
		if pass {
			passCount++
		}
	}
	
	if passCount == 0 {
		return nil, nil // No rows pass filter
	}
	
	if passCount == int(record.NumRows()) {
		record.Retain()
		return record, nil // All rows pass
	}
	
	// For simplicity, return the original record
	// In production, you'd create a new record with only passing rows
	record.Retain()
	return record, nil
}

// applyEqualsFilter applies equals filter to a column
func (ae *AnalyticsEngine) applyEqualsFilter(col arrow.Array, value interface{}, passFilter []bool) {
	switch col.DataType().ID() {
	case arrow.STRING:
		strCol := col.(*array.String)
		strValue, ok := value.(string)
		if !ok {
			return
		}
		for i := 0; i < strCol.Len(); i++ {
			if passFilter[i] && (!strCol.IsValid(i) || strCol.Value(i) != strValue) {
				passFilter[i] = false
			}
		}
	case arrow.INT64:
		int64Col := col.(*array.Int64)
		int64Value, ok := value.(int64)
		if !ok {
			return
		}
		for i := 0; i < int64Col.Len(); i++ {
			if passFilter[i] && (!int64Col.IsValid(i) || int64Col.Value(i) != int64Value) {
				passFilter[i] = false
			}
		}
	}
}

// applyGreaterThanFilter applies greater than filter to a column
func (ae *AnalyticsEngine) applyGreaterThanFilter(col arrow.Array, value interface{}, passFilter []bool) {
	switch col.DataType().ID() {
	case arrow.INT64:
		int64Col := col.(*array.Int64)
		int64Value, ok := value.(int64)
		if !ok {
			return
		}
		for i := 0; i < int64Col.Len(); i++ {
			if passFilter[i] && (!int64Col.IsValid(i) || int64Col.Value(i) <= int64Value) {
				passFilter[i] = false
			}
		}
	case arrow.FLOAT64:
		float64Col := col.(*array.Float64)
		float64Value, ok := value.(float64)
		if !ok {
			return
		}
		for i := 0; i < float64Col.Len(); i++ {
			if passFilter[i] && (!float64Col.IsValid(i) || float64Col.Value(i) <= float64Value) {
				passFilter[i] = false
			}
		}
	}
}

// cleanOldData removes data outside the aggregation window
func (ae *AnalyticsEngine) cleanOldData(agg *Aggregation) {
	// For Phase 3, we'll keep a simple number of records
	// In production, you'd check actual timestamps
	maxRecords := 100
	
	if len(agg.data) > maxRecords {
		// Remove oldest records
		toRemove := len(agg.data) - maxRecords
		for i := 0; i < toRemove; i++ {
			agg.data[i].Release()
		}
		agg.data = agg.data[toRemove:]
	}
}

// computeAggregation computes the aggregation results
func (ae *AnalyticsEngine) computeAggregation(agg *Aggregation) error {
	// Combine all records in window
	if len(agg.data) == 0 {
		return nil
	}
	
	// For simplicity, we'll process the most recent record
	// In practice, this would combine all records in the window
	record := agg.data[len(agg.data)-1]
	
	result := &AggregationResult{
		Timestamp:   time.Now(),
		WindowStart: time.Now().Add(-agg.WindowSize),
		WindowEnd:   time.Now(),
		Groups:      make(map[string]*GroupResult),
		TotalRows:   int64(record.NumRows()),
	}
	
	// Perform aggregation based on type
	switch agg.Type {
	case AggregationCount:
		result.Groups["_total"] = &GroupResult{
			GroupKey: "_total",
			Values:   map[string]interface{}{"count": record.NumRows()},
			Count:    int64(record.NumRows()),
		}
		
	case AggregationSum:
		// Example: sum transaction amounts
		for i, field := range record.Schema().Fields() {
			if field.Name == "amount" {
				col := record.Column(i)
				if col.DataType().ID() == arrow.INT64 {
					int64Col := col.(*array.Int64)
					sum := int64(0)
					for j := 0; j < int64Col.Len(); j++ {
						if !int64Col.IsNull(j) {
							sum += int64Col.Value(j)
						}
					}
					result.Groups["_total"] = &GroupResult{
						GroupKey: "_total",
						Values:   map[string]interface{}{"sum": sum},
						Count:    int64(record.NumRows()),
					}
				}
			}
		}
		
	// Add more aggregation types as needed
	}
	
	agg.results = result
	agg.LastUpdate = time.Now()
	
	return nil
}

// GetResults returns the current aggregation results
func (ae *AnalyticsEngine) GetResults(aggregationID string) (*AggregationResult, error) {
	ae.mu.RLock()
	agg, exists := ae.aggregations[aggregationID]
	ae.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("aggregation %s not found", aggregationID)
	}
	
	agg.mu.RLock()
	defer agg.mu.RUnlock()
	
	// Return a copy of results
	resultCopy := *agg.results
	resultCopy.Groups = make(map[string]*GroupResult)
	for k, v := range agg.results.Groups {
		groupCopy := *v
		groupCopy.Values = make(map[string]interface{})
		for vk, vv := range v.Values {
			groupCopy.Values[vk] = vv
		}
		resultCopy.Groups[k] = &groupCopy
	}
	
	return &resultCopy, nil
}

// TTPAnalytics provides TTP-specific analytics
type TTPAnalytics struct {
	engine *AnalyticsEngine
	logger *logging.ComponentLogger
}

// NewTTPAnalytics creates TTP-specific analytics
func NewTTPAnalytics(engine *AnalyticsEngine, logger *logging.ComponentLogger) *TTPAnalytics {
	return &TTPAnalytics{
		engine: engine,
		logger: logger,
	}
}

// AnalyzePaymentVolume analyzes payment volume over time
func (ta *TTPAnalytics) AnalyzePaymentVolume(windowSize time.Duration, assetFilter string) (*PaymentVolumeResult, error) {
	filters := []Filter{}
	if assetFilter != "" {
		filters = append(filters, Filter{
			Field:    "asset_code",
			Operator: FilterEquals,
			Value:    assetFilter,
		})
	}
	
	agg, err := ta.engine.CreateAggregation(
		fmt.Sprintf("payment_volume_%s", time.Now().Format("20060102150405")),
		AggregationSum,
		windowSize,
		[]string{"asset_code"},
		filters,
	)
	if err != nil {
		return nil, err
	}
	
	// Return aggregation ID for tracking
	return &PaymentVolumeResult{
		AggregationID: agg.ID,
		WindowSize:    windowSize,
		AssetFilter:   assetFilter,
		Created:       time.Now(),
	}, nil
}

// PaymentVolumeResult represents payment volume analysis results
type PaymentVolumeResult struct {
	AggregationID string
	WindowSize    time.Duration
	AssetFilter   string
	Created       time.Time
}

// GetPaymentVolumeResults retrieves payment volume results
func (ta *TTPAnalytics) GetPaymentVolumeResults(pvr *PaymentVolumeResult) (*PaymentVolumeData, error) {
	results, err := ta.engine.GetResults(pvr.AggregationID)
	if err != nil {
		return nil, err
	}
	
	data := &PaymentVolumeData{
		Timestamp:   results.Timestamp,
		WindowStart: results.WindowStart,
		WindowEnd:   results.WindowEnd,
		Volumes:     make(map[string]int64),
	}
	
	for _, group := range results.Groups {
		if volume, ok := group.Values["sum"].(int64); ok {
			data.Volumes[group.GroupKey] = volume
		}
	}
	
	return data, nil
}

// PaymentVolumeData contains payment volume data
type PaymentVolumeData struct {
	Timestamp   time.Time
	WindowStart time.Time
	WindowEnd   time.Time
	Volumes     map[string]int64 // asset_code -> volume
}