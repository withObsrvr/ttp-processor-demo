package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// QueryRequest represents an incoming SQL query request
type QueryRequest struct {
	SQL    string `json:"sql"`
	Limit  int    `json:"limit,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

// QueryResponse represents the query result
type QueryResponse struct {
	Columns         []string        `json:"columns"`
	Rows            [][]interface{} `json:"rows"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs int64           `json:"execution_time_ms"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status      string `json:"status"`
	DBConnected bool   `json:"db_connected"`
}

// QueryServer provides HTTP endpoints for querying the DuckDB catalog
type QueryServer struct {
	db     *sql.DB
	router *http.ServeMux
	port   string
	server *http.Server
}

// NewQueryServer creates a new query server
func NewQueryServer(db *sql.DB, port string) *QueryServer {
	router := http.NewServeMux()

	qs := &QueryServer{
		db:     db,
		router: router,
		port:   port,
	}

	// Register handlers
	router.HandleFunc("/health", qs.handleHealth)
	router.HandleFunc("/query", qs.handleQuery)
	router.HandleFunc("/metrics", qs.handleMetrics)

	return qs
}

// Start starts the HTTP server
func (qs *QueryServer) Start() error {
	qs.server = &http.Server{
		Addr:         qs.port,
		Handler:      qs.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second, // Longer to handle query execution
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Query API server starting on %s", qs.port)
	log.Printf("Endpoints:")
	log.Printf("  GET  %s/health - Health check", qs.port)
	log.Printf("  POST %s/query  - Execute SQL query", qs.port)
	log.Printf("  GET  %s/metrics - Prometheus metrics", qs.port)

	if err := qs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("query server error: %w", err)
	}

	return nil
}

// Stop gracefully shuts down the HTTP server
func (qs *QueryServer) Stop(ctx context.Context) error {
	if qs.server != nil {
		log.Printf("Shutting down query API server...")
		return qs.server.Shutdown(ctx)
	}
	return nil
}

// handleHealth handles GET /health
func (qs *QueryServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check database connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dbConnected := false
	if err := qs.db.PingContext(ctx); err == nil {
		dbConnected = true
	}

	response := HealthResponse{
		Status:      "ok",
		DBConnected: dbConnected,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleQuery handles POST /query
func (qs *QueryServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		qs.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate SQL
	if err := qs.validateSQL(req.SQL); err != nil {
		qs.sendError(w, fmt.Sprintf("Invalid SQL: %v", err), http.StatusBadRequest)
		return
	}

	// Apply default limits
	if req.Limit == 0 {
		req.Limit = 1000 // Default limit
	}
	if req.Limit > 10000 {
		req.Limit = 10000 // Max limit
	}

	// Add LIMIT and OFFSET to query if not already present
	sql := req.SQL
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.Contains(sqlUpper, "LIMIT") {
		sql = fmt.Sprintf("%s LIMIT %d", sql, req.Limit)
	}
	if req.Offset > 0 && !strings.Contains(sqlUpper, "OFFSET") {
		sql = fmt.Sprintf("%s OFFSET %d", sql, req.Offset)
	}

	// Execute query with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	rows, err := qs.db.QueryContext(ctx, sql)
	if err != nil {
		qs.sendError(w, fmt.Sprintf("Query execution failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		qs.sendError(w, fmt.Sprintf("Failed to get columns: %v", err), http.StatusInternalServerError)
		return
	}

	// Fetch all rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold column values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			qs.sendError(w, fmt.Sprintf("Row scan failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Convert byte arrays to strings for JSON serialization
		row := make([]interface{}, len(values))
		for i, val := range values {
			if b, ok := val.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = val
			}
		}

		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		qs.sendError(w, fmt.Sprintf("Row iteration failed: %v", err), http.StatusInternalServerError)
		return
	}

	executionTime := time.Since(startTime)

	// Build response
	response := QueryResponse{
		Columns:         columns,
		Rows:            resultRows,
		RowCount:        len(resultRows),
		ExecutionTimeMs: executionTime.Milliseconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMetrics handles GET /metrics (Prometheus format)
func (qs *QueryServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get connection pool stats
	stats := qs.db.Stats()

	// Output Prometheus-format metrics
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# HELP ducklake_db_connections_open Number of open database connections\n")
	fmt.Fprintf(w, "# TYPE ducklake_db_connections_open gauge\n")
	fmt.Fprintf(w, "ducklake_db_connections_open %d\n", stats.OpenConnections)

	fmt.Fprintf(w, "# HELP ducklake_db_connections_in_use Number of connections currently in use\n")
	fmt.Fprintf(w, "# TYPE ducklake_db_connections_in_use gauge\n")
	fmt.Fprintf(w, "ducklake_db_connections_in_use %d\n", stats.InUse)

	fmt.Fprintf(w, "# HELP ducklake_db_connections_idle Number of idle connections\n")
	fmt.Fprintf(w, "# TYPE ducklake_db_connections_idle gauge\n")
	fmt.Fprintf(w, "ducklake_db_connections_idle %d\n", stats.Idle)

	fmt.Fprintf(w, "# HELP ducklake_db_wait_count Total number of connections waited for\n")
	fmt.Fprintf(w, "# TYPE ducklake_db_wait_count counter\n")
	fmt.Fprintf(w, "ducklake_db_wait_count %d\n", stats.WaitCount)

	fmt.Fprintf(w, "# HELP ducklake_db_wait_duration_seconds Total time blocked waiting for connections\n")
	fmt.Fprintf(w, "# TYPE ducklake_db_wait_duration_seconds counter\n")
	fmt.Fprintf(w, "ducklake_db_wait_duration_seconds %.3f\n", stats.WaitDuration.Seconds())
}

// validateSQL performs basic SQL validation to prevent dangerous operations
func (qs *QueryServer) validateSQL(sql string) error {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	// Prevent write operations
	dangerousKeywords := []string{
		"DROP",
		"DELETE",
		"TRUNCATE",
		"INSERT",
		"UPDATE",
		"ALTER",
		"CREATE",
		"GRANT",
		"REVOKE",
	}

	for _, keyword := range dangerousKeywords {
		if strings.Contains(sqlUpper, keyword) {
			return fmt.Errorf("operation not allowed: %s", keyword)
		}
	}

	// Require SELECT for read queries
	if !strings.HasPrefix(sqlUpper, "SELECT") &&
		!strings.HasPrefix(sqlUpper, "WITH") &&
		!strings.HasPrefix(sqlUpper, "SHOW") &&
		!strings.HasPrefix(sqlUpper, "DESCRIBE") &&
		!strings.HasPrefix(sqlUpper, "EXPLAIN") {
		return fmt.Errorf("only SELECT, WITH, SHOW, DESCRIBE, and EXPLAIN queries are allowed")
	}

	if len(sql) == 0 {
		return fmt.Errorf("empty query")
	}

	return nil
}

// sendError sends a JSON error response
func (qs *QueryServer) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}
