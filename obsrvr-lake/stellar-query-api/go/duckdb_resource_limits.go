package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultDuckDBMemoryLimit      = "2GB"
	defaultDuckDBThreads          = "2"
	defaultDuckDBTempDirectory    = "/tmp/duckdb"
	defaultDuckDBMaxTempDirectory = "20GB"
	envDuckDBMemoryLimit          = "DUCKDB_MEMORY_LIMIT"
	envDuckDBThreads              = "DUCKDB_THREADS"
	envDuckDBTempDirectory        = "DUCKDB_TEMP_DIRECTORY"
	envDuckDBMaxTempDirectorySize = "DUCKDB_MAX_TEMP_DIRECTORY_SIZE"
)

func configureDuckDBForAPI(db *sql.DB) error {
	if db == nil {
		return nil
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	memoryLimit := envOrDefault(envDuckDBMemoryLimit, defaultDuckDBMemoryLimit)
	threads := envOrDefault(envDuckDBThreads, defaultDuckDBThreads)
	tempDirectory := envOrDefault(envDuckDBTempDirectory, defaultDuckDBTempDirectory)
	maxTempDirectorySize := envOrDefault(envDuckDBMaxTempDirectorySize, defaultDuckDBMaxTempDirectory)

	if err := os.MkdirAll(tempDirectory, 0o755); err != nil {
		return fmt.Errorf("failed to create DuckDB temp directory %q: %w", tempDirectory, err)
	}

	if _, err := db.Exec("SET memory_limit = '" + escapeDuckDBSetting(memoryLimit) + "'"); err != nil {
		return fmt.Errorf("failed to set DuckDB memory_limit: %w", err)
	}
	if _, err := strconv.Atoi(threads); err != nil {
		return fmt.Errorf("invalid %s %q: %w", envDuckDBThreads, threads, err)
	}
	if _, err := db.Exec("SET threads = " + threads); err != nil {
		return fmt.Errorf("failed to set DuckDB threads: %w", err)
	}
	if _, err := db.Exec("SET temp_directory = '" + escapeDuckDBSetting(tempDirectory) + "'"); err != nil {
		return fmt.Errorf("failed to set DuckDB temp_directory: %w", err)
	}
	if _, err := db.Exec("SET max_temp_directory_size = '" + escapeDuckDBSetting(maxTempDirectorySize) + "'"); err != nil {
		return fmt.Errorf("failed to set DuckDB max_temp_directory_size: %w", err)
	}

	return nil
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func escapeDuckDBSetting(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
