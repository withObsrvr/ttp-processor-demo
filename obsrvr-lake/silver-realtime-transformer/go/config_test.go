package main

import (
	"strings"
	"testing"
)

func TestDatabaseConnectionStringIncludesOptionalQueryExecMode(t *testing.T) {
	config := DatabaseConfig{
		Host:          "localhost",
		Port:          6432,
		Database:      "silver_hot",
		User:          "obsrvr",
		Password:      "secret",
		SSLMode:       "disable",
		QueryExecMode: "exec",
	}

	dsn := config.ConnectionString()
	if !strings.Contains(dsn, "default_query_exec_mode=exec") {
		t.Fatalf("connection string does not include query exec mode: %s", dsn)
	}

	config.QueryExecMode = ""
	if strings.Contains(config.ConnectionString(), "default_query_exec_mode") {
		t.Fatal("connection string includes query exec mode when it is unset")
	}
}
