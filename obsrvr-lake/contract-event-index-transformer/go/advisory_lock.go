package main

import (
	"context"
	"database/sql"
	"fmt"
)

// AdvisoryLock holds a PostgreSQL session-level advisory lock on a dedicated connection.
type AdvisoryLock struct {
	db        *sql.DB
	conn      *sql.Conn
	namespace string
	resource  string
}

func NewAdvisoryLock(db *sql.DB, namespace, resource string) *AdvisoryLock {
	return &AdvisoryLock{db: db, namespace: namespace, resource: resource}
}

func (l *AdvisoryLock) Acquire(ctx context.Context) error {
	if l == nil || l.conn != nil {
		return nil
	}
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	var ok bool
	err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))`, l.namespace, l.resource).Scan(&ok)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("try advisory lock: %w", err)
	}
	if !ok {
		_ = conn.Close()
		return fmt.Errorf("another transformer instance already holds advisory lock namespace=%q resource=%q", l.namespace, l.resource)
	}
	l.conn = conn
	return nil
}

func (l *AdvisoryLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	var ok bool
	err := l.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock(hashtext($1), hashtext($2))`, l.namespace, l.resource).Scan(&ok)
	if err != nil {
		_ = l.conn.Close()
		l.conn = nil
		return fmt.Errorf("release advisory lock: %w", err)
	}
	closeErr := l.conn.Close()
	l.conn = nil
	if closeErr != nil {
		return fmt.Errorf("close advisory lock connection: %w", closeErr)
	}
	if !ok {
		return fmt.Errorf("advisory lock was not held at release time namespace=%q resource=%q", l.namespace, l.resource)
	}
	return nil
}
