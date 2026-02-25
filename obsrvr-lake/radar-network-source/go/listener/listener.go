package listener

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	channel            = "scan_completed"
	pollInterval       = 10 * time.Minute
	reconnectBaseDelay = 1 * time.Second
	reconnectMaxDelay  = 60 * time.Second
)

// Listener uses PostgreSQL LISTEN/NOTIFY to receive scan completion events,
// with a fallback poll if no notification arrives within pollInterval.
type Listener struct {
	db          *pgxpool.Pool
	lastScanID  uint32
	scanIDChan  chan uint32
}

func New(db *pgxpool.Pool, startAfterScanID uint32) *Listener {
	return &Listener{
		db:         db,
		lastScanID: startAfterScanID,
		scanIDChan: make(chan uint32, 16),
	}
}

// ScanIDs returns a channel that emits scan IDs as they complete.
func (l *Listener) ScanIDs() <-chan uint32 {
	return l.scanIDChan
}

// Run starts the LISTEN loop with reconnect and fallback polling.
// It blocks until ctx is cancelled.
func (l *Listener) Run(ctx context.Context) error {
	defer close(l.scanIDChan)

	for {
		if err := l.listenLoop(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("LISTEN connection lost: %v, reconnecting...", err)
		}

		// Reconnect with exponential backoff
		delay := reconnectBaseDelay
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			if err := l.catchUp(ctx); err != nil {
				log.Printf("catch-up after reconnect failed: %v", err)
			}

			// Try to re-enter listen loop
			break
		}
	}
}

func (l *Listener) listenLoop(ctx context.Context) error {
	conn, err := l.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
	if err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}
	log.Printf("Listening on channel %q", channel)

	for {
		// Wait for notification with a poll timeout
		waitCtx, cancel := context.WithTimeout(ctx, pollInterval)
		notification, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Timeout — fallback poll
			if waitCtx.Err() != nil {
				log.Printf("No notification in %v, polling for new scans...", pollInterval)
				if pollErr := l.catchUp(ctx); pollErr != nil {
					log.Printf("poll catch-up error: %v", pollErr)
				}
				continue
			}
			return fmt.Errorf("wait for notification: %w", err)
		}

		scanID, err := strconv.ParseUint(notification.Payload, 10, 32)
		if err != nil {
			log.Printf("invalid notification payload %q: %v", notification.Payload, err)
			continue
		}

		sid := uint32(scanID)
		if sid > l.lastScanID {
			l.lastScanID = sid
			select {
			case l.scanIDChan <- sid:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// catchUp polls the database for any completed scans since lastScanID.
func (l *Listener) catchUp(ctx context.Context) error {
	rows, err := l.db.Query(ctx, `
		SELECT id FROM network_scan
		WHERE completed = true AND id > $1
		ORDER BY id ASC
	`, l.lastScanID)
	if err != nil {
		return fmt.Errorf("catch-up query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var scanID uint32
		if err := rows.Scan(&scanID); err != nil {
			return fmt.Errorf("catch-up scan: %w", err)
		}
		if scanID > l.lastScanID {
			l.lastScanID = scanID
			select {
			case l.scanIDChan <- scanID:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return rows.Err()
}
