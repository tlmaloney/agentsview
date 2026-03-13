package pgsync

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/wesm/agentsview/internal/db"
)

// PGSync manages push-only sync from local SQLite to a remote
// PostgreSQL database.
type PGSync struct {
	pg       *sql.DB
	local    *db.DB
	machine  string
	interval time.Duration

	closeOnce sync.Once
	closeErr  error

	schemaMu   sync.Mutex
	schemaDone bool
}

// New creates a PGSync instance and verifies the PG connection.
// The machine name must not be "local", which is reserved as the
// SQLite sentinel for sessions that originated on this machine.
func New(
	pgURL string, local *db.DB, machine string,
	interval time.Duration,
) (*PGSync, error) {
	if pgURL == "" {
		return nil, fmt.Errorf("postgres URL is required")
	}
	if machine == "" {
		return nil, fmt.Errorf("machine name must not be empty")
	}
	if machine == "local" {
		return nil, fmt.Errorf(
			"machine name %q is reserved; choose a different pg_sync.machine_name", machine,
		)
	}
	if local == nil {
		return nil, fmt.Errorf("local db is required")
	}
	pg, err := sql.Open("pgx", pgURL)
	if err != nil {
		return nil, fmt.Errorf("opening pg connection: %w", err)
	}
	pg.SetMaxOpenConns(5)
	pg.SetMaxIdleConns(5)
	pg.SetConnMaxLifetime(30 * time.Minute)
	pg.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()
	if err := pg.PingContext(ctx); err != nil {
		pg.Close()
		return nil, fmt.Errorf("pg ping failed: %w", err)
	}

	return &PGSync{
		pg:       pg,
		local:    local,
		machine:  machine,
		interval: interval,
	}, nil
}

// Close closes the PostgreSQL connection pool.
// Callers must ensure no Push operations are in-flight
// before calling Close; otherwise those operations will fail
// with connection errors.
func (p *PGSync) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.pg.Close()
	})
	return p.closeErr
}

// EnsureSchema creates the agentsview schema and tables in PG
// if they don't already exist. It also marks the schema as
// initialized so subsequent Push calls skip redundant checks.
func (p *PGSync) EnsureSchema(ctx context.Context) error {
	p.schemaMu.Lock()
	defer p.schemaMu.Unlock()
	if p.schemaDone {
		return nil
	}
	if err := ensureSchema(ctx, p.pg); err != nil {
		return err
	}
	p.schemaDone = true
	return nil
}

// EnsureSchemaDB creates the agentsview schema and tables in PG
// if they don't already exist.
func EnsureSchemaDB(ctx context.Context, pg *sql.DB) error {
	return ensureSchema(ctx, pg)
}

// StartPeriodicSync runs push on a recurring interval. It
// blocks until ctx is cancelled.
func (p *PGSync) StartPeriodicSync(ctx context.Context) {
	if p.interval <= 0 {
		log.Printf("pg sync: interval is %v; skipping periodic sync", p.interval)
		return
	}

	// Run once immediately at startup.
	p.runSyncCycle(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.runSyncCycle(ctx)
		}
	}
}

func (p *PGSync) runSyncCycle(ctx context.Context) {
	pushResult, err := p.Push(ctx, false)
	if err != nil {
		log.Printf("pg sync push error: %v", err)
		if ctx.Err() != nil {
			return
		}
	} else if pushResult.SessionsPushed > 0 {
		log.Printf(
			"pg sync push: %d sessions, %d messages in %s",
			pushResult.SessionsPushed,
			pushResult.MessagesPushed,
			pushResult.Duration.Round(time.Millisecond),
		)
	}
}

// Status returns sync status information.
// Sync state reads (last_push_at) are non-fatal because these
// are informational watermarks stored in SQLite. PG query
// failures are fatal because they indicate a connectivity
// problem that the caller needs to know about.
func (p *PGSync) Status(ctx context.Context) (SyncStatus, error) {
	lastPush, err := p.local.GetSyncState("last_push_at")
	if err != nil {
		log.Printf("warning: reading last_push_at: %v", err)
		lastPush = ""
	}

	var pgSessions int
	err = p.pg.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM agentsview.sessions",
	).Scan(&pgSessions)
	if err != nil {
		return SyncStatus{}, fmt.Errorf(
			"counting pg sessions: %w", err,
		)
	}

	var pgMessages int
	err = p.pg.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM agentsview.messages",
	).Scan(&pgMessages)
	if err != nil {
		return SyncStatus{}, fmt.Errorf(
			"counting pg messages: %w", err,
		)
	}

	return SyncStatus{
		Machine:    p.machine,
		LastPushAt: lastPush,
		PGSessions: pgSessions,
		PGMessages: pgMessages,
	}, nil
}

// SyncStatus holds summary information about the sync state.
type SyncStatus struct {
	Machine    string `json:"machine"`
	LastPushAt string `json:"last_push_at"`
	PGSessions int    `json:"pg_sessions"`
	PGMessages int    `json:"pg_messages"`
}
