package pgdb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/wesm/agentsview/internal/db"
)

// Compile-time check: *PGDB satisfies db.Store.
var _ db.Store = (*PGDB)(nil)

// redactDSN returns the host portion of the DSN for diagnostics,
// stripping credentials, query parameters, and path components
// that may contain secrets.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid-dsn>"
	}
	return u.Hostname()
}

// New opens a PostgreSQL connection and returns a PGDB.
func New(pgURL string) (*PGDB, error) {
	pg, err := sql.Open("pgx", pgURL)
	if err != nil {
		return nil, fmt.Errorf("opening pg (host=%s): %w",
			redactDSN(pgURL), err)
	}
	if err := pg.Ping(); err != nil {
		pg.Close()
		return nil, fmt.Errorf("pg ping (host=%s): %w",
			redactDSN(pgURL), err)
	}
	pg.SetMaxOpenConns(4)
	return &PGDB{pg: pg}, nil
}

// Close closes the underlying database connection.
func (p *PGDB) Close() error {
	return p.pg.Close()
}

// SetCursorSecret sets the HMAC key used for cursor signing.
func (p *PGDB) SetCursorSecret(secret []byte) {
	p.cursorMu.Lock()
	defer p.cursorMu.Unlock()
	p.cursorSecret = append([]byte(nil), secret...)
}

// ReadOnly returns true; this is a read-only data source.
func (p *PGDB) ReadOnly() bool { return true }

// ------------------------------------------------------------
// Write stubs (all return db.ErrReadOnly)
// ------------------------------------------------------------

// StarSession is not supported in read-only mode.
func (p *PGDB) StarSession(_ string) (bool, error) {
	return false, db.ErrReadOnly
}

// UnstarSession is not supported in read-only mode.
func (p *PGDB) UnstarSession(_ string) error {
	return db.ErrReadOnly
}

// ListStarredSessionIDs returns an empty slice (no local star storage).
func (p *PGDB) ListStarredSessionIDs(_ context.Context) ([]string, error) {
	return []string{}, nil
}

// BulkStarSessions is not supported in read-only mode.
func (p *PGDB) BulkStarSessions(_ []string) error {
	return db.ErrReadOnly
}

// PinMessage is not supported in read-only mode.
func (p *PGDB) PinMessage(_ string, _ int64, _ *string) (int64, error) {
	return 0, db.ErrReadOnly
}

// UnpinMessage is not supported in read-only mode.
func (p *PGDB) UnpinMessage(_ string, _ int64) error {
	return db.ErrReadOnly
}

// ListPinnedMessages returns nil (no local pin storage).
func (p *PGDB) ListPinnedMessages(_ context.Context, _ string) ([]db.PinnedMessage, error) {
	return nil, nil
}

// InsertInsight is not supported in read-only mode.
func (p *PGDB) InsertInsight(_ db.Insight) (int64, error) {
	return 0, db.ErrReadOnly
}

// DeleteInsight is not supported in read-only mode.
func (p *PGDB) DeleteInsight(_ int64) error {
	return db.ErrReadOnly
}

// ListInsights returns nil (no local insight storage).
func (p *PGDB) ListInsights(_ context.Context, _ db.InsightFilter) ([]db.Insight, error) {
	return nil, nil
}

// GetInsight returns nil (no local insight storage).
func (p *PGDB) GetInsight(_ context.Context, _ int64) (*db.Insight, error) {
	return nil, nil
}

// RenameSession is not supported in read-only mode.
func (p *PGDB) RenameSession(_ string, _ *string) error {
	return db.ErrReadOnly
}

// SoftDeleteSession is not supported in read-only mode.
func (p *PGDB) SoftDeleteSession(_ string) error {
	return db.ErrReadOnly
}

// RestoreSession is not supported in read-only mode.
func (p *PGDB) RestoreSession(_ string) (int64, error) {
	return 0, db.ErrReadOnly
}

// DeleteSessionIfTrashed is not supported in read-only mode.
func (p *PGDB) DeleteSessionIfTrashed(_ string) (int64, error) {
	return 0, db.ErrReadOnly
}

// ListTrashedSessions returns an empty slice (no local trash).
func (p *PGDB) ListTrashedSessions(_ context.Context) ([]db.Session, error) {
	return []db.Session{}, nil
}

// EmptyTrash is not supported in read-only mode.
func (p *PGDB) EmptyTrash() (int, error) {
	return 0, db.ErrReadOnly
}

// UpsertSession is not supported in read-only mode.
func (p *PGDB) UpsertSession(_ db.Session) error {
	return db.ErrReadOnly
}

// ReplaceSessionMessages is not supported in read-only mode.
func (p *PGDB) ReplaceSessionMessages(_ string, _ []db.Message) error {
	return db.ErrReadOnly
}

// GetSessionVersion returns 0, 0, false; PG mode has no file metadata.
func (p *PGDB) GetSessionVersion(_ string) (int, int64, bool) {
	return 0, 0, false
}
