package pgdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"

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

// warnInsecureSSL logs a warning when the PG connection string
// targets a non-loopback host without TLS encryption. Handles
// both URI (postgres://...) and key-value (host=... sslmode=...)
// connection string formats.
func warnInsecureSSL(dsn string) {
	host, mode := parseSSLParams(dsn)
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return
	}
	if mode == "" || mode == "disable" || mode == "prefer" || mode == "allow" {
		log.Printf("warning: pg connection to %s uses sslmode=%q; "+
			"consider sslmode=require or verify-full for non-local hosts",
			host, mode)
	}
}

// parseSSLParams extracts host and sslmode from a DSN. It tries
// URI format first, then falls back to key-value format.
func parseSSLParams(dsn string) (host, sslmode string) {
	if u, err := url.Parse(dsn); err == nil && u.Host != "" {
		return u.Hostname(), u.Query().Get("sslmode")
	}
	// Key-value format: host=... sslmode=...
	host = kvParam(dsn, "host")
	sslmode = kvParam(dsn, "sslmode")
	// Unix-socket paths (host=/var/run/...) are local; treat as
	// loopback so the warning is not triggered.
	if strings.HasPrefix(host, "/") {
		host = ""
	}
	return host, sslmode
}

// kvParam extracts a key-value parameter from a libpq-style DSN.
// Handles optional quoting (key='value with spaces').
func kvParam(dsn, key string) string {
	prefix := key + "="
	idx := strings.Index(dsn, prefix)
	if idx < 0 {
		return ""
	}
	// Ensure we matched a full key (not a substring like "hostaddr=").
	if idx > 0 && dsn[idx-1] != ' ' && dsn[idx-1] != '\t' {
		return ""
	}
	val := dsn[idx+len(prefix):]
	if len(val) > 0 && val[0] == '\'' {
		// Quoted value: find closing quote.
		end := strings.IndexByte(val[1:], '\'')
		if end >= 0 {
			return val[1 : end+1]
		}
		return val[1:]
	}
	// Unquoted: take until next whitespace.
	if end := strings.IndexAny(val, " \t"); end >= 0 {
		return val[:end]
	}
	return val
}

// New opens a PostgreSQL connection and returns a PGDB.
func New(pgURL string) (*PGDB, error) {
	warnInsecureSSL(pgURL)
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

// GetSessionVersion returns the message count and a hash of
// updated_at for SSE change detection. The updated_at hash
// serves as a version signal for metadata-only changes
// (renames, deletes, display name updates) that don't change
// message_count.
func (p *PGDB) GetSessionVersion(id string) (int, int64, bool) {
	var count int
	var updatedAt string
	err := p.pg.QueryRow(
		`SELECT message_count, COALESCE(updated_at, '')
		 FROM agentsview.sessions WHERE id = $1`,
		id,
	).Scan(&count, &updatedAt)
	if err != nil {
		return 0, 0, false
	}
	// Use a simple hash of updated_at as the mtime-equivalent
	// signal. The SSE watcher compares this value across polls.
	var h int64
	for _, c := range updatedAt {
		h = h*31 + int64(c)
	}
	return count, h, true
}
