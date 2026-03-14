package pgutil

import (
	"fmt"
	"log"
	"net/url"
	"strings"
)

// RedactDSN returns the host portion of the DSN for diagnostics,
// stripping credentials, query parameters, and path components
// that may contain secrets.
func RedactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// CheckSSL returns an error when the PG connection string targets
// a non-loopback host without TLS encryption. Callers should fail
// startup on error unless the user has explicitly opted into
// insecure connections.
func CheckSSL(dsn string) error {
	host, mode := ParseSSLParams(dsn)
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return nil
	}
	if mode == "" || mode == "disable" || mode == "prefer" || mode == "allow" {
		return fmt.Errorf(
			"pg connection to %s uses sslmode=%q; "+
				"set sslmode=require (or verify-full) for non-local hosts, "+
				"or set allow_insecure_pg: true in config to override",
			host, mode,
		)
	}
	return nil
}

// WarnInsecureSSL logs a warning when the PG connection string
// targets a non-loopback host without TLS encryption. Handles
// both URI (postgres://...) and key-value (host=... sslmode=...)
// connection string formats.
func WarnInsecureSSL(dsn string) {
	host, mode := ParseSSLParams(dsn)
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return
	}
	if mode == "" || mode == "disable" || mode == "prefer" || mode == "allow" {
		log.Printf("warning: pg connection to %s uses sslmode=%q; "+
			"consider sslmode=require or verify-full for non-local hosts",
			host, mode)
	}
}

// ParseSSLParams extracts host and sslmode from a DSN. It tries
// URI format first, then falls back to key-value format. For URI
// DSNs with an empty authority (postgres:///db?host=remote), the
// host is read from query parameters.
func ParseSSLParams(dsn string) (host, sslmode string) {
	if u, err := url.Parse(dsn); err == nil && (u.Host != "" || u.Scheme == "postgres" || u.Scheme == "postgresql") {
		host = u.Hostname()
		sslmode = u.Query().Get("sslmode")
		// When hostaddr is present (in authority or query), it
		// determines the actual network address the driver connects
		// to, even if host is loopback. Prefer it for SSL checks.
		if ha := u.Query().Get("hostaddr"); ha != "" {
			host = ha
		}
		if host == "" {
			// Empty authority: host may be in query params
			// (e.g. postgres:///db?host=remote&sslmode=disable).
			host = u.Query().Get("host")
		}
		if strings.HasPrefix(host, "/") {
			host = ""
		}
		return host, sslmode
	}
	// Key-value format: host=... hostaddr=... sslmode=...
	// Prefer hostaddr over host since it determines the actual
	// network address the driver connects to.
	host = KVParam(dsn, "hostaddr")
	if host == "" {
		host = KVParam(dsn, "host")
	}
	sslmode = KVParam(dsn, "sslmode")
	// Unix-socket paths (host=/var/run/...) are local; treat as
	// loopback so the warning is not triggered.
	if strings.HasPrefix(host, "/") {
		host = ""
	}
	return host, sslmode
}

// KVParam extracts a key-value parameter from a libpq-style DSN.
// Handles optional quoting (key='value with spaces').
func KVParam(dsn, key string) string {
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
