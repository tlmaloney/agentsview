package pgutil

import (
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
// URI format first, then falls back to key-value format.
func ParseSSLParams(dsn string) (host, sslmode string) {
	if u, err := url.Parse(dsn); err == nil && u.Host != "" {
		return u.Hostname(), u.Query().Get("sslmode")
	}
	// Key-value format: host=... hostaddr=... sslmode=...
	host = KVParam(dsn, "host")
	if host == "" {
		host = KVParam(dsn, "hostaddr")
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
