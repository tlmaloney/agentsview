package pgutil

import "testing"

func TestParseSSLParams(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		wantHost string
		wantMode string
	}{
		{
			"uri format",
			"postgres://user:pass@myhost:5432/db?sslmode=require",
			"myhost", "require",
		},
		{
			"uri no sslmode",
			"postgres://user:pass@myhost:5432/db",
			"myhost", "",
		},
		{
			"kv host",
			"host=myhost sslmode=verify-full",
			"myhost", "verify-full",
		},
		{
			"kv hostaddr only",
			"hostaddr=10.0.0.1 sslmode=disable",
			"10.0.0.1", "disable",
		},
		{
			"kv host and hostaddr prefers host",
			"host=myhost hostaddr=10.0.0.1 sslmode=prefer",
			"myhost", "prefer",
		},
		{
			"kv unix socket",
			"host=/var/run/postgresql sslmode=disable",
			"", "disable",
		},
		{
			"empty dsn",
			"",
			"", "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, mode := ParseSSLParams(tt.dsn)
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if mode != tt.wantMode {
				t.Errorf("sslmode = %q, want %q", mode, tt.wantMode)
			}
		})
	}
}
