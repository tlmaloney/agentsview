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
			"kv host and hostaddr prefers hostaddr",
			"host=myhost hostaddr=10.0.0.1 sslmode=prefer",
			"10.0.0.1", "prefer",
		},
		{
			"kv unix socket",
			"host=/var/run/postgresql sslmode=disable",
			"", "disable",
		},
		{
			"uri empty authority host in query",
			"postgres:///db?host=remote&sslmode=disable",
			"remote", "disable",
		},
		{
			"uri empty authority hostaddr in query",
			"postgres:///db?hostaddr=10.0.0.1&sslmode=require",
			"10.0.0.1", "require",
		},
		{
			"uri empty authority unix socket in query",
			"postgres:///db?host=/var/run/postgresql&sslmode=disable",
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

func TestCheckSSL(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"loopback localhost", "postgres://user:pass@localhost:5432/db", false},
		{"loopback 127.0.0.1", "postgres://user:pass@127.0.0.1:5432/db", false},
		{"loopback ::1", "postgres://user:pass@[::1]:5432/db", false},
		{"empty host", "", false},
		{"remote with require", "postgres://user:pass@remote:5432/db?sslmode=require", false},
		{"remote with verify-full", "postgres://user:pass@remote:5432/db?sslmode=verify-full", false},
		{"remote no sslmode", "postgres://user:pass@remote:5432/db", true},
		{"remote sslmode=disable", "postgres://user:pass@remote:5432/db?sslmode=disable", true},
		{"remote sslmode=prefer", "postgres://user:pass@remote:5432/db?sslmode=prefer", true},
		{"remote sslmode=allow", "postgres://user:pass@remote:5432/db?sslmode=allow", true},
		{"kv remote require", "host=remote sslmode=require", false},
		{"kv remote disable", "host=remote sslmode=disable", true},
		{"kv unix socket", "host=/var/run/postgresql sslmode=disable", false},
		{"uri query host no ssl", "postgres:///db?host=remote", true},
		{"uri query host disable", "postgres:///db?host=remote&sslmode=disable", true},
		{"uri query host require", "postgres:///db?host=remote&sslmode=require", false},
		{"kv hostaddr overrides loopback host", "host=localhost hostaddr=203.0.113.10 sslmode=disable", true},
		{"uri hostaddr overrides loopback host", "postgres://localhost:5432/db?hostaddr=203.0.113.10&sslmode=disable", true},
		{"uri query hostaddr overrides loopback host", "postgres:///db?host=localhost&hostaddr=203.0.113.10&sslmode=disable", true},
		{"uri loopback hostaddr overrides remote host", "postgres://remote:5432/db?hostaddr=127.0.0.1&sslmode=disable", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckSSL(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckSSL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
