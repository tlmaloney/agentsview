package pgdb

import "testing"

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			"postgres://user:secret@myhost:5432/db?sslmode=disable",
			"myhost",
		},
		{
			"postgres://user:secret@myhost:5432/db?password=leaked",
			"myhost",
		},
		{
			"postgres://myhost/db",
			"myhost",
		},
		{
			"not a url",
			"",
		},
	}
	for _, tt := range tests {
		got := redactDSN(tt.input)
		if got != tt.want {
			t.Errorf("redactDSN(%q) = %q, want %q",
				tt.input, got, tt.want)
		}
	}
}
