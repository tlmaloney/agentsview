//go:build pgtest

package pgsync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/wesm/agentsview/internal/db"
)

func TestOlympusConnectivity(t *testing.T) {
	pgURL := os.Getenv("OLYMPUS_PG_URL")
	if pgURL == "" {
		pgURL = "postgres://postgres:secret@olympus/postgres?sslmode=disable"
	}

	local := testDB(t)
	ps, err := New(pgURL, local, "olympus-test-machine", time.Hour)
	if err != nil {
		t.Fatalf("creating pgsync: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ps.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	status, err := ps.Status(ctx)
	if err != nil {
		t.Fatalf("get status: %v", err)
	}

	t.Logf("Olympus Sync Status: %+v", status)
}

func TestOlympusPushCycle(t *testing.T) {
	pgURL := os.Getenv("OLYMPUS_PG_URL")
	if pgURL == "" {
		pgURL = "postgres://postgres:secret@olympus/postgres?sslmode=disable"
	}

	// Clean up schema before starting
	cleanPGSchema(t, pgURL)
	t.Cleanup(func() { cleanPGSchema(t, pgURL) })

	local := testDB(t)
	ps, err := New(pgURL, local, "machine-a", time.Hour)
	if err != nil {
		t.Fatalf("creating pgsync: %v", err)
	}
	defer ps.Close()

	ctx := context.Background()
	if err := ps.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Create a session and message
	started := time.Now().UTC().Format(time.RFC3339)
	firstMsg := "hello from olympus"
	sess := db.Session{
		ID:           "olympus-sess-001",
		Project:      "olympus-project",
		Machine:      "local",
		Agent:        "test-agent",
		FirstMessage: &firstMsg,
		StartedAt:    &started,
		MessageCount: 1,
	}
	if err := local.UpsertSession(sess); err != nil {
		t.Fatalf("upsert session: %v", err)
	}
	if err := local.InsertMessages([]db.Message{{
		SessionID: "olympus-sess-001",
		Ordinal:   0,
		Role:      "user",
		Content:   firstMsg,
	}}); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	// Push
	pushResult, err := ps.Push(ctx, false)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if pushResult.SessionsPushed != 1 || pushResult.MessagesPushed != 1 {
		t.Fatalf("pushed %d sessions, %d messages; want 1/1",
			pushResult.SessionsPushed, pushResult.MessagesPushed)
	}

	// Verify via status
	status, err := ps.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.PGSessions != 1 {
		t.Errorf("pg sessions = %d, want 1", status.PGSessions)
	}
	if status.PGMessages != 1 {
		t.Errorf("pg messages = %d, want 1", status.PGMessages)
	}
}
