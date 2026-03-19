//go:build pgtest

package postgres

import (
	"context"
	"testing"

	"github.com/wesm/agentsview/internal/db"
)

func TestStoreSearchILIKE(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	store, err := NewStore(pgURL, testSchema, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	page, err := store.Search(ctx, db.SearchFilter{
		Query: "hello",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) == 0 {
		t.Error("expected at least 1 search result")
	}
	for _, r := range page.Results {
		if r.Agent == "" {
			t.Error("Agent field is empty, want populated")
		}
		if r.SessionEndedAt == "" {
			t.Error("SessionEndedAt is empty, want populated")
		}
	}
}

func TestPGSearchDeduplication(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	// store-test-001 has 2 messages; searching "hello" only matches ordinal 0.
	// With session grouping, should return exactly 1 result.
	store, err := NewStore(pgURL, testSchema, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	page, err := store.Search(ctx, db.SearchFilter{
		Query: "hello",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) != 1 {
		t.Errorf("got %d results, want 1 (deduplicated to session)", len(page.Results))
	}
}

func TestPGSearchRecencySort(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	// Open a write connection to insert additional test data.
	pg, err := Open(pgURL, testSchema, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pg.Close()

	// Insert a newer session that also matches "hello".
	_, err = pg.Exec(`
		INSERT INTO sessions
			(id, machine, project, agent, first_message,
			 started_at, ended_at, message_count,
			 user_message_count)
		VALUES
			('recency-test-002', 'test-machine',
			 'test-project', 'codex',
			 'hello again',
			 '2026-04-01T10:00:00Z'::timestamptz,
			 '2026-04-01T10:30:00Z'::timestamptz,
			 1, 1)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting newer session: %v", err)
	}
	_, err = pg.Exec(`
		INSERT INTO messages
			(session_id, ordinal, role, content,
			 timestamp, content_length)
		VALUES
			('recency-test-002', 0, 'user',
			 'hello again newer',
			 '2026-04-01T10:00:00Z'::timestamptz, 17)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting newer message: %v", err)
	}

	store, err := NewStore(pgURL, testSchema, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	page, err := store.Search(ctx, db.SearchFilter{
		Query: "hello",
		Limit: 10,
		Sort:  "recency",
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) < 2 {
		t.Fatalf("want >= 2 results, got %d", len(page.Results))
	}
	// recency-test-002 has ended_at 2026-04-01, store-test-001 has 2026-03-12
	if page.Results[0].SessionID != "recency-test-002" {
		t.Errorf("recency sort: first result = %q, want %q",
			page.Results[0].SessionID, "recency-test-002")
	}
}

func TestPGSearchRelevanceSort(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	pg, err := Open(pgURL, testSchema, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pg.Close()

	// Insert two sessions:
	// - relevance-early: match appears at position 1 (start of content)
	// - relevance-late: match appears after 50 chars of prefix
	_, err = pg.Exec(`
		INSERT INTO sessions
			(id, machine, project, agent, first_message,
			 started_at, ended_at, message_count,
			 user_message_count)
		VALUES
			('relevance-early', 'test-machine',
			 'test-project', 'claude',
			 'needle at start',
			 '2025-01-01T10:00:00Z'::timestamptz,
			 '2025-01-01T10:30:00Z'::timestamptz,
			 1, 1),
			('relevance-late', 'test-machine',
			 'test-project', 'claude',
			 'lots of text before needle',
			 '2025-01-02T10:00:00Z'::timestamptz,
			 '2025-01-02T10:30:00Z'::timestamptz,
			 1, 1)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting sessions: %v", err)
	}
	_, err = pg.Exec(`
		INSERT INTO messages
			(session_id, ordinal, role, content, timestamp, content_length)
		VALUES
			('relevance-early', 0, 'user',
			 'needleunique at the very beginning of content',
			 '2025-01-01T10:00:00Z'::timestamptz, 45),
			('relevance-late', 0, 'user',
			 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaneedleunique at the end',
			 '2025-01-02T10:00:00Z'::timestamptz, 73)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting messages: %v", err)
	}

	store, err := NewStore(pgURL, testSchema, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	page, err := store.Search(ctx, db.SearchFilter{
		Query: "needleunique",
		Limit: 10,
		Sort:  "relevance",
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) < 2 {
		t.Fatalf("want >= 2 results, got %d", len(page.Results))
	}
	// relevance-early has match at position 1; relevance-late has it after 50 chars
	// relevance sort = match_pos ASC, so relevance-early must come first
	if page.Results[0].SessionID != "relevance-early" {
		t.Errorf("relevance sort: first result = %q, want %q",
			page.Results[0].SessionID, "relevance-early")
	}
}

func TestPGSearchSystemMessageExcluded(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	pg, err := Open(pgURL, testSchema, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pg.Close()

	// Insert a session whose only matching message is a system message.
	_, err = pg.Exec(`
		INSERT INTO sessions
			(id, machine, project, agent, first_message,
			 started_at, ended_at, message_count,
			 user_message_count)
		VALUES
			('sysonly-session', 'test-machine',
			 'test-project', 'claude',
			 'sysonly unique term',
			 '2026-03-01T10:00:00Z'::timestamptz,
			 '2026-03-01T10:30:00Z'::timestamptz,
			 1, 0)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting session: %v", err)
	}
	_, err = pg.Exec(`
		INSERT INTO messages
			(session_id, ordinal, role, content,
			 timestamp, content_length, is_system)
		VALUES
			('sysonly-session', 0, 'user',
			 'sysonly unique term',
			 '2026-03-01T10:00:00Z'::timestamptz, 19, TRUE)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		t.Fatalf("inserting system message: %v", err)
	}

	store, err := NewStore(pgURL, testSchema, true)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	page, err := store.Search(ctx, db.SearchFilter{
		Query: "sysonly unique",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) != 0 {
		t.Errorf("got %d results for system-only session, want 0",
			len(page.Results))
	}
}
