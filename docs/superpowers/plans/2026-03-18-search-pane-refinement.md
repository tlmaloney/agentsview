# Search Pane Refinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve the Command Palette (Cmd+K) search to show one session-grouped result per
session with agent dot, relative time, and a short ID, plus a Relevance/Recency sort toggle.

**Architecture:** The SQLite path changes `db.Search()` to `GROUP BY session_id` with
`min(rank)` and joins sessions for `agent`/`session_ended_at`. The PostgreSQL path uses a CTE
with `DISTINCT ON (session_id)` for equivalent grouping. The HTTP handler reads and validates a
`sort` query param. The frontend removes per-message fields, adds a `sort` state with
`setSort()`, and rewrites the result row and sort toggle.

**Tech Stack:** Go, SQLite FTS5, PostgreSQL (ILIKE + DISTINCT ON), Svelte 5, TypeScript, Vitest

---

## File Structure

| File | Change |
| --- | --- |
| `internal/db/search.go` | Update `SearchResult`, `SearchFilter`; add sort validation; group SQL |
| `internal/db/search_test.go` | Add `TestSearch` covering dedup, agent, sort, pagination, injection guard |
| `internal/server/search.go` | Read and validate `sort` param; pass to filter |
| `internal/postgres/schema.go` | Bump `SchemaVersion` to 2; add `is_system` to DDL and `alters` |
| `internal/postgres/push.go` | Add `is_system` to message INSERT |
| `internal/postgres/messages.go` | Rewrite `Search()` with CTE + `DISTINCT ON`; update scan |
| `internal/postgres/messages_pgtest_test.go` | Expand to cover dedup, agent, sort, system-msg exclusion |
| `frontend/src/lib/api/types/core.ts` | Remove `role`/`timestamp`; add `agent`/`session_ended_at` |
| `frontend/src/lib/api/client.ts` | Add `sort` param to `search()` |
| `frontend/src/lib/stores/search.svelte.ts` | Add `sort` state and `setSort()` |
| `frontend/src/lib/stores/search.test.ts` | Update factory; add sort state and setSort tests |
| `frontend/src/lib/components/command-palette/CommandPalette.svelte` | New result row + sort toggle |

---

### Task 1: SQLite search.go — session-grouped query + tests

**Files:**

- Modify: `internal/db/search.go`
- Modify: `internal/db/search_test.go`

- [ ] **Step 1: Write failing tests in `internal/db/search_test.go`**

Add a new `TestSearch` function after the existing `TestSearchSession`. The helpers `testDB`,
`insertSession`, `insertMessages`, `userMsg`, `asstMsg`, and `Ptr` are all defined in
`internal/db/db_test.go` in the same `package db` — they are available to this test:

```go
func TestSearch(t *testing.T) {
	t.Parallel()
	d := testDB(t)

	// Session s1: older ended_at, agent "claude"
	insertSession(t, d, "s1", "proj-a",
		func(s *Session) {
			s.Agent = "claude"
			s.StartedAt = Ptr("2024-01-01T10:00:00Z")
			s.EndedAt = Ptr("2024-01-01T11:00:00Z")
		},
	)
	// Session s2: newer ended_at, agent "codex"
	insertSession(t, d, "s2", "proj-b",
		func(s *Session) {
			s.Agent = "codex"
			s.StartedAt = Ptr("2024-01-02T10:00:00Z")
			s.EndedAt = Ptr("2024-01-02T11:00:00Z")
		},
	)
	// Session s3: system messages only — should be excluded
	insertSession(t, d, "s3", "proj-c",
		func(s *Session) {
			s.Agent = "claude"
			s.StartedAt = Ptr("2024-01-03T10:00:00Z")
			s.EndedAt = Ptr("2024-01-03T11:00:00Z")
		},
	)

	// s1: two messages both containing "alpha" — should collapse to 1 result
	insertMessages(t, d,
		userMsg("s1", 0, "alpha beta gamma"),
		asstMsg("s1", 1, "alpha zeta unique-s1-1"),
	)
	// s2: one matching message
	insertMessages(t, d,
		userMsg("s2", 0, "alpha delta epsilon"),
	)
	// s3: system message — must be excluded
	sysMsg := userMsg("s3", 0, "alpha system hidden")
	sysMsg.IsSystem = true
	insertMessages(t, d, sysMsg)

	t.Run("deduplication: two messages in same session → one result", func(t *testing.T) {
		t.Parallel()
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha", Limit: 10,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		// s1 and s2 each have alpha matches; s3 is excluded (system msg)
		if len(page.Results) != 2 {
			t.Errorf("got %d results, want 2 (one per session)", len(page.Results))
		}
	})

	t.Run("agent field populated from sessions join", func(t *testing.T) {
		t.Parallel()
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha beta", Limit: 10,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(page.Results) == 0 {
			t.Fatal("expected at least one result")
		}
		if page.Results[0].Agent == "" {
			t.Error("Agent field is empty, want populated")
		}
		if page.Results[0].Agent != "claude" {
			t.Errorf("Agent = %q, want %q", page.Results[0].Agent, "claude")
		}
	})

	t.Run("session_ended_at populated from COALESCE(ended_at, started_at)", func(t *testing.T) {
		t.Parallel()
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha beta", Limit: 10,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(page.Results) == 0 {
			t.Fatal("expected at least one result")
		}
		if page.Results[0].SessionEndedAt == "" {
			t.Error("SessionEndedAt is empty, want populated")
		}
	})

	t.Run("sort recency: newer session appears first", func(t *testing.T) {
		t.Parallel()
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha", Limit: 10, Sort: "recency",
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(page.Results) < 2 {
			t.Fatalf("want >= 2 results, got %d", len(page.Results))
		}
		// s2 has ended_at 2024-01-02, s1 has 2024-01-01 — s2 must be first
		if page.Results[0].SessionID != "s2" {
			t.Errorf("recency sort: first result = %q, want %q",
				page.Results[0].SessionID, "s2")
		}
	})

	t.Run("system messages excluded from results", func(t *testing.T) {
		t.Parallel()
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "system hidden", Limit: 10,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(page.Results) != 0 {
			t.Errorf("got %d results for system-only session, want 0",
				len(page.Results))
		}
	})

	t.Run("invalid sort value defaults to relevance (SQL injection guard)", func(t *testing.T) {
		t.Parallel()
		// Must not return an error or panic — just treats as relevance
		_, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha", Limit: 10, Sort: "'; DROP TABLE sessions; --",
		})
		if err != nil {
			t.Errorf("invalid Sort caused error: %v", err)
		}
	})

	t.Run("pagination at session level", func(t *testing.T) {
		t.Parallel()
		// Limit 1 should return 1 session with a NextCursor
		page, err := d.Search(context.Background(), SearchFilter{
			Query: "alpha", Limit: 1,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(page.Results) != 1 {
			t.Errorf("got %d results with limit=1, want 1", len(page.Results))
		}
		if page.NextCursor == 0 {
			t.Error("NextCursor = 0, want non-zero (more results exist)")
		}
	})
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview
CGO_ENABLED=1 go test -tags fts5 ./internal/db/... -run TestSearch -v 2>&1 | head -40
```

Expected: compilation error on `SearchFilter.Sort` and `SearchResult.Agent`/`SessionEndedAt`
(fields don't exist yet).

- [ ] **Step 3: Update `internal/db/search.go`**

Replace the `SearchResult` struct, `SearchFilter` struct, and `Search()` function. Keep
`SearchSession` and `escapeLike` unchanged.

```go
// SearchResult holds a session-level match with the best-ranked snippet.
type SearchResult struct {
	SessionID      string  `json:"session_id"`
	Project        string  `json:"project"`
	Agent          string  `json:"agent"`
	Ordinal        int     `json:"ordinal"`
	SessionEndedAt string  `json:"session_ended_at"`
	Snippet        string  `json:"snippet"`
	Rank           float64 `json:"rank"`
}

// SearchFilter specifies search parameters.
type SearchFilter struct {
	Query   string
	Project string
	Sort    string // "relevance" (default) or "recency"
	Cursor  int    // offset for pagination
	Limit   int
}
```

Replace the `Search()` function body:

```go
// Search performs FTS5 full-text search across messages, grouped by session.
// Each session produces one result: the best-ranked matching message supplies
// the snippet and ordinal via SQLite's min/max aggregate optimization.
func (db *DB) Search(
	ctx context.Context, f SearchFilter,
) (SearchPage, error) {
	if f.Limit <= 0 || f.Limit > MaxSearchLimit {
		f.Limit = DefaultSearchLimit
	}

	// Validate Sort before interpolating into ORDER BY (cannot parameterise).
	// Any value other than "recency" defaults to relevance ordering.
	orderBy := "min(rank)"
	if f.Sort == "recency" {
		orderBy = "COALESCE(s.ended_at, s.started_at) DESC"
	}

	whereClauses := []string{
		"messages_fts MATCH ?",
		"s.deleted_at IS NULL",
		"m.is_system = 0",
	}
	args := []any{f.Query}

	if f.Project != "" {
		whereClauses = append(whereClauses, "s.project = ?")
		args = append(args, f.Project)
	}

	query := fmt.Sprintf(`
		SELECT m.session_id, s.project, s.agent,
			COALESCE(s.ended_at, s.started_at) as session_ended_at,
			m.ordinal,
			snippet(messages_fts, 0, '<mark>', '</mark>',
				'...', %d) as snippet,
			min(rank) as rank
		FROM messages_fts
		JOIN messages m ON messages_fts.rowid = m.id
		JOIN sessions s ON m.session_id = s.id
		WHERE %s
		GROUP BY m.session_id
		ORDER BY %s
		LIMIT ? OFFSET ?`,
		snippetTokenLength,
		strings.Join(whereClauses, " AND "),
		orderBy,
	)
	args = append(args, f.Limit+1, f.Cursor)

	rows, err := db.getReader().QueryContext(ctx, query, args...)
	if err != nil {
		return SearchPage{}, fmt.Errorf("searching: %w", err)
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(
			&r.SessionID, &r.Project, &r.Agent,
			&r.SessionEndedAt, &r.Ordinal,
			&r.Snippet, &r.Rank,
		); err != nil {
			return SearchPage{},
				fmt.Errorf("scanning result: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return SearchPage{}, err
	}

	page := SearchPage{Results: results}
	if len(results) > f.Limit {
		page.Results = results[:f.Limit]
		page.NextCursor = f.Cursor + f.Limit
	}
	return page, nil
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/db/... -run TestSearch -v
```

Expected: all `TestSearch` subtests PASS. `TestSearchSession` still passes.

- [ ] **Step 5: Run vet and full db tests**

```bash
go fmt ./internal/db/...
go vet -tags fts5 ./internal/db/...
CGO_ENABLED=1 go test -tags fts5 ./internal/db/... -v 2>&1 | tail -20
```

Expected: no errors, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/db/search.go internal/db/search_test.go
git commit -m "feat: group SQLite search results by session with agent and sort support"
```

---

### Task 2: HTTP handler — add sort query param

**Files:**

- Modify: `internal/server/search.go`

- [ ] **Step 1: Write a failing test**

There are no unit tests for `handleSearch` currently. Add one to verify the `sort` param is
accepted and forwarded. Create `internal/server/search_test.go`:

```go
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPrepareFTSQuery(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"hello world", `"hello world"`},
		{`"already quoted"`, `"already quoted"`},
	}
	for _, tt := range tests {
		got := prepareFTSQuery(tt.input)
		if got != tt.want {
			t.Errorf("prepareFTSQuery(%q) = %q, want %q",
				tt.input, got, tt.want)
		}
	}
}

func TestHandleSearchSortParam(t *testing.T) {
	tests := []struct {
		name     string
		sortParam string
		wantSort string
	}{
		{"recency accepted", "recency", "recency"},
		{"relevance accepted", "relevance", "relevance"},
		{"empty defaults to relevance", "", "relevance"},
		{"invalid defaults to relevance", "injection", "relevance"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// validateSort is the same whitelist logic extracted for testability
			got := validateSort(tt.sortParam)
			if got != tt.wantSort {
				t.Errorf("validateSort(%q) = %q, want %q",
					tt.sortParam, got, tt.wantSort)
			}
		})
	}
}
```

Note: this test references `validateSort` — a small helper we'll extract in the implementation.

- [ ] **Step 2: Run the test to verify it fails**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/server/... -run TestHandleSearchSortParam -v 2>&1 | head -20
```

Expected: compilation error (`validateSort` undefined).

- [ ] **Step 3: Update `internal/server/search.go`**

Add the `validateSort` helper and update `handleSearch`:

```go
// validateSort returns "recency" only for the exact string "recency";
// all other values (including empty) return "relevance".
// This is also the whitelist guard applied inside db.Search() before
// ORDER BY interpolation.
func validateSort(s string) string {
	if s == "recency" {
		return "recency"
	}
	return "relevance"
}
```

In `handleSearch`, after the cursor parse and before calling `s.db.Search`, add:

```go
sort := validateSort(q.Get("sort"))
```

Then update the filter to include it:

```go
filter := db.SearchFilter{
	Query:   prepareFTSQuery(query),
	Project: q.Get("project"),
	Sort:    sort,
	Cursor:  cursor,
	Limit:   limit,
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/server/... -run "TestPrepareFTSQuery|TestHandleSearchSortParam" -v
```

Expected: both tests PASS.

- [ ] **Step 5: Vet and run all server tests**

```bash
go fmt ./internal/server/...
go vet -tags fts5 ./internal/server/...
CGO_ENABLED=1 go test -tags fts5 ./internal/server/... -v 2>&1 | tail -20
```

- [ ] **Step 6: Commit**

```bash
git add internal/server/search.go internal/server/search_test.go
git commit -m "feat: accept and validate sort param in search handler"
```

---

### Task 3: PG schema — add is_system column, bump SchemaVersion

**Files:**

- Modify: `internal/postgres/schema.go`

- [ ] **Step 1: Update `coreDDL` in `internal/postgres/schema.go`**

In the `messages` table definition inside `coreDDL`, add `is_system` after `content_length`:

```sql
CREATE TABLE IF NOT EXISTS messages (
    session_id     TEXT NOT NULL,
    ordinal        INT NOT NULL,
    role           TEXT NOT NULL,
    content        TEXT NOT NULL,
    timestamp      TIMESTAMPTZ,
    has_thinking   BOOLEAN NOT NULL DEFAULT FALSE,
    has_tool_use   BOOLEAN NOT NULL DEFAULT FALSE,
    content_length INT NOT NULL DEFAULT 0,
    is_system      BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (session_id, ordinal),
    FOREIGN KEY (session_id)
        REFERENCES sessions(id) ON DELETE CASCADE
);
```

- [ ] **Step 2: Add ALTER to the `alters` slice**

Add a new entry at the end of the `alters` slice (after the `tool_calls.call_index` entry):

```go
{
    `ALTER TABLE messages
     ADD COLUMN IF NOT EXISTS is_system BOOLEAN NOT NULL DEFAULT FALSE`,
    "adding messages.is_system",
},
```

- [ ] **Step 3: Bump SchemaVersion**

Change line 16:

```go
const SchemaVersion = 2
```

- [ ] **Step 4: Verify compilation**

```bash
go vet -tags fts5 ./internal/postgres/...
```

Expected: no errors.

- [ ] **Step 5: Run non-PG tests to catch any regressions**

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/... -v 2>&1 | tail -20
```

- [ ] **Step 6: Commit**

```bash
git add internal/postgres/schema.go
git commit -m "feat: add is_system column to PG messages table, bump SchemaVersion to 2"
```

---

### Task 4: PG push — include is_system in message INSERT

**Files:**

- Modify: `internal/postgres/push.go`

- [ ] **Step 1: Locate the message INSERT statement**

The prepared statement is at line 628–633:

```go
msgStmt, err := tx.PrepareContext(ctx, `
    INSERT INTO messages (
        session_id, ordinal, role, content,
        timestamp, has_thinking, has_tool_use,
        content_length
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
```

- [ ] **Step 2: Update the INSERT statement**

```go
msgStmt, err := tx.PrepareContext(ctx, `
    INSERT INTO messages (
        session_id, ordinal, role, content,
        timestamp, has_thinking, has_tool_use,
        content_length, is_system
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
```

- [ ] **Step 3: Update the ExecContext call**

The call at line 694 is:

```go
_, err := msgStmt.ExecContext(ctx,
    sessionID, m.Ordinal, m.Role,
    m.Content, ts, m.HasThinking,
    m.HasToolUse, m.ContentLength,
)
```

Update to add `m.IsSystem`:

```go
_, err := msgStmt.ExecContext(ctx,
    sessionID, m.Ordinal, m.Role,
    m.Content, ts, m.HasThinking,
    m.HasToolUse, m.ContentLength, m.IsSystem,
)
```

- [ ] **Step 4: Verify compilation**

```bash
go vet -tags fts5 ./internal/postgres/...
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add internal/postgres/push.go
git commit -m "feat: push is_system field to PostgreSQL messages table"
```

---

### Task 5: PG Search() — session-grouped DISTINCT ON + tests

**Files:**

- Modify: `internal/postgres/messages.go`
- Modify: `internal/postgres/messages_pgtest_test.go`

- [ ] **Step 1: Write failing tests in `messages_pgtest_test.go`**

Replace the entire file content:

```go
//go:build pgtest

package postgres

import (
	"context"
	"testing"

	"github.com/wesm/agentsview/internal/db"
)

// insertPGSession inserts a session row directly for PG tests.
func insertPGSession(t *testing.T, pg interface {
	Exec(query string, args ...any) (interface{}, error)
}, id, project, agent, startedAt, endedAt string) {
	t.Helper()
}

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
		if r.SessionID != "store-test-001" {
			t.Errorf("unexpected session %q", r.SessionID)
		}
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

	// store-test-001 has 2 messages; searching "hello" should return 1 result
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

	// Open a write connection to insert additional test data
	pg, err := Open(pgURL, testSchema, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pg.Close()

	// Insert a newer session that also matches "hello"
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

func TestPGSearchSystemMessageExcluded(t *testing.T) {
	pgURL := testPGURL(t)
	ensureStoreSchema(t, pgURL)

	pg, err := Open(pgURL, testSchema, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer pg.Close()

	// Insert a session whose only matching message is a system message
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
```

- [ ] **Step 2: Run to verify tests fail**

```bash
TEST_PG_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  CGO_ENABLED=1 go test -tags "fts5,pgtest" ./internal/postgres/... \
  -run "TestStoreSearchILIKE|TestPGSearch" -v 2>&1 | head -50
```

Expected: compile error (fields `Agent`, `SessionEndedAt` don't exist on `db.SearchResult` yet
from the PG path — the SQLite path already has them from Task 1, but the PG scan still assigns
`Role`/`Timestamp`).

- [ ] **Step 3: Rewrite `Search()` in `internal/postgres/messages.go`**

Replace the existing `Search()` function (lines 193–281) with:

```go
// Search performs ILIKE-based full-text search across messages,
// grouped to one result per session via DISTINCT ON.
func (s *Store) Search(
	ctx context.Context, f db.SearchFilter,
) (db.SearchPage, error) {
	if f.Limit <= 0 || f.Limit > db.MaxSearchLimit {
		f.Limit = db.DefaultSearchLimit
	}

	searchTerm := stripFTSQuotes(f.Query)
	if searchTerm == "" {
		return db.SearchPage{}, nil
	}

	// Validate Sort before interpolating into ORDER BY.
	outerOrderBy := "match_pos ASC"
	if f.Sort == "recency" {
		outerOrderBy = "session_ended_at DESC"
	}

	// $1 = escaped ILIKE pattern (for WHERE clause)
	// $2 = raw search term (for POSITION — case folded in expression)
	args := []any{escapeLike(searchTerm), searchTerm}
	argIdx := 3

	projectClause := ""
	if f.Project != "" {
		projectClause = fmt.Sprintf(
			"AND s.project = $%d", argIdx,
		)
		args = append(args, f.Project)
		argIdx++
	}

	query := fmt.Sprintf(`
		WITH best_per_session AS (
			SELECT DISTINCT ON (m.session_id)
				m.session_id,
				s.project,
				s.agent,
				COALESCE(s.ended_at, s.started_at) AS session_ended_at,
				m.ordinal,
				POSITION(LOWER($2) IN LOWER(m.content)) AS match_pos,
				CASE
					WHEN POSITION(LOWER($2) IN LOWER(m.content)) > 100
						THEN '...' || SUBSTRING(m.content
							FROM GREATEST(1, POSITION(
								LOWER($2) IN LOWER(m.content)
							) - 50) FOR 200) || '...'
					ELSE SUBSTRING(m.content FROM 1 FOR 200)
						|| CASE WHEN LENGTH(m.content) > 200
							THEN '...' ELSE '' END
				END AS snippet
			FROM messages m
			JOIN sessions s ON m.session_id = s.id
			WHERE m.content ILIKE '%%' || $1 || '%%' ESCAPE E'\\'
				AND s.deleted_at IS NULL
				AND m.is_system = FALSE
				%s
			ORDER BY m.session_id,
				POSITION(LOWER($2) IN LOWER(m.content)) ASC
		)
		SELECT session_id, project, agent,
			session_ended_at, ordinal,
			snippet, 1.0 AS rank
		FROM best_per_session
		ORDER BY %s
		LIMIT $%d OFFSET $%d`,
		projectClause,
		outerOrderBy,
		argIdx, argIdx+1,
	)
	args = append(args, f.Limit+1, f.Cursor)

	rows, err := s.pg.QueryContext(ctx, query, args...)
	if err != nil {
		return db.SearchPage{},
			fmt.Errorf("searching: %w", err)
	}
	defer rows.Close()

	results := []db.SearchResult{}
	for rows.Next() {
		var r db.SearchResult
		var endedAt *time.Time
		if err := rows.Scan(
			&r.SessionID, &r.Project, &r.Agent,
			&endedAt, &r.Ordinal,
			&r.Snippet, &r.Rank,
		); err != nil {
			return db.SearchPage{},
				fmt.Errorf(
					"scanning search result: %w", err,
				)
		}
		if endedAt != nil {
			r.SessionEndedAt = FormatISO8601(*endedAt)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return db.SearchPage{}, err
	}

	page := db.SearchPage{Results: results}
	if len(results) > f.Limit {
		page.Results = results[:f.Limit]
		page.NextCursor = f.Cursor + f.Limit
	}
	return page, nil
}
```

Note: `%%` in the format string produces literal `%` in the SQL string (Go `fmt.Sprintf`
escaping). The ILIKE pattern `'%' || $1 || '%'` remains correct.

- [ ] **Step 4: Run PG tests to confirm they pass**

```bash
make test-postgres
```

Or manually:

```bash
TEST_PG_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  CGO_ENABLED=1 go test -tags "fts5,pgtest" ./internal/postgres/... \
  -run "TestStoreSearchILIKE|TestPGSearch" -v
```

Expected: all tests PASS.

- [ ] **Step 5: Run full PG test suite**

```bash
make test-postgres
```

Expected: all tests pass including pre-existing store tests.

- [ ] **Step 6: Format and vet**

```bash
go fmt ./internal/postgres/...
go vet -tags fts5 ./internal/postgres/...
```

- [ ] **Step 7: Commit**

```bash
git add internal/postgres/messages.go internal/postgres/messages_pgtest_test.go
git commit -m "feat: session-grouped PG search with DISTINCT ON, is_system filter, and sort support"
```

---

### Task 6: Frontend types + API client

**Files:**

- Modify: `frontend/src/lib/api/types/core.ts`
- Modify: `frontend/src/lib/api/client.ts`

- [ ] **Step 1: Update `SearchResult` in `frontend/src/lib/api/types/core.ts`**

Replace lines 81–89:

```typescript
/** Matches Go SearchResult struct in internal/db/search.go */
export interface SearchResult {
  session_id: string;
  project: string;
  agent: string;
  ordinal: number;
  session_ended_at: string;
  snippet: string;
  rank: number;
}
```

- [ ] **Step 2: Update `search()` in `frontend/src/lib/api/client.ts`**

Replace lines 198–211:

```typescript
export function search(
  query: string,
  params: {
    project?: string;
    limit?: number;
    cursor?: number;
    sort?: "relevance" | "recency";
  } = {},
  init?: RequestInit,
): Promise<SearchResponse> {
  if (!query) {
    throw new Error("search query must not be empty");
  }
  return fetchJSON(`/search${buildQuery({ q: query, ...params })}`, init);
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview/frontend
npm run check 2>&1 | tail -20
```

Expected: no type errors (there will be cascade errors in `search.test.ts` and
`CommandPalette.svelte` due to removed fields — those are fixed in Tasks 7 and 8).

- [ ] **Step 4: Commit**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview
git add frontend/src/lib/api/types/core.ts frontend/src/lib/api/client.ts
git commit -m "feat: update SearchResult type and search() API client to add sort param"
```

---

### Task 7: SearchStore — sort state + setSort() + tests

**Files:**

- Modify: `frontend/src/lib/stores/search.svelte.ts`
- Modify: `frontend/src/lib/stores/search.test.ts`

- [ ] **Step 1: Update `makeSearchResponse` and write new sort tests in `search.test.ts`**

Replace `makeSearchResponse` (lines 18–36) with:

```typescript
function makeSearchResponse(
  query: string,
  count: number,
): SearchResponse {
  return {
    query,
    results: Array.from({ length: count }, (_, i) => ({
      session_id: `s${i}`,
      project: "proj",
      agent: "claude",
      ordinal: i,
      session_ended_at: new Date().toISOString(),
      snippet: `result ${i}`,
      rank: i,
    })),
    count,
    next: 0,
  };
}
```

Update the existing test at line 249 that checks the exact `api.search` call:

```typescript
it("should pass signal to api.search", async () => {
  vi.mocked(api.search).mockResolvedValueOnce(
    makeSearchResponse("test", 1),
  );

  searchStore.search("test");
  vi.advanceTimersByTime(DEBOUNCE_MS);

  await vi.runAllTimersAsync();
  await Promise.resolve();

  expect(api.search).toHaveBeenCalledWith(
    "test",
    { project: undefined, limit: 30, sort: "relevance" },
    { signal: expect.any(AbortSignal) },
  );
});
```

Add new tests at the end of the `describe("SearchStore")` block:

```typescript
it("sort defaults to relevance", () => {
  expect(searchStore.sort).toBe("relevance");
});

it("setSort updates sort state", () => {
  searchStore.setSort("recency");
  expect(searchStore.sort).toBe("recency");
  searchStore.setSort("relevance");
  expect(searchStore.sort).toBe("relevance");
});

it("setSort re-runs search when query is active", async () => {
  vi.mocked(api.search)
    .mockResolvedValueOnce(makeSearchResponse("hello", 2))
    .mockResolvedValueOnce(makeSearchResponse("hello", 1));

  // Run first search
  searchStore.search("hello");
  vi.advanceTimersByTime(DEBOUNCE_MS);
  await vi.runAllTimersAsync();
  await Promise.resolve();

  expect(searchStore.results.length).toBe(2);

  // Switch sort — should trigger a new search immediately
  searchStore.setSort("recency");
  await vi.runAllTimersAsync();
  await Promise.resolve();

  expect(api.search).toHaveBeenCalledTimes(2);
  expect(api.search).toHaveBeenLastCalledWith(
    "hello",
    expect.objectContaining({ sort: "recency" }),
    expect.objectContaining({ signal: expect.any(AbortSignal) }),
  );
  expect(searchStore.results.length).toBe(1);
});

it("setSort does nothing when no query is active", () => {
  searchStore.clear();
  searchStore.setSort("recency");
  expect(api.search).not.toHaveBeenCalled();
});
```

- [ ] **Step 2: Run tests to verify new ones fail**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview/frontend
npx vitest run --reporter=verbose src/lib/stores/search.test.ts 2>&1 | tail -30
```

Expected failures:
- `sort defaults to relevance` — `searchStore.sort` property does not exist yet
- `setSort updates sort state` — `searchStore.setSort` method does not exist yet
- `setSort re-runs search when query is active` — same missing method
- `setSort does nothing when no query is active` — same
- `"should pass signal"` — now expects `sort: "relevance"` in the params object, but
  `executeSearch` does not yet pass `sort`

- [ ] **Step 3: Update `search.svelte.ts`**

Replace the entire file:

```typescript
import * as api from "../api/client.js";
import { debounce } from "../utils/debounce.js";
import type { SearchResult } from "../api/types.js";

class SearchStore {
  query: string = $state("");
  project: string = $state("");
  sort: "relevance" | "recency" = $state("relevance");
  results: SearchResult[] = $state([]);
  isSearching: boolean = $state(false);

  private abortController: AbortController | null = null;

  private debouncedSearch = debounce(
    (q: string, project: string) => {
      this.executeSearch(q, project);
    },
    300,
  );

  search(q: string, project?: string) {
    this.query = q;
    if (project !== undefined) this.project = project;

    if (!q.trim()) {
      this.debouncedSearch.cancel();
      this.abortController?.abort();
      this.results = [];
      this.isSearching = false;
      return;
    }

    this.abortController?.abort();
    this.abortController = null;
    this.debouncedSearch(q, this.project);
  }

  setSort(s: "relevance" | "recency") {
    this.sort = s;
    if (this.query.trim()) {
      this.executeSearch(this.query, this.project);
    }
  }

  clear() {
    this.query = "";
    this.results = [];
    this.isSearching = false;
    this.debouncedSearch.cancel();
    this.abortController?.abort();
  }

  private async executeSearch(
    q: string, project: string,
  ) {
    this.abortController?.abort();
    this.abortController = new AbortController();
    const { signal } = this.abortController;

    this.isSearching = true;
    try {
      const res = await api.search(
        q,
        { project: project || undefined, limit: 30, sort: this.sort },
        { signal },
      );
      this.results = res.results ?? [];
    } catch (error: unknown) {
      if (error instanceof DOMException
        && error.name === "AbortError") {
        return;
      }
      this.results = [];
    } finally {
      if (!signal.aborted) {
        this.isSearching = false;
      }
    }
  }
}

export const searchStore = new SearchStore();
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview/frontend
npx vitest run --reporter=verbose src/lib/stores/search.test.ts
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview
git add frontend/src/lib/stores/search.svelte.ts frontend/src/lib/stores/search.test.ts
git commit -m "feat: add sort state and setSort() to SearchStore"
```

---

### Task 8: CommandPalette — new result layout + sort toggle

**Files:**

- Modify: `frontend/src/lib/components/command-palette/CommandPalette.svelte`

- [ ] **Step 1: Update the `<script>` block**

Add `copyToClipboard` import alongside the existing imports. This utility already exists at
`frontend/src/lib/utils/clipboard.ts` and returns `Promise<boolean>` (silent no-op on failure):

```typescript
import { copyToClipboard } from "../../utils/clipboard.js";
```

The existing imports of `formatRelativeTime`, `truncate`, `sanitizeSnippet`, and `agentColor`
are already present and remain unchanged.

- [ ] **Step 2: Update the search results block in the template**

Replace the `{#if showSearchResults}` section (lines 151–175) with:

```svelte
{#if showSearchResults}
  {#if searchStore.isSearching}
    <div class="palette-empty">Searching...</div>
  {:else if searchStore.results.length === 0}
    <div class="palette-empty">No results</div>
  {:else}
    <div class="palette-sort">
      <button
        class="sort-btn"
        class:active={searchStore.sort === "relevance"}
        onclick={() => searchStore.setSort("relevance")}
      >Relevance</button>
      <button
        class="sort-btn"
        class:active={searchStore.sort === "recency"}
        onclick={() => searchStore.setSort("recency")}
      >Recency</button>
    </div>
    {#each searchStore.results as result, i}
      <button
        class="palette-item"
        class:selected={i === selectedIndex}
        onclick={() => selectSearchResult(result)}
        onmouseenter={() => (selectedIndex = i)}
      >
        <span
          class="item-dot"
          style:background={agentColor(result.agent)}
        ></span>
        <span class="item-text">
          {@html sanitizeSnippet(result.snippet)}
        </span>
        <span class="item-meta">
          {truncate(result.project, 20)} · {formatRelativeTime(result.session_ended_at)}
        </span>
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <span
          class="item-id"
          title="Copy session ID"
          onclick={(e) => {
            e.stopPropagation();
            copyToClipboard(result.session_id);
          }}
        >{result.session_id.slice(0, 8)}</span>
      </button>
    {/each}
  {/if}
```

- [ ] **Step 3: Add CSS for the new elements**

In the `<style>` block, remove the `.item-role` and `.item-role.user` rules (lines 299–317)
and add:

```css
.palette-sort {
  display: flex;
  gap: 4px;
  padding: 6px 14px 2px;
}

.sort-btn {
  padding: 2px 8px;
  font-size: 11px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  background: none;
  color: var(--text-muted);
  cursor: pointer;
  font-family: var(--font-sans);
}

.sort-btn.active {
  background: var(--bg-surface-hover);
  color: var(--text-primary);
  border-color: var(--accent-purple);
}

.item-id {
  font-family: var(--font-mono);
  font-size: 10px;
  color: var(--text-muted);
  white-space: nowrap;
  flex-shrink: 0;
  cursor: pointer;
  padding: 1px 3px;
  border-radius: var(--radius-sm);
}

.item-id:hover {
  background: var(--bg-inset);
  color: var(--text-primary);
}
```

- [ ] **Step 4: Verify TypeScript and Svelte compilation**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview/frontend
npm run check 2>&1 | tail -20
```

Expected: no type errors.

- [ ] **Step 5: Build the frontend**

```bash
npm run build 2>&1 | tail -20
```

Expected: build completes without errors.

- [ ] **Step 6: Run all frontend tests**

```bash
npx vitest run --reporter=verbose 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 7: Run Go tests to ensure nothing broke**

```bash
cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview
CGO_ENABLED=1 go test -tags fts5 ./... -short 2>&1 | tail -20
```

- [ ] **Step 8: Commit**

```bash
git add frontend/src/lib/components/command-palette/CommandPalette.svelte
git commit -m "feat: update CommandPalette with session-grouped result layout and sort toggle"
```

---

## Manual Verification Checklist

After all tasks complete, verify end-to-end with a running server:

1. Open the app, press Cmd+K, type a 3+ character query.
2. Confirm each result shows: agent color dot, snippet, `project · Xh ago`, 8-char ID.
3. Click a short ID — clipboard should be populated (no visible feedback).
4. Click a result — navigates to the session and scrolls to the matching message ordinal.
5. Click **Recency** toggle — results reorder; **Recency** button appears highlighted.
6. Click **Relevance** toggle — results reorder back.
7. Test `pg serve` path: connect to a remote PG server and repeat steps 1–6.
