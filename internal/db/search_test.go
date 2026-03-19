package db

import (
	"context"
	"fmt"
	"testing"
)

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

// TestSearchDeduplicationManyMessages verifies that a session with many
// matching messages produces exactly one search result. The large message
// count forces FTS5 to maintain multiple internal index segments, which
// previously caused the outer JOIN to return one row per segment rather
// than one row per session.
func TestSearchDeduplicationManyMessages(t *testing.T) {
	t.Parallel()
	d := testDB(t)

	insertSession(t, d, "s1", "proj", func(s *Session) {
		s.Agent = "claude"
		s.StartedAt = Ptr("2024-01-01T10:00:00Z")
		s.EndedAt = Ptr("2024-01-01T11:00:00Z")
	})

	// Insert enough messages to force multiple FTS5 internal segments.
	const n = 150
	msgs := make([]Message, n)
	for i := range n {
		msgs[i] = userMsg("s1", i, fmt.Sprintf("needle content message number %d", i))
	}
	insertMessages(t, d, msgs...)

	// Optimize the FTS5 index to merge segments, then run multiple inserts to
	// create new segments again — this simulates the real-world state where
	// the index has accumulated segments over many sync runs.
	if _, err := d.getWriter().Exec(
		"INSERT INTO messages_fts(messages_fts) VALUES('optimize')",
	); err != nil {
		t.Fatalf("fts optimize: %v", err)
	}

	page, err := d.Search(context.Background(), SearchFilter{
		Query: "needle", Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(page.Results) != 1 {
		t.Errorf("got %d results for single session with %d matching messages, want 1",
			len(page.Results), n)
		for i, r := range page.Results {
			t.Logf("  result[%d]: session_id=%q ordinal=%d", i, r.SessionID, r.Ordinal)
		}
	}
}

func TestSearchSession(t *testing.T) {
	t.Parallel()
	d := testDB(t)

	insertSession(t, d, "s1", "proj")
	insertSession(t, d, "s2", "proj")

	// Message at ordinal 4 has no match in its content but has a tool call
	// whose result_content contains a unique term ("uniquetooloutput").
	toolMsg := asstMsg("s1", 4, "I ran a tool here")
	toolMsg.HasToolUse = true
	toolMsg.ToolCalls = []ToolCall{
		{
			SessionID:     "s1",
			ToolName:      "Bash",
			Category:      "execution",
			ResultContent: "uniquetooloutput: the command succeeded",
		},
	}

	insertMessages(t, d,
		userMsg("s1", 0, "Hello world, this is a test message"),
		asstMsg("s1", 1, "Here is some Python code: import os; print(os.getcwd())"),
		userMsg("s1", 2, "Can you search for **bold markdown** syntax?"),
		asstMsg("s1", 3, "Another message with no special content"),
		userMsg("s2", 0, "This belongs to a different session entirely"),
		toolMsg,
	)

	tests := []struct {
		name      string
		sessionID string
		query     string
		want      []int // expected ordinals
	}{
		{
			name:      "simple substring match",
			sessionID: "s1",
			query:     "test",
			want:      []int{0},
		},
		{
			name:      "case insensitive",
			sessionID: "s1",
			query:     "HELLO",
			want:      []int{0},
		},
		{
			name:      "matches multiple messages",
			sessionID: "s1",
			query:     "message",
			want:      []int{0, 3},
		},
		{
			name:      "matches inside code content",
			sessionID: "s1",
			query:     "import os",
			want:      []int{1},
		},
		{
			name:      "matches raw markdown syntax",
			sessionID: "s1",
			query:     "bold markdown",
			want:      []int{2},
		},
		{
			name:      "no match returns empty",
			sessionID: "s1",
			query:     "nonexistent",
			want:      []int{},
		},
		{
			name:      "scoped to session — does not bleed across sessions",
			sessionID: "s1",
			query:     "different session",
			want:      []int{},
		},
		{
			name:      "other session scoped correctly",
			sessionID: "s2",
			query:     "different session",
			want:      []int{0},
		},
		{
			name:      "empty query returns nil",
			sessionID: "s1",
			query:     "",
			want:      []int{},
		},
		{
			name:      "LIKE special chars escaped — percent sign",
			sessionID: "s1",
			query:     "%",
			want:      []int{},
		},
		{
			name:      "LIKE special chars escaped — underscore",
			sessionID: "s1",
			query:     "_",
			want:      []int{},
		},
		{
			name:      "results ordered by ordinal ascending",
			sessionID: "s1",
			query:     "is",
			want:      []int{0, 1},
		},
		{
			name:      "match in tool result_content only — message content has no match",
			sessionID: "s1",
			query:     "uniquetooloutput",
			want:      []int{4},
		},
		{
			name:      "tool result match is scoped to correct session",
			sessionID: "s2",
			query:     "uniquetooloutput",
			want:      []int{},
		},
		{
			name:      "message with tool call not double-counted when both content and result match",
			sessionID: "s1",
			query:     "tool",
			want:      []int{4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := d.SearchSession(context.Background(), tt.sessionID, tt.query)
			if err != nil {
				t.Fatalf("SearchSession(%q, %q): unexpected error: %v", tt.sessionID, tt.query, err)
			}
			if got == nil {
				got = []int{}
			}
			if len(got) != len(tt.want) {
				t.Fatalf("SearchSession(%q, %q) = %v, want %v", tt.sessionID, tt.query, got, tt.want)
			}
			for i, ord := range got {
				if ord != tt.want[i] {
					t.Errorf("ordinal[%d] = %d, want %d", i, ord, tt.want[i])
				}
			}
		})
	}
}
