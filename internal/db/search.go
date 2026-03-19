package db

import (
	"context"
	"fmt"
	"strings"
)

const (
	DefaultSearchLimit = 50
	MaxSearchLimit     = 500
	snippetTokenLength = 32
)

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

// SearchPage holds paginated search results.
type SearchPage struct {
	Results    []SearchResult `json:"results"`
	NextCursor int            `json:"next_cursor,omitempty"`
}

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
	// best.best_rank holds the MIN(rank) from the subquery (lower = better).
	orderBy := "best.best_rank"
	if f.Sort == "recency" {
		orderBy = "COALESCE(s.ended_at, s.started_at) DESC"
	}

	// Build WHERE clauses for the inner subquery using s2/m2 aliases.
	innerWhere := []string{
		"messages_fts MATCH ?",
		"s2.deleted_at IS NULL",
		"m2.is_system = 0",
	}
	args := []any{f.Query}

	if f.Project != "" {
		innerWhere = append(innerWhere, "s2.project = ?")
		args = append(args, f.Project)
	}

	// SQLite FTS5 auxiliary functions (snippet, rank) cannot be used alongside
	// GROUP BY. We use a subquery to select the best-ranked FTS rowid per
	// session, then join back to the FTS table to call snippet() on that
	// specific row.
	//
	// The outer JOIN messages_fts must also include a MATCH clause. Without it,
	// SQLite FTS5 scans all internal index segments and can return the same
	// rowid multiple times (once per segment), causing duplicate results even
	// though best_rowid is unique per session. The MATCH constrains the FTS5
	// scan to documents matching the query, ensuring exactly one row per rowid
	// and providing query context required by snippet().
	query := fmt.Sprintf(`
		SELECT m.session_id, s.project, s.agent,
			COALESCE(s.ended_at, s.started_at, '') AS session_ended_at,
			best.best_ordinal,
			snippet(messages_fts, 0, '<mark>', '</mark>',
				'...', %d) AS snippet,
			best.best_rank AS rank
		FROM (
			SELECT m2.session_id,
				messages_fts.rowid AS best_rowid,
				m2.ordinal AS best_ordinal,
				MIN(rank) AS best_rank
			FROM messages_fts
			JOIN messages m2 ON messages_fts.rowid = m2.id
			JOIN sessions s2 ON m2.session_id = s2.id
			WHERE %s
			GROUP BY m2.session_id
		) AS best
		JOIN messages_fts ON messages_fts.rowid = best.best_rowid
		JOIN messages m ON m.id = best.best_rowid
		JOIN sessions s ON m.session_id = s.id
		WHERE messages_fts MATCH ?
		ORDER BY %s
		LIMIT ? OFFSET ?`,
		snippetTokenLength,
		strings.Join(innerWhere, " AND "),
		orderBy,
	)
	// Append the outer MATCH arg before LIMIT/OFFSET.
	args = append(args, f.Query, f.Limit+1, f.Cursor)

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

// SearchSession performs a case-insensitive substring search within a single
// session's messages, returning matching ordinals in document order.
// This is used by the in-session find bar (analogous to browser Cmd+F).
// Both message content and tool-call result_content are searched so that
// matches inside tool output blocks are reachable. Only fields that the
// frontend renders and highlights are included to avoid phantom matches.
func (db *DB) SearchSession(
	ctx context.Context, sessionID, query string,
) ([]int, error) {
	if query == "" {
		return nil, nil
	}
	// Use LIKE for substring semantics consistent with browser find-bar UX.
	// SQLite LIKE is case-insensitive for ASCII by default.
	// LEFT JOIN tool_calls so that a hit in result_content also surfaces
	// the parent message ordinal; DISTINCT collapses multiple tool calls
	// on the same message into a single result.
	like := "%" + escapeLike(query) + "%"
	rows, err := db.getReader().QueryContext(ctx,
		`SELECT DISTINCT m.ordinal
		 FROM messages m
		 LEFT JOIN tool_calls tc ON tc.message_id = m.id
		 WHERE m.session_id = ?
		   AND m.is_system = 0
		   AND (m.content LIKE ? ESCAPE '\'
		        OR tc.result_content LIKE ? ESCAPE '\')
		 ORDER BY m.ordinal ASC`,
		sessionID, like, like,
	)
	if err != nil {
		return nil, fmt.Errorf("session search: %w", err)
	}
	defer rows.Close()

	var ordinals []int
	for rows.Next() {
		var ord int
		if err := rows.Scan(&ord); err != nil {
			return nil, fmt.Errorf("scanning ordinal: %w", err)
		}
		ordinals = append(ordinals, ord)
	}
	return ordinals, rows.Err()
}
