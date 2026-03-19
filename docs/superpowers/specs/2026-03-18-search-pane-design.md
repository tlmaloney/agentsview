# Search Pane Improvements Design

Date: 2026-03-18

## Overview

Improve the Command Palette search experience (Cmd+K) by grouping FTS results
by session, displaying session-level metadata (agent, timestamp, short ID), and
adding a relevance/recency sort toggle.

## Problem

The current search returns flat per-message FTS5 results ordered by relevance
rank. This means:

- One session with many matches consumes multiple result slots, crowding out
  other sessions.
- The result limit (30) applies to messages, not sessions, so you may see 30
  matches from 5 sessions and miss other relevant sessions entirely.
- No timestamp is shown, making it hard to gauge how recent a match is.
- No sort control — results are always ranked by relevance.
- No session identifier visible for reference.

## Design

### Backend (`internal/db/search.go`, `internal/server/search.go`)

**SearchResult struct** — add two fields:

```go
type SearchResult struct {
    SessionID      string  `json:"session_id"`
    Project        string  `json:"project"`
    Agent          string  `json:"agent"`
    Ordinal        int     `json:"ordinal"`
    Role           string  `json:"role"`
    SessionEndedAt string  `json:"session_ended_at"`
    Snippet        string  `json:"snippet"`
    Rank           float64 `json:"rank"`
}
```

`Agent` is the agent name (e.g. `"claude"`, `"codex"`). `SessionEndedAt` is the
session's `ended_at` timestamp; falls back to `started_at` when `ended_at` is
NULL (active session).

**SearchFilter struct** — add `Sort` field:

```go
type SearchFilter struct {
    Query   string
    Project string
    Sort    string // "relevance" (default) or "recency"
    Cursor  int
    Limit   int
}
```

**Search() SQL** — change to GROUP BY session, one row per session:

```sql
SELECT m.session_id, s.project, s.agent,
    COALESCE(s.ended_at, s.started_at) as session_ended_at,
    m.ordinal,
    snippet(messages_fts, 0, '<mark>', '</mark>', '...', 32) as snippet,
    min(rank) as rank
FROM messages_fts
JOIN messages m ON messages_fts.rowid = m.id
JOIN sessions s ON m.session_id = s.id
WHERE messages_fts MATCH ?
  AND s.deleted_at IS NULL
  AND m.is_system = 0
GROUP BY m.session_id
ORDER BY <rank | s.ended_at DESC>
LIMIT ? OFFSET ?
```

SQLite's documented behavior: when `MIN()` is present in the result set,
non-aggregate columns (including FTS5 auxiliary `snippet()`) take their values
from the row that produced the minimum — i.e., the best-ranked message. This
gives the most relevant snippet without a subquery.

`ORDER BY` switches between `min(rank)` (relevance) and
`COALESCE(s.ended_at, s.started_at) DESC` (recency) based on `SearchFilter.Sort`.

The `LIMIT`/`OFFSET` now applies at the session level, so the result count
means "N matching sessions" rather than "N matching messages".

**handleSearch()** — accept `sort` query param and pass to filter. Invalid
values default to `"relevance"`.

### Frontend

**`SearchResult` type** (`frontend/src/lib/api/types/core.ts`):

```typescript
export interface SearchResult {
  session_id: string;
  project: string;
  agent: string;
  ordinal: number;
  role: string;
  session_ended_at: string;
  snippet: string;
  rank: number;
}
```

**`SearchStore`** (`frontend/src/lib/stores/search.svelte.ts`):

- Add `sort: "relevance" | "recency"` state, defaulting to `"relevance"`.
- Expose `setSort(s)` method that updates state and re-runs the current query.
- Pass `sort` to `api.search()`.

**`CommandPalette`** (`frontend/src/lib/components/command-palette/CommandPalette.svelte`):

_Sort toggle_ — a two-button control shown above the results when a search is
active (query >= 3 chars): **Relevance** | **Recency**. The active option is
highlighted. Switching calls `searchStore.setSort()`.

_Result row_ — single line, replacing the current per-message layout:

```
[agent dot] [snippet with highlights...]  [project · 2h ago] [abc12345]
```

- **Agent dot**: colored circle using `agentColor(result.agent)`, same as the
  sidebar.
- **Snippet**: `{@html sanitizeSnippet(result.snippet)}` — the `<mark>` tags
  are already sanitized by the existing helper. Truncated with ellipsis if
  overflow.
- **Meta**: `project · relative time` — `project` truncated to 20 chars,
  relative time derived from `session_ended_at` using `formatRelativeTime()`.
- **Short ID**: first 8 characters of `session_id`, muted monospace style, far
  right. Click copies the full `session_id` to clipboard (silent no-op on
  failure).
- **Role badge** (U/A): removed — results are now session-level.

Navigation is unchanged: clicking a result calls `sessions.selectSession()` and
`ui.scrollToOrdinal(result.ordinal, result.session_id)`.

The "recent sessions" pre-search view (query < 3 chars) is unchanged.

### Error Handling

| Case | Behaviour |
| --- | --- |
| Session with no `ended_at` | Use `started_at` via `COALESCE` in SQL |
| Invalid `sort` param | Default to `"relevance"` |
| Clipboard copy failure | Silent no-op |

### Testing

**Go** (`internal/db/search_test.go`):

- Verify `GROUP BY` deduplication: multiple messages in same session produce one
  result.
- Verify `agent` and `session_ended_at` fields are populated.
- Verify `Sort: "recency"` orders by session timestamp descending.
- Verify existing limit/cursor pagination still works at session level.

**TypeScript**:

- `SearchStore`: test `sort` state; verify `setSort()` triggers re-search with
  new param.
- `CommandPalette`: update snapshot/unit tests for new single-line result
  layout; verify sort toggle renders and calls `setSort`.

## Files Changed

| File | Change |
| --- | --- |
| `internal/db/search.go` | Extend `SearchResult`, `SearchFilter`; update SQL |
| `internal/server/search.go` | Accept and pass `sort` query param |
| `frontend/src/lib/api/types/core.ts` | Add `agent`, `session_ended_at` to `SearchResult` |
| `frontend/src/lib/api/client.ts` | Pass `sort` param to search endpoint |
| `frontend/src/lib/stores/search.svelte.ts` | Add `sort` state and `setSort()` |
| `frontend/src/lib/components/command-palette/CommandPalette.svelte` | New result layout + sort toggle |

## Out of Scope

- Showing multiple snippet matches per session (expandable) — keep one per
  session for now.
- Persistent sort preference across page reloads.
- Changing the pre-search "recent sessions" view.
