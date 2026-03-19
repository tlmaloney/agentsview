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

**SearchResult struct** — remove `Role` and `Timestamp`, add `Agent` and `SessionEndedAt`:

```go
type SearchResult struct {
    SessionID      string  `json:"session_id"`
    Project        string  `json:"project"`
    Agent          string  `json:"agent"`
    Ordinal        int     `json:"ordinal"`
    SessionEndedAt string  `json:"session_ended_at"`
    Snippet        string  `json:"snippet"`
    Rank           float64 `json:"rank"`
}
```

`Role` and `Timestamp` are removed — results are now session-level, so
per-message role and message timestamp are no longer meaningful. `Agent` is the
agent name (e.g. `"claude"`, `"codex"`). `SessionEndedAt` is
`COALESCE(s.ended_at, s.started_at)` — the most recent session timestamp, used
for relative time display.

`Ordinal` is the ordinal of the best-ranked matching message. It is used by the
frontend to scroll to the match after selecting the session. See the SQL section
below for correctness guarantees.

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

**Sort validation in `db.Search()`** — `Sort` must be validated inside
`db.Search()` before it is interpolated into the `ORDER BY` clause (which
cannot be parameterised). Any value other than `"recency"` defaults to
`"relevance"`. This protects against SQL injection regardless of call site:

```go
orderBy := "min(rank)"
if f.Sort == "recency" {
    orderBy = "COALESCE(s.ended_at, s.started_at) DESC"
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
ORDER BY <min(rank) | COALESCE(s.ended_at, s.started_at) DESC>
LIMIT ? OFFSET ?
```

**SQLite min/max optimization and `snippet()`**: SQLite documents that when a
`MIN()` or `MAX()` aggregate is present in the SELECT list, non-aggregate
columns in the same SELECT take their values from the row that produced the
minimum/maximum. This guarantees that `m.ordinal` comes from the best-ranked
message row. The same optimization applies to the `snippet()` FTS5 auxiliary
function in practice (it evaluates against the current row's match context),
though this is not explicitly documented for auxiliary functions. This has been
verified manually. If this behavior regresses in a future SQLite version, the
fallback is a subquery that selects `rowid` of the best-ranked message per
session and calls `snippet()` on that single row.

`ORDER BY` switches between `min(rank)` (relevance) and
`COALESCE(s.ended_at, s.started_at) DESC` (recency) based on
`SearchFilter.Sort`. Project filters combine naturally with both sort modes.

The `LIMIT`/`OFFSET` now applies at the session level, so the result count
means "N matching sessions" rather than "N matching messages".

**handleSearch()** — accept `sort` query param, validate it (only `"relevance"`
and `"recency"` are accepted; others default to `"relevance"`), and pass to
filter.

### PostgreSQL path (`internal/postgres/messages.go`)

The PG `Store.Search()` currently returns flat per-message results using ILIKE.
It must be updated to produce the same session-grouped shape as the SQLite path.

PostgreSQL does not have FTS5 or a native relevance rank. The equivalent of
`GROUP BY + min(rank)` is `DISTINCT ON (session_id)`, which picks one row per
session based on a secondary `ORDER BY`. For "relevance" ordering, the best
available proxy is position of the match in the message content (earlier =
closer to the top = more relevant). For "recency", sessions are ordered by
`COALESCE(s.ended_at, s.started_at) DESC`.

**`is_system` column** — the PG `messages` table currently has no `is_system`
column. This feature requires adding it:

- `internal/postgres/schema.go`: add `is_system BOOLEAN NOT NULL DEFAULT FALSE`
  to the `messages` DDL.
- `internal/postgres/push.go`: add `is_system` to the message INSERT statement
  and its corresponding bind value (sourced from the SQLite message's
  `is_system` field).

**Updated SQL** — two-step using a CTE:

```sql
WITH best_per_session AS (
    SELECT DISTINCT ON (m.session_id)
        m.session_id, s.project, s.agent,
        COALESCE(s.ended_at, s.started_at) AS session_ended_at,
        m.ordinal,
        POSITION(LOWER($2) IN LOWER(m.content)) AS match_pos,
        <snippet expression> AS snippet
    FROM messages m
    JOIN sessions s ON m.session_id = s.id
    WHERE m.content ILIKE '%' || $1 || '%' ESCAPE E'\\'
      AND s.deleted_at IS NULL
      AND m.is_system = FALSE
    ORDER BY m.session_id,
        POSITION(LOWER($2) IN LOWER(m.content)) ASC
)
SELECT session_id, project, agent, session_ended_at, ordinal,
       snippet, 1.0 AS rank
FROM best_per_session
ORDER BY <session_ended_at DESC | match_pos ASC>
LIMIT $N OFFSET $N+1
```

`DISTINCT ON` requires the sort key to start with the `DISTINCT ON` column
(`m.session_id`), so the inner query always picks the earliest-match message per
session (best relevance proxy) regardless of the `Sort` value. `match_pos` is
computed in the inner query and exposed as a CTE column so the outer `ORDER BY`
for relevance uses the same value — avoiding applying `POSITION` to the already-
truncated snippet.

The outer query sorts session rows by recency (`session_ended_at DESC`) or
relevance (`match_pos ASC`). Both columns are in the CTE output, so no
additional parameter references are needed in the outer query.

The `LIMIT`/`OFFSET` parameter indices shift when a project filter is added
(same dynamic `argIdx` pattern as the current `messages.go` implementation).
The indices shown above (`$N`/`$N+1`) are placeholders; the actual indices
depend on whether the optional project clause is present.

**Snippet highlights**: PG snippets are built with `SUBSTRING`/`POSITION` and
do not produce `<mark>` tags (ILIKE has no highlight auxiliary). This is the
same as today. The `sanitizeSnippet()` helper on the frontend already handles
plain text safely, so no frontend change is needed for this case.

**Scan changes**: remove `Role` and `Timestamp` from the scan; add `Agent` and
`SessionEndedAt`. The `Rank` field stays as `1.0`.

**Sort validation**: same whitelist pattern as SQLite — any value other than
`"recency"` defaults to relevance ordering.

### Frontend

**`SearchResult` type** (`frontend/src/lib/api/types/core.ts`) — remove `role`
and `timestamp`, add `agent` and `session_ended_at`:

```typescript
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

**`api.search()` signature** (`frontend/src/lib/api/client.ts`) — add `sort`
to the options object:

```typescript
search(
  query: string,
  opts?: { project?: string; limit?: number; cursor?: number; sort?: "relevance" | "recency" },
  init?: RequestInit,
): Promise<SearchResponse>
```

The `sort` value is appended as a `sort=recency` query param when present.

**`SearchStore`** (`frontend/src/lib/stores/search.svelte.ts`):

- Add `sort: "relevance" | "recency"` state, defaulting to `"relevance"`.
- Expose `setSort(s: "relevance" | "recency")` method that updates state and
  re-runs the current query if one is active.
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
| Invalid `sort` param | Default to `"relevance"` at both handler and `db.Search()` |
| Clipboard copy failure | Silent no-op |

### Testing

**Go** (`internal/db/search_test.go`):

- Verify `GROUP BY` deduplication: multiple messages in same session produce one
  result.
- Verify `agent` and `session_ended_at` fields are populated correctly.
- Verify the returned `snippet` corresponds to the highest-ranked message
  content, not an arbitrary one (regression guard for the min/max optimization).
- Verify `Sort: "recency"` orders by session timestamp descending.
- Verify existing limit/cursor pagination still works at session level.
- Verify that an unrecognised `Sort` value inside `db.Search()` defaults to
  relevance ordering (SQL injection guard).
- Verify system messages (`is_system = 1`) are excluded from results.

**PostgreSQL** (`internal/postgres/` integration tests, requires `pgtest` tag):

- Verify `DISTINCT ON` deduplication: multiple matching messages in one session
  produce one result.
- Verify `agent` and `session_ended_at` fields are populated from the sessions
  join.
- Verify `Sort: "recency"` orders by `session_ended_at` descending.
- Verify `Sort: "relevance"` picks the message with the earliest match position.
- Verify system messages (`is_system = TRUE`) are excluded from results.

**TypeScript**:

- `SearchStore`: test `sort` state; verify `setSort()` triggers re-search with
  new param; verify default is `"relevance"`.
- `CommandPalette`: update snapshot/unit tests for new single-line result
  layout; verify sort toggle renders and calls `setSort`.

## Files Changed

| File | Change |
| --- | --- |
| `internal/db/search.go` | Extend `SearchResult`, `SearchFilter`; remove `Role` and `Timestamp`; validate `Sort`; update SQL |
| `internal/server/search.go` | Accept and validate `sort` query param; pass to filter |
| `internal/postgres/schema.go` | Add `is_system BOOLEAN NOT NULL DEFAULT FALSE` to messages DDL |
| `internal/postgres/push.go` | Add `is_system` to message INSERT |
| `internal/postgres/messages.go` | Update `Search()` to session-grouped `DISTINCT ON` query; remove `Role`/`Timestamp`; add `Agent`/`SessionEndedAt`/`match_pos`; support `Sort`; filter `is_system` |
| `frontend/src/lib/api/types/core.ts` | Remove `role` and `timestamp`; add `agent`, `session_ended_at` to `SearchResult` |
| `frontend/src/lib/api/client.ts` | Add `sort` option to `search()` signature |
| `frontend/src/lib/stores/search.svelte.ts` | Add `sort` state and `setSort()` |
| `frontend/src/lib/components/command-palette/CommandPalette.svelte` | New result layout + sort toggle |

## Out of Scope

- Showing multiple snippet matches per session (expandable) — keep one per
  session for now.
- Persistent sort preference across page reloads.
- Changing the pre-search "recent sessions" view.
- Adding `<mark>` highlight tags to PostgreSQL snippets — ILIKE has no
  highlight auxiliary function; this is a known limitation of the PG path.
