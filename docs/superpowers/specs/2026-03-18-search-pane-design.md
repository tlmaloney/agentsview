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
- Sessions cannot be found by their display name or first message — only
  message content is indexed, so a renamed session ("my-refactor") is
  unreachable unless its messages happen to contain that text.

## Design

### Backend (`internal/db/search.go`, `internal/server/search.go`)

**SearchResult struct** — remove `Role` and `Timestamp`, add `Agent`,
`SessionEndedAt`, and `Name`:

```go
type SearchResult struct {
    SessionID      string  `json:"session_id"`
    Project        string  `json:"project"`
    Agent          string  `json:"agent"`
    Name           string  `json:"name"`
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
for relative time display. `Name` is `COALESCE(s.display_name, s.first_message,
'')` — the user-visible session title; always populated.

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

**Search() SQL** — two-branch UNION: FTS message matches + session name LIKE
matches, deduplicated by session_id.

The message-match branch uses the two-phase subquery approach (inner subquery
finds best FTS rowid per session, outer JOIN calls `snippet()` on that row):

```sql
-- Branch 1: sessions that match via message content (FTS)
SELECT m.session_id, s.project, s.agent,
    COALESCE(s.display_name, s.first_message, '') AS name,
    COALESCE(s.ended_at, s.started_at, '') AS session_ended_at,
    best.best_ordinal AS ordinal,
    snippet(messages_fts, 0, '<mark>', '</mark>', '...', 32) AS snippet,
    best.best_rank AS rank
FROM ( <inner: GROUP BY session_id, MIN(rank)> ) AS best
JOIN messages_fts ON messages_fts.rowid = best.best_rowid
JOIN messages m ON m.id = best.best_rowid
JOIN sessions s ON m.session_id = s.id
WHERE messages_fts MATCH ?
UNION
-- Branch 2: sessions whose name matches but have no FTS-matched messages
SELECT s.id, s.project, s.agent,
    COALESCE(s.display_name, s.first_message, '') AS name,
    COALESCE(s.ended_at, s.started_at, '') AS session_ended_at,
    -1 AS ordinal,
    COALESCE(s.display_name, s.first_message, '') AS snippet,
    0.0 AS rank
FROM sessions s
WHERE (s.display_name LIKE ? ESCAPE '\'
    OR s.first_message LIKE ? ESCAPE '\')
  AND s.deleted_at IS NULL
  AND s.id NOT IN (SELECT session_id FROM <inner FTS subquery>)
ORDER BY <rank | session_ended_at DESC>
LIMIT ? OFFSET ?
```

`UNION` (not `UNION ALL`) deduplicates — a session matching both branches
appears once with the FTS row (which has a real snippet and rank). Sessions
matching only by name have `ordinal = -1`; the frontend treats this as "no
specific message to scroll to" and just navigates to the session.

`ORDER BY` switches between `rank ASC` (relevance — lower FTS rank = better
match; name-only rows rank 0.0, sorted after FTS matches) and
`session_ended_at DESC` (recency). Project filters apply to both branches.

The `LIMIT`/`OFFSET` applies at the session level.

**SQLite min/max optimization and `snippet()`**: See original note above. The
same two-phase subquery approach is used: the inner query uses `GROUP BY
session_id` with `MIN(rank)` and SQLite's min/max optimization to pick the
best-ranked FTS rowid per session. The outer query joins back to call
`snippet()` on that specific row with a `WHERE messages_fts MATCH ?` to avoid
FTS index segment duplication.

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

**Updated SQL** — two-branch UNION CTE, mirroring the SQLite shape:

```sql
WITH msg_matches AS (
    SELECT DISTINCT ON (m.session_id)
        m.session_id, s.project, s.agent,
        COALESCE(s.display_name, s.first_message, '') AS name,
        COALESCE(s.ended_at, s.started_at) AS session_ended_at,
        m.ordinal,
        POSITION(LOWER($2) IN LOWER(m.content)) AS match_pos,
        <snippet expression> AS snippet,
        1 AS match_priority
    FROM messages m
    JOIN sessions s ON m.session_id = s.id
    WHERE m.content ILIKE '%' || $1 || '%' ESCAPE E'\\'
      AND s.deleted_at IS NULL
      AND m.is_system = FALSE
      <optional project clause>
    ORDER BY m.session_id,
        POSITION(LOWER($2) IN LOWER(m.content)) ASC,
        m.ordinal ASC
),
name_matches AS (
    SELECT s.id, s.project, s.agent,
        COALESCE(s.display_name, s.first_message, '') AS name,
        COALESCE(s.ended_at, s.started_at) AS session_ended_at,
        -1 AS ordinal,
        0 AS match_pos,
        COALESCE(s.display_name, s.first_message, '') AS snippet,
        2 AS match_priority
    FROM sessions s
    WHERE (s.display_name ILIKE '%' || $1 || '%' ESCAPE E'\\'
        OR s.first_message ILIKE '%' || $1 || '%' ESCAPE E'\\')
      AND s.deleted_at IS NULL
      AND s.id NOT IN (SELECT session_id FROM msg_matches)
      <optional project clause>
),
combined AS (SELECT * FROM msg_matches UNION ALL SELECT * FROM name_matches)
SELECT session_id, project, agent, name,
       session_ended_at, ordinal, snippet, 1.0 AS rank
FROM combined
ORDER BY <match_pos ASC, session_ended_at DESC | session_ended_at DESC>
LIMIT $N OFFSET $N+1
```

`DISTINCT ON` in `msg_matches` picks one row per session (earliest match
position). `name_matches` excludes any session already in `msg_matches` via
`NOT IN`. The `combined` CTE uses `UNION ALL` (disjoint sets, so no
duplicates). `match_pos = 0` on name-only rows sorts them last under relevance
ordering (position 0 means "no message position" — placed after real message
matches by the outer `ORDER BY match_pos ASC`).

The outer query sorts by recency (`session_ended_at DESC`) or relevance
(`match_pos ASC, session_ended_at DESC` as tiebreaker). Both columns are in
the CTE output.

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
and `timestamp`, add `agent`, `session_ended_at`, and `name`:

```typescript
export interface SearchResult {
  session_id: string;
  project: string;
  agent: string;
  name: string;
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

_Result row_ — two-line layout replacing the current per-message layout:

```
[agent dot] [session name / first message]         [project · 2h ago] [abc12345]
            [snippet with highlights...]
```

- **Agent dot**: colored circle using `agentColor(result.agent)`, same as the
  sidebar.
- **Session name**: `result.name` (first line, primary text) — the display name
  if set, otherwise the first message. Truncated with ellipsis. When the match
  was on the session name itself (`ordinal === -1`), this is the only text line
  (snippet is omitted or equals name). When the match was on message content,
  the snippet is shown below.
- **Snippet**: `{@html sanitizeSnippet(result.snippet)}` — the `<mark>` tags
  are sanitized by the existing helper. Shown only when it differs from the
  session name (i.e., when the match came from message content, not the name).
  Muted color, smaller font.
- **Meta**: `project · relative time` — `project` truncated to 20 chars,
  relative time derived from `session_ended_at` using `formatRelativeTime()`.
- **Short ID**: first 8 characters of `session_id` (stripped of agent prefix
  via `stripIdPrefix`), muted monospace style, far right. Click copies the full
  `session_id` to clipboard (silent no-op on failure).
- **Role badge** (U/A): removed — results are now session-level.

When `ordinal === -1` the result matched only via session name. Clicking it
calls `sessions.selectSession()` without `ui.scrollToOrdinal()` (there is no
specific message to scroll to).

Navigation is unchanged: clicking a result calls `sessions.selectSession()` and
`ui.scrollToOrdinal(result.ordinal, result.session_id)`.

The "recent sessions" pre-search view (query < 3 chars) is unchanged.

### Error Handling

| Case | Behaviour |
| --- | --- |
| Session with no `ended_at` | Use `started_at` via `COALESCE` in SQL |
| Invalid `sort` param | Default to `"relevance"` at both handler and `db.Search()` |
| Clipboard copy failure | Silent no-op |
| Session with no name | `name` field is empty string; frontend shows nothing in the name line |
| Name-only match (`ordinal = -1`) | Navigate to session, skip `scrollToOrdinal` |

### Testing

**Go** (`internal/db/search_test.go`):

- Verify `GROUP BY` deduplication: multiple messages in same session produce one
  result.
- Verify `agent`, `session_ended_at`, and `name` fields are populated correctly.
- Verify the returned `snippet` corresponds to the highest-ranked message
  content, not an arbitrary one (regression guard for the min/max optimization).
- Verify `Sort: "recency"` orders by session timestamp descending.
- Verify existing limit/cursor pagination still works at session level.
- Verify that an unrecognised `Sort` value inside `db.Search()` defaults to
  relevance ordering (SQL injection guard).
- Verify system messages (`is_system = 1`) are excluded from results.
- Verify session name match: a session whose `display_name` contains the query
  term but has no matching messages appears in results with `ordinal = -1`.
- Verify session name match with `first_message`: same as above but using the
  auto-populated first message field.
- Verify deduplication across branches: a session that matches both by message
  content and by session name appears exactly once (with the FTS snippet).

**PostgreSQL** (`internal/postgres/` integration tests, requires `pgtest` tag):

- Verify `DISTINCT ON` deduplication: multiple matching messages in one session
  produce one result.
- Verify `agent`, `session_ended_at`, and `name` fields are populated.
- Verify `Sort: "recency"` orders by `session_ended_at` descending.
- Verify `Sort: "relevance"` picks the message with the earliest match position.
- Verify system messages (`is_system = TRUE`) are excluded from results.
- Verify session name match (display_name ILIKE) returns session with
  `ordinal = -1`.
- Verify no duplication when session matches both name and message content.

**TypeScript**:

- `SearchStore`: test `sort` state; verify `setSort()` triggers re-search with
  new param; verify default is `"relevance"`.
- `CommandPalette`: update snapshot/unit tests for new two-line result layout;
  verify sort toggle renders and calls `setSort`; verify name-only result
  (`ordinal === -1`) navigates without scroll.

## Files Changed

| File | Change |
| --- | --- |
| `internal/db/search.go` | Extend `SearchResult` (add `Name`), `SearchFilter`; remove `Role` and `Timestamp`; validate `Sort`; update SQL to UNION message + session name search |
| `internal/db/search_test.go` | Add tests for session name match, cross-branch deduplication, `name` field |
| `internal/server/search.go` | Accept and validate `sort` query param; pass to filter |
| `internal/postgres/schema.go` | Add `is_system BOOLEAN NOT NULL DEFAULT FALSE` to messages DDL |
| `internal/postgres/push.go` | Add `is_system` to message INSERT |
| `internal/postgres/messages.go` | Update `Search()` to UNION CTE with `DISTINCT ON`; add `Name`; session name ILIKE branch; support `Sort`; filter `is_system` |
| `internal/postgres/messages_pgtest_test.go` | Add tests for session name match and cross-branch deduplication |
| `frontend/src/lib/api/types/core.ts` | Remove `role` and `timestamp`; add `agent`, `session_ended_at`, `name` to `SearchResult` |
| `frontend/src/lib/api/client.ts` | Add `sort` option to `search()` signature |
| `frontend/src/lib/stores/search.svelte.ts` | Add `sort` state and `setSort()` |
| `frontend/src/lib/stores/search.test.ts` | Update factory; sort state and setSort tests |
| `frontend/src/lib/components/command-palette/CommandPalette.svelte` | Two-line result layout (name + snippet), sort toggle, name-only navigation |

## Out of Scope

- Showing multiple snippet matches per session (expandable) — keep one per
  session for now.
- Persistent sort preference across page reloads.
- Changing the pre-search "recent sessions" view.
- Adding `<mark>` highlight tags to PostgreSQL snippets — ILIKE has no
  highlight auxiliary function; this is a known limitation of the PG path.
- Highlighting matched text within the session name — the name is plain text,
  not processed through FTS5 `snippet()`.
