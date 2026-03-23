# Claude Legacy Cache JSON Parser

## Summary

Add support for parsing older Claude Code session files stored as single
JSON objects (not JSONL) in `cache/` subdirectories under Claude project
directories. These files are the only remaining copy of historical
sessions whose original JSONL source files were deleted.

## Background

Claude Code historically stored session data in a JSON cache format
alongside JSONL session files. The cache files live at
`~/.claude/projects/<project>/cache/<uuid>.json` and contain full
conversation data. When the original JSONL files were removed, these
cache files became the sole archive of those sessions.

### JSON cache format

The file is a single JSON object. Top-level keys are ISO 8601 timestamps
(e.g. `"2025-06-27T00:10:14.288Z"`), each mapping to an array of one or
more message entries. A special `"_no_timestamp"` key holds summary
entries (type `"summary"`) that are not conversation messages.

Each message entry has the same inner structure as JSONL entries:

```json
{
  "type": "user" | "assistant",
  "uuid": "...",
  "parentUuid": "...",
  "timestamp": "2025-06-27T00:10:14.288Z",
  "sessionId": "...",
  "message": {
    "role": "user" | "assistant",
    "model": "claude-sonnet-4-20250514",
    "content": "..." | [{"type": "text", "text": "..."}, ...],
    "usage": { ... }
  }
}
```

An `index.json` file in the cache directory maps JSONL source files to
cached equivalents; it is not a session file and must be skipped.

## Design

### 1. Parser: `internal/parser/claude_cache.go`

New file with a single public function:

```go
func ParseClaudeCacheSession(
    path, project, machine string,
) ([]ParseResult, error)
```

Behavior:

1. Read the entire file with `os.ReadFile` and unmarshal as a JSON
   object (`map[string]json.RawMessage`). Cache files are expected to be
   at most a few MB; observed sizes are under 1 MB. No streaming is
   needed.
2. Skip the `_no_timestamp` key.
3. Sort remaining keys (timestamps) lexicographically to establish
   message order.
4. For each timestamp key, decode the array of entries. Collect entries
   where `type` is `"user"` or `"assistant"`.
5. Extract the session ID from the filename (strip `.json` extension).
6. Process entries linearly (no DAG/fork detection). Reuse
   `ExtractTextContent` for content extraction and
   `extractClaudeTokenFields` for token/model metadata.
7. Return a single `ParseResult` with `Agent: AgentClaude`.

`agent-*.json` files are parsed as standalone sessions with
`Agent: AgentClaude` (no subagent linking). They live in the same
`cache/` directory as regular session files.

The `project` value comes from the `DiscoveredFile.Project` field set
during discovery (the parent directory name). No `cwd` or `gitBranch`
hint extraction is needed — cache files may not contain these fields
reliably, and the directory-based project name is sufficient.

### 2. Discovery: `internal/parser/discovery.go`

New functions:

```go
func DiscoverClaudeCacheSessions(projectsDir string) []DiscoveredFile
func FindClaudeCacheSourceFile(projectsDir, sessionID string) string
```

`DiscoverClaudeCacheSessions`:

1. Iterates project directories under `projectsDir`.
2. For each, checks for a `cache/` subdirectory.
3. Collects all `*.json` files, excluding `index.json`.
4. Returns `DiscoveredFile` entries with `Agent: AgentClaude`.

`FindClaudeCacheSourceFile`:

1. Searches `<project>/cache/<sessionID>.json` across project
   directories.
2. Called as a fallback from `FindClaudeSourceFile` when the `.jsonl`
   lookup fails and `ClaudeLegacyCache` is enabled. This ensures
   existing source-file lookups for cache-originated sessions resolve
   correctly without modifying the `AgentDef.FindSourceFunc` signature.

### 3. Config: `internal/config/config.go`

New field on `Config`:

```go
ClaudeLegacyCache bool `json:"claude_legacy_cache" toml:"claude_legacy_cache"`
```

Default: `false`. Enables scanning of `cache/` subdirectories. TOML
config only (no env var). Threaded from `Config` into `EngineConfig` in
`main.go` when constructing the sync engine.

### 4. Sync engine: `internal/sync/engine.go`

New field on `EngineConfig`:

```go
ClaudeLegacyCache bool
```

**Discovery integration:** In `syncAllLocked`, after the normal
`Registry`-driven discovery loop, add a conditional block: if
`ClaudeLegacyCache` is true, call `DiscoverClaudeCacheSessions` on each
Claude project directory and append the results to the Claude work
queue. This is a special case after the main loop, not a Registry
modification.

**Dispatch:** Add a new `processClaudeCache` method (parallel to
`processClaude`) that:

1. Calls `ParseClaudeCacheSession` directly (no incremental parsing, no
   `ExtractClaudeProjectHints`, no `tryIncrementalJSONL`).
2. Returns the `ParseResult` for storage.

The `.json` extension is used to distinguish cache files from JSONL
files in the Claude dispatch path.

**Deduplication:** If a JSONL session already exists in the database
with the same session ID, the cache file is skipped. JSONL-parsed
sessions are considered more authoritative. The existing `UpsertSession`
path handles this naturally — JSONL files are discovered first, so their
data is already in the DB. Cache files with a matching session ID are
added to the skip cache.

**File watcher:** Cache files are static and do not change after
creation. No watcher integration is needed. Cache sessions are
discovered only during full periodic syncs (every 15 min) and the
initial startup sync.

**Stats:** Cache file counts are included in the existing Claude agent
count in sync stats and log output.

### 5. Testing

**`internal/parser/claude_cache_test.go`** -- table-driven tests:

- Normal session with user/assistant messages
- `_no_timestamp` entries are skipped
- `agent-*.json` files parse as standalone sessions
- Empty/malformed files return appropriate errors
- Timestamp extraction and session bounds
- Token usage extraction from assistant messages

**`internal/parser/discovery_test.go`** -- additional tests:

- `DiscoverClaudeCacheSessions` finds `*.json` in `cache/` subdirs
- Skips `index.json`
- Skips non-JSON files

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Sessions per file | One | Simpler; historical data does not need fork splitting |
| `agent-*.json` handling | Parse as standalone sessions | Simple; no subagent linking needed |
| `_no_timestamp` / summaries | Skip | Not conversation messages; no mapping to `ParsedMessage` |
| Opt-in mechanism | TOML config flag | Historical feature; most users don't have these files |
| DAG/fork detection | None (linear) | Agreed during design; keeps parser simple |
| Deduplication | Skip cache file if JSONL session exists | JSONL is more authoritative |
| File watcher | Not needed | Cache files are static |
| Project name | From directory structure | Cache files lack reliable cwd/gitBranch hints |
| File size | `os.ReadFile` (no streaming) | Observed cache files are under 1 MB |
| Sync dispatch | Separate `processClaudeCache` method | Avoids contaminating JSONL-specific logic |
