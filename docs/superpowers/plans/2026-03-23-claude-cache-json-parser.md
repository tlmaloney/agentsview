# Claude Legacy Cache JSON Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse older Claude Code session files stored as single JSON
objects in `cache/` subdirectories, opt-in via config flag.

**Architecture:** New `ParseClaudeCacheSession` parser reads JSON cache
files linearly, reusing existing content extraction. Discovery,
config, and sync engine are extended to support the new file format
behind a `claude_legacy_cache` boolean flag.

**Tech Stack:** Go, gjson, SQLite (existing), table-driven tests

**Spec:**
`docs/superpowers/specs/2026-03-23-claude-cache-json-parser-design.md`

---

## File Structure

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `internal/parser/claude_cache.go` | Parse JSON cache files into `ParseResult` |
| Create | `internal/parser/claude_cache_test.go` | Table-driven parser tests |
| Modify | `internal/parser/discovery.go` | Add `DiscoverClaudeCacheSessions`, `FindClaudeCacheSourceFile` |
| Modify | `internal/parser/discovery_test.go` | Tests for cache discovery |
| Modify | `internal/config/config.go` | Add `ClaudeLegacyCache` field |
| Modify | `internal/sync/engine.go` | Add `ClaudeLegacyCache` to `EngineConfig`, discovery integration, `processClaudeCache` |
| Modify | `cmd/agentsview/main.go` | Thread config flag into `EngineConfig` |

---

### Task 1: Parser -- Failing Tests

**Files:**
- Create: `internal/parser/claude_cache_test.go`
- Create: `internal/parser/claude_cache.go` (stub)

- [ ] **Step 1: Write test file with table-driven tests**

Create `internal/parser/claude_cache_test.go`. Uses the same
`runClaudeParserTest` pattern from `claude_parser_test.go` but adapted
for JSON input. Since the parser reads from a file path, tests write
JSON to temp files.

```go
package parser

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeJSONCache writes a JSON cache file to a temp directory and
// returns the path. The data map is marshaled as-is.
func writeJSONCache(
	t *testing.T, name string, data map[string]any,
) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestParseClaudeCacheSession(t *testing.T) {
	tests := []struct {
		name         string
		filename     string
		data         map[string]any
		project      string
		wantMsgCount int
		wantFirstMsg string
		wantAgent    AgentType
		wantModel    string
		wantErr      bool
	}{
		{
			name:     "basic user and assistant messages",
			filename: "abc123.json",
			data: map[string]any{
				"2025-06-27T00:10:14.288Z": []any{
					map[string]any{
						"type":      "user",
						"uuid":      "u1",
						"timestamp": "2025-06-27T00:10:14.288Z",
						"message": map[string]any{
							"role":    "user",
							"content": "Hello world",
						},
					},
				},
				"2025-06-27T00:10:21.082Z": []any{
					map[string]any{
						"type":      "assistant",
						"uuid":      "a1",
						"parentUuid": "u1",
						"timestamp": "2025-06-27T00:10:21.082Z",
						"message": map[string]any{
							"role":    "assistant",
							"model":   "claude-sonnet-4-20250514",
							"content": "Hi there!",
						},
					},
				},
			},
			project:      "test-project",
			wantMsgCount: 2,
			wantFirstMsg: "Hello world",
			wantAgent:    AgentClaude,
			wantModel:    "claude-sonnet-4-20250514",
		},
		{
			name:     "skips _no_timestamp summaries",
			filename: "def456.json",
			data: map[string]any{
				"_no_timestamp": []any{
					map[string]any{
						"type":     "summary",
						"summary":  "Some summary",
						"leafUuid": "leaf1",
					},
				},
				"2025-06-27T00:10:14.288Z": []any{
					map[string]any{
						"type":      "user",
						"uuid":      "u1",
						"timestamp": "2025-06-27T00:10:14.288Z",
						"message": map[string]any{
							"role":    "user",
							"content": "Only real message",
						},
					},
				},
			},
			project:      "test-project",
			wantMsgCount: 1,
			wantFirstMsg: "Only real message",
			wantAgent:    AgentClaude,
		},
		{
			name:     "agent file parses as standalone session",
			filename: "agent-abc123.json",
			data: map[string]any{
				"2025-07-01T12:00:00.000Z": []any{
					map[string]any{
						"type":      "user",
						"uuid":      "u1",
						"timestamp": "2025-07-01T12:00:00.000Z",
						"message": map[string]any{
							"role":    "user",
							"content": "Agent task",
						},
					},
				},
			},
			project:      "test-project",
			wantMsgCount: 1,
			wantFirstMsg: "Agent task",
			wantAgent:    AgentClaude,
		},
		{
			name:     "token usage extracted from assistant",
			filename: "tok123.json",
			data: map[string]any{
				"2025-06-27T00:10:14.288Z": []any{
					map[string]any{
						"type":      "assistant",
						"uuid":      "a1",
						"timestamp": "2025-06-27T00:10:14.288Z",
						"message": map[string]any{
							"role":  "assistant",
							"model": "claude-sonnet-4-20250514",
							"content": "Response",
							"usage": map[string]any{
								"input_tokens":  100,
								"output_tokens": 50,
							},
						},
					},
				},
			},
			project:      "test-project",
			wantMsgCount: 1,
			wantAgent:    AgentClaude,
			wantModel:    "claude-sonnet-4-20250514",
		},
		{
			name:         "empty object produces zero messages",
			filename:     "empty.json",
			data:         map[string]any{},
			project:      "test-project",
			wantMsgCount: 0,
			wantAgent:    AgentClaude,
		},
		{
			name:     "skips non user/assistant entries",
			filename: "mixed.json",
			data: map[string]any{
				"2025-06-27T00:10:14.288Z": []any{
					map[string]any{
						"type":      "progress",
						"timestamp": "2025-06-27T00:10:14.288Z",
					},
					map[string]any{
						"type":      "user",
						"uuid":      "u1",
						"timestamp": "2025-06-27T00:10:14.288Z",
						"message": map[string]any{
							"role":    "user",
							"content": "Real message",
						},
					},
				},
			},
			project:      "test-project",
			wantMsgCount: 1,
			wantFirstMsg: "Real message",
			wantAgent:    AgentClaude,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeJSONCache(t, tt.filename, tt.data)
			results, err := ParseClaudeCacheSession(
				path, tt.project, "test-machine",
			)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf(
					"want 1 result, got %d", len(results),
				)
			}

			sess := results[0].Session
			msgs := results[0].Messages

			if sess.Agent != tt.wantAgent {
				t.Errorf(
					"agent = %q, want %q",
					sess.Agent, tt.wantAgent,
				)
			}
			if len(msgs) != tt.wantMsgCount {
				t.Errorf(
					"message count = %d, want %d",
					len(msgs), tt.wantMsgCount,
				)
			}
			if tt.wantFirstMsg != "" {
				firstUser := ""
				for _, m := range msgs {
					if m.Role == RoleUser {
						firstUser = m.Content
						break
					}
				}
				if firstUser != tt.wantFirstMsg {
					t.Errorf(
						"first user msg = %q, want %q",
						firstUser, tt.wantFirstMsg,
					)
				}
			}
			if tt.wantModel != "" {
				for _, m := range msgs {
					if m.Role == RoleAssistant &&
						m.Model != tt.wantModel {
						t.Errorf(
							"model = %q, want %q",
							m.Model, tt.wantModel,
						)
					}
				}
			}
		})
	}
}

func TestParseClaudeCacheSession_Timestamps(t *testing.T) {
	data := map[string]any{
		"2025-06-27T00:10:14.288Z": []any{
			map[string]any{
				"type":      "user",
				"uuid":      "u1",
				"timestamp": "2025-06-27T00:10:14.288Z",
				"message": map[string]any{
					"role":    "user",
					"content": "First",
				},
			},
		},
		"2025-06-27T00:15:00.000Z": []any{
			map[string]any{
				"type":      "assistant",
				"uuid":      "a1",
				"timestamp": "2025-06-27T00:15:00.000Z",
				"message": map[string]any{
					"role":    "assistant",
					"content": "Last",
				},
			},
		},
	}
	path := writeJSONCache(t, "ts-test.json", data)
	results, err := ParseClaudeCacheSession(
		path, "proj", "machine",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sess := results[0].Session
	wantStart := time.Date(
		2025, 6, 27, 0, 10, 14, 288000000, time.UTC,
	)
	wantEnd := time.Date(
		2025, 6, 27, 0, 15, 0, 0, time.UTC,
	)
	if !sess.StartedAt.Equal(wantStart) {
		t.Errorf(
			"StartedAt = %v, want %v",
			sess.StartedAt, wantStart,
		)
	}
	if !sess.EndedAt.Equal(wantEnd) {
		t.Errorf(
			"EndedAt = %v, want %v",
			sess.EndedAt, wantEnd,
		)
	}
}

func TestParseClaudeCacheSession_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	os.WriteFile(path, []byte("not json"), 0o644)

	_, err := ParseClaudeCacheSession(path, "proj", "machine")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
```

- [ ] **Step 2: Write minimal stub to allow compilation**

Create `internal/parser/claude_cache.go`:

```go
package parser

import "fmt"

// ParseClaudeCacheSession parses a Claude Code JSON cache
// session file. These are older session files stored as a
// single JSON object keyed by timestamps, found in cache/
// subdirectories under Claude project directories.
func ParseClaudeCacheSession(
	path, project, machine string,
) ([]ParseResult, error) {
	return nil, fmt.Errorf("not implemented")
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run TestParseClaudeCache -v`

Expected: Tests compile but FAIL (not implemented).

- [ ] **Step 4: Commit**

```bash
git add internal/parser/claude_cache.go internal/parser/claude_cache_test.go
git commit -m "test: add failing tests for Claude cache JSON parser"
```

---

### Task 2: Parser -- Implementation

**Files:**
- Modify: `internal/parser/claude_cache.go`

- [ ] **Step 1: Implement ParseClaudeCacheSession**

Replace the stub in `internal/parser/claude_cache.go`:

```go
package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// ParseClaudeCacheSession parses a Claude Code JSON cache
// session file. These are older session files stored as a
// single JSON object keyed by timestamps, found in cache/
// subdirectories under Claude project directories.
func ParseClaudeCacheSession(
	path, project, machine string,
) ([]ParseResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	sessionID := strings.TrimSuffix(
		filepath.Base(path), ".json",
	)

	// Collect and sort timestamp keys.
	keys := make([]string, 0, len(raw))
	for k := range raw {
		if k == "_no_timestamp" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Decode entries in timestamp order.
	var (
		messages  []ParsedMessage
		startedAt time.Time
		endedAt   time.Time
		ordinal   int
		userCount int
		firstMsg  string
	)

	for _, key := range keys {
		var entries []json.RawMessage
		if err := json.Unmarshal(
			raw[key], &entries,
		); err != nil {
			continue
		}

		for _, entryRaw := range entries {
			entry := string(entryRaw)
			if !gjson.Valid(entry) {
				continue
			}

			entryType := gjson.Get(entry, "type").Str
			if entryType != "user" &&
				entryType != "assistant" {
				continue
			}

			ts := extractTimestamp(entry)
			if !ts.IsZero() {
				if startedAt.IsZero() ||
					ts.Before(startedAt) {
					startedAt = ts
				}
				if ts.After(endedAt) {
					endedAt = ts
				}
			}

			// Skip meta/compact summary entries.
			if entryType == "user" {
				if gjson.Get(entry, "isMeta").Bool() ||
					gjson.Get(
						entry, "isCompactSummary",
					).Bool() {
					continue
				}
			}

			content := gjson.Get(entry, "message.content")
			text, hasThinking, hasToolUse, tcs, trs :=
				ExtractTextContent(content)
			if strings.TrimSpace(text) == "" &&
				len(trs) == 0 {
				continue
			}

			if entryType == "user" &&
				isClaudeSystemMessage(text) {
				continue
			}

			msg := ParsedMessage{
				Ordinal:       ordinal,
				Role:          RoleType(entryType),
				Content:       text,
				Timestamp:     ts,
				HasThinking:   hasThinking,
				HasToolUse:    hasToolUse,
				ContentLength: len(text),
				ToolCalls:     tcs,
				ToolResults:   trs,
			}

			if entryType == "assistant" {
				extractClaudeTokenFields(&msg, entry)
			}

			if entryType == "user" && msg.Content != "" {
				userCount++
				if firstMsg == "" {
					firstMsg = truncate(
						strings.ReplaceAll(
							msg.Content, "\n", " ",
						), 300,
					)
				}
			}

			messages = append(messages, msg)
			ordinal++
		}
	}

	sess := ParsedSession{
		ID:               sessionID,
		Project:          project,
		Machine:          machine,
		Agent:            AgentClaude,
		FirstMessage:     firstMsg,
		StartedAt:        startedAt,
		EndedAt:          endedAt,
		MessageCount:     len(messages),
		UserMessageCount: userCount,
		File: FileInfo{
			Path:  path,
			Size:  info.Size(),
			Mtime: info.ModTime().UnixNano(),
		},
	}
	sumTokenUsage(&sess, messages)

	return []ParseResult{
		{Session: sess, Messages: messages},
	}, nil
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run TestParseClaudeCache -v`

Expected: All tests PASS.

- [ ] **Step 3: Run full parser test suite for regressions**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -v -count=1`

Expected: All existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add internal/parser/claude_cache.go
git commit -m "feat: implement Claude cache JSON parser"
```

---

### Task 3: Discovery -- Failing Tests

**Files:**
- Modify: `internal/parser/discovery_test.go`

- [ ] **Step 1: Write discovery tests**

Append to `internal/parser/discovery_test.go`. Uses the existing
`setupFileSystem` and `assertDiscoveredFiles` helpers.

```go
func TestDiscoverClaudeCacheSessions(t *testing.T) {
	tests := []struct {
		name      string
		files     map[string]string
		wantFiles []string
	}{
		{
			name: "finds json files in cache subdirs",
			files: map[string]string{
				"proj1/cache/abc123.json":       "{}",
				"proj1/cache/def456.json":       "{}",
				"proj1/session1.jsonl":          "",
			},
			wantFiles: []string{
				"proj1/cache/abc123.json",
				"proj1/cache/def456.json",
			},
		},
		{
			name: "skips index.json",
			files: map[string]string{
				"proj1/cache/abc123.json":  "{}",
				"proj1/cache/index.json":   "{}",
			},
			wantFiles: []string{
				"proj1/cache/abc123.json",
			},
		},
		{
			name: "skips non-json files",
			files: map[string]string{
				"proj1/cache/abc123.json":  "{}",
				"proj1/cache/notes.txt":    "",
				"proj1/cache/data.jsonl":   "",
			},
			wantFiles: []string{
				"proj1/cache/abc123.json",
			},
		},
		{
			name: "includes agent json files",
			files: map[string]string{
				"proj1/cache/abc123.json":       "{}",
				"proj1/cache/agent-xyz.json":    "{}",
			},
			wantFiles: []string{
				"proj1/cache/abc123.json",
				"proj1/cache/agent-xyz.json",
			},
		},
		{
			name: "no cache dir returns empty",
			files: map[string]string{
				"proj1/session1.jsonl": "",
			},
			wantFiles: nil,
		},
		{
			name: "multiple projects with caches",
			files: map[string]string{
				"proj1/cache/a.json": "{}",
				"proj2/cache/b.json": "{}",
			},
			wantFiles: []string{
				"proj1/cache/a.json",
				"proj2/cache/b.json",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			setupFileSystem(t, dir, tt.files)
			got := DiscoverClaudeCacheSessions(dir)
			assertDiscoveredFiles(
				t, got, tt.wantFiles, AgentClaude,
			)
		})
	}
}
```

- [ ] **Step 2: Run to verify tests fail (function not defined)**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run TestDiscoverClaudeCache -v`

Expected: Compilation error -- `DiscoverClaudeCacheSessions` not defined.

- [ ] **Step 3: Commit**

```bash
git add internal/parser/discovery_test.go
git commit -m "test: add failing tests for Claude cache discovery"
```

---

### Task 4: Discovery -- Implementation

**Files:**
- Modify: `internal/parser/discovery.go`

- [ ] **Step 1: Implement DiscoverClaudeCacheSessions**

Add to `internal/parser/discovery.go` after the `FindClaudeSourceFile`
function (around line 216):

```go
// DiscoverClaudeCacheSessions finds JSON session cache files
// under the Claude projects dir. These are older session files
// stored at <project>/cache/<uuid>.json.
func DiscoverClaudeCacheSessions(
	projectsDir string,
) []DiscoveredFile {
	entries, err := os.ReadDir(projectsDir)
	if err != nil {
		return nil
	}

	var files []DiscoveredFile
	for _, entry := range entries {
		if !isDirOrSymlink(entry, projectsDir) {
			continue
		}

		cacheDir := filepath.Join(
			projectsDir, entry.Name(), "cache",
		)
		cacheFiles, err := os.ReadDir(cacheDir)
		if err != nil {
			continue
		}

		for _, cf := range cacheFiles {
			if cf.IsDir() {
				continue
			}
			name := cf.Name()
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			if name == "index.json" {
				continue
			}
			files = append(files, DiscoveredFile{
				Path:    filepath.Join(cacheDir, name),
				Project: entry.Name(),
				Agent:   AgentClaude,
			})
		}
	}

	return files
}

// FindClaudeCacheSourceFile locates a Claude cache session
// file by session ID. Searches <project>/cache/<id>.json
// across all project directories.
func FindClaudeCacheSourceFile(
	projectsDir, sessionID string,
) string {
	entries, err := os.ReadDir(projectsDir)
	if err != nil {
		return ""
	}

	target := sessionID + ".json"
	for _, entry := range entries {
		if !isDirOrSymlink(entry, projectsDir) {
			continue
		}
		path := filepath.Join(
			projectsDir, entry.Name(), "cache", target,
		)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}
```

- [ ] **Step 2: Run discovery tests**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run TestDiscoverClaudeCache -v`

Expected: All tests PASS.

- [ ] **Step 3: Run full discovery test suite**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run TestDiscover -v`

Expected: All tests PASS (no regressions).

- [ ] **Step 4: Commit**

```bash
git add internal/parser/discovery.go
git commit -m "feat: add Claude cache session discovery"
```

---

### Task 5: Config Flag

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Add ClaudeLegacyCache field to Config**

In `internal/config/config.go`, add the field to the `Config` struct
after the `ResultContentBlockedCategories` field (around line 93):

```go
ClaudeLegacyCache bool `json:"claude_legacy_cache" toml:"claude_legacy_cache"`
```

- [ ] **Step 2: Verify compilation**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go build -tags fts5 ./...`

Expected: Compiles successfully.

- [ ] **Step 3: Commit**

```bash
git add internal/config/config.go
git commit -m "feat: add claude_legacy_cache config flag"
```

---

### Task 6: Sync Engine Integration

**Files:**
- Modify: `internal/sync/engine.go`
- Modify: `cmd/agentsview/main.go`

- [ ] **Step 1: Add ClaudeLegacyCache to EngineConfig**

In `internal/sync/engine.go`, add to the `EngineConfig` struct (around
line 28):

```go
ClaudeLegacyCache bool
```

- [ ] **Step 2: Store the flag in Engine**

Add a field to the `Engine` struct (around line 35):

```go
claudeLegacyCache bool
```

In `NewEngine` (around line 56), assign it:

```go
claudeLegacyCache: cfg.ClaudeLegacyCache,
```

- [ ] **Step 3: Add cache discovery to syncAllLocked**

In `syncAllLocked`, after the main Registry discovery loop (around
line 860, after the `for _, def := range parser.Registry` block), add:

```go
// Discover Claude legacy cache sessions if enabled.
if e.claudeLegacyCache {
	if dirs, ok := e.agentDirs[parser.AgentClaude]; ok {
		for _, dir := range dirs {
			cacheFiles := parser.DiscoverClaudeCacheSessions(dir)
			allFiles = append(allFiles, cacheFiles...)
		}
	}
}
```

(Use the same `allFiles` slice that the Registry loop appends to.)

- [ ] **Step 4: Add processClaudeCache method**

Add a new method to `Engine`, modeled on `processClaude` but simpler
(no incremental parsing, no project hints). Note: uses
`shouldSkipByPath` (not `shouldSkipFile` which takes a session ID),
`ComputeFileHash` (not `hashFile`), and returns `processResult{}`
for empty/error cases (no `markSkipped` method exists -- the caller
handles skip-cache population):

```go
// processClaudeCache parses a Claude JSON cache file.
func (e *Engine) processClaudeCache(
	file parser.DiscoveredFile, info os.FileInfo,
) processResult {
	if e.shouldSkipByPath(file.Path, info) {
		return processResult{skip: true}
	}

	results, err := parser.ParseClaudeCacheSession(
		file.Path, file.Project, e.machine,
	)
	if err != nil {
		return processResult{err: err}
	}
	if len(results) == 0 ||
		results[0].Session.MessageCount == 0 {
		return processResult{}
	}

	hash, err := ComputeFileHash(file.Path)
	if err == nil {
		for i := range results {
			results[i].Session.File.Hash = hash
		}
	}
	parser.InferRelationshipTypes(results)

	return processResult{results: results}
}
```

- [ ] **Step 5: Route .json files to processClaudeCache in dispatch**

In the `processFile` method's switch statement (around line 1261),
modify the `case parser.AgentClaude:` branch to check the file
extension:

```go
case parser.AgentClaude:
	if strings.HasSuffix(file.Path, ".json") {
		res = e.processClaudeCache(file, info)
	} else {
		res = e.processClaude(file, info)
	}
```

This replaces the existing single-line `res = e.processClaude(file, info)`.

- [ ] **Step 6: Thread config flag in main.go**

In `cmd/agentsview/main.go`, where `EngineConfig` is constructed
(around line 218), add:

```go
ClaudeLegacyCache: cfg.ClaudeLegacyCache,
```

- [ ] **Step 7: Verify compilation**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go build -tags fts5 ./...`

Expected: Compiles successfully.

- [ ] **Step 8: Run go vet and fmt**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && go fmt ./... && go vet ./...`

Expected: No issues.

- [ ] **Step 9: Run full test suite**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./... -short`

Expected: All tests pass.

- [ ] **Step 10: Commit**

```bash
git add internal/sync/engine.go cmd/agentsview/main.go
git commit -m "feat: integrate Claude cache parser into sync engine"
```

---

### Task 7: FindClaudeSourceFile Fallback and Test

**Files:**
- Modify: `internal/parser/discovery.go`
- Modify: `internal/parser/discovery_test.go`

The existing `FindClaudeSourceFile` only looks for `.jsonl` files. For
cache-originated sessions, we need it to fall back to
`FindClaudeCacheSourceFile`. Since `FindSourceFunc` in the Registry
doesn't receive a config flag, the cleanest approach is to have
`FindClaudeSourceFile` itself try the cache path as a fallback.

This is intentionally unconditional (not gated by `ClaudeLegacyCache`).
The `FindSourceFunc` signature doesn't carry config, and the fallback
is harmless: it only returns a path if the file actually exists on disk.

- [ ] **Step 1: Write test for FindClaudeCacheSourceFile**

Append to `internal/parser/discovery_test.go`:

```go
func TestFindClaudeCacheSourceFile(t *testing.T) {
	tests := []struct {
		name      string
		files     map[string]string
		sessionID string
		wantFound bool
	}{
		{
			name: "finds cache file by session ID",
			files: map[string]string{
				"proj1/cache/abc123.json": "{}",
			},
			sessionID: "abc123",
			wantFound: true,
		},
		{
			name: "returns empty when not found",
			files: map[string]string{
				"proj1/cache/other.json": "{}",
			},
			sessionID: "abc123",
			wantFound: false,
		},
		{
			name:      "returns empty when no cache dirs",
			files:     map[string]string{
				"proj1/session.jsonl": "",
			},
			sessionID: "abc123",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			setupFileSystem(t, dir, tt.files)
			got := FindClaudeCacheSourceFile(
				dir, tt.sessionID,
			)
			if tt.wantFound && got == "" {
				t.Error("expected to find file, got empty")
			}
			if !tt.wantFound && got != "" {
				t.Errorf("expected empty, got %q", got)
			}
		})
	}
}
```

- [ ] **Step 2: Add cache fallback to FindClaudeSourceFile**

In `internal/parser/discovery.go`, at the end of `FindClaudeSourceFile`
(around line 216), before the final `return ""`, add:

```go
// Fallback: check cache/ subdirectories for legacy
// JSON session files.
return FindClaudeCacheSourceFile(projectsDir, sessionID)
```

This is safe because `FindClaudeCacheSourceFile` returns `""` when
nothing is found, preserving existing behavior.

- [ ] **Step 3: Run discovery tests**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./internal/parser/ -run "TestFind|TestDiscoverClaudeCache" -v`

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/parser/discovery.go internal/parser/discovery_test.go
git commit -m "feat: add cache fallback to FindClaudeSourceFile"
```

---

### Task 8: Final Verification

- [ ] **Step 1: Run go fmt and go vet**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && go fmt ./... && go vet ./...`

Expected: Clean.

- [ ] **Step 2: Run full test suite**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && CGO_ENABLED=1 go test -tags fts5 ./... -count=1`

Expected: All tests pass.

- [ ] **Step 3: Run lint**

Run: `cd /home/tmaloney/gitdev/com.github/tlmaloney/agentsview && make lint`

Expected: No new warnings.

- [ ] **Step 4: Manual smoke test**

1. Add `claude_legacy_cache = true` to
   `~/.agentsview/config.toml`
2. Run `make dev`
3. Verify cache sessions appear in the session list
4. Remove the config line when done testing
