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

// extractClaudeTokenFields populates the Model field on an
// assistant ParsedMessage from the raw JSON entry. The entry
// is expected to have a "message.model" field.
func extractClaudeTokenFields(msg *ParsedMessage, entry string) {
	msg.Model = gjson.Get(entry, "message.model").Str
}

// sumTokenUsage is a placeholder for aggregating token usage
// from messages into the session. Token totals are not yet
// stored on ParsedSession.
func sumTokenUsage(_ *ParsedSession, _ []ParsedMessage) {}

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
		messages        []ParsedMessage
		startedAt       time.Time
		endedAt         time.Time
		ordinal         int
		userCount       int
		firstMsg        string
		parentSessionID string
		foundParentSID  bool
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

			// Capture parentSessionID from the first
			// user/assistant entry's sessionId field.
			if !foundParentSID {
				if sid := gjson.Get(entry, "sessionId").Str; sid != "" {
					foundParentSID = true
					if sid != sessionID {
						parentSessionID = sid
					}
				}
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
		ParentSessionID:  parentSessionID,
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
