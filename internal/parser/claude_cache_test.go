package parser

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
						"type":       "assistant",
						"uuid":       "a1",
						"parentUuid": "u1",
						"timestamp":  "2025-06-27T00:10:21.082Z",
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
							"role":    "assistant",
							"model":   "claude-sonnet-4-20250514",
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
