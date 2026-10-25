package ntduckbox

import (
	"context"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestMCPTools(t *testing.T) {
	db := openTest(t, Options{})
	s := server.NewMCPServer("test", "1")
	if err := db.RegisterTools(s); err != nil {
		t.Fatal(err)
	}
	if len(s.ListTools()) != 4 {
		t.Fatal("expected four tools")
	}
	if err := db.RegisterTools(s); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	if err := db.RegisterTools(nil); err == nil {
		t.Fatal("nil server accepted")
	}
	for _, tc := range []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{"query", map[string]any{"sql": "SELECT 1"}, false},
		{"query", map[string]any{"sql": "LOAD httpfs"}, true},
		{"query", map[string]any{}, true},
		{"query", map[string]any{"sql": "SELECT 1", "format": 42}, true},
		{"describe", map[string]any{"sql": "SELECT 1 AS x"}, false},
		{"describe", map[string]any{"sql": "SELECT 1", "table": "x"}, true},
		{"list_tables", nil, false}, {"list_views", nil, false},
	} {
		req := mcp.CallToolRequest{}
		req.Params.Name = tc.name
		req.Params.Arguments = tc.args
		result, err := s.ListTools()[tc.name].Handler(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil || result.IsError != tc.wantError {
			t.Fatalf("%s %v: %#v", tc.name, tc.args, result)
		}
	}
}
