package ntduckbox

import (
	"context"
	"errors"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// RegisterTools registers query, list_tables, list_views, and describe. These
// names are reserved; registration fails if the server already has any of them.
func (db *DB) RegisterTools(s *server.MCPServer) error {
	if s == nil {
		return errors.New("nil MCP server")
	}
	for _, name := range []string{"query", "list_tables", "list_views", "describe"} {
		if _, ok := s.ListTools()[name]; ok {
			return errors.New("MCP tool already registered: " + name)
		}
	}
	result := func(text string, err error) (*mcp.CallToolResult, error) {
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(text), nil
	}
	s.AddTool(mcp.NewTool("query", mcp.WithDescription("Run one read-only SELECT query. Results are bounded; use SQL LIMIT for large tables."), mcp.WithReadOnlyHintAnnotation(true), mcp.WithString("sql", mcp.Required()), mcp.WithString("format", mcp.Enum("csv", "markdown"))), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query, err := req.RequireString("sql")
		if err != nil {
			return result("", err)
		}
		format := string(db.opts.Format)
		if _, ok := req.GetArguments()["format"]; ok {
			format, err = req.RequireString("format")
			if err != nil {
				return result("", err)
			}
		}
		text, err := db.QueryFormat(ctx, query, Format(format))
		return result(text, err)
	})
	for _, item := range []struct {
		name, description string
		run               func(context.Context) (string, error)
	}{
		{"list_tables", "List available tables.", db.ListTables}, {"list_views", "List available views.", db.ListViews},
	} {
		s.AddTool(mcp.NewTool(item.name, mcp.WithDescription(item.description), mcp.WithReadOnlyHintAnnotation(true)), func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			text, err := item.run(ctx)
			return result(text, err)
		})
	}
	s.AddTool(mcp.NewTool("describe", mcp.WithDescription("Describe exactly one of: a literal table name (optional schema), or a SELECT query in sql."), mcp.WithReadOnlyHintAnnotation(true), mcp.WithString("table"), mcp.WithString("schema"), mcp.WithString("sql")), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		_, hasSQL := args["sql"]
		_, hasTable := args["table"]
		if hasSQL == hasTable {
			return result("", errors.New("provide exactly one of table or sql"))
		}
		if hasSQL {
			if _, ok := args["schema"]; ok {
				return result("", errors.New("schema requires table"))
			}
			query, err := req.RequireString("sql")
			if err != nil {
				return result("", err)
			}
			text, err := db.Describe(ctx, query)
			return result(text, err)
		}
		table, err := req.RequireString("table")
		if err != nil {
			return result("", err)
		}
		names := []string{table}
		if _, ok := args["schema"]; ok {
			schema, err := req.RequireString("schema")
			if err != nil {
				return result("", err)
			}
			names = []string{schema, table}
		}
		text, err := db.DescribeTable(ctx, names...)
		return result(text, err)
	})
	return nil
}
