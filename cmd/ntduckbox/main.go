// Command ntduckbox serves a restricted DuckDB over STDIO MCP.
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"nimbleterminal.dev/ntduckbox"
)

type directories []string

// Release packaging sets version from the Git tag through -ldflags.
var version = "dev"

func (d *directories) String() string         { return "" }
func (d *directories) Set(value string) error { *d = append(*d, value); return nil }

func main() {
	var opts ntduckbox.Options
	var dirs directories
	var format string
	flag.StringVar(&opts.Path, "db", ":memory:", "existing DuckDB file or :memory:")
	flag.Var(&dirs, "allow-dir", "allow local file reads in this trusted directory (repeatable)")
	flag.IntVar(&opts.MaxRows, "max-rows", 1000, "maximum result rows")
	flag.IntVar(&opts.MaxChars, "max-chars", 100000, "maximum UTF-8 output bytes")
	flag.DurationVar(&opts.QueryTimeout, "timeout", 10*time.Second, "query timeout")
	flag.StringVar(&format, "format", "csv", "csv, markdown, json_rows, or json_columnar")
	flag.Parse()
	opts.AllowedDirs = dirs
	opts.Format = ntduckbox.Format(format)
	if err := serve(opts); err != nil {
		slog.Error("ntduckbox failed", "error", err)
		os.Exit(1)
	}
}

func serve(opts ntduckbox.Options) error {
	db, err := ntduckbox.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()
	s := server.NewMCPServer("ntduckbox", version, server.WithToolCapabilities(false))
	if err := db.RegisterTools(s); err != nil {
		return err
	}
	slog.Info("serving MCP over stdio", "version", version, "engine", ntduckbox.Engine)
	return server.ServeStdio(s)
}
