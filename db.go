// Package ntduckbox provides restricted, read-only analytical queries over
// DuckDB, or over Haybarn when built with -tags haybarn; see Engine.
package ntduckbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Options configures a DB. Zero values select conservative defaults. There is
// deliberately no write-access option or exposed underlying SQL connection.
type Options struct {
	Path         string        // Existing database file; empty or :memory: creates an empty memory database.
	AllowedDirs  []string      // Explicit opt-in to local file reads, including all descendants.
	MaxRows      int           // Default 1000.
	MaxChars     int           // Default 100000; measured in UTF-8 bytes (a stricter bound than characters).
	QueryTimeout time.Duration // Default 10 seconds; includes waiting for a connection.
	Format       Format        // Default CSV.
}

// DB owns an isolated DuckDB connection pool with immutable security settings.
type DB struct {
	pool *sql.DB
	opts Options
}

// Open opens an existing file read-only or creates an empty in-memory database.
// Any failure to apply a security setting fails the entire open operation.
func Open(opts Options) (*DB, error) {
	if opts.MaxRows < 0 || opts.MaxChars < 0 || opts.QueryTimeout < 0 {
		return nil, errors.New("limits must not be negative")
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = 1000
	}
	if opts.MaxChars == 0 {
		opts.MaxChars = 100000
	}
	if opts.QueryTimeout == 0 {
		opts.QueryTimeout = 10 * time.Second
	}
	if opts.Format == "" {
		opts.Format = CSV
	}
	if opts.Format != CSV && opts.Format != Markdown {
		return nil, errors.New("format must be csv or markdown")
	}
	dirs := make([]string, 0, len(opts.AllowedDirs))
	for _, dir := range opts.AllowedDirs {
		if dir == "" {
			return nil, errors.New("allowed directory must not be empty")
		}
		abs, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		abs, err = filepath.EvalSymlinks(abs)
		if err != nil {
			return nil, fmt.Errorf("allowed directory: %w", err)
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("not a directory: %s", abs)
		}
		dirs = append(dirs, literal(abs+string(os.PathSeparator)))
	}
	config := url.Values{
		"access_mode":               {"read_only"},
		"autoload_known_extensions": {"false"}, "autoinstall_known_extensions": {"false"},
		"allow_unsigned_extensions": {"false"}, "allow_community_extensions": {"false"},
	}
	path := opts.Path
	if path == "" || path == ":memory:" {
		path = ":memory:"
		config.Set("access_mode", "read_write")
	} else {
		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		// The driver's DSN uses a literal filename followed by '?' configuration.
		if strings.ContainsAny(path, "?#") {
			return nil, errors.New("database path must not contain '?' or '#'")
		}
	}
	connector, err := newEngineConnector(path + "?" + config.Encode())
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", Engine, err)
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	fail := func(err error) (*DB, error) { return nil, errors.Join(err, pool.Close()) }
	for _, setting := range []string{
		"SET temp_directory = ''",
		"SET allowed_directories = [" + strings.Join(dirs, ",") + "]",
		"SET allowed_paths = []", "SET allowed_configs = []",
		"SET disabled_filesystems = 'HTTPFileSystem,S3FileSystem'",
		"SET enable_external_access = false", "SET lock_configuration = true",
	} {
		if _, err := pool.ExecContext(context.Background(), setting); err != nil {
			return fail(fmt.Errorf("lockdown: %w", err))
		}
	}
	return &DB{pool: pool, opts: opts}, nil
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Close releases the database and waits for active queries to finish.
func (db *DB) Close() error { return db.pool.Close() }

// Query returns a bounded result using the configured default format.
func (db *DB) Query(ctx context.Context, query string) (string, error) {
	return db.QueryFormat(ctx, query, db.opts.Format)
}

// QueryFormat executes one SELECT-style query. Commands, multiple statements,
// writes, and configuration changes are rejected. Oversized results return errors.
func (db *DB) QueryFormat(ctx context.Context, query string, format Format) (string, error) {
	return db.run(ctx, query, format, false)
}

func (db *DB) run(ctx context.Context, query string, format Format, describe bool) (string, error) {
	if format != CSV && format != Markdown {
		return "", errors.New("format must be csv or markdown")
	}
	ctx, cancel := context.WithTimeout(ctx, db.opts.QueryTimeout)
	defer cancel()
	conn, err := db.pool.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	// DuckDB's Go BeginTx does not support ReadOnly, but the SQL engine does.
	if _, err = conn.ExecContext(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
		return "", err
	}
	defer func() {
		if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
			// Never return a connection with unknown transaction state to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	query = strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	inner := "(\n" + query + "\n) AS ntduckbox_query"
	// DuckDB casts every value to VARCHAR so results use its canonical text
	// rendering (dates, intervals, lists, structs, blobs) rather than Go's.
	wrapped := "SELECT COLUMNS(*)::VARCHAR FROM " + inner
	if describe {
		wrapped = "SELECT * FROM " + inner
	}
	// Conn.Prepare (unlike PrepareContext) extracts exactly one statement and
	// rejects batches without executing their leading statements. Do not replace
	// this with database/sql.PrepareContext: the driver executes batch prefixes.
	err = conn.Raw(func(raw any) error {
		stmt, err := raw.(*engineConn).Prepare(wrapped)
		if err != nil {
			return err
		}
		defer stmt.Close()
		typ, err := stmt.(*engineStmt).StatementType()
		if err != nil {
			return err
		}
		if typ != engineSelect {
			return errors.New("only SELECT queries are allowed")
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("query rejected: %w", err)
	}
	if describe {
		wrapped = "DESCRIBE " + wrapped
	}
	rows, err := conn.QueryContext(ctx, wrapped)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	return render(ctx, rows, format, db.opts.MaxRows, db.opts.MaxChars)
}

// Describe returns the schema of a SELECT-style query without executing its rows.
func (db *DB) Describe(ctx context.Context, query string) (string, error) {
	return db.run(ctx, query, db.opts.Format, true)
}

// DescribeTable describes a relation. Each argument is one literal identifier,
// for example DescribeTable(ctx, "main", "events").
func (db *DB) DescribeTable(ctx context.Context, names ...string) (string, error) {
	if len(names) == 0 || len(names) > 3 {
		return "", errors.New("provide one to three identifier components")
	}
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return db.Describe(ctx, "SELECT * FROM "+strings.Join(parts, "."))
}

// ListTables lists user tables visible in information_schema.
func (db *DB) ListTables(ctx context.Context) (string, error) { return db.relations(ctx, "BASE TABLE") }

// ListViews lists user views visible in information_schema.
func (db *DB) ListViews(ctx context.Context) (string, error) { return db.relations(ctx, "VIEW") }
func (db *DB) relations(ctx context.Context, kind string) (string, error) {
	return db.Query(ctx, "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_type = "+literal(kind)+" ORDER BY 1,2,3")
}
