// Package ntduckbox provides restricted, read-only analytical queries over
// DuckDB, or over Haybarn when built with -tags haybarn; see Engine.
package ntduckbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Attachment names an existing DuckDB file to attach read-only at Open.
type Attachment struct {
	Path  string // Existing database file.
	Alias string // Catalog name in SQL; must not be empty or reserved.
}

// CSVColumn is one column of a CSVView in file order.
type CSVColumn struct {
	Name string // Column name in the CSV header.
	Type string // DuckDB column type, for example "VARCHAR" or "DOUBLE".
}

// CSVView configures a CSV-backed view for SetCSVViews.
type CSVView struct {
	Path    string      // CSV file path or glob inside AllowedDirs.
	Columns []CSVColumn // Optional ordered column names and types; validated against the header and pinned in read_csv.
}

// Options configures a DB. Zero values select conservative defaults. There is
// deliberately no write-access option or exposed underlying SQL connection.
type Options struct {
	Path         string        // Existing database file; empty or :memory: creates an empty memory database.
	Attach       []Attachment  // Existing database files attached read-only before lockdown; their main schemas are added to search_path.
	AllowedDirs  []string      // Explicit opt-in to local file reads, including all descendants.
	MaxRows      int           // Default 1000; must not exceed math.MaxInt-1.
	MaxChars     int           // Default 100000; measured in UTF-8 bytes (a stricter bound than characters).
	QueryTimeout time.Duration // Default 10 seconds; includes waiting for a connection.
	Format       Format        // Default CSV.
}

type viewKind uint8

const (
	viewParquet viewKind = iota + 1
	viewCSV
	viewSQL
)

// DB owns an isolated DuckDB connection pool with immutable security settings.
type DB struct {
	pool       *sql.DB
	opts       Options
	searchPath string

	viewsMu         sync.Mutex
	views           map[string]viewKind // Names created by SetParquetViews, SetCSVViews, and SetViews.
	resetSearchPath atomic.Bool
	memory          bool
}

// Open opens an existing file read-only or creates an empty in-memory database.
// Any failure to apply a security setting fails the entire open operation.
func Open(opts Options) (*DB, error) {
	if opts.MaxRows < 0 || opts.MaxChars < 0 || opts.QueryTimeout < 0 {
		return nil, errors.New("limits must not be negative")
	}
	// Queries fetch one extra row to detect results exceeding the cap.
	if opts.MaxRows > math.MaxInt-1 {
		return nil, fmt.Errorf("max rows must not exceed %d", math.MaxInt-1)
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
	if !opts.Format.valid() {
		return nil, errors.New("format must be csv, markdown, json_rows, or json_columnar")
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
	memory := path == "" || path == ":memory:"
	primaryCatalog := "memory"
	if memory {
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
		base := filepath.Base(path)
		primaryCatalog = strings.TrimSuffix(base, filepath.Ext(base))
	}
	seenAliases := map[string]struct{}{
		"memory": {}, "system": {}, "temp": {}, "main": {},
		strings.ToLower(primaryCatalog): {},
	}
	attachments := make([]Attachment, 0, len(opts.Attach))
	searchSchemas := []string{"main"}
	for _, att := range opts.Attach {
		if att.Alias == "" {
			return nil, errors.New("attachment alias must not be empty")
		}
		key := strings.ToLower(att.Alias)
		if _, exists := seenAliases[key]; exists {
			return nil, fmt.Errorf("invalid or duplicate attachment alias: %s", att.Alias)
		}
		seenAliases[key] = struct{}{}
		if att.Path == "" || att.Path == ":memory:" {
			return nil, errors.New("attachment path must name an existing database file")
		}
		abs, err := filepath.Abs(att.Path)
		if err != nil {
			return nil, err
		}
		abs, err = filepath.EvalSymlinks(abs)
		if err != nil {
			return nil, fmt.Errorf("attachment path: %w", err)
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			return nil, fmt.Errorf("not a database file: %s", abs)
		}
		attachments = append(attachments, Attachment{Path: abs, Alias: att.Alias})
		searchSchemas = append(searchSchemas, identifier(att.Alias)+".main")
	}
	connector, err := newEngineConnector(path + "?" + config.Encode())
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", Engine, err)
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	fail := func(err error) (*DB, error) { return nil, errors.Join(err, pool.Close()) }
	setup := make([]string, 0, len(attachments)+8)
	for _, att := range attachments {
		setup = append(setup, "ATTACH "+literal(att.Path)+" AS "+identifier(att.Alias)+" (READ_ONLY)")
	}
	var searchPath string
	if len(attachments) > 0 {
		searchPath = strings.Join(searchSchemas, ",")
		setup = append(setup, "SET search_path = "+literal(searchPath))
	}
	setup = append(setup,
		"SET temp_directory = ''",
		"SET allowed_directories = ["+strings.Join(dirs, ",")+"]",
		"SET allowed_paths = []", "SET allowed_configs = []",
		"SET disabled_filesystems = 'HTTPFileSystem,S3FileSystem'",
		"SET enable_external_access = false", "SET lock_configuration = true",
	)
	for _, setting := range setup {
		if _, err := pool.ExecContext(context.Background(), setting); err != nil {
			return fail(fmt.Errorf("lockdown: %w", err))
		}
	}
	return &DB{pool: pool, opts: opts, searchPath: searchPath, memory: memory}, nil
}

// SetParquetViews replaces the host-defined Parquet views of an in-memory
// database. Keys are view names in the main schema; values are Parquet file
// paths or globs inside AllowedDirs. Parquet views from earlier calls that are
// absent from views are dropped; views managed by SetCSVViews or SetViews are
// untouched. The engine binds every view at creation, so a path outside
// AllowedDirs fails the whole call and leaves the previous set in place.
// File-backed databases are opened read-only and return an error.
func (db *DB) SetParquetViews(ctx context.Context, views map[string]string) error {
	for name, path := range views {
		if name == "" || path == "" {
			return errors.New("view names and paths must not be empty")
		}
	}
	return db.replaceViews(ctx, viewParquet, slices.Sorted(maps.Keys(views)), func(_ context.Context, _ *sql.Conn, name string) (string, error) {
		return "SELECT * FROM read_parquet(" + literal(views[name]) + ")", nil
	})
}

// SetCSVViews replaces the host-defined CSV views of an in-memory database.
// Keys are view names in the main schema; values specify a CSV file path or
// glob inside AllowedDirs and an optional ordered slice of pinned columns. When
// Columns is non-empty, SetCSVViews checks that the file header matches the
// column names in order and pins each column's DuckDB type in read_csv. CSV
// views from earlier calls that are absent from views are dropped; views
// managed by SetParquetViews or SetViews are untouched. A path outside
// AllowedDirs, a header mismatch, or an invalid column type fails the call and
// leaves the previous set in place. File-backed databases return an error.
func (db *DB) SetCSVViews(ctx context.Context, views map[string]CSVView) error {
	for name, v := range views {
		if name == "" || v.Path == "" {
			return errors.New("view names and paths must not be empty")
		}
		seen := make(map[string]struct{}, len(v.Columns))
		for _, col := range v.Columns {
			if col.Name == "" || strings.TrimSpace(col.Type) == "" {
				return errors.New("csv column names and types must not be empty")
			}
			if _, dup := seen[col.Name]; dup {
				return fmt.Errorf("duplicate csv column %q in view %q", col.Name, name)
			}
			seen[col.Name] = struct{}{}
		}
	}
	return db.replaceViews(ctx, viewCSV, slices.Sorted(maps.Keys(views)), func(ctx context.Context, conn *sql.Conn, name string) (string, error) {
		v := views[name]
		if len(v.Columns) == 0 {
			return "SELECT * FROM read_csv(" + literal(v.Path) + ", header = true)", nil
		}
		if err := validateCSVHeader(ctx, conn, name, v); err != nil {
			return "", err
		}
		cols := make([]string, len(v.Columns))
		for i, col := range v.Columns {
			cols[i] = literal(col.Name) + ": " + literal(col.Type)
		}
		return "SELECT * FROM read_csv(" + literal(v.Path) + ", header = true, columns = {" + strings.Join(cols, ", ") + "})", nil
	})
}

func validateCSVHeader(ctx context.Context, conn *sql.Conn, name string, v CSVView) error {
	rows, err := conn.QueryContext(ctx, "SELECT * FROM read_csv("+literal(v.Path)+", header = true, all_varchar = true) LIMIT 0")
	if err != nil {
		return err
	}
	cols, colErr := rows.Columns()
	closeErr := rows.Close()
	if err := errors.Join(colErr, closeErr); err != nil {
		return err
	}
	want := make([]string, len(v.Columns))
	for i, c := range v.Columns {
		want[i] = c.Name
	}
	if !slices.Equal(cols, want) {
		return fmt.Errorf("csv header mismatch for %s: got %v, want %v", name, cols, want)
	}
	return nil
}

// SetViews replaces the host-defined SQL views of an in-memory database. Keys
// are view names in the main schema; values are single SELECT or WITH...SELECT
// query bodies (without CREATE VIEW). Views in the same map may reference one
// another in any acyclic order; cycles are rejected. Each body is wrapped as a
// subquery and validated with DuckDB's single-statement Prepare API before
// execution, then bound by the engine under the locked configuration. SQL
// views from earlier calls that are absent from views are dropped; views
// managed by SetParquetViews or SetCSVViews are untouched. Any non-SELECT
// statement, batch, cycle, or disallowed file access fails the call and leaves
// the previous set in place. File-backed databases return an error.
func (db *DB) SetViews(ctx context.Context, views map[string]string) error {
	cleaned := make(map[string]string, len(views))
	for name, query := range views {
		q := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
		if name == "" || q == "" {
			return errors.New("view names and queries must not be empty")
		}
		cleaned[name] = q
	}
	return db.replaceViews(ctx, viewSQL, slices.Sorted(maps.Keys(cleaned)), func(_ context.Context, conn *sql.Conn, name string) (string, error) {
		body := "SELECT * FROM (\n" + cleaned[name] + "\n) AS ntduckbox_view"
		if err := validateSelect(conn, body); err != nil {
			return "", fmt.Errorf("view %s rejected: %w", name, err)
		}
		return body, nil
	})
}

func (db *DB) replaceViews(ctx context.Context, kind viewKind, names []string, build func(context.Context, *sql.Conn, string) (string, error)) error {
	if !db.memory {
		return errors.New("views require an in-memory database")
	}
	db.viewsMu.Lock()
	defer db.viewsMu.Unlock()
	for _, name := range names {
		if existing, ok := db.views[name]; ok && existing != kind {
			return fmt.Errorf("view %q is already managed by another view set", name)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, db.opts.QueryTimeout)
	defer cancel()
	conn, err := db.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return err
	}
	apply := func() error {
		created := make([]string, 0, len(names))
		createdSQL := make(map[string]string, len(names))
		resetTx := func() error {
			for _, name := range slices.Sorted(maps.Keys(db.views)) {
				if db.views[name] != kind {
					continue
				}
				if _, err := conn.ExecContext(ctx, "DROP VIEW IF EXISTS main."+identifier(name)); err != nil {
					return err
				}
			}
			for _, name := range created {
				stmt := "CREATE OR REPLACE VIEW main." + identifier(name) + " AS " + createdSQL[name]
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return err
				}
			}
			return nil
		}
		if err := resetTx(); err != nil {
			return err
		}
		pending := names
		for len(pending) > 0 {
			var next []string
			var firstErr error
			progressed := false
			for _, name := range pending {
				selectSQL, err := build(ctx, conn, name)
				if err == nil {
					stmt := "CREATE OR REPLACE VIEW main." + identifier(name) + " AS " + selectSQL
					_, err = conn.ExecContext(ctx, stmt)
				}
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					next = append(next, name)
					// A failed Prepare or CREATE VIEW marks the DuckDB transaction
					// as aborted; roll back and replay the known-good prefix.
					db.rollback(conn)
					if _, rbErr := conn.ExecContext(ctx, "BEGIN TRANSACTION"); rbErr != nil {
						return firstErr
					}
					if rbErr := resetTx(); rbErr != nil {
						return firstErr
					}
					continue
				}
				created = append(created, name)
				createdSQL[name] = selectSQL
				progressed = true
			}
			if !progressed {
				return firstErr
			}
			pending = next
		}
		if kind != viewSQL {
			for _, name := range slices.Sorted(maps.Keys(db.views)) {
				if db.views[name] != viewSQL {
					continue
				}
				if err := validateSelect(conn, "SELECT * FROM main."+identifier(name)); err != nil {
					return fmt.Errorf("view %s invalidated: %w", name, err)
				}
			}
		}
		_, err := conn.ExecContext(ctx, "COMMIT")
		return err
	}
	if err := apply(); err != nil {
		db.rollback(conn)
		return fmt.Errorf("set views: %w", err)
	}
	if db.views == nil {
		db.views = make(map[string]viewKind, len(names))
	}
	for name, existing := range db.views {
		if existing == kind {
			delete(db.views, name)
		}
	}
	for _, name := range names {
		db.views[name] = kind
	}
	return nil
}

func (db *DB) conn(ctx context.Context) (*sql.Conn, error) {
	conn, err := db.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if db.searchPath != "" && db.resetSearchPath.Load() {
		if _, err := conn.ExecContext(ctx, "SET search_path = "+literal(db.searchPath)); err != nil {
			_ = conn.Close()
			return nil, err
		}
		db.resetSearchPath.Store(false)
	}
	return conn, nil
}

func (db *DB) rollback(conn *sql.Conn) {
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		// Never return a connection with unknown transaction state to the pool.
		db.resetSearchPath.Store(true)
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

// validateSelect checks that query is a single SELECT statement using the
// engine's native Prepare API. Conn.Prepare (unlike PrepareContext) extracts
// one statement and rejects batches without executing their leading statements.
func validateSelect(conn *sql.Conn, query string) error {
	return conn.Raw(func(raw any) error {
		stmt, err := raw.(*engineConn).Prepare(query)
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
}

func identifier(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Close releases the database and waits for active queries to finish.
func (db *DB) Close() error { return db.pool.Close() }

// QueryOptions adjusts one call. Zero values use the Options defaults.
type QueryOptions struct {
	Format  Format // Result encoding.
	MaxRows int    // Row cap for this call; must not exceed Options.MaxRows.
	// Truncate returns the rows that fit instead of ErrResultLimit and sets
	// Result.Truncated. CSV and Markdown stop at the last whole row within
	// both limits; JSON formats stop at MaxRows rows but a result over
	// MaxChars is still an error.
	Truncate bool
}

// Result is a rendered query result.
type Result struct {
	Text      string
	Rows      int  // Data rows in Text.
	Truncated bool // More rows existed than Text holds; only with Truncate.
}

// Query returns a bounded result using the configured default format.
func (db *DB) Query(ctx context.Context, query string) (string, error) {
	return db.QueryFormat(ctx, query, db.opts.Format)
}

// QueryFormat executes one SELECT-style query. Commands, multiple statements,
// writes, and configuration changes are rejected. Oversized results return errors.
func (db *DB) QueryFormat(ctx context.Context, query string, format Format) (string, error) {
	res, err := db.QueryWith(ctx, query, QueryOptions{Format: format})
	return res.Text, err
}

// QueryWith executes one SELECT-style query with per-call options. The host
// limits from Open remain the ceiling; a larger MaxRows is an error.
func (db *DB) QueryWith(ctx context.Context, query string, opts QueryOptions) (Result, error) {
	return db.run(ctx, query, opts, false)
}

func (db *DB) run(ctx context.Context, query string, opts QueryOptions, describe bool) (Result, error) {
	if opts.Format == "" {
		opts.Format = db.opts.Format
	}
	if !opts.Format.valid() {
		return Result{}, errors.New("format must be csv, markdown, json_rows, or json_columnar")
	}
	if opts.MaxRows < 0 || opts.MaxRows > db.opts.MaxRows {
		return Result{}, fmt.Errorf("max rows must be between 0 and %d", db.opts.MaxRows)
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = db.opts.MaxRows
	}
	format := opts.Format
	ctx, cancel := context.WithTimeout(ctx, db.opts.QueryTimeout)
	defer cancel()
	conn, err := db.conn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()
	// DuckDB's Go BeginTx does not support ReadOnly, but the SQL engine does.
	if _, err = conn.ExecContext(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
		return Result{}, err
	}
	defer db.rollback(conn)
	query = strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	inner := "(\n" + query + "\n) AS ntduckbox_query"
	// DuckDB casts every value to VARCHAR so results use its canonical text
	// rendering (dates, intervals, lists, structs, blobs) rather than Go's.
	// One row past the cap is enough to detect overflow without materializing
	// the rest of the result in the driver.
	wrapped := "SELECT COLUMNS(*)::VARCHAR FROM " + inner + " LIMIT " + strconv.Itoa(opts.MaxRows+1)
	if describe {
		wrapped = "SELECT * FROM " + inner
	}
	if format.isJSON() {
		// JSON wraps the same SELECT in an outer SELECT, so the executed
		// statement is the one validated below, including DESCRIBE.
		source := "SELECT * FROM " + inner
		if describe {
			source = "DESCRIBE " + source
		}
		wrapped = jsonQuery(source, format, opts.MaxRows)
	}
	if err := validateSelect(conn, wrapped); err != nil {
		return Result{}, fmt.Errorf("query rejected: %w", err)
	}
	if describe && !format.isJSON() {
		wrapped = "DESCRIBE " + wrapped
	}
	rows, err := conn.QueryContext(ctx, wrapped)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()
	if format.isJSON() {
		return renderJSON(rows, opts.MaxRows, db.opts.MaxChars, opts.Truncate)
	}
	return render(ctx, rows, format, opts.MaxRows, db.opts.MaxChars, opts.Truncate)
}

// Describe returns the schema of a SELECT-style query without executing its rows.
func (db *DB) Describe(ctx context.Context, query string) (string, error) {
	res, err := db.run(ctx, query, QueryOptions{}, true)
	return res.Text, err
}

// DescribeTable describes a relation. Each argument is one literal identifier,
// for example DescribeTable(ctx, "main", "events").
func (db *DB) DescribeTable(ctx context.Context, names ...string) (string, error) {
	if len(names) == 0 || len(names) > 3 {
		return "", errors.New("provide one to three identifier components")
	}
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = identifier(name)
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
