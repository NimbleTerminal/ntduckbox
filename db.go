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

// Column describes one column's name and DuckDB type, such as "VARCHAR",
// "DOUBLE", or "TIMESTAMPTZ_NS".
type Column struct {
	Name string // Column name.
	Type string // DuckDB column type, for example "VARCHAR" or "DOUBLE".
}

// CSVColumn is an alias for Column used in CSVView definitions.
type CSVColumn = Column

// CSVView configures a CSV-backed view for SetCSVViews or SetAllViews.
type CSVView struct {
	Path    string      // CSV file path or glob inside AllowedDirs.
	Columns []CSVColumn // Optional ordered column names and types; validated against the header and pinned in read_csv.
}

// ViewKind identifies how a host-managed view was defined.
type ViewKind string

const (
	// ParquetViewKind identifies a view created by SetParquetViews or ViewSet.Parquet.
	ParquetViewKind ViewKind = "parquet"
	// CSVViewKind identifies a view created by SetCSVViews or ViewSet.CSV.
	CSVViewKind ViewKind = "csv"
	// SQLViewKind identifies a view created by SetViews or ViewSet.SQL.
	SQLViewKind ViewKind = "sql"
)

// ViewInfo describes a host-managed view in the main schema.
type ViewInfo struct {
	Name   string   // View name in main.
	Kind   ViewKind // ParquetViewKind, CSVViewKind, or SQLViewKind.
	Source string   // Parquet/CSV file path or glob, or SQL SELECT body.
}

// ViewSet groups Parquet, CSV, and SQL view definitions for atomic replacement
// by SetAllViews. View names must be distinct across all three maps.
type ViewSet struct {
	Parquet map[string]string
	CSV     map[string]CSVView
	SQL     map[string]string
}

var (
	// ErrNotSelect indicates a query was rejected because it is empty,
	// contains multiple statements, or is not a SELECT query (for example
	// DDL, DML, COPY, ATTACH, LOAD, INSTALL, SET, or PRAGMA).
	ErrNotSelect = errors.New("only single SELECT queries are allowed")
	// ErrPermission indicates the engine denied access to a file,
	// directory, filesystem, extension, or locked setting.
	ErrPermission = errors.New("permission denied")
	// ErrBinder indicates a SELECT query failed parsing, catalog lookup,
	// or type/column binding (for example an unknown table or column name).
	ErrBinder = errors.New("query binding failed")
)

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

// DB owns an isolated DuckDB connection pool with immutable security settings.
type DB struct {
	pool       *sql.DB
	opts       Options
	searchPath string

	viewsMu         sync.Mutex
	views           map[string]ViewInfo // Host-managed views in main.
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
	return db.replaceViewSet(ctx, map[ViewKind]bool{ParquetViewKind: true}, ViewSet{Parquet: views})
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
	return db.replaceViewSet(ctx, map[ViewKind]bool{CSVViewKind: true}, ViewSet{CSV: views})
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
	return db.replaceViewSet(ctx, map[ViewKind]bool{SQLViewKind: true}, ViewSet{SQL: views})
}

// SetAllViews replaces the host-defined Parquet, CSV, and SQL views of an
// in-memory database in one transaction. Omitted names of any kind are dropped
// before the new set is bound in dependency order, so a base view and its
// dependent SQL views can be updated or removed together. Any failure rolls
// back the entire change. File-backed databases return an error.
func (db *DB) SetAllViews(ctx context.Context, set ViewSet) error {
	return db.replaceViewSet(ctx, map[ViewKind]bool{
		ParquetViewKind: true,
		CSVViewKind:     true,
		SQLViewKind:     true,
	}, set)
}

// Views returns metadata for the currently registered host-defined views in
// ascending name order.
func (db *DB) Views(ctx context.Context) ([]ViewInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.viewsMu.Lock()
	defer db.viewsMu.Unlock()
	names := slices.Sorted(maps.Keys(db.views))
	out := make([]ViewInfo, len(names))
	for i, name := range names {
		out[i] = db.views[name]
	}
	return out, nil
}

type viewSpec struct {
	info  ViewInfo
	build func(context.Context, *sql.Conn) (string, error)
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

func (db *DB) replaceViewSet(ctx context.Context, replaceKinds map[ViewKind]bool, set ViewSet) error {
	if !db.memory {
		return errors.New("views require an in-memory database")
	}
	specs := make(map[string]viewSpec, len(set.Parquet)+len(set.CSV)+len(set.SQL))
	if replaceKinds[ParquetViewKind] {
		for name, path := range set.Parquet {
			if name == "" || path == "" {
				return errors.New("view names and paths must not be empty")
			}
			p := path
			specs[name] = viewSpec{
				info: ViewInfo{Name: name, Kind: ParquetViewKind, Source: p},
				build: func(context.Context, *sql.Conn) (string, error) {
					return "SELECT * FROM read_parquet(" + literal(p) + ")", nil
				},
			}
		}
	}
	if replaceKinds[CSVViewKind] {
		for name, v := range set.CSV {
			if name == "" || v.Path == "" {
				return errors.New("view names and paths must not be empty")
			}
			if _, dup := specs[name]; dup {
				return fmt.Errorf("view %q is defined in multiple view sets", name)
			}
			seen := make(map[string]struct{}, len(v.Columns))
			for _, col := range v.Columns {
				if col.Name == "" || strings.TrimSpace(col.Type) == "" {
					return errors.New("csv column names and types must not be empty")
				}
				if _, dupCol := seen[col.Name]; dupCol {
					return fmt.Errorf("duplicate csv column %q in view %q", col.Name, name)
				}
				seen[col.Name] = struct{}{}
			}
			viewName, cv := name, v
			specs[name] = viewSpec{
				info: ViewInfo{Name: name, Kind: CSVViewKind, Source: cv.Path},
				build: func(ctx context.Context, conn *sql.Conn) (string, error) {
					if len(cv.Columns) == 0 {
						return "SELECT * FROM read_csv(" + literal(cv.Path) + ", header = true)", nil
					}
					if err := validateCSVHeader(ctx, conn, viewName, cv); err != nil {
						return "", err
					}
					cols := make([]string, len(cv.Columns))
					for i, col := range cv.Columns {
						cols[i] = literal(col.Name) + ": " + literal(col.Type)
					}
					return "SELECT * FROM read_csv(" + literal(cv.Path) + ", header = true, columns = {" + strings.Join(cols, ", ") + "})", nil
				},
			}
		}
	}
	if replaceKinds[SQLViewKind] {
		for name, query := range set.SQL {
			q := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
			if name == "" || q == "" {
				return errors.New("view names and queries must not be empty")
			}
			if _, dup := specs[name]; dup {
				return fmt.Errorf("view %q is defined in multiple view sets", name)
			}
			viewName, bodySQL := name, q
			specs[name] = viewSpec{
				info: ViewInfo{Name: name, Kind: SQLViewKind, Source: bodySQL},
				build: func(_ context.Context, conn *sql.Conn) (string, error) {
					body := "SELECT * FROM (\n" + bodySQL + "\n) AS ntduckbox_view"
					if err := validateSelect(conn, body, bodySQL); err != nil {
						return "", fmt.Errorf("view %s rejected: %w", viewName, err)
					}
					return body, nil
				},
			}
		}
	}

	db.viewsMu.Lock()
	defer db.viewsMu.Unlock()
	for name := range specs {
		if existing, ok := db.views[name]; ok && !replaceKinds[existing.Kind] {
			return fmt.Errorf("view %q is already managed by another view set", name)
		}
	}
	// Create base file views before SQL views when updating multiple kinds at once.
	var baseNames, sqlNames []string
	for _, name := range slices.Sorted(maps.Keys(specs)) {
		if specs[name].info.Kind == SQLViewKind {
			sqlNames = append(sqlNames, name)
		} else {
			baseNames = append(baseNames, name)
		}
	}
	names := append(baseNames, sqlNames...)

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
				if !replaceKinds[db.views[name].Kind] {
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
				selectSQL, err := specs[name].build(ctx, conn)
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
		if !replaceKinds[SQLViewKind] {
			var broken []error
			for _, name := range slices.Sorted(maps.Keys(db.views)) {
				if db.views[name].Kind != SQLViewKind {
					continue
				}
				probe := "SELECT * FROM main." + identifier(name)
				if err := validateSelect(conn, probe, probe); err != nil {
					broken = append(broken, fmt.Errorf("view %s invalidated: %w", name, err))
					db.rollback(conn)
					if _, rbErr := conn.ExecContext(ctx, "BEGIN TRANSACTION"); rbErr != nil {
						break
					}
					if rbErr := resetTx(); rbErr != nil {
						break
					}
				}
			}
			if len(broken) > 0 {
				return errors.Join(broken...)
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
		db.views = make(map[string]ViewInfo, len(specs))
	}
	for name, existing := range db.views {
		if replaceKinds[existing.Kind] {
			delete(db.views, name)
		}
	}
	for name, spec := range specs {
		db.views[name] = spec.info
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

// Validate checks that query is a single SELECT statement whose referenced
// tables, columns, functions, and file paths are permitted and bind cleanly
// against the current catalog. It uses the same subquery wrapper and native
// single-statement Prepare check as QueryWith without executing rows.
// Rejected queries wrap ErrNotSelect, ErrPermission, or ErrBinder.
func (db *DB) Validate(ctx context.Context, query string) error {
	ctx, cancel := context.WithTimeout(ctx, db.opts.QueryTimeout)
	defer cancel()
	conn, err := db.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
		return err
	}
	defer db.rollback(conn)
	trimmed := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	if trimmed == "" {
		return fmt.Errorf("query rejected: %w: empty query", ErrNotSelect)
	}
	wrapped := "SELECT * FROM (\n" + trimmed + "\n) AS ntduckbox_query"
	if err := validateSelect(conn, wrapped, trimmed); err != nil {
		return fmt.Errorf("query rejected: %w", err)
	}
	return nil
}

// validateSelect checks that wrapped is a single SELECT statement using the
// engine's native Prepare API. Conn.Prepare (unlike PrepareContext) extracts
// one statement and rejects batches without executing their leading statements.
func validateSelect(conn *sql.Conn, wrapped, userQuery string) error {
	if strings.TrimSpace(userQuery) == "" {
		return fmt.Errorf("%w: empty query", ErrNotSelect)
	}
	return conn.Raw(func(raw any) error {
		stmt, err := raw.(*engineConn).Prepare(wrapped)
		if err != nil {
			return classifyPrepError(err, userQuery)
		}
		defer stmt.Close()
		typ, err := stmt.(*engineStmt).StatementType()
		if err != nil {
			return err
		}
		if typ != engineSelect {
			return fmt.Errorf("%w: only SELECT queries are allowed", ErrNotSelect)
		}
		return nil
	})
}

func classifyPrepError(err error, userQuery string) error {
	var e *engineError
	if !errors.As(err, &e) {
		if strings.Contains(err.Error(), "multi-statement") || !hasSelectPrefix(userQuery) {
			return fmt.Errorf("%w: %w", ErrNotSelect, err)
		}
		return err
	}
	switch e.Type {
	case engineErrorPermission:
		return fmt.Errorf("%w: %w", ErrPermission, err)
	case engineErrorBinder, engineErrorCatalog:
		if !hasSelectPrefix(userQuery) {
			return fmt.Errorf("%w: %w", ErrNotSelect, err)
		}
		return fmt.Errorf("%w: %w", ErrBinder, err)
	case engineErrorParser, engineErrorSyntax:
		if strings.Contains(e.Msg, `at or near ";"`) ||
			strings.Contains(e.Msg, "Cannot prepare multiple statements") ||
			!hasSelectPrefix(userQuery) {
			return fmt.Errorf("%w: %w", ErrNotSelect, err)
		}
		return fmt.Errorf("%w: %w", ErrBinder, err)
	default:
		return err
	}
}

func hasSelectPrefix(s string) bool {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			_, rest, ok := strings.Cut(s, "\n")
			if !ok {
				return false
			}
			s = rest
			continue
		}
		if strings.HasPrefix(s, "/*") {
			_, rest, ok := strings.Cut(s[2:], "*/")
			if !ok {
				return false
			}
			s = rest
			continue
		}
		break
	}
	for strings.HasPrefix(s, "(") {
		s = strings.TrimSpace(s[1:])
	}
	var word strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			word.WriteByte(c)
		} else {
			break
		}
	}
	switch strings.ToUpper(word.String()) {
	case "SELECT", "WITH", "VALUES", "FROM", "TABLE", "PIVOT", "UNPIVOT":
		return true
	default:
		return false
	}
}

func identifier(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Close releases the database and waits for active queries to finish.
func (db *DB) Close() error { return db.pool.Close() }

// QueryOptions adjusts one call. Zero values use the Options defaults.
type QueryOptions struct {
	Format   Format // Result encoding.
	MaxRows  int    // Row cap for this call; must not exceed Options.MaxRows.
	MaxChars int    // Byte cap for this call; must not exceed Options.MaxChars.
	// Truncate returns the rows that fit instead of ErrResultLimit and sets
	// Result.Truncated. All formats stop at the last whole row within both
	// MaxRows and MaxChars; if not even one row fits within MaxChars,
	// ErrResultLimit is still returned.
	Truncate bool
}

// Result is a rendered query result.
type Result struct {
	Text      string
	Rows      int           // Data rows in Text.
	Bytes     int           // len(Text).
	Truncated bool          // More rows existed than Text holds; only with Truncate.
	Elapsed   time.Duration // Wall time inside QueryWith, including pool wait.
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
// limits from Open remain the ceiling; a larger MaxRows or MaxChars is an error.
func (db *DB) QueryWith(ctx context.Context, query string, opts QueryOptions) (Result, error) {
	return db.run(ctx, query, opts, false)
}

func (db *DB) run(ctx context.Context, query string, opts QueryOptions, describe bool) (Result, error) {
	start := time.Now()
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
	if opts.MaxChars < 0 || opts.MaxChars > db.opts.MaxChars {
		return Result{}, fmt.Errorf("max chars must be between 0 and %d", db.opts.MaxChars)
	}
	if opts.MaxChars == 0 {
		opts.MaxChars = db.opts.MaxChars
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
	trimmed := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	if trimmed == "" {
		return Result{}, fmt.Errorf("query rejected: %w: empty query", ErrNotSelect)
	}
	inner := "(\n" + trimmed + "\n) AS ntduckbox_query"
	// DuckDB casts every value to VARCHAR so results use its canonical text
	// rendering (dates, intervals, lists, structs, blobs) rather than Go's.
	// One row past the cap is enough to detect overflow without materializing
	// the rest of the result in the driver.
	wrapped := "SELECT COLUMNS(*)::VARCHAR FROM " + inner + " LIMIT " + strconv.Itoa(opts.MaxRows+1)
	if describe {
		wrapped = "SELECT * FROM " + inner
	}
	var jsonSource string
	if format.isJSON() {
		// JSON wraps the same SELECT in an outer SELECT, so the executed
		// statement is the one validated below, including DESCRIBE.
		jsonSource = "SELECT * FROM " + inner
		if describe {
			jsonSource = "DESCRIBE " + jsonSource
		}
		wrapped = jsonQuery(jsonSource, format, opts.MaxRows)
	}
	if err := validateSelect(conn, wrapped, trimmed); err != nil {
		return Result{}, fmt.Errorf("query rejected: %w", err)
	}
	if format.isJSON() {
		res, err := queryJSON(ctx, conn, jsonSource, format, opts.MaxRows, opts.MaxChars, opts.Truncate)
		if err != nil {
			return Result{}, err
		}
		res.Elapsed = time.Since(start)
		return res, nil
	}
	if describe {
		wrapped = "DESCRIBE " + wrapped
	}
	rows, err := conn.QueryContext(ctx, wrapped)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()
	res, err := render(ctx, rows, format, opts.MaxRows, opts.MaxChars, opts.Truncate)
	if err != nil {
		return Result{}, err
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

// Describe returns the schema of a SELECT-style query without executing its rows.
func (db *DB) Describe(ctx context.Context, query string) (string, error) {
	res, err := db.run(ctx, query, QueryOptions{}, true)
	return res.Text, err
}

// DescribeTable describes a relation. Each argument is one literal identifier,
// for example DescribeTable(ctx, "main", "events").
func (db *DB) DescribeTable(ctx context.Context, names ...string) (string, error) {
	rel, err := relationIdent(names)
	if err != nil {
		return "", err
	}
	return db.Describe(ctx, "SELECT * FROM "+rel)
}

// Columns returns the ordered column names and DuckDB types of a table or view.
// Each argument is one literal identifier component (table, schema+table, or
// catalog+schema+table).
func (db *DB) Columns(ctx context.Context, names ...string) ([]Column, error) {
	rel, err := relationIdent(names)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, db.opts.QueryTimeout)
	defer cancel()
	conn, err := db.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
		return nil, err
	}
	defer db.rollback(conn)
	probe := "SELECT * FROM " + rel
	if err := validateSelect(conn, probe, probe); err != nil {
		return nil, fmt.Errorf("query rejected: %w", err)
	}
	rows, err := conn.QueryContext(ctx, "DESCRIBE "+probe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []Column
	for rows.Next() {
		var name, colType string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &colType, &null, &key, &def, &extra); err != nil {
			return nil, err
		}
		cols = append(cols, Column{Name: name, Type: colType})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func relationIdent(names []string) (string, error) {
	if len(names) == 0 || len(names) > 3 {
		return "", errors.New("provide one to three identifier components")
	}
	parts := make([]string, len(names))
	for i, name := range names {
		if name == "" {
			return "", errors.New("identifier components must not be empty")
		}
		parts[i] = identifier(name)
	}
	return strings.Join(parts, "."), nil
}

// ListTables lists user tables visible in information_schema.
func (db *DB) ListTables(ctx context.Context) (string, error) { return db.relations(ctx, "BASE TABLE") }

// ListViews lists user views visible in information_schema.
func (db *DB) ListViews(ctx context.Context) (string, error) { return db.relations(ctx, "VIEW") }
func (db *DB) relations(ctx context.Context, kind string) (string, error) {
	return db.Query(ctx, "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_type = "+literal(kind)+" ORDER BY 1,2,3")
}
