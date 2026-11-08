package ntduckbox

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func openTest(tb testing.TB, opts Options) *DB {
	tb.Helper()
	db, err := Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	return db
}

func TestMaxRowsOverflow(t *testing.T) {
	db, err := Open(Options{MaxRows: math.MaxInt})
	if db != nil {
		db.Close()
		t.Fatal("Open returned a DB for an overflowing row limit")
	}
	if err == nil || !strings.Contains(err.Error(), "max rows must not exceed") {
		t.Fatalf("expected row limit validation error, got %v", err)
	}

	db = openTest(t, Options{MaxRows: math.MaxInt - 1})
	for _, format := range Formats {
		t.Run(string(format), func(t *testing.T) {
			for _, maxRows := range []int{0, math.MaxInt - 1} {
				res, err := db.QueryWith(context.Background(), "SELECT 42 AS answer", QueryOptions{Format: format, MaxRows: maxRows})
				if err != nil || res.Rows != 1 || res.Truncated || !strings.Contains(res.Text, "42") {
					t.Fatalf("MaxRows=%d: result=%+v, error=%v", maxRows, res, err)
				}
			}
			if _, err := db.QueryWith(context.Background(), "SELECT 42", QueryOptions{Format: format, MaxRows: math.MaxInt}); err == nil {
				t.Fatal("QueryWith accepted a row limit above the host limit")
			}
		})
	}
}

func TestQueryAndLimits(t *testing.T) {
	db := openTest(t, Options{MaxRows: 2, MaxChars: 100})
	got, err := db.Query(context.Background(), "WITH x AS (SELECT 'hello, world' AS msg) SELECT * FROM x;")
	if err != nil || got != "msg\n\"hello, world\"\n" {
		t.Fatalf("%q %v", got, err)
	}
	for _, query := range []string{"SELECT * FROM range(3)", "SELECT repeat('a', 101) AS text"} {
		if _, err := db.Query(context.Background(), query); !errors.Is(err, ErrResultLimit) {
			t.Fatalf("expected result limit: %v", err)
		}
	}
	got, err = db.QueryFormat(context.Background(), "SELECT 'a|b' AS x", Markdown)
	if err != nil || !strings.Contains(got, "a&#124;b") {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := db.Describe(context.Background(), "SELECT 1 AS x"); err != nil {
		t.Fatal(err)
	}
}

func TestCSVPreservesEmptyRows(t *testing.T) {
	db := openTest(t, Options{})
	got, err := db.Query(context.Background(), "SELECT '' AS value FROM range(2)")
	if err != nil {
		t.Fatal(err)
	}
	if want := "value\n\"\"\n\"\"\n"; got != want {
		t.Fatalf("empty rows must be quoted: got %q, want %q", got, want)
	}
}

func TestLockdown(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.csv")
	inside := filepath.Join(dir, "data.csv")
	for _, p := range []string{inside, outside} {
		if err := os.WriteFile(p, []byte("x\n42\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	db := openTest(t, Options{AllowedDirs: []string{dir}})
	if got, err := db.Query(context.Background(), "SELECT * FROM read_csv("+literal(inside)+")"); err != nil || !strings.Contains(got, "42") {
		t.Fatalf("allowed read: %q %v", got, err)
	}
	for _, query := range []string{
		"LOAD httpfs", "INSTALL httpfs", "SET enable_external_access=true", "RESET ALL", "CREATE TABLE bad(x INT)", "COPY (SELECT 1) TO " + literal(filepath.Join(dir, "bad.csv")),
		"ATTACH ':memory:' AS bad", "SELECT 1; SELECT 2", "SELECT 1); SET enable_external_access=true; SELECT * FROM (SELECT 2",
		"SELECT * FROM query('CREATE TABLE bad(x INT)')",
	} {
		t.Run(query, func(t *testing.T) {
			// Rejection happens in Prepare, before any statement runs.
			if _, err := db.Query(context.Background(), query); err == nil || !strings.HasPrefix(err.Error(), "query rejected: ") {
				t.Fatalf("unsafe query not rejected at preparation: %v", err)
			}
		})
	}
	// Test the underlying engine restrictions independently of the query wrapper.
	for _, query := range []string{"SET enable_external_access=true", "SET lock_configuration=false", "SET allowed_directories=['/']", "SET threads=2"} {
		_, err := db.pool.Exec(query)
		assertEngineError(t, err, engineErrorInvalidInput, "configuration has been locked")
	}
	_, err := db.pool.Exec("LOAD httpfs")
	assertEngineError(t, err, engineErrorPermission, "disabled through configuration")
	_, err = db.pool.Exec("SELECT * FROM read_csv(" + literal(outside) + ")")
	assertEngineError(t, err, engineErrorPermission, "Cannot access file")
	if _, err := db.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestFileReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.duckdb")
	seed, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = seed.Exec("CREATE TABLE events AS SELECT 42 AS n; CREATE VIEW event_view AS SELECT * FROM events; CREATE SEQUENCE seq"); err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	db := openTest(t, Options{Path: path})
	if got, err := db.Query(context.Background(), "SELECT * FROM events"); err != nil || got != "n\n42\n" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := db.Query(context.Background(), "SELECT nextval('seq')"); err == nil {
		t.Fatal("sequence write succeeded")
	}
	if _, err := db.pool.Exec("INSERT INTO events VALUES (1)"); err == nil {
		t.Fatal("file write succeeded")
	}
	if got, err := db.ListTables(context.Background()); err != nil || !strings.Contains(got, "events") {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.ListViews(context.Background()); err != nil || !strings.Contains(got, "event_view") {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := db.DescribeTable(context.Background(), "main", "events"); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutAndRecovery(t *testing.T) {
	db := openTest(t, Options{QueryTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Query(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := db.Query(context.Background(), "SELECT sum(a.i*b.i) FROM range(1000000000) a(i), range(1000000000) b(i)"); err == nil {
		t.Fatal("expected timeout")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("cancellation took too long")
	}
	if _, err := db.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultDeny(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "secret.csv")
	if err := os.WriteFile(secret, []byte("x\n42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	db := openTest(t, Options{})
	_, err := db.Query(context.Background(), "SELECT * FROM read_csv("+literal(secret)+")")
	assertEngineError(t, err, engineErrorPermission, "Cannot access file")
}

// assertEngineError fails unless err is an engine error of the given type whose
// message contains text. A bare err != nil would also accept a typo in the
// payload or a function the engine does not have.
func assertEngineError(t *testing.T, err error, typ engineErrorType, text string) {
	t.Helper()
	var engineErr *engineError
	if !errors.As(err, &engineErr) || engineErr.Type != typ || !strings.Contains(engineErr.Msg, text) {
		t.Fatalf("expected engine error type %d containing %q, got %v", typ, text, err)
	}
}

// TestFileBoundary runs every known way to name a file against the allowlist,
// through the public API and directly against the engine. The allowed directory
// is "cache" and the secret lives in its sibling "cache-other": Open appends a
// separator to the allowlist entry and the engine normalizes it the same way, so
// this pins that a bare prefix match never creeps back in.
func TestFileBoundary(t *testing.T) {
	root := t.TempDir()
	allowed := filepath.Join(root, "cache")
	sibling := filepath.Join(root, "cache-other")
	for _, dir := range []string{allowed, sibling} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	inside := filepath.Join(allowed, "data.csv")
	outside := filepath.Join(sibling, "secret.csv")
	for _, p := range []string{inside, outside} {
		if err := os.WriteFile(p, []byte("x\n42\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(allowed, "link.csv")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	// Keep ".." in the SQL path: filepath.Join would clean it away.
	traversal := allowed + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "cache-other" + string(os.PathSeparator) + "secret.csv"
	db := openTest(t, Options{AllowedDirs: []string{allowed}})

	if got, err := db.Query(context.Background(), "SELECT * FROM read_csv("+literal(inside)+")"); err != nil || got != "x\n42\n" {
		t.Fatalf("allowed read: %q %v", got, err)
	}
	for name, query := range map[string]string{
		"read_csv":        "SELECT * FROM read_csv(" + literal(outside) + ")",
		"read_text":       "SELECT * FROM read_text(" + literal(outside) + ")",
		"sniff_csv":       "SELECT * FROM sniff_csv(" + literal(outside) + ")",
		"glob":            "SELECT * FROM glob(" + literal(filepath.Join(sibling, "*")) + ")",
		"query_literal":   "SELECT * FROM query(" + literal("SELECT content FROM read_text("+literal(outside)+")") + ")",
		"quoted_from":     "SELECT * FROM " + literal(outside),
		"identifier_from": `SELECT * FROM "` + outside + `"`,
		"quoted_function": `SELECT * FROM "read_text"(` + literal(outside) + `)`,
		"sibling_prefix":  "SELECT * FROM read_csv(" + literal(outside) + ")",
		"traversal":       "SELECT * FROM read_csv(" + literal(traversal) + ")",
		"symlink":         "SELECT * FROM read_csv(" + literal(link) + ")",
		"https":           "SELECT * FROM read_csv('https://example.com/data.csv')",
		"s3":              "SELECT * FROM read_csv('s3://bucket/data.csv')",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.Query(context.Background(), query)
			assertEngineError(t, err, engineErrorPermission, "Cannot access")
			_, err = db.pool.Exec(query)
			assertEngineError(t, err, engineErrorPermission, "Cannot access")
		})
	}
}

func TestParquetAndMemoryReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.parquet")
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	if _, err := seed.Exec("COPY (SELECT 7 AS n) TO " + literal(path) + " (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	db := openTest(t, Options{AllowedDirs: []string{dir}})
	if got, err := db.Query(context.Background(), "SELECT * FROM read_parquet("+literal(path)+")"); err != nil || got != "n\n7\n" {
		t.Fatalf("%q %v", got, err)
	}
	// Internal setup only: the public API never exposes a writable connection.
	if _, err := db.pool.Exec("CREATE SEQUENCE seq"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(context.Background(), "SELECT nextval('seq')"); err == nil {
		t.Fatal("in-memory sequence write succeeded")
	}
}

func TestLockedSettings(t *testing.T) {
	db := openTest(t, Options{})
	for name, want := range map[string]string{
		"enable_external_access": "false", "autoload_known_extensions": "false",
		"autoinstall_known_extensions": "false", "allow_unsigned_extensions": "false",
		"allow_community_extensions": "false", "lock_configuration": "true",
		"allowed_configs": "[]", "allowed_paths": "[]", "allowed_directories": "[]",
		"temp_directory": "",
	} {
		var got string
		if err := db.pool.QueryRow("SELECT current_setting(?)::VARCHAR", name).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestCanonicalTextRendering(t *testing.T) {
	db := openTest(t, Options{})
	got, err := db.Query(context.Background(), `SELECT DATE '2024-01-02' AS dt, TIMESTAMP '2024-01-02 03:04:05' AS ts, INTERVAL 1 DAY AS iv, [1,2,3] AS l, {'a':1} AS s, 1.5::DECIMAL(18,3) AS d, 'ab'||chr(1)::BLOB AS b, NULL AS n`)
	if err != nil {
		t.Fatal(err)
	}
	want := "dt,ts,iv,l,s,d,b,n\n2024-01-02,2024-01-02 03:04:05,1 day,\"[1, 2, 3]\",{'a': 1},1.500,ab\\x01,NULL\n"
	if got != want {
		t.Fatalf("values must use DuckDB text rendering:\n got %q\nwant %q", got, want)
	}
}

func TestTrailingSemicolons(t *testing.T) {
	db := openTest(t, Options{})
	for _, query := range []string{"SELECT 1 AS x;;", "SELECT 1 AS x ; ;\n", "SELECT ';' AS x;"} {
		if _, err := db.Query(context.Background(), query); err != nil {
			t.Errorf("%q: %v", query, err)
		}
	}
}

func TestEngine(t *testing.T) {
	if Engine != "duckdb" && Engine != "haybarn" {
		t.Fatalf("unexpected engine %q", Engine)
	}
	db := openTest(t, Options{})
	got, err := db.Query(context.Background(), "SELECT version() AS v")
	if err != nil || !strings.HasPrefix(got, "v\nv") {
		t.Fatalf("%s engine version: %q %v", Engine, got, err)
	}
	t.Logf("engine=%s %s", Engine, strings.TrimSpace(strings.TrimPrefix(got, "v\n")))
}

func writeParquet(tb testing.TB, path string, n int) {
	tb.Helper()
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatal(err)
	}
	defer seed.Close()
	if _, err := seed.Exec("COPY (SELECT " + strconv.Itoa(n) + " AS n) TO " + literal(path) + " (FORMAT PARQUET)"); err != nil {
		tb.Fatal(err)
	}
}

func TestSetParquetViews(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeParquet(t, filepath.Join(dir, "a.parquet"), 1)
	writeParquet(t, filepath.Join(dir, "b.parquet"), 2)
	db := openTest(t, Options{AllowedDirs: []string{dir}})

	// Names may contain slashes and quotes; values may be globs.
	views := map[string]string{
		`XNAS.ITCH/ohlcv-1s`: filepath.Join(dir, "a.parquet"),
		`odd"name`:           filepath.Join(dir, "*.parquet"),
	}
	if err := db.SetParquetViews(ctx, views); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, `SELECT n FROM "XNAS.ITCH/ohlcv-1s"`); err != nil || got != "n\n1\n" {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.Query(ctx, `SELECT count(*) AS c FROM "odd""name"`); err != nil || got != "c\n2\n" {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.ListViews(ctx); err != nil || got != "table_catalog,table_schema,table_name\nmemory,main,XNAS.ITCH/ohlcv-1s\nmemory,main,\"odd\"\"name\"\n" {
		t.Fatalf("%q %v", got, err)
	}

	// A later call replaces the whole set: changed definitions are rebound and
	// omitted names are dropped.
	if err := db.SetParquetViews(ctx, map[string]string{`XNAS.ITCH/ohlcv-1s`: filepath.Join(dir, "b.parquet")}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, `SELECT n FROM "XNAS.ITCH/ohlcv-1s"`); err != nil || got != "n\n2\n" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := db.Query(ctx, `SELECT * FROM "odd""name"`); err == nil {
		t.Fatal("dropped view still queryable")
	}

	// Paths outside AllowedDirs are rejected by the engine at creation time,
	// and a failed call leaves the previous set untouched.
	outside := filepath.Join(t.TempDir(), "x.parquet")
	writeParquet(t, outside, 3)
	err := db.SetParquetViews(ctx, map[string]string{"ok": filepath.Join(dir, "a.parquet"), "bad": outside})
	if err == nil {
		t.Fatal("view outside allowed directories created")
	}
	if _, err := db.Query(ctx, `SELECT * FROM ok`); err == nil {
		t.Fatal("partial view set applied after failure")
	}
	if got, err := db.Query(ctx, `SELECT n FROM "XNAS.ITCH/ohlcv-1s"`); err != nil || got != "n\n2\n" {
		t.Fatalf("%q %v", got, err)
	}

	// An empty map removes every managed view.
	if err := db.SetParquetViews(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := db.ListViews(ctx); err != nil || got != "table_catalog,table_schema,table_name\n" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestSetParquetViewsRejectsFileBacked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.duckdb")
	seed, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec("CREATE TABLE t AS SELECT 1 AS n"); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	writeParquet(t, filepath.Join(dir, "a.parquet"), 1)
	db := openTest(t, Options{Path: path, AllowedDirs: []string{dir}})
	if err := db.SetParquetViews(context.Background(), map[string]string{"v": filepath.Join(dir, "a.parquet")}); err == nil {
		t.Fatal("views created in a read-only file-backed database")
	}
	if _, err := db.Query(context.Background(), "SELECT * FROM v"); err == nil {
		t.Fatal("view exists in file-backed database")
	}
}

func TestAttachAndIsolation(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	basePath := filepath.Join(baseDir, "dank.duckdb")
	secretCSV := filepath.Join(baseDir, "secret.csv")
	otherDB := filepath.Join(baseDir, "other.duckdb")
	if err := os.WriteFile(secretCSV, []byte("secret\n99\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("duckdb", basePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec(`
		CREATE TABLE ct_brands AS SELECT 'C0000000001' AS registration_number, 'Blue Dream' AS brand_name;
		CREATE VIEW brand_names AS SELECT brand_name FROM ct_brands;
		CREATE SEQUENCE reg_seq;
	`); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	otherSeed, err := sql.Open("duckdb", otherDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherSeed.Exec("CREATE TABLE hidden AS SELECT 1 AS x"); err != nil {
		t.Fatal(err)
	}
	if err := otherSeed.Close(); err != nil {
		t.Fatal(err)
	}

	// Attach basePath without adding baseDir to AllowedDirs.
	db := openTest(t, Options{
		Attach: []Attachment{{Alias: "ct", Path: basePath}},
	})
	for _, q := range []string{
		"SELECT registration_number, brand_name FROM ct_brands",
		"SELECT registration_number, brand_name FROM ct.ct_brands",
		"SELECT registration_number, brand_name FROM ct.main.ct_brands",
	} {
		if got, err := db.Query(ctx, q); err != nil || got != "registration_number,brand_name\nC0000000001,Blue Dream\n" {
			t.Fatalf("%s: %q %v", q, got, err)
		}
	}
	for _, parts := range [][]string{{"ct_brands"}, {"ct", "ct_brands"}, {"ct", "main", "ct_brands"}} {
		if got, err := db.DescribeTable(ctx, parts...); err != nil || !strings.Contains(got, "registration_number") {
			t.Fatalf("DescribeTable(%v): %q %v", parts, got, err)
		}
	}
	if got, err := db.ListTables(ctx); err != nil || !strings.Contains(got, "ct,main,ct_brands") {
		t.Fatalf("ListTables: %q %v", got, err)
	}
	if got, err := db.ListViews(ctx); err != nil || !strings.Contains(got, "ct,main,brand_names") {
		t.Fatalf("ListViews: %q %v", got, err)
	}

	// Attached databases are strictly read-only at both the public API and raw engine levels.
	if _, err := db.Query(ctx, "SELECT nextval('ct.reg_seq')"); err == nil {
		t.Fatal("attached sequence write succeeded")
	}
	for _, stmt := range []string{
		"INSERT INTO ct.ct_brands VALUES ('C2', 'Bad')",
		"DELETE FROM ct.ct_brands",
		"CREATE TABLE ct.bad(x INT)",
		"DROP TABLE ct.ct_brands",
	} {
		_, err := db.pool.Exec(stmt)
		assertEngineError(t, err, engineErrorInvalidInput, "read-only mode")
	}
	for _, stmt := range []string{"SET search_path = 'memory'", "DETACH ct"} {
		if _, err := db.Query(ctx, stmt); err == nil || !strings.HasPrefix(err.Error(), "query rejected: ") {
			t.Fatalf("expected %s rejection, got %v", stmt, err)
		}
	}

	// Disposing of the pooled connection (as rollback does on failure) must
	// restore search_path on the replacement connection.
	conn, err := db.pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db.rollback(conn)
	_ = conn.Close()
	if got, err := db.Query(ctx, "SELECT registration_number FROM ct_brands"); err != nil || got != "registration_number\nC0000000001\n" {
		t.Fatalf("unqualified query after bad-connection recovery: %q %v", got, err)
	}

	// DuckDB treats an attached database file as an implicit readable path even
	// after allowed_paths is cleared and enable_external_access is false: its
	// canonical path is visible in duckdb_databases() and its bytes are readable
	// via read_blob. Sibling files and writes in the same directory remain denied.
	canonicalBase, err := filepath.EvalSymlinks(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, "SELECT database_name FROM duckdb_databases() WHERE path = "+literal(canonicalBase)); err != nil || got != "database_name\nct\n" {
		t.Fatalf("duckdb_databases() attached path: %q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT to_hex(content[9:12]) AS magic FROM read_blob("+literal(canonicalBase)+")"); err != nil || got != "magic\n4455434B\n" {
		t.Fatalf("read_blob of attached database file: %q %v", got, err)
	}
	for _, q := range []string{
		"SELECT * FROM read_csv(" + literal(secretCSV) + ")",
		"SELECT * FROM read_text(" + literal(secretCSV) + ")",
		"SELECT * FROM read_blob(" + literal(secretCSV) + ")",
		"SELECT * FROM read_blob(" + literal(otherDB) + ")",
		"SELECT * FROM glob(" + literal(filepath.Join(baseDir, "*")) + ")",
	} {
		_, err := db.Query(ctx, q)
		assertEngineError(t, err, engineErrorPermission, "Cannot access")
		_, err = db.pool.Exec(q)
		assertEngineError(t, err, engineErrorPermission, "Cannot access")
	}
	for _, dest := range []string{basePath, secretCSV, filepath.Join(baseDir, "new.csv")} {
		_, err := db.pool.Exec("COPY (SELECT 1) TO " + literal(dest))
		assertEngineError(t, err, engineErrorPermission, "Cannot access")
	}
	if _, err := db.Query(ctx, "ATTACH "+literal(otherDB)+" AS other (READ_ONLY)"); err == nil || !strings.HasPrefix(err.Error(), "query rejected: ") {
		t.Fatalf("expected ATTACH rejection, got %v", err)
	}
	_, err = db.pool.Exec("ATTACH " + literal(otherDB) + " AS other (READ_ONLY)")
	assertEngineError(t, err, engineErrorPermission, "disabled")

	// Validate Attachment options at Open.
	for name, att := range map[string][]Attachment{
		"empty_alias":     {{Alias: "", Path: basePath}},
		"reserved_memory": {{Alias: "memory", Path: basePath}},
		"reserved_temp":   {{Alias: "TEMP", Path: basePath}},
		"reserved_system": {{Alias: "system", Path: basePath}},
		"reserved_main":   {{Alias: "main", Path: basePath}},
		"duplicate_alias": {{Alias: "ct", Path: basePath}, {Alias: "CT", Path: otherDB}},
		"empty_path":      {{Alias: "ct", Path: ""}},
		"memory_path":     {{Alias: "ct", Path: ":memory:"}},
		"missing_path":    {{Alias: "ct", Path: filepath.Join(baseDir, "missing.duckdb")}},
		"dir_path":        {{Alias: "ct", Path: baseDir}},
	} {
		t.Run(name, func(t *testing.T) {
			bad, err := Open(Options{Attach: att})
			if bad != nil {
				bad.Close()
				t.Fatal("Open succeeded for invalid attachment")
			}
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestSetCSVViews(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	csvA := filepath.Join(dir, "inv_a.csv")
	csvB := filepath.Join(dir, "inv_b.csv")
	headerOnly := filepath.Join(dir, "header_only.csv")
	emptyCSV := filepath.Join(dir, "empty.csv")
	badHeader := filepath.Join(dir, "bad_header.csv")
	reorderedHeader := filepath.Join(dir, "reordered_header.csv")
	outside := filepath.Join(t.TempDir(), "outside.csv")
	for path, body := range map[string]string{
		csvA:            "item_id,price,in_stock\nabc,31.5,true\n",
		csvB:            "item_id,price,in_stock\ndef,45.0,false\n",
		headerOnly:      "item_id,price,in_stock\n",
		emptyCSV:        "",
		badHeader:       "wrong,price,in_stock\nabc,31.5,true\n",
		reorderedHeader: "price,item_id,in_stock\n31.5,abc,true\n",
		outside:         "item_id,price,in_stock\nsecret,99.0,true\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cols := []CSVColumn{
		{Name: "item_id", Type: "VARCHAR"},
		{Name: "price", Type: "DOUBLE"},
		{Name: "in_stock", Type: "BOOLEAN"},
	}
	db := openTest(t, Options{AllowedDirs: []string{dir}})

	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: csvA, Columns: cols},
		"raw_csv":   {Path: csvA},
	}); err != nil {
		t.Fatal(err)
	}
	// Pinned types are preserved in JSONRows (number and boolean, not strings).
	if got, err := db.QueryFormat(ctx, "SELECT item_id, price, in_stock FROM inventory", JSONRows); err != nil || got != `[{"item_id":"abc","price":31.5,"in_stock":true}]` {
		t.Fatalf("inventory JSON: %q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT item_id FROM raw_csv"); err != nil || got != "item_id\nabc\n" {
		t.Fatalf("raw_csv: %q %v", got, err)
	}

	// Reloading swaps the CSV view and drops omitted CSV views.
	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: csvB, Columns: cols},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.QueryFormat(ctx, "SELECT item_id, price, in_stock FROM inventory", JSONRows); err != nil || got != `[{"item_id":"def","price":45.0,"in_stock":false}]` {
		t.Fatalf("reloaded inventory: %q %v", got, err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM raw_csv"); err == nil {
		t.Fatal("omitted CSV view still queryable")
	}

	// Denied path, traversal, symlink, header mismatch, empty file, or invalid column type fails and rolls back.
	traversal := dir + string(os.PathSeparator) + ".." + string(os.PathSeparator) + filepath.Base(filepath.Dir(outside)) + string(os.PathSeparator) + "outside.csv"
	for _, denied := range []string{outside, traversal} {
		err := db.SetCSVViews(ctx, map[string]CSVView{
			"inventory": {Path: denied, Columns: cols},
		})
		assertEngineError(t, err, engineErrorPermission, "Cannot access")
	}
	link := filepath.Join(dir, "link.csv")
	if err := os.Symlink(outside, link); err == nil {
		err = db.SetCSVViews(ctx, map[string]CSVView{
			"inventory": {Path: link, Columns: cols},
		})
		assertEngineError(t, err, engineErrorPermission, "Cannot access")
	}
	for _, badCSV := range []string{badHeader, reorderedHeader, emptyCSV} {
		if err := db.SetCSVViews(ctx, map[string]CSVView{
			"inventory": {Path: badCSV, Columns: cols},
		}); err == nil || !strings.Contains(err.Error(), "csv header mismatch") {
			t.Fatalf("%s: expected header mismatch error, got %v", badCSV, err)
		}
	}
	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: csvA, Columns: []CSVColumn{{Name: "item_id", Type: "NOT_A_TYPE"}, {Name: "price", Type: "DOUBLE"}, {Name: "in_stock", Type: "BOOLEAN"}}},
	}); err == nil {
		t.Fatal("invalid column type accepted")
	}
	// Previous inventory view remains intact after every failed update.
	if got, err := db.Query(ctx, "SELECT item_id FROM inventory"); err != nil || got != "item_id\ndef\n" {
		t.Fatalf("rollback failed: %q %v", got, err)
	}
	// A header-only CSV with matching column names succeeds and returns 0 rows.
	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: headerOnly, Columns: cols},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.QueryFormat(ctx, "SELECT * FROM inventory", JSONRows); err != nil || got != "[]" {
		t.Fatalf("header-only inventory: %q %v", got, err)
	}

	// Validation errors for malformed inputs.
	for _, bad := range []map[string]CSVView{
		{"": {Path: csvA}},
		{"v": {Path: ""}},
		{"v": {Path: csvA, Columns: []CSVColumn{{Name: "", Type: "VARCHAR"}}}},
		{"v": {Path: csvA, Columns: []CSVColumn{{Name: "a", Type: " "}}}},
		{"v": {Path: csvA, Columns: []CSVColumn{{Name: "a", Type: "VARCHAR"}, {Name: "a", Type: "VARCHAR"}}}},
	} {
		if err := db.SetCSVViews(ctx, bad); err == nil {
			t.Fatalf("invalid CSVView accepted: %+v", bad)
		}
	}
}

func TestSetViewsAndDankBotWorkflow(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	csvDir := t.TempDir()
	outsideDir := t.TempDir()

	basePath := filepath.Join(baseDir, "dank-mcp.duckdb")
	seed, err := sql.Open("duckdb", basePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec(`
		CREATE TABLE ct_brands AS
		SELECT 'RN-100' AS registration_number, 'Blue Dream C0000000001' AS brand_name,
		       'Acme Growers' AS producer, 'C0000000001' AS national_drug_code, 22.4 AS reg_thc
		UNION ALL
		SELECT 'RN-200', 'Northern Lights C0000000002', 'North Farms', 'C0000000002', 19.1;
		CREATE SEQUENCE base_seq;
	`); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	inv1 := filepath.Join(csvDir, "inv1.csv")
	inv2 := filepath.Join(csvDir, "inv2.csv")
	outsideCSV := filepath.Join(outsideDir, "secret.csv")
	for path, body := range map[string]string{
		inv1:       "item_id,product_name,brand,price,in_stock\ni1,Blue Dream 3.5g C0000000001,Acme,31.5,true\n",
		inv2:       "item_id,product_name,brand,price,in_stock\ni2,Northern Lights C0000000002,North,40.0,false\n",
		outsideCSV: "item_id,product_name,brand,price,in_stock\nx,Secret,Bad,0.0,false\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}

	cols := []CSVColumn{
		{Name: "item_id", Type: "VARCHAR"},
		{Name: "product_name", Type: "VARCHAR"},
		{Name: "brand", Type: "VARCHAR"},
		{Name: "price", Type: "DOUBLE"},
		{Name: "in_stock", Type: "BOOLEAN"},
	}
	inventoryBrandsSQL := `
		-- Reconcile inventory against ct_brands in the attached read-only database.
		WITH inv AS (
			SELECT *, regexp_extract(product_name, 'C[0-9]{10}', 0) AS ndc
			FROM inventory
		)
		SELECT inv.item_id, inv.price, inv.in_stock,
		       b.registration_number, b.producer, b.reg_thc
		FROM inv
		LEFT JOIN ct_brands b ON b.national_drug_code = inv.ndc;
	`

	db := openTest(t, Options{
		Attach:      []Attachment{{Alias: "ct", Path: basePath}},
		AllowedDirs: []string{csvDir},
	})
	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: inv1, Columns: cols},
	}); err != nil {
		t.Fatal(err)
	}
	// Views in one SetViews map may reference each other even when the dependent
	// view name ("a_top") sorts before its dependency ("inventory_brands").
	if err := db.SetViews(ctx, map[string]string{
		"a_top":            "SELECT item_id, registration_number, price FROM inventory_brands",
		"inventory_brands": inventoryBrandsSQL,
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number, price FROM a_top"); err != nil || got != "item_id,registration_number,price\ni1,RN-100,31.5\n" {
		t.Fatalf("a_top initial: %q %v", got, err)
	}

	// Updating both views in one call rebinds against the new dependency schema,
	// and cyclic dependencies fail and roll back.
	if err := db.SetViews(ctx, map[string]string{
		"a": "SELECT * FROM b",
		"b": "SELECT * FROM a",
	}); err == nil {
		t.Fatal("cyclic views accepted")
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number, price FROM a_top"); err != nil || got != "item_id,registration_number,price\ni1,RN-100,31.5\n" {
		t.Fatalf("a_top after rejected cycle: %q %v", got, err)
	}

	// R4: Reloading the CSV view in place updates inventory_brands and a_top
	// without reopening the base database or re-running SetViews.
	if err := db.SetCSVViews(ctx, map[string]CSVView{
		"inventory": {Path: inv2, Columns: cols},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number, producer, price FROM inventory_brands"); err != nil || got != "item_id,registration_number,producer,price\ni2,RN-200,North Farms,40.0\n" {
		t.Fatalf("inventory_brands after CSV swap: %q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number, price FROM a_top"); err != nil || got != "item_id,registration_number,price\ni2,RN-200,40.0\n" {
		t.Fatalf("a_top after CSV swap: %q %v", got, err)
	}

	// Dropping a CSV view while a SQL view still depends on it fails and rolls back.
	if err := db.SetCSVViews(ctx, nil); err == nil || !strings.Contains(err.Error(), "invalidated") {
		t.Fatalf("expected dependent SQL view invalidation error, got %v", err)
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number FROM inventory_brands"); err != nil || got != "item_id,registration_number\ni2,RN-200\n" {
		t.Fatalf("inventory_brands after rejected CSV drop: %q %v", got, err)
	}

	// Cross-set name collisions are rejected before touching existing views.
	if err := db.SetViews(ctx, map[string]string{"inventory": "SELECT 1"}); err == nil {
		t.Fatal("SetViews overwrote a CSVView")
	}
	if err := db.SetCSVViews(ctx, map[string]CSVView{"inventory_brands": {Path: inv1}}); err == nil {
		t.Fatal("SetCSVViews overwrote a SQL view")
	}
	if err := db.SetParquetViews(ctx, map[string]string{"inventory": filepath.Join(csvDir, "x.parquet")}); err == nil {
		t.Fatal("SetParquetViews overwrote a CSVView")
	}

	// Unsafe view bodies are rejected at preparation or bind time and roll back.
	if _, err := db.pool.Exec("CREATE TABLE canary AS SELECT 42 AS n"); err != nil {
		t.Fatal(err)
	}
	for _, badSQL := range []string{
		"",
		"   ; ; ",
		"CREATE TABLE bad(x INT)",
		"DELETE FROM canary",
		"COPY (SELECT 1) TO " + literal(filepath.Join(csvDir, "out.csv")),
		"SET enable_external_access = true",
		"LOAD httpfs",
		"ATTACH ':memory:' AS bad",
		"SELECT 1; DELETE FROM canary",
		"SELECT 1); DELETE FROM canary; SELECT * FROM (SELECT 2",
		"SELECT * FROM query('DELETE FROM canary')",
	} {
		if err := db.SetViews(ctx, map[string]string{"inventory_brands": badSQL}); err == nil {
			t.Fatalf("unsafe view SQL accepted: %q", badSQL)
		}
	}
	err = db.SetViews(ctx, map[string]string{
		"inventory_brands": "SELECT * FROM read_csv(" + literal(outsideCSV) + ")",
	})
	assertEngineError(t, err, engineErrorPermission, "Cannot access")

	// Untrusted SQL cannot drop, replace, or bypass the host-defined views.
	for _, q := range []string{
		"DROP VIEW inventory",
		"DROP VIEW inventory_brands",
		"CREATE OR REPLACE VIEW inventory_brands AS SELECT 1 AS x",
	} {
		if _, err := db.Query(ctx, q); err == nil || !strings.HasPrefix(err.Error(), "query rejected: ") {
			t.Fatalf("untrusted DDL not rejected at preparation: %s -> %v", q, err)
		}
	}
	var locked, external bool
	if err := db.pool.QueryRow("SELECT current_setting('lock_configuration'), current_setting('enable_external_access')").Scan(&locked, &external); err != nil {
		t.Fatal(err)
	}
	if !locked || external {
		t.Fatalf("engine security settings changed: lock_configuration=%v enable_external_access=%v", locked, external)
	}

	// Canary and previous inventory_brands view remain untouched after rejected calls.
	if got, err := db.Query(ctx, "SELECT n FROM canary"); err != nil || got != "n\n42\n" {
		t.Fatalf("canary mutated: %q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT item_id, registration_number FROM inventory_brands"); err != nil || got != "item_id,registration_number\ni2,RN-200\n" {
		t.Fatalf("inventory_brands damaged after rollback: %q %v", got, err)
	}

	// Even if a view body calls nextval on an in-memory sequence, querying the
	// view runs inside BEGIN TRANSACTION READ ONLY and is rejected.
	if _, err := db.pool.Exec("CREATE SEQUENCE mem_seq"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetViews(ctx, map[string]string{"seq_view": "SELECT nextval('mem_seq') AS n"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM seq_view"); err == nil {
		t.Fatal("mutating sequence view succeeded in read-only query transaction")
	}

	// Clearing views removes them cleanly.
	if err := db.SetViews(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.SetCSVViews(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := db.ListViews(ctx); err != nil || got != "table_catalog,table_schema,table_name\n" {
		t.Fatalf("expected no views after clearing, got %q %v", got, err)
	}
}

func TestSetCSVAndSQLViewsRejectFileBacked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.duckdb")
	csvPath := filepath.Join(dir, "a.csv")
	if err := os.WriteFile(csvPath, []byte("x\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec("CREATE TABLE t AS SELECT 1 AS n"); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db := openTest(t, Options{Path: path, AllowedDirs: []string{dir}})
	if err := db.SetCSVViews(context.Background(), map[string]CSVView{"v": {Path: csvPath}}); err == nil {
		t.Fatal("SetCSVViews succeeded on file-backed database")
	}
	if err := db.SetViews(context.Background(), map[string]string{"v": "SELECT 1"}); err == nil {
		t.Fatal("SetViews succeeded on file-backed database")
	}
}

func TestJSONRows(t *testing.T) {
	ctx := context.Background()
	db := openTest(t, Options{Format: JSONRows})
	// Native JSON types survive; no header row.
	got, err := db.Query(ctx, "SELECT 1 AS n, 1.5 AS f, true AS b, NULL AS z, 'x' AS s")
	if err != nil || got != `[{"n":1,"f":1.5,"b":true,"z":null,"s":"x"}]` {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT 1 AS n WHERE false"); err != nil || got != "[]" {
		t.Fatalf("%q %v", got, err)
	}
	// Row order follows the query's ORDER BY.
	if got, err := db.Query(ctx, "SELECT i FROM range(3) r(i) ORDER BY i DESC"); err != nil || got != `[{"i":2},{"i":1},{"i":0}]` {
		t.Fatalf("%q %v", got, err)
	}
	// User CTEs are untouched by the wrapper.
	if got, err := db.Query(ctx, "WITH x AS (SELECT 7 AS n) SELECT n FROM x;"); err != nil || got != `[{"n":7}]` {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.Describe(ctx, "SELECT 1 AS n"); err != nil || !strings.HasPrefix(got, `[{"column_name":"n","column_type":"INTEGER"`) {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.ListViews(ctx); err != nil || got != "[]" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := db.Query(ctx, "SELECT 1; SELECT 2"); err == nil {
		t.Fatal("batch accepted")
	}
	if _, err := db.Query(ctx, "CREATE TABLE t (n INT)"); err == nil {
		t.Fatal("DDL accepted")
	}
}

func TestJSONColumnar(t *testing.T) {
	ctx := context.Background()
	db := openTest(t, Options{})
	got, err := db.QueryFormat(ctx, "SELECT i AS n, 'x' || i AS s FROM range(2) r(i) ORDER BY i", JSONColumnar)
	if err != nil || got != `{"n":[0,1],"s":["x0","x1"]}` {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.QueryFormat(ctx, "SELECT 1 AS n, 'x' AS s WHERE false", JSONColumnar); err != nil || got != `{"n":[],"s":[]}` {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := db.QueryFormat(ctx, "SELECT NULL AS z", JSONColumnar); err != nil || got != `{"z":[null]}` {
		t.Fatalf("%q %v", got, err)
	}
}

func TestJSONLimits(t *testing.T) {
	ctx := context.Background()
	db := openTest(t, Options{MaxRows: 2, MaxChars: 40})
	for _, format := range []Format{JSONRows, JSONColumnar} {
		if _, err := db.QueryFormat(ctx, "SELECT i FROM range(2) r(i)", format); err != nil {
			t.Fatal(format, err)
		}
		if _, err := db.QueryFormat(ctx, "SELECT i FROM range(3) r(i)", format); !errors.Is(err, ErrResultLimit) {
			t.Fatalf("%s rows: %v", format, err)
		}
		if _, err := db.QueryFormat(ctx, "SELECT repeat('x', 50) AS s", format); !errors.Is(err, ErrResultLimit) {
			t.Fatalf("%s chars: %v", format, err)
		}
	}
	if _, err := Open(Options{Format: "json"}); err == nil {
		t.Fatal("unknown format accepted")
	}
}

func TestQueryWith(t *testing.T) {
	ctx := context.Background()
	db := openTest(t, Options{MaxRows: 5, MaxChars: 60})
	three := "SELECT i FROM range(3) r(i) ORDER BY i"

	// Per-call rows cap below the host ceiling, still all-or-error by default.
	res, err := db.QueryWith(ctx, three, QueryOptions{MaxRows: 3})
	if err != nil || res.Text != "i\n0\n1\n2\n" || res.Rows != 3 || res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxRows: 2}); !errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}
	// The host ceiling is a hard cap.
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxRows: 6}); err == nil || errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxRows: -1}); err == nil {
		t.Fatal("negative accepted")
	}

	// Truncate keeps the first rows and says so.
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxRows: 2, Truncate: true})
	if err != nil || res.Text != "i\n0\n1\n" || res.Rows != 2 || !res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{Truncate: true, Format: Markdown})
	if err != nil || res.Text != "| i |\n| --- |\n| 0 |\n| 1 |\n| 2 |\n" || res.Rows != 3 || res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	// Truncate also stops at a row boundary before the character limit.
	res, err = db.QueryWith(ctx, "SELECT repeat('x', 20) AS s FROM range(4)", QueryOptions{Truncate: true})
	if err != nil || res.Text != "s\n"+strings.Repeat("x", 20)+"\n"+strings.Repeat("x", 20)+"\n" || res.Rows != 2 || !res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	// An oversized header is still an error.
	if _, err := db.QueryWith(ctx, "SELECT 1 AS "+identifier(strings.Repeat("h", 70)), QueryOptions{Truncate: true}); !errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}

	// JSON formats truncate by rows, keep order, and still error on characters.
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxRows: 2, Truncate: true, Format: JSONRows})
	if err != nil || res.Text != `[{"i":0},{"i":1}]` || res.Rows != 2 || !res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxRows: 2, Truncate: true, Format: JSONColumnar})
	if err != nil || res.Text != `{"i":[0,1]}` || res.Rows != 2 || !res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{Format: JSONRows})
	if err != nil || res.Rows != 3 || res.Truncated {
		t.Fatalf("%#v %v", res, err)
	}
	if _, err := db.QueryWith(ctx, "SELECT repeat('x', 70) AS s", QueryOptions{Truncate: true, Format: JSONRows}); !errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}
}
