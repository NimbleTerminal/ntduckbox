package ntduckbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	_, err := db.Query(context.Background(), "SELECT sum(a.i*b.i) FROM range(1000000000) a(i), range(1000000000) b(i)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if strings.Contains(err.Error(), "INTERRUPT") || strings.Contains(err.Error(), "\n") {
		t.Fatalf("timed-out error should not include multi-line engine interrupt text: %q", err.Error())
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
		"temp_directory": "", "TimeZone": "UTC", "Calendar": "gregorian",
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

	// JSON formats truncate by rows or characters, keep order, and still error
	// when not even one row fits within MaxChars.
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxRows: 2, Truncate: true, Format: JSONRows})
	if err != nil || res.Text != `[{"i":0},{"i":1}]` || res.Rows != 2 || !res.Truncated || res.Bytes != len(res.Text) || res.Elapsed <= 0 {
		t.Fatalf("%#v %v", res, err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxRows: 2, Truncate: true, Format: JSONColumnar})
	if err != nil || res.Text != `{"i":[0,1]}` || res.Rows != 2 || !res.Truncated || res.Bytes != len(res.Text) || res.Elapsed <= 0 {
		t.Fatalf("%#v %v", res, err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{Format: JSONRows})
	if err != nil || res.Rows != 3 || res.Truncated || res.Bytes != len(res.Text) || res.Elapsed <= 0 {
		t.Fatalf("%#v %v", res, err)
	}
	if _, err := db.QueryWith(ctx, "SELECT repeat('x', 70) AS s", QueryOptions{Truncate: true, Format: JSONRows}); !errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}
	if _, err := db.QueryWith(ctx, "SELECT repeat('x', 70) AS s", QueryOptions{Truncate: true, Format: JSONColumnar}); !errors.Is(err, ErrResultLimit) {
		t.Fatal(err)
	}

	// Per-call MaxChars cap below the host ceiling, with and without Truncate.
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxChars: 61}); err == nil || errors.Is(err, ErrResultLimit) {
		t.Fatalf("MaxChars above host limit: %v", err)
	}
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxChars: -1}); err == nil {
		t.Fatal("negative MaxChars accepted")
	}
	if _, err := db.QueryWith(ctx, three, QueryOptions{MaxChars: 6}); !errors.Is(err, ErrResultLimit) {
		t.Fatalf("MaxChars without Truncate: %v", err)
	}
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxChars: 6, Truncate: true})
	if err != nil || res.Text != "i\n0\n1\n" || res.Rows != 2 || !res.Truncated || res.Bytes != 6 {
		t.Fatalf("CSV MaxChars truncate: %#v %v", res, err)
	}
	// JSONRows for 3 rows is `[{"i":0},{"i":1},{"i":2}]` (25 bytes); MaxChars: 18 fits 2 rows (`[{"i":0},{"i":1}]`, 17 bytes).
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxChars: 18, Truncate: true, Format: JSONRows})
	if err != nil || res.Text != `[{"i":0},{"i":1}]` || res.Rows != 2 || !res.Truncated || res.Bytes != 17 {
		t.Fatalf("JSONRows MaxChars truncate: %#v %v", res, err)
	}
	var parsedRows []map[string]int
	if err := json.Unmarshal([]byte(res.Text), &parsedRows); err != nil || len(parsedRows) != 2 {
		t.Fatalf("JSONRows unmarshal: %v %v", parsedRows, err)
	}
	// JSONColumnar for 3 rows is `{"i":[0,1,2]}` (13 bytes); MaxChars: 11 fits 2 rows (`{"i":[0,1]}`, 11 bytes).
	res, err = db.QueryWith(ctx, three, QueryOptions{MaxChars: 11, Truncate: true, Format: JSONColumnar})
	if err != nil || res.Text != `{"i":[0,1]}` || res.Rows != 2 || !res.Truncated || res.Bytes != 11 {
		t.Fatalf("JSONColumnar MaxChars truncate: %#v %v", res, err)
	}
}

func TestSetAllViewsAndInvalidation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tradesParquet := filepath.Join(dir, "trades.parquet")
	bboParquet := filepath.Join(dir, "bbo.parquet")
	metaCSV := filepath.Join(dir, "meta.csv")
	if err := os.WriteFile(metaCSV, []byte("symbol,venue\nAAPL,XNAS\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"COPY (SELECT 'AAPL' AS symbol, 100 AS price) TO " + literal(tradesParquet) + " (FORMAT PARQUET)",
		"COPY (SELECT 'AAPL' AS symbol, 99 AS bid, 101 AS ask) TO " + literal(bboParquet) + " (FORMAT PARQUET)",
	} {
		if _, err := seed.Exec(stmt); err != nil {
			seed.Close()
			t.Fatal(err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	db := openTest(t, Options{Path: ":memory:", AllowedDirs: []string{dir}})

	// SetAllViews creates Parquet, CSV, and derived SQL views in one transaction.
	err = db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{
			"XNAS.ITCH/trades": tradesParquet,
			"EQUS.MINI/bbo":    bboParquet,
		},
		CSV: map[string]CSVView{
			"meta": {Path: metaCSV, Columns: []Column{{Name: "symbol", Type: "VARCHAR"}, {Name: "venue", Type: "VARCHAR"}}},
		},
		SQL: map[string]string{
			"trades_enriched": `SELECT t.symbol, t.price, m.venue FROM "XNAS.ITCH/trades" t JOIN meta m USING (symbol)`,
			"bbo_spread":      `SELECT symbol, ask - bid AS spread FROM "EQUS.MINI/bbo"`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := db.Query(ctx, "SELECT symbol, price, venue FROM trades_enriched"); err != nil || got != "symbol,price,venue\nAAPL,100,XNAS\n" {
		t.Fatalf("trades_enriched: %q %v", got, err)
	}
	if got, err := db.Query(ctx, "SELECT symbol, spread FROM bbo_spread"); err != nil || got != "symbol,spread\nAAPL,2\n" {
		t.Fatalf("bbo_spread: %q %v", got, err)
	}

	// Standalone SetParquetViews dropping both Parquet views enumerates all broken SQL views.
	err = db.SetParquetViews(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "bbo_spread") || !strings.Contains(err.Error(), "trades_enriched") {
		t.Fatalf("expected both broken SQL views in error, got %v", err)
	}
	if !errors.Is(err, ErrBinder) {
		t.Fatalf("expected ErrBinder in invalidation error, got %v", err)
	}

	// SetAllViews atomically drops a base Parquet view and its dependent SQL view together.
	err = db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{
			"XNAS.ITCH/trades": tradesParquet,
		},
		CSV: map[string]CSVView{
			"meta": {Path: metaCSV, Columns: []Column{{Name: "symbol", Type: "VARCHAR"}, {Name: "venue", Type: "VARCHAR"}}},
		},
		SQL: map[string]string{
			"trades_enriched": `SELECT t.symbol, t.price, m.venue FROM "XNAS.ITCH/trades" t JOIN meta m USING (symbol)`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM bbo_spread"); !errors.Is(err, ErrBinder) {
		t.Fatalf("expected dropped bbo_spread to fail with ErrBinder, got %v", err)
	}

	// Duplicate view names across kinds in ViewSet fail without damaging existing views.
	err = db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{"dup": tradesParquet},
		SQL:     map[string]string{"dup": "SELECT 1"},
	})
	if err == nil {
		t.Fatal("duplicate view name across kinds accepted")
	}
	if got, err := db.Query(ctx, "SELECT symbol, price, venue FROM trades_enriched"); err != nil || got != "symbol,price,venue\nAAPL,100,XNAS\n" {
		t.Fatalf("trades_enriched after rejected SetAllViews: %q %v", got, err)
	}

	// Empty glob at view creation fails and rolls back; removing files after
	// creation leaves the view in the catalog and fails when queried.
	globDir := filepath.Join(dir, "glob")
	if err := os.MkdirAll(globDir, 0700); err != nil {
		t.Fatal(err)
	}
	globPattern := filepath.Join(globDir, "*.parquet")
	if err := db.SetParquetViews(ctx, map[string]string{"empty_glob": globPattern}); err == nil {
		t.Fatal("empty Parquet glob accepted at creation")
	}
	globFile := filepath.Join(globDir, "part0.parquet")
	raw, err := os.ReadFile(tradesParquet)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(globFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{"live_glob": globPattern},
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(globFile); err != nil {
		t.Fatal(err)
	}
	views, err := db.Views(ctx)
	if err != nil || len(views) != 1 || views[0].Name != "live_glob" {
		t.Fatalf("expected live_glob to remain registered after file removal, got %v %v", views, err)
	}
	if _, err := db.Query(ctx, "SELECT * FROM live_glob"); err == nil {
		t.Fatal("querying Parquet glob after all files removed succeeded")
	}
}

func TestValidateAndSentinelErrors(t *testing.T) {
	ctx := context.Background()
	allowedDir := t.TempDir()
	outsideDir := t.TempDir()
	outsideParquet := filepath.Join(outsideDir, "secret.parquet")
	outsideCSV := filepath.Join(outsideDir, "secret.csv")
	if err := os.WriteFile(outsideCSV, []byte("a\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec("COPY (SELECT 1 AS a) TO " + literal(outsideParquet) + " (FORMAT PARQUET)"); err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	db := openTest(t, Options{Path: ":memory:", AllowedDirs: []string{allowedDir}})
	if err := db.SetViews(ctx, map[string]string{"v": "SELECT 1 AS n"}); err != nil {
		t.Fatal(err)
	}

	for _, valid := range []string{
		"SELECT n FROM v",
		"WITH c AS (SELECT n FROM v) SELECT * FROM c;",
		"VALUES (1, 'a'), (2, 'b')",
		"FROM v",
		"-- leading comment\n/* block */ (SELECT n FROM v)",
	} {
		if err := db.Validate(ctx, valid); err != nil {
			t.Fatalf("Validate(%q) failed: %v", valid, err)
		}
	}

	for _, notSelect := range []string{
		"",
		"   ; ; ",
		"CREATE TABLE t(x INT)",
		"INSERT INTO v VALUES (2)",
		"UPDATE v SET n = 2",
		"DELETE FROM v",
		"DROP VIEW v",
		"COPY v TO 'out.parquet'",
		"ATTACH ':memory:' AS other",
		"DETACH main",
		"LOAD httpfs",
		"INSTALL httpfs",
		"SET enable_external_access = true",
		"RESET enable_external_access",
		"PRAGMA version",
		"CALL duckdb_tables()",
		"EXPORT DATABASE 'out'",
		"IMPORT DATABASE 'out'",
		"SELECT 1; SELECT 2",
		"SELECT 1; DROP VIEW v",
		"SELECT 1); DROP VIEW v; SELECT * FROM (SELECT 2",
	} {
		for _, err := range []error{
			db.Validate(ctx, notSelect),
			func() error { _, err := db.Query(ctx, notSelect); return err }(),
		} {
			if !errors.Is(err, ErrNotSelect) {
				t.Fatalf("query %q: expected ErrNotSelect, got %v", notSelect, err)
			}
		}
	}

	for _, permQuery := range []string{
		"SELECT * FROM read_parquet(" + literal(outsideParquet) + ")",
		"SELECT * FROM parquet_scan(" + literal(outsideParquet) + ")",
		"SELECT * FROM read_csv(" + literal(outsideCSV) + ")",
		"SELECT * FROM read_blob(" + literal(outsideCSV) + ")",
		"SELECT * FROM read_text(" + literal(outsideCSV) + ")",
		"SELECT * FROM glob(" + literal(filepath.Join(outsideDir, "*")) + ")",
		"SELECT * FROM " + literal(outsideParquet),
		"SELECT * FROM " + identifier(outsideParquet),
		"SELECT * FROM " + literal(outsideCSV),
		"SELECT * FROM " + identifier(outsideCSV),
	} {
		for _, err := range []error{
			db.Validate(ctx, permQuery),
			func() error { _, err := db.Query(ctx, permQuery); return err }(),
		} {
			if !errors.Is(err, ErrPermission) {
				t.Fatalf("query %q: expected ErrPermission, got %v", permQuery, err)
			}
		}
	}

	for _, binderQuery := range []string{
		"SELECT * FROM missing_table",
		"SELECT missing_col FROM v",
		"SELECT 1 FROM",
		"SELECT * FROM query('DELETE FROM v')",
	} {
		for _, err := range []error{
			db.Validate(ctx, binderQuery),
			func() error { _, err := db.Query(ctx, binderQuery); return err }(),
		} {
			if !errors.Is(err, ErrBinder) {
				t.Fatalf("query %q: expected ErrBinder, got %v", binderQuery, err)
			}
		}
	}
}

func TestColumnsAndViews(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	pqPath := filepath.Join(dir, "trades.parquet")
	csvPath := filepath.Join(dir, "items.csv")
	if err := os.WriteFile(csvPath, []byte("id,price\ni1,12.5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec("COPY (SELECT 'AAPL' AS symbol, 42::BIGINT AS size) TO " + literal(pqPath) + " (FORMAT PARQUET)"); err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	db := openTest(t, Options{Path: ":memory:", AllowedDirs: []string{dir}})
	sqlBody := `SELECT symbol, size FROM "XNAS.ITCH/trades"`
	if err := db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{"XNAS.ITCH/trades": pqPath},
		CSV: map[string]CSVView{
			"items": {Path: csvPath, Columns: []Column{{Name: "id", Type: "VARCHAR"}, {Name: "price", Type: "DOUBLE"}}},
		},
		SQL: map[string]string{"derived": sqlBody},
	}); err != nil {
		t.Fatal(err)
	}

	gotViews, err := db.Views(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantViews := []ViewInfo{
		{Name: "XNAS.ITCH/trades", Kind: ParquetViewKind, Source: pqPath},
		{Name: "derived", Kind: SQLViewKind, Source: sqlBody},
		{Name: "items", Kind: CSVViewKind, Source: csvPath},
	}
	if !slices.Equal(gotViews, wantViews) {
		t.Fatalf("Views() = %#v, want %#v", gotViews, wantViews)
	}

	gotCols, err := db.Columns(ctx, "XNAS.ITCH/trades")
	if err != nil {
		t.Fatal(err)
	}
	wantCols := []Column{{Name: "symbol", Type: "VARCHAR"}, {Name: "size", Type: "BIGINT"}}
	if !slices.Equal(gotCols, wantCols) {
		t.Fatalf("Columns(XNAS.ITCH/trades) = %#v, want %#v", gotCols, wantCols)
	}

	gotCSVCols, err := db.Columns(ctx, "main", "items")
	if err != nil {
		t.Fatal(err)
	}
	wantCSVCols := []Column{{Name: "id", Type: "VARCHAR"}, {Name: "price", Type: "DOUBLE"}}
	if !slices.Equal(gotCSVCols, wantCSVCols) {
		t.Fatalf("Columns(main, items) = %#v, want %#v", gotCSVCols, wantCSVCols)
	}

	if _, err := db.Columns(ctx); err == nil {
		t.Fatal("Columns() with no args accepted")
	}
	if _, err := db.Columns(ctx, ""); err == nil {
		t.Fatal("Columns(\"\") accepted")
	}
	if _, err := db.Columns(ctx, "nonexistent"); !errors.Is(err, ErrBinder) {
		t.Fatalf("Columns(nonexistent): expected ErrBinder, got %v", err)
	}
}

func TestValuesMetadataAndNanosecondTimestamps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	pqPath := filepath.Join(dir, "bbo.parquet")
	seed, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	// TIMESTAMP_NS is supported by both DuckDB 2.0 and Haybarn 1.5.x;
	// TIMESTAMPTZ_NS is additionally tested below when Engine == "duckdb".
	_, err = seed.Exec(`COPY (
		SELECT
			'EQUS.MINI' AS dataset,
			'AAPL' AS symbol,
			make_timestamp_ns(1700000000123456789) AS ts_event,
			9223372036.85::DOUBLE AS ask_px
		UNION ALL
		SELECT
			'EQUS.MINI' AS dataset,
			'AAPL' AS symbol,
			make_timestamp_ns(-1) AS ts_event,
			150.25::DOUBLE AS ask_px
	) TO ` + literal(pqPath) + ` (FORMAT PARQUET)`)
	if err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	db := openTest(t, Options{Path: ":memory:", AllowedDirs: []string{dir}})
	err = db.SetAllViews(ctx, ViewSet{
		Parquet: map[string]string{"raw_bbo": pqPath},
		SQL: map[string]string{
			"cache_manifest": `SELECT * FROM (VALUES ('EQUS.MINI', 'bbo-1m', 2)) AS t(dataset, schema_name, row_count)`,
			"clean_bbo": `SELECT * REPLACE (
				CASE WHEN ts_event < TIMESTAMP_NS '1970-01-01 00:00:00' THEN NULL ELSE ts_event::TIMESTAMPTZ END AS ts_event,
				CASE WHEN ask_px > 1e9 THEN NULL ELSE ask_px END AS ask_px
			) FROM raw_bbo`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify raw TIMESTAMP_NS rendering in CSV and JSONRows without a cast.
	if rawCSV, err := db.Query(ctx, "SELECT ts_event FROM raw_bbo"); err != nil || !strings.Contains(rawCSV, ".123456789") {
		t.Fatalf("raw_bbo CSV should preserve nanosecond fractional digits: %q %v", rawCSV, err)
	}
	if rawJSON, err := db.QueryFormat(ctx, "SELECT ts_event FROM raw_bbo", JSONRows); err != nil || !strings.Contains(rawJSON, ".123456789") {
		t.Fatalf("raw_bbo JSONRows should preserve nanosecond fractional digits: %q %v", rawJSON, err)
	}
	if err := db.Validate(ctx, "SELECT date_trunc('minute', ts_event), ts_event + INTERVAL 1 MINUTE FROM clean_bbo WHERE ts_event >= TIMESTAMPTZ '2020-01-01 00:00:00Z'"); err != nil {
		t.Fatalf("clean_bbo TIMESTAMPTZ comparison/date_trunc/interval failed: %v", err)
	}

	rawCols, err := db.Columns(ctx, "raw_bbo")
	if err != nil {
		t.Fatal(err)
	}
	if len(rawCols) != 4 || rawCols[2] != (Column{Name: "ts_event", Type: "TIMESTAMP_NS"}) {
		t.Fatalf("unexpected raw_bbo columns: %#v", rawCols)
	}

	cleanCols, err := db.Columns(ctx, "clean_bbo")
	if err != nil {
		t.Fatal(err)
	}
	if cleanCols[2] != (Column{Name: "ts_event", Type: "TIMESTAMP WITH TIME ZONE"}) {
		t.Fatalf("unexpected clean_bbo ts_event column: %#v", cleanCols[2])
	}

	got, err := db.QueryFormat(ctx, `
		SELECT b.symbol, m.schema_name, b.ts_event IS NULL AS ts_null, b.ask_px
		FROM clean_bbo b
		JOIN cache_manifest m USING (dataset)
		ORDER BY b.ask_px NULLS FIRST
	`, JSONRows)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"symbol":"AAPL","schema_name":"bbo-1m","ts_null":false,"ask_px":null},{"symbol":"AAPL","schema_name":"bbo-1m","ts_null":true,"ask_px":150.25}]`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}

	if Engine == "duckdb" {
		tzPath := filepath.Join(dir, "bbo_tz.parquet")
		seedTZ, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := seedTZ.Exec(`COPY (SELECT TIMESTAMPTZ_NS '2023-11-14 22:13:20.123456789Z' AS ts_event) TO ` + literal(tzPath) + ` (FORMAT PARQUET)`); err != nil {
			seedTZ.Close()
			t.Fatal(err)
		}
		if err := seedTZ.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.SetParquetViews(ctx, map[string]string{"raw_bbo": pqPath, "tz_bbo": tzPath}); err != nil {
			t.Fatal(err)
		}
		if err := db.Validate(ctx, "SELECT * FROM tz_bbo WHERE ts_event >= TIMESTAMPTZ '2020-01-01 00:00:00Z'"); !errors.Is(err, ErrBinder) {
			t.Fatalf("expected TIMESTAMPTZ_NS vs TIMESTAMPTZ comparison to fail with ErrBinder, got %v", err)
		}
	}
}

func TestQueueWaitDoesNotConsumeQueryTimeout(t *testing.T) {
	db := openTest(t, Options{QueryTimeout: 120 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var errA, errB, errC error
	var resB, resC Result

	wg.Add(3)
	go func() {
		defer wg.Done()
		_, errA = db.Query(ctx, "SELECT sum(a.i*b.i) FROM range(1000000000) a(i), range(1000000000) b(i)")
	}()
	time.Sleep(25 * time.Millisecond)
	go func() {
		defer wg.Done()
		resB, errB = db.QueryWith(ctx, "SELECT 1 AS n", QueryOptions{})
	}()
	time.Sleep(25 * time.Millisecond)
	go func() {
		defer wg.Done()
		resC, errC = db.QueryWith(ctx, "SELECT 2 AS n", QueryOptions{})
	}()
	wg.Wait()

	if !errors.Is(errA, context.DeadlineExceeded) {
		t.Fatalf("expected slow query A to fail with DeadlineExceeded, got %v", errA)
	}
	if errB != nil || resB.Text != "n\n1\n" || resB.Elapsed < 40*time.Millisecond {
		t.Fatalf("queued query B: res=%#v err=%v", resB, errB)
	}
	if errC != nil || resC.Text != "n\n2\n" || resC.Elapsed < 40*time.Millisecond {
		t.Fatalf("queued query C: res=%#v err=%v", resC, errC)
	}
}

func TestTimeZoneCalendarAndJSONTimestamps(t *testing.T) {
	ctx := context.Background()
	if bad, err := Open(Options{TimeZone: "Not/AZone"}); err == nil {
		bad.Close()
		t.Fatal("invalid TimeZone accepted")
	}

	dbUTC := openTest(t, Options{})
	var tz, cal string
	if err := dbUTC.pool.QueryRow("SELECT current_setting('TimeZone')::VARCHAR, current_setting('Calendar')::VARCHAR").Scan(&tz, &cal); err != nil {
		t.Fatal(err)
	}
	if tz != "UTC" || cal != "gregorian" {
		t.Fatalf("settings: TimeZone=%q Calendar=%q, want UTC and gregorian", tz, cal)
	}
	for _, stmt := range []string{"SET TimeZone = 'America/New_York'", "SET Calendar = 'japanese'"} {
		_, err := dbUTC.pool.Exec(stmt)
		assertEngineError(t, err, engineErrorInvalidInput, "configuration has been locked")
	}

	gotRows, err := dbUTC.QueryFormat(ctx, `
		SELECT
			TIMESTAMPTZ '2026-09-01 08:01:42.622909Z' AS tstz,
			TIMESTAMP '2026-09-01 08:01:42.5' AS ts,
			make_timestamp_ns(1700000000123456789) AS ts_ns,
			NULL::TIMESTAMPTZ AS z
	`, JSONRows)
	if err != nil {
		t.Fatal(err)
	}
	wantRows := `[{"tstz":"2026-09-01T08:01:42.622909Z","ts":"2026-09-01T08:01:42.5","ts_ns":"2023-11-14T22:13:20.123456789","z":null}]`
	if gotRows != wantRows {
		t.Fatalf("UTC JSONRows:\n got %s\nwant %s", gotRows, wantRows)
	}
	if _, err := time.Parse(time.RFC3339Nano, "2026-09-01T08:01:42.622909Z"); err != nil {
		t.Fatal(err)
	}

	gotCol, err := dbUTC.QueryFormat(ctx, `SELECT TIMESTAMPTZ '2026-09-01 08:01:42.622909Z' AS tstz, NULL::TIMESTAMPTZ AS z`, JSONColumnar)
	if err != nil {
		t.Fatal(err)
	}
	if gotCol != `{"tstz":["2026-09-01T08:01:42.622909Z"],"z":[null]}` {
		t.Fatalf("UTC JSONColumnar: got %s", gotCol)
	}

	if Engine == "duckdb" {
		gotNS, err := dbUTC.QueryFormat(ctx, `SELECT TIMESTAMPTZ_NS '2026-09-01 08:01:42.622909564Z' AS tstz_ns`, JSONRows)
		if err != nil || gotNS != `[{"tstz_ns":"2026-09-01T08:01:42.622909564Z"}]` {
			t.Fatalf("UTC TIMESTAMPTZ_NS JSONRows: %s %v", gotNS, err)
		}
	}

	dbNY := openTest(t, Options{TimeZone: "America/New_York"})
	gotNY, err := dbNY.QueryFormat(ctx, `SELECT TIMESTAMPTZ '2026-09-01 08:01:42.622909Z' AS tstz`, JSONRows)
	if err != nil || gotNY != `[{"tstz":"2026-09-01T04:01:42.622909-04:00"}]` {
		t.Fatalf("America/New_York JSONRows: %s %v", gotNY, err)
	}
	if _, err := time.Parse(time.RFC3339Nano, "2026-09-01T04:01:42.622909-04:00"); err != nil {
		t.Fatal(err)
	}
	gotNYCSV, err := dbNY.Query(ctx, `SELECT TIMESTAMPTZ '2026-09-01 08:01:42.622909Z' AS tstz`)
	if err != nil || gotNYCSV != "tstz\n2026-09-01 04:01:42.622909-04\n" {
		t.Fatalf("America/New_York CSV: %q %v", gotNYCSV, err)
	}
	conn, err := dbNY.pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dbNY.rollback(conn)
	_ = conn.Close()
	gotNYAfter, err := dbNY.QueryFormat(ctx, `SELECT TIMESTAMPTZ '2026-09-01 08:01:42.622909Z' AS tstz`, JSONRows)
	if err != nil || gotNYAfter != `[{"tstz":"2026-09-01T04:01:42.622909-04:00"}]` {
		t.Fatalf("America/New_York after connection replacement: %s %v", gotNYAfter, err)
	}
}

func TestStatementSurfacePivotAndExplain(t *testing.T) {
	ctx := context.Background()
	db := openTest(t, Options{Path: ":memory:"})
	if err := db.SetViews(ctx, map[string]string{
		"sales": "SELECT * FROM (VALUES ('AAPL', 'Q1', 10, 20), ('AAPL', 'Q2', 30, 40)) AS t(symbol, quarter, bids, asks)",
	}); err != nil {
		t.Fatal(err)
	}

	for name, q := range map[string]string{
		"select":       "SELECT symbol, bids FROM sales",
		"with_select":  "WITH c AS (SELECT * FROM sales) SELECT symbol FROM c",
		"from_first":   "FROM sales SELECT symbol, quarter",
		"table":        "TABLE sales",
		"values":       "VALUES ('AAPL', 10), ('MSFT', 20)",
		"describe":     "DESCRIBE sales",
		"summarize":    "SUMMARIZE sales",
		"show_tables":  "SHOW TABLES",
		"unpivot":      "UNPIVOT sales ON bids, asks INTO NAME side VALUE qty",
		"static_pivot": "PIVOT sales ON quarter IN ('Q1', 'Q2') USING sum(bids)",
	} {
		t.Run(name, func(t *testing.T) {
			if err := db.Validate(ctx, q); err != nil {
				t.Fatalf("Validate(%s) failed: %v", q, err)
			}
			for _, format := range []Format{CSV, JSONRows} {
				if _, err := db.QueryFormat(ctx, q, format); err != nil {
					t.Fatalf("QueryFormat(%s, %s) failed: %v", q, format, err)
				}
			}
		})
	}

	// Dynamic PIVOT without IN (...) requires temporary enum creation and is rejected clearly.
	for _, err := range []error{
		db.Validate(ctx, "PIVOT sales ON quarter USING sum(bids)"),
		func() error { _, err := db.Query(ctx, "PIVOT sales ON quarter USING sum(bids)"); return err }(),
	} {
		if !errors.Is(err, ErrNotSelect) || !strings.Contains(err.Error(), "IN (...)") {
			t.Fatalf("expected ErrNotSelect mentioning IN (...), got %v", err)
		}
	}

	// EXPLAIN is rejected with a direct message rather than a wrapper parser error.
	for _, explainQuery := range []string{
		"EXPLAIN SELECT * FROM sales",
		"/* comment */ EXPLAIN ANALYZE SELECT 1",
	} {
		for _, err := range []error{
			db.Validate(ctx, explainQuery),
			func() error { _, err := db.Query(ctx, explainQuery); return err }(),
		} {
			if !errors.Is(err, ErrNotSelect) || !strings.Contains(err.Error(), "EXPLAIN is not supported") {
				t.Fatalf("query %q: expected ErrNotSelect with 'EXPLAIN is not supported', got %v", explainQuery, err)
			}
		}
	}
}
