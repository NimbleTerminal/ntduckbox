package ntduckbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
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
