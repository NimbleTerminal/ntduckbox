package ntduckbox

import (
	"context"
	"encoding/csv"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// Each fuzz worker owns a connection. Keep inputs small: this targets the Go
// boundary, not unbounded SQL workloads or native DuckDB coverage (CGO isn't
// instrumented by Go's coverage-guided fuzzer).
var fuzzOptions = Options{MaxRows: 8, MaxChars: 4096, QueryTimeout: 100 * time.Millisecond}

func FuzzQueryIsolation(f *testing.F) {
	for _, s := range []string{
		"SELECT 1", "SELECT * FROM fuzz_canary", "SELECT ';' AS x", "SELECT $$;$$ AS x",
		"SELECT 1 -- comment", "SELECT /* nested /* comment */ */ 1", "SELECT 1; SELECT 2",
		"SELECT 1); COMMIT; DELETE FROM fuzz_canary; SELECT * FROM (SELECT 1",
		"SELECT 1); SET enable_external_access=true; SELECT * FROM (SELECT 1",
		"UPDATE fuzz_canary SET n=0", "LOAD httpfs", "INSTALL httpfs", "COPY fuzz_canary TO '/tmp/fuzz.csv'",
		"ATTACH ':memory:' AS escape", "RESET ALL", "SELECT * FROM query('DELETE FROM fuzz_canary')",
		"SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM read_csv('https://example.invalid/x')",
		"SELECT 1\x00; DELETE FROM fuzz_canary", `fuzz_canary"; DROP TABLE fuzz_canary; --`,
	} {
		for mode := uint8(0); mode < 3; mode++ {
			f.Add(s, mode)
		}
	}
	db := openTest(f, fuzzOptions)
	if _, err := db.pool.Exec("CREATE TABLE fuzz_canary AS SELECT 42 AS n"); err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, input string, mode uint8) {
		if len(input) > 2048 {
			t.Skip()
		}
		var output string
		var err error
		switch mode % 3 {
		case 0:
			output, err = db.Query(context.Background(), input)
		case 1:
			output, err = db.Describe(context.Background(), input)
		case 2:
			output, err = db.DescribeTable(context.Background(), input)
		}
		if err != nil && output != "" {
			t.Fatalf("error returned partial output: input=%q mode=%d output=%q err=%v", input, mode, output, err)
		}
		if len(output) > db.opts.MaxChars {
			t.Fatalf("output exceeded limit: input=%q mode=%d len=%d", input, mode, len(output))
		}
		// Check state independently of the public wrapper, after its transaction ends.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var count, total int
		var locked, external, autoload, autoinstall bool
		if err := db.pool.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(n),0),
			current_setting('lock_configuration'), current_setting('enable_external_access'),
			current_setting('autoload_known_extensions'), current_setting('autoinstall_known_extensions')
			FROM fuzz_canary`).Scan(&count, &total, &locked, &external, &autoload, &autoinstall); err != nil {
			t.Fatalf("connection/canary damaged: input=%q mode=%d err=%v", input, mode, err)
		}
		if count != 1 || total != 42 {
			t.Fatalf("canary mutated: input=%q mode=%d count=%d sum=%d", input, mode, count, total)
		}
		if !locked || external || autoload || autoinstall {
			t.Fatalf("security configuration changed: input=%q mode=%d", input, mode)
		}
	})
}

func FuzzResultFormatting(f *testing.F) {
	for _, s := range []string{"", "hello", "a,b", "\"quoted\"", "\r\n", "a\nb", "|<>&\\", "日本語🙂", "\x00", "NULL"} {
		f.Add(s, uint8(2), uint8(3), uint16(1024), false)
		f.Add(s, uint8(2), uint8(3), uint16(1024), true)
	}
	db := openTest(f, fuzzOptions)
	f.Fuzz(func(t *testing.T, value string, rowByte, limitByte uint8, charLimit uint16, markdown bool) {
		if len(value) > 2048 || !utf8.ValidString(value) {
			t.Skip()
		}
		n := int(rowByte % 5)
		maxRows := 1 + int(limitByte%4)
		maxChars := 1 + int(charLimit%4096)
		format := CSV
		if markdown {
			format = Markdown
		}
		renderBounded := func(rowsLimit, charsLimit int) (string, error) {
			t.Helper()
			rows, err := db.pool.QueryContext(context.Background(), "SELECT ?::VARCHAR AS value FROM range(?)", value, n)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			return render(context.Background(), rows, format, rowsLimit, charsLimit)
		}
		full, err := renderBounded(5, 100000)
		if err != nil {
			t.Fatal(err)
		}
		if format == CSV {
			records, err := csv.NewReader(strings.NewReader(full)).ReadAll()
			if err != nil {
				t.Fatalf("CSV unreadable: value=%q output=%q err=%v", value, full, err)
			}
			if len(records) != n+1 {
				t.Fatalf("CSV lost rows: got %d, want %d; value=%q output=%q", len(records), n+1, value, full)
			}
			expected := strings.ReplaceAll(value, "\r\n", "\n") // encoding/csv normalizes CRLF on read.
			for _, record := range records[1:] {
				if len(record) != 1 || record[0] != expected {
					t.Fatalf("CSV round trip: got %q, want %q; output=%q", record, expected, full)
				}
			}
		} else {
			lines := strings.Split(strings.TrimSuffix(full, "\n"), "\n")
			if len(lines) != n+2 {
				t.Fatalf("Markdown row boundary escaped: value=%q output=%q", value, full)
			}
			for _, line := range lines {
				if strings.Count(line, "|") != 2 || strings.ContainsRune(line, '\r') {
					t.Fatalf("Markdown cell boundary escaped: value=%q line=%q", value, line)
				}
			}
		}
		got, err := renderBounded(maxRows, maxChars)
		if n > maxRows || len(full) > maxChars {
			if !errors.Is(err, ErrResultLimit) || got != "" {
				t.Fatalf("expected limit error without partial output: value=%q got=%q err=%v", value, got, err)
			}
		} else if err != nil || got != full {
			t.Fatalf("bounded output changed: value=%q got=%q err=%v", value, got, err)
		}
	})
}
