package ntduckbox

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Format is the result encoding.
type Format string

const (
	// CSV includes a header row and uses standard CSV quoting.
	CSV Format = "csv"
	// Markdown emits a pipe-delimited Markdown table.
	Markdown Format = "markdown"
	// JSONRows emits a JSON array of row objects keyed by column name, with
	// DuckDB's native JSON typing. No header row.
	JSONRows Format = "json_rows"
	// JSONColumnar emits one JSON object keyed by column name whose values are
	// arrays of that column's values, with DuckDB's native JSON typing.
	JSONColumnar Format = "json_columnar"
)

// Formats lists every supported Format in declaration order.
var Formats = []Format{CSV, Markdown, JSONRows, JSONColumnar}

func (f Format) valid() bool  { return slices.Contains(Formats, f) }
func (f Format) isJSON() bool { return f == JSONRows || f == JSONColumnar }

// jsonQuery wraps a validated SELECT so DuckDB serializes at most maxRows+1
// rows as JSON in one cell, with the row count alongside for limit checks.
// DuckDB's to_json keeps native numbers, booleans, and nulls; everything else
// uses its text rendering. Duplicate column names are suffixed by DuckDB.
func jsonQuery(source string, format Format, maxRows int) string {
	rows := "WITH ntduckbox_rows AS (SELECT * FROM (" + source + ") LIMIT " + strconv.Itoa(maxRows+1) + "), " +
		"ntduckbox_page AS (SELECT * FROM ntduckbox_rows LIMIT " + strconv.Itoa(maxRows) + ") " +
		"SELECT (SELECT count(*) FROM ntduckbox_rows), "
	if format == JSONColumnar {
		return rows + "to_json(t)::VARCHAR FROM (SELECT coalesce(list(COLUMNS(*)), []) FROM ntduckbox_page) AS t"
	}
	return rows + "coalesce(to_json(list(t)), '[]')::VARCHAR FROM ntduckbox_page AS t"
}

// queryJSON runs jsonQuery and, when truncate is true and the serialized JSON
// exceeds maxChars, binary-searches the row cap (starting from the average-size
// estimate) so DuckDB's to_json returns the largest whole-row prefix within
// maxChars.
func queryJSON(ctx context.Context, conn *sql.Conn, source string, format Format, maxRows, maxChars int, truncate bool) (Result, error) {
	if !strings.HasPrefix(source, "DESCRIBE ") {
		rewritten, err := rewriteJSONTimestamps(ctx, conn, source)
		if err != nil {
			return Result{}, err
		}
		source = rewritten
	}
	count, text, err := scanJSON(ctx, conn, jsonQuery(source, format, maxRows))
	if err != nil {
		return Result{}, err
	}
	pageRows := min(count, maxRows)
	truncated := count > maxRows
	if truncated && !truncate {
		return Result{}, ErrResultLimit
	}
	if len(text) <= maxChars {
		return Result{Text: text, Rows: pageRows, Bytes: len(text), Truncated: truncated}, nil
	}
	if !truncate || pageRows <= 1 {
		return Result{}, ErrResultLimit
	}
	lo, hi := 1, pageRows-1
	mid := min(max(int(int64(pageRows)*int64(maxChars)/int64(len(text))), lo), hi)
	bestRows := 0
	var bestText string
	for lo <= hi {
		_, candidate, err := scanJSON(ctx, conn, jsonQuery(source, format, mid))
		if err != nil {
			return Result{}, err
		}
		if len(candidate) <= maxChars {
			bestRows = mid
			bestText = candidate
			lo = mid + 1
		} else {
			hi = mid - 1
		}
		mid = lo + (hi-lo)/2
	}
	if bestRows == 0 {
		return Result{}, ErrResultLimit
	}
	return Result{Text: bestText, Rows: bestRows, Bytes: len(bestText), Truncated: true}, nil
}

// rewriteJSONTimestamps wraps source with SELECT * REPLACE (...) when top-level
// columns are DuckDB timestamp types so to_json emits ISO 8601 / RFC 3339 strings
// (for example "2026-09-01T08:01:42.622909564Z" or "...-04:00") while keeping
// nanosecond precision and SQL NULL -> JSON null.
func rewriteJSONTimestamps(ctx context.Context, conn *sql.Conn, source string) (string, error) {
	rows, err := conn.QueryContext(ctx, "DESCRIBE "+source)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	type colMeta struct {
		name string
		typ  string
	}
	var cols []colMeta
	counts := make(map[string]int)
	for rows.Next() {
		var name, colType string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &colType, &null, &key, &def, &extra); err != nil {
			return "", err
		}
		cols = append(cols, colMeta{name: name, typ: colType})
		counts[name]++
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	var replacements []string
	for _, c := range cols {
		if counts[c.name] != 1 {
			continue
		}
		id := identifier(c.name)
		switch c.typ {
		case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "TIMESTAMPTZ_NS":
			replacements = append(replacements,
				"regexp_replace(regexp_replace(replace("+id+"::VARCHAR, ' ', 'T'), '[+-]00$', 'Z'), '([+-][0-9]{2})$', '\\1:00') AS "+id)
		case "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S":
			replacements = append(replacements,
				"replace("+id+"::VARCHAR, ' ', 'T') AS "+id)
		}
	}
	if len(replacements) == 0 {
		return source, nil
	}
	return "SELECT * REPLACE (" + strings.Join(replacements, ", ") + ") FROM (" + source + ")", nil
}

func scanJSON(ctx context.Context, conn *sql.Conn, query string) (int, string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, "", err
		}
		return 0, "", errors.New("json result missing")
	}
	var count int
	var text string
	if err := rows.Scan(&count, &text); err != nil {
		return 0, "", err
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	return count, text, nil
}

// ErrResultLimit means no partial result was returned. Add a SQL LIMIT, select
// fewer columns, or increase the host-configured limits.
var ErrResultLimit = errors.New("result exceeds configured row or character limit; use LIMIT or select less data")

// markdownEscaper keeps cell content inside its table cell and on one line.
var markdownEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\\", "&#92;", "|", "&#124;", "\r", "&#13;", "\n", "<br>")

// render encodes rows within maxRows and maxChars (UTF-8 bytes): all-or-error,
// or with truncate the whole rows that fit, flagged in Result.Truncated.
func render(ctx context.Context, rows *sql.Rows, format Format, maxRows, maxChars int, truncate bool) (Result, error) {
	cols, err := rows.Columns()
	if err != nil {
		return Result{}, err
	}
	var out strings.Builder
	appendRow := func(fields []string) error {
		var record strings.Builder
		if format == CSV {
			// encoding/csv writes a lone empty field as a blank line, which
			// readers skip. Quote it explicitly to preserve the result row.
			if len(fields) == 1 && fields[0] == "" {
				record.WriteString("\"\"\n")
			} else {
				w := csv.NewWriter(&record)
				if err := w.Write(fields); err != nil {
					return err
				}
				w.Flush()
				if err := w.Error(); err != nil {
					return err
				}
			}
		} else {
			record.WriteString("| ")
			for i, field := range fields {
				if i > 0 {
					record.WriteString(" | ")
				}
				record.WriteString(markdownEscaper.Replace(field))
			}
			record.WriteString(" |\n")
		}
		if record.Len() > maxChars-out.Len() {
			return ErrResultLimit
		}
		out.WriteString(record.String())
		return nil
	}
	if err := appendRow(cols); err != nil {
		return Result{}, err
	}
	if format == Markdown {
		separators := make([]string, len(cols))
		for i := range separators {
			separators[i] = "---"
		}
		if err := appendRow(separators); err != nil {
			return Result{}, err
		}
	}
	values := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	res := Result{}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if res.Rows >= maxRows {
			if truncate {
				res.Truncated = true
				break
			}
			return Result{}, ErrResultLimit
		}
		if err := rows.Scan(dest...); err != nil {
			return Result{}, err
		}
		fields := make([]string, len(cols))
		for i, v := range values {
			switch v := v.(type) {
			case nil:
				fields[i] = "NULL"
			case []byte:
				fields[i] = string(v)
			default:
				fields[i] = fmt.Sprint(v)
			}
		}
		if err := appendRow(fields); err != nil {
			if truncate && errors.Is(err, ErrResultLimit) {
				res.Truncated = true
				break
			}
			return Result{}, err
		}
		res.Rows++
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	res.Text = out.String()
	res.Bytes = len(res.Text)
	return res, nil
}
