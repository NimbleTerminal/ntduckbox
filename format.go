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
// exceeds maxChars, re-runs with a smaller row cap derived from average row
// size so DuckDB's to_json still produces a valid JSON document within maxChars.
func queryJSON(ctx context.Context, conn *sql.Conn, source string, format Format, maxRows, maxChars int, truncate bool) (Result, error) {
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
	rowCap := pageRows
	for len(text) > maxChars && rowCap > 1 {
		est := int(int64(rowCap) * int64(maxChars) / int64(len(text)))
		if est >= rowCap {
			est = rowCap - 1
		}
		if est < 1 {
			est = 1
		}
		rowCap = est
		_, text, err = scanJSON(ctx, conn, jsonQuery(source, format, rowCap))
		if err != nil {
			return Result{}, err
		}
		if rowCap == 1 && len(text) > maxChars {
			return Result{}, ErrResultLimit
		}
	}
	return Result{Text: text, Rows: rowCap, Bytes: len(text), Truncated: true}, nil
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
