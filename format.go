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
	rows := "WITH ntduckbox_rows AS (SELECT * FROM (" + source + ") LIMIT " + strconv.Itoa(maxRows+1) + ") SELECT (SELECT count(*) FROM ntduckbox_rows), "
	if format == JSONColumnar {
		return rows + "to_json(t)::VARCHAR FROM (SELECT coalesce(list(COLUMNS(*)), []) FROM ntduckbox_rows) AS t"
	}
	return rows + "coalesce(to_json(list(t)), '[]')::VARCHAR FROM ntduckbox_rows AS t"
}

// renderJSON reads the single cell produced by jsonQuery within the limits.
func renderJSON(rows *sql.Rows, maxRows, maxChars int) (string, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", errors.New("json result missing")
	}
	var count int64
	var text string
	if err := rows.Scan(&count, &text); err != nil {
		return "", err
	}
	if count > int64(maxRows) || len(text) > maxChars {
		return "", ErrResultLimit
	}
	return text, rows.Err()
}

// ErrResultLimit means no partial result was returned. Add a SQL LIMIT, select
// fewer columns, or increase the host-configured limits.
var ErrResultLimit = errors.New("result exceeds configured row or character limit; use LIMIT or select less data")

// markdownEscaper keeps cell content inside its table cell and on one line.
var markdownEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\\", "&#92;", "|", "&#124;", "\r", "&#13;", "\n", "<br>")

// render encodes rows all-or-error within maxRows and maxChars (UTF-8 bytes).
func render(ctx context.Context, rows *sql.Rows, format Format, maxRows, maxChars int) (string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
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
		return "", err
	}
	if format == Markdown {
		separators := make([]string, len(cols))
		for i := range separators {
			separators[i] = "---"
		}
		if err := appendRow(separators); err != nil {
			return "", err
		}
	}
	values := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	count := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if count >= maxRows {
			return "", ErrResultLimit
		}
		count++
		if err := rows.Scan(dest...); err != nil {
			return "", err
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
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return out.String(), nil
}
