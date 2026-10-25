//go:build haybarn

package ntduckbox

import (
	"database/sql/driver"

	duckdb "github.com/Query-farm-haybarn/haybarn-go/v2"
)

// Engine names the linked database engine: "duckdb" by default, or "haybarn"
// when built with -tags haybarn. Haybarn is Query Farm's independent derived
// distribution of DuckDB; it is ABI- and file-format-compatible with the DuckDB
// release it is built from and shares every setting ntduckbox relies on.
const Engine = "haybarn"

type (
	engineConn = duckdb.Conn
	engineStmt = duckdb.Stmt
)

const engineSelect = duckdb.STATEMENT_TYPE_SELECT

// Engine errors carry a type, which tests use to tell a sandbox denial from
// an unrelated failure such as a typo in the payload.
type (
	engineError     = duckdb.Error
	engineErrorType = duckdb.ErrorType
)

const (
	engineErrorPermission   = duckdb.ErrorTypePermission
	engineErrorInvalidInput = duckdb.ErrorTypeInvalidInput
)

func newEngineConnector(dsn string) (driver.Connector, error) { return duckdb.NewConnector(dsn, nil) }
