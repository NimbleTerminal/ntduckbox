# ntduckbox — Restricted DuckDB exploration for Go and MCP

<p>
    <a href="https://github.com/NimbleTerminal/ntduckbox/tags"><img src="https://img.shields.io/github/tag/NimbleTerminal/ntduckbox.svg" alt="Latest Release"></a>
    <a href="https://pkg.go.dev/nimbleterminal.dev/ntduckbox"><img src="https://pkg.go.dev/badge/nimbleterminal.dev/ntduckbox.svg" alt="Go Reference"></a>
    <a href="https://github.com/NimbleTerminal/ntduckbox/actions/workflows/ci.yml"><img src="https://github.com/NimbleTerminal/ntduckbox/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="./CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Code Of Conduct"></a>
</p>

`ntduckbox` (`nimbleterminal.dev/ntduckbox`) lets agents query analytical data
through a small read-only API and four MCP tools. It opens existing DuckDB files,
or an empty in-memory database with explicitly allowed local CSV/Parquet reads.
Results are bounded CSV or Markdown; database configuration stays locked. It links
DuckDB by default or [Haybarn](https://github.com/Query-farm-haybarn/haybarn), an
independent derived distribution of DuckDB, with one build tag; see [Engines](#engines).

Source: [NimbleTerminal/ntduckbox](https://github.com/NimbleTerminal/ntduckbox).

See the [changelog](CHANGELOG.md) for changes, [security
considerations](#security-considerations) for the scope of the restrictions, and
[SECURITY.md](SECURITY.md) for reporting vulnerabilities.

## Install

Requires Go 1.26+, CGO, and a C/C++ toolchain supported by
[DuckDB's Go driver](https://github.com/duckdb/duckdb-go). The driver bundles the
native engine library; a separate DuckDB or Haybarn installation is not required.
The only direct dependencies are the two engine drivers, of which exactly one is
linked, and `github.com/mark3labs/mcp-go`.

Once the module is published:

```sh
go get nimbleterminal.dev/ntduckbox
```

For this checkout, run `task ci` and `task build` as described under
[validation](#validation).

## Quickstart

This complete example uses an empty in-memory database so no fixture is needed.
Set `Path` to an existing DuckDB file to query its tables and views.

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "time"

    "nimbleterminal.dev/ntduckbox"
)

func main() {
    if err := run(); err != nil {
        slog.Error("query failed", "error", err)
        os.Exit(1)
    }
}

func run() error {
    db, err := ntduckbox.Open(ntduckbox.Options{
        Path: ":memory:",
        MaxRows: 1000,
        MaxChars: 100_000,
        QueryTimeout: 10 * time.Second,
    })
    if err != nil {
        return err
    }
    defer db.Close()

    result, err := db.Query(context.Background(), "SELECT 42 AS answer")
    if err != nil {
        return err
    }
    fmt.Print(result)
    return nil
}
```

## Results and lifecycle

The caller owns the DB and must close it. Calls may run concurrently; each DB
uses a single pooled connection (`SetMaxOpenConns(1)`) so connection-scoped
`search_path` state and `Set*Views` catalog replacements stay serialized against
read-only query transactions. Waiting for the connection is governed by the
caller's `ctx`, while `QueryTimeout` starts only after the connection is
acquired and applies to query execution. Timed-out or canceled queries return
`context.DeadlineExceeded` or `context.Canceled` directly (without the engine's
multi-line `INTERRUPT Error` text). Pass a context with a shorter deadline to
bound queue wait + execution together, or add a host semaphore in front of
`QueryWith` if you want immediate rejection when busy.

`Open` sets `Calendar = 'gregorian'` and `TimeZone` (default `"UTC"`, overridable
via `Options.TimeZone`) before locking configuration, so timestamp rendering and
date-literal comparisons do not drift with the host machine's local zone. Hosts
that want exchange-local output for specific queries can use `AT TIME ZONE` in
SQL or set `Options.TimeZone` at `Open`.

Zero limits select the defaults shown above; negatives are errors. `MaxRows`
must not exceed `math.MaxInt-1`, leaving room to fetch one extra row. CSV is the
default format. `QueryFormat(ctx, sql, ntduckbox.Markdown)` selects a format for
one call: `CSV`, `Markdown`, `JSONRows`, or `JSONColumnar` (see `Formats`). `Describe(ctx, sql)` returns a query schema;
`DescribeTable(ctx, "main", "events")` quotes each identifier component safely.
For structured Go introspection without parsing formatted text, `Columns(ctx, "main", "events")`
returns `[]Column{Name, Type}` for a table or view, and `Views(ctx)` returns
`[]ViewInfo{Name, Kind, Source}` (`ParquetViewKind`, `CSVViewKind`, or
`SQLViewKind`) for the host-defined views registered on an in-memory DB.

CSV and Markdown include headers. DuckDB casts every value to VARCHAR before
encoding, so dates, timestamps, intervals, lists, structs, and blobs use DuckDB's
text rendering rather than Go's. SQL NULL is rendered as `NULL` (indistinguishable
from that literal string), and a lone empty string is written as a quoted `""` in
CSV so readers keep the row.

`JSONRows` returns a JSON array of row objects, `[{"n":1,"s":"x"}]`, and
`JSONColumnar` one object of column arrays, `{"n":[1],"s":["x"]}`. Both are
serialized by DuckDB's `to_json`, so numbers, booleans, nulls, lists, and structs
keep native JSON types while dates, timestamps, intervals, and blobs are strings.
Top-level `TIMESTAMPTZ` and `TIMESTAMPTZ_NS` columns are formatted in SQL as
RFC 3339 strings in the configured `TimeZone` (for example
`"2026-09-01T08:01:42.622909564Z"` under the default `"UTC"`, or
`"2026-09-01T04:01:42.622909564-04:00"` under `"America/New_York"`), and naive
`TIMESTAMP` / `TIMESTAMP_NS` columns use the ISO 8601 `'T'` separator
(`"2026-09-01T08:01:42.5"`), while `NULL` timestamps remain JSON `null`.
Empty results are `[]` and `{"n":[]}`. Row order follows the query's `ORDER BY`.
DuckDB suffixes duplicate column names (`n`, `n_1`) and writes non-finite doubles
as bare `Infinity` or `NaN`, which strict parsers reject; cast or filter them in
SQL. `Describe`, `ListTables`, and `ListViews` return their rows in the same
format.

MaxChars counts UTF-8 bytes, including headers and escaping. Results exceeding
either limit return `ErrResultLimit` with no partial output. Add a SQL `LIMIT`
or select fewer columns. Trailing semicolons are ignored.

`QueryWith(ctx, sql, ntduckbox.QueryOptions{...})` adjusts one call and returns
a `Result` with `Text`, `Rows` (data rows), `Bytes` (`len(Text)`), `Truncated`,
and `Elapsed` (total wall time inside `QueryWith`, including connection queue
wait). `MaxRows` and `MaxChars` lower the row and byte caps for that call; the
host limits from `Open` stay the ceiling and larger values are errors. `Truncate`
returns the whole rows that fit within both `MaxRows` and `MaxChars` instead of
`ErrResultLimit` and sets `Truncated`, so a host can tell its caller that more
rows exist. CSV and Markdown stop at the last whole row within both limits;
`JSONRows` and `JSONColumnar` re-run `to_json` with a smaller row cap when the
initial page exceeds `MaxChars`. If not even a single row (or header) fits
within `MaxChars`, `ErrResultLimit` is still returned. Every query asks the
engine for at most one row past the cap, so oversized results are not
materialized.

`Validate(ctx, sql)` runs the same subquery wrapper and single-statement
`Conn.Prepare` check as `QueryWith` inside a read-only transaction without
executing rows. Both `Validate` and `QueryWith` wrap rejected queries with
sentinel errors so callers can branch with `errors.Is`:

- `ErrNotSelect`: empty query, multi-statement batch, or non-`SELECT` statement
  (DDL, DML, `COPY`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`, `SET`, `RESET`,
  `PRAGMA`, `CALL`, `EXPORT`, `IMPORT`, `EXPLAIN`, or dynamic `PIVOT` without
  `IN (...)`).
- `ErrPermission`: access denied by `allowed_directories`, `enable_external_access=false`,
  `disabled_filesystems`, or `lock_configuration=true` (including `read_parquet`,
  `read_csv`, `read_blob`, `read_text`, `glob`, and single- or double-quoted file
  paths in `FROM` outside `AllowedDirs`).
- `ErrBinder`: syntax, catalog, or binder error inside a `SELECT` query (such as
  an unknown table or column, a type mismatch, or `query('...')` of a non-`SELECT`
  statement).

| Statement form | Status | Notes |
| --- | --- | --- |
| `SELECT ...`, `WITH ... SELECT ...` | Accepted | Standard queries and CTEs |
| `FROM t [SELECT ...]`, `TABLE t`, `VALUES (...)` | Accepted | DuckDB query shorthands |
| `DESCRIBE <table\|query>`, `SUMMARIZE <table\|query>`, `SHOW TABLES` | Accepted | Compile to `STATEMENT_TYPE_SELECT` inside the subquery wrapper |
| `UNPIVOT t ON ... INTO NAME ... VALUE ...` | Accepted | Static output schema |
| `PIVOT t ON col IN (...) USING ...` | Accepted | Explicit `IN (...)` list keeps the output schema static |
| `PIVOT t ON col USING ...` (no `IN`) | Rejected (`ErrNotSelect`) | Dynamic `PIVOT` rewrites to a multi-statement `CREATE TEMPORARY TYPE` batch |
| `EXPLAIN ...` | Rejected (`ErrNotSelect`) | Returns `"EXPLAIN is not supported"` |
| DDL, DML, `COPY`, `ATTACH`, `DETACH`, `LOAD`, `INSTALL`, `SET`, `RESET`, `PRAGMA`, `CALL`, `EXPORT`, `IMPORT`, `a; b` | Rejected (`ErrNotSelect`) | Rejected at `Conn.Prepare` before execution |
| File reads or quoted `FROM` paths outside `AllowedDirs` | Rejected (`ErrPermission`) | Enforced by `allowed_directories` and `enable_external_access=false` |

An empty Path or `:memory:` creates an empty in-memory database. DuckDB does not
support opening an in-memory database with `access_mode=read_only`; internally
this uses read-write mode, but every public query runs in a read-only transaction
and the API exposes no write operations. There is no ambiguous `ReadOnly bool`
and no write-access escape hatch.

## MCP registration

```go
import (
    "github.com/mark3labs/mcp-go/server"
    "nimbleterminal.dev/ntduckbox"
)

// After opening db as above:
s := server.NewMCPServer("analytics", "1.0.0", server.WithToolCapabilities(false))
if err := db.RegisterTools(s); err != nil {
    return err
}
return server.ServeStdio(s)
```

The server type in mcp-go is `*server.MCPServer`. Register once before serving;
existing tools with any of these names cause registration to fail.

| Tool | Arguments |
| --- | --- |
| `query` | Required `sql`; optional `format`: `csv`, `markdown`, `json_rows`, or `json_columnar` |
| `list_tables` | None |
| `list_views` | None |
| `describe` | Exactly one of `sql` or `table`; optional `schema` with `table` |

Table and schema arguments are literal identifiers, quoted safely. Use separate
`schema` and `table` arguments for qualified names. All tool failures are returned
as `mcp.NewToolResultError`, including invalid input, SQL errors, timeouts, and
output limits. Limits are controlled by the host, not the agent.

## STDIO server

```sh
task build
./bin/ntduckbox -db ./analytics.duckdb -max-rows 500 -timeout 5s
```

Flags: `-db`, repeatable `-allow-dir`, `-max-rows`, `-max-chars`, `-timeout`,
and `-format`. Logging uses `slog` on stderr, including the version and engine at
startup; stdout is reserved for MCP.

## Engines

The same code links one of two ABI- and file-format-compatible engines. The
public API, settings, lockdown, and tests are identical; `ntduckbox.Engine`
reports which one is linked, and the CLI logs it at startup.

| Build | Engine | Driver |
| --- | --- | --- |
| default | DuckDB | `github.com/duckdb/duckdb-go/v2` |
| `-tags haybarn` | [Haybarn](https://github.com/Query-farm-haybarn/haybarn), Query Farm's independent derived distribution of DuckDB | `github.com/Query-farm-haybarn/haybarn-go/v2` |

```sh
go build -tags haybarn ./...            # library consumers add the tag to their own build
GOFLAGS=-tags=haybarn task ci           # run the full preflight against Haybarn
task package VERSION=v0.0.0 ENGINE=haybarn
```

Both drivers stay in `go.mod` so either build is reproducible from the lock
file; only the selected one is compiled and linked. Each engine reads database
files written by its own or an older storage version. The pinned DuckDB driver
bundles DuckDB 2.0.0 and the pinned Haybarn driver bundles DuckDB 1.5.6, so a
file created by the default build cannot be opened by the Haybarn build until
Haybarn's 2.0 line is released; files created by 1.5.x open under both. Haybarn
uses its own extension repository and trust root, which is irrelevant here
because extension loading and installation are disabled. Haybarn is not
affiliated with or endorsed by the DuckDB Foundation. See
[docs/haybarn.md](docs/haybarn.md) for a full guide.

## Local Parquet and CSV

`AllowedDirs` is an explicit relaxation of the default prohibition on external
file reads. Directories must exist; roots are canonicalized to absolute paths.
Use absolute paths in queries.

```go
db, err := ntduckbox.Open(ntduckbox.Options{
    Path: ":memory:",
    AllowedDirs: []string{"/srv/analytics/cache"},
})
// Handle err and defer db.Close().
csv, err := db.Query(ctx,
    "SELECT * FROM read_parquet('/srv/analytics/cache/*.parquet') LIMIT 50")
```

CSV and Parquet support bundled into DuckDB works without loading extensions.

Hosts can name Parquet or CSV files (or globs) as views in an in-memory
database, and replace either set at runtime as files appear or disappear,
without any SQL:

```go
err = db.SetParquetViews(ctx, map[string]string{
    "XNAS.ITCH/ohlcv-1s": "/srv/analytics/cache/XNAS.ITCH/ohlcv-1s/*.parquet",
})
csv, err = db.Query(ctx, `SELECT * FROM "XNAS.ITCH/ohlcv-1s" LIMIT 50`)

err = db.SetCSVViews(ctx, map[string]ntduckbox.CSVView{
    "inventory": {
        Path: "/srv/analytics/cache/inventory.csv",
        Columns: []ntduckbox.CSVColumn{
            {Name: "item_id", Type: "VARCHAR"},
            {Name: "price", Type: "DOUBLE"},
            {Name: "in_stock", Type: "BOOLEAN"},
        },
    },
})
```

When `CSVView.Columns` is provided, `SetCSVViews` checks that the file header
matches those column names in order before pinning each column's DuckDB type in
`read_csv`.

To join runtime views in `main` against an existing read-only `.duckdb` file,
attach the file at `Open` with `Options.Attach` and define host-trusted `SELECT`
views with `SetViews`:

```go
db, err := ntduckbox.Open(ntduckbox.Options{
    Attach:      []ntduckbox.Attachment{{Alias: "ct", Path: "/srv/analytics/brands.duckdb"}},
    AllowedDirs: []string{"/srv/analytics/cache"},
})
// Attached catalogs are always read-only; their main schemas are added to
// search_path after memory.main, so both ct.ct_brands and ct_brands resolve.
err = db.SetViews(ctx, map[string]string{
    "inventory_brands": `SELECT i.item_id, b.brand_name FROM inventory i LEFT JOIN ct_brands b USING (item_id)`,
})
```

When a host layers derived SQL views on top of Parquet or CSV views and refreshes
them together, `SetAllViews(ctx, ntduckbox.ViewSet{Parquet: ..., CSV: ..., SQL: ...})`
replaces all three kinds in a single transaction so base views and their
dependent SQL views can be added, updated, or dropped atomically. Each standalone
`SetParquetViews`, `SetCSVViews`, or `SetViews` call replaces only its own
managed set: views from earlier calls to the same method that are missing from
the map are dropped, an empty map drops them all, and views managed by the other
methods are untouched. Removing or changing a Parquet or CSV view via a
standalone call when a remaining `SetViews` view still depends on it fails,
enumerates every invalidated SQL view in the error, and rolls back; use
`SetAllViews` when updating base and derived views together.

`SetParquetViews` and `SetCSVViews` quote names, paths, and column definitions
themselves; `SetViews` wraps each query body as a subquery, validates it with
DuckDB's single-statement `Prepare` API (`STATEMENT_TYPE_SELECT`), and creates
views in dependency passes so acyclic views in one map may reference each other
regardless of map key order. Small host-provided metadata tables (such as cache
manifests) can also be exposed as `VALUES`-backed views in `SetViews` without
writing a sidecar file (`SELECT * FROM (VALUES (...)) AS t(...)`). A Parquet
glob that matches zero files fails at `SetParquetViews` / `SetAllViews` time and
rolls back; if all matching files are removed from disk after the view is
created, the view remains in the catalog (`Views` still lists it) and queries
against it fail until matching files exist again or the view is dropped. The
engine binds every view under the locked settings, so a path outside
`AllowedDirs`, a header mismatch, a cycle, or a non-SELECT statement fails the
call and leaves the previous set in place. File-backed primary databases are
opened read-only and reject view updates. See [docs/host-views.md](docs/host-views.md)
for the design, threat-model analysis, nanosecond timestamp (`TIMESTAMPTZ_NS`)
handling, and `VALUES`-view patterns.

## Security considerations

The untrusted input is SQL supplied through this library or its MCP tools. The
host application, database files, allowlisted directories, DuckDB engine and
native dependencies must be trusted. Do not expose underlying DuckDB connections
or load extensions elsewhere in the process. Do not share an open database with
another driver instance that changes its settings; DuckDB can cache database
instances in-process. Conflicting configuration causes Open to fail closed.

Before returning a DB, Open configures:

- File-backed `access_mode=read_only`, and `READ_ONLY` attachments for every
  `Options.Attach` entry (with `search_path` set to `main` followed by each
  attached catalog's `main` schema).
- `Calendar = 'gregorian'` and `TimeZone` (default `"UTC"`, or `Options.TimeZone`).
- Extension autoloading, autoinstallation, unsigned extensions, and community
  extensions disabled at engine creation.
- Explicit `allowed_directories`, empty `allowed_paths` and `allowed_configs`.
- HTTP/S3 filesystems disabled, `enable_external_access=false`, and disk spilling
  disabled with an empty `temp_directory`.
- `lock_configuration=true`, after all other settings are applied.

There is no user SQL during initialization. Every setting must succeed. DuckDB
requires attachments and the allowlist to be configured before disabling external
access. `SetParquetViews`, `SetCSVViews`, `SetViews`, and `SetAllViews` are the
only host-side catalog changes; they run after the lock on in-memory databases
only, cannot touch settings or file-backed databases, and are never exposed
through MCP.

Public queries are wrapped as SELECT subqueries whose columns are cast to VARCHAR,
then checked with DuckDB's native single-statement preparation API before execution. This is deliberately not a
keyword/regex SQL filter; hosts do not need a regex allowlist in front of
`ntduckbox`. Batches are rejected before any batch prefix executes. Commands
such as `LOAD`, `INSTALL`, `ATTACH`, `DETACH`, `COPY`, DDL (`CREATE`, `ALTER`,
`DROP`), DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`), `SET`, `RESET`, `PRAGMA`,
`CALL`, `EXPORT`, and `IMPORT` fail inside the subquery wrapper at `Prepare`
time (`ErrNotSelect`), and `query('...')` of a non-`SELECT` statement fails at
bind time (`ErrBinder`). File readers (`read_parquet`, `parquet_scan`,
`read_csv`, `read_blob`, `read_text`, `glob`) and single- or double-quoted file
paths in `FROM` (`FROM '/outside/x.parquet'`, `FROM "/outside/x.parquet"`)
outside `AllowedDirs` fail at bind time with `ErrPermission`. Each query also
runs in a read-only transaction, rolled back on completion, blocking mutating
expressions such as `nextval`. Existing tables and views, SELECT expressions,
and allowlisted local file readers remain available.

Within this threat model: no SQL writes, no extension loading, no user
configuration changes, no network reads, and no external file reads outside the
explicit allowlist and any attached database files. With no allowlist, queries
use only the opened or attached databases and built-in SQL functionality.
DuckDB treats each `Options.Attach` database file as an implicit readable path
even after `allowed_paths = []` and `enable_external_access = false`: queries
can discover its absolute path via `duckdb_databases()` and read the raw
database file bytes via `read_blob` (including unprojected tables or free-list
pages, in `MaxChars`-sized slices), while sibling files and writes in that
directory remain denied. Attach only database files whose full contents agents
may read, and vacuum or repack them beforehand if deleted data must not be
recoverable. All data within an allowed directory is likewise readable; there is
no per-table or per-file authorization layer. The engine also permits writes
inside an allowed directory: only the SELECT-only statement check stops
`COPY ... TO` from landing there. Allowlist directories that hold nothing but the
data agents should see, and that can tolerate being written to if that check were
ever bypassed. Query results are untrusted data and may contain prompt injection
or spreadsheet formulas.

This is an engine policy boundary, not an OS/process sandbox. It does not defend
against native-engine vulnerabilities, malicious database/Parquet files, host
filesystem mutation, or CPU/memory exhaustion. Output limits bound returned text,
not engine allocations or intermediate results. Timeouts use driver cancellation
and apply to execution after acquiring the connection (while connection queueing
is governed by the caller's context); native single-statement validation
currently uses a non-cancellable prepare call, so the timeout is not a hard
wall-clock guarantee during preparation or blocked native I/O. When those
guarantees are required, run the host process in a container or VM, give it a
read-only filesystem apart from the allowed directories, a user that owns nothing
else, no network, and CPU and memory limits. Keep DuckDB patched. See
[DuckDB security](https://duckdb.org/security) and
[configuration](https://duckdb.org/docs/stable/configuration/overview).

## Repository structure

The root module is the library. `db.go` handles lockdown and queries, `format.go`
encodes bounded results, and `mcp.go` registers the tools. `cmd/ntduckbox` contains
the STDIO server in the same module. `docs/` holds longer guides such as the
[Haybarn guide](docs/haybarn.md) and
[host-defined views design](docs/host-views.md). `scripts/package.sh` builds release archives;
`.github/workflows/ci.yml` runs checks and publishes tagged releases.

## Validation

Install [Task](https://taskfile.dev/), then run the same preflight used by CI:

```sh
task ci
task test -- -run TestLockdown -v
task run -- -db ./analytics.duckdb
```

`task ci` checks formatting, module tidiness and checksums, whitespace, uncached
race tests, vet, and native builds. `task fmt`, `task go-tidy`, `task test`,
`task test-race`, `task vet`, and `task build` are also available separately.
Go commands can be run directly with `GOWORK=off CGO_ENABLED=1`.

GitHub Actions runs the preflight on Linux amd64 and macOS arm64 with Go 1.26
and the current stable Go release, and repeats the race tests, vet, and build
against the Haybarn engine on stable Go. CGO requires building on each native runner;
this project does not claim pure-Go or WASM support. Dependabot checks Go modules
and Actions weekly.

Tests exercise both the public query boundary and the underlying engine locks,
file-backed and in-memory writes, allowed and denied file reads, traversal and
symlinks, Parquet, MCP errors, formatting limits, and cancellation recovery.

```sh
task fuzz                                   # two minutes per target, two workers
task fuzz FUZZTIME=30s                       # shorter local campaign
task fuzz FUZZTIME=30m FUZZ_TIMEOUT=35m      # longer campaign; the Go test timeout must exceed FUZZTIME
```

The fuzz targets check SQL query/describe isolation, unchanged security settings
and table contents after arbitrary input, CSV round trips, Markdown cell
boundaries, and all-or-error result limits. Seeds run with ordinary Go tests;
Linux/stable CI also runs ten seconds of fuzzing per target and uploads any
failing inputs as a workflow artifact. Retain failing inputs in `testdata/fuzz/`
so subsequent tests replay them. Go's coverage-guided
fuzzer instruments the Go boundary, not the native DuckDB engine. These runs
can expose bugs; they do not prove sandbox safety.

Repository development boundaries are in [AGENTS.md](AGENTS.md).

## Releases

A pushed stable `vX.Y.Z` tag triggers CI. After every matrix job succeeds, the
workflow publishes Linux amd64 and macOS arm64 CLI archives for both engines
(Haybarn archives carry a `_haybarn` suffix) plus `SHA256SUMS` to GitHub Releases. Only that job has repository write permission. Ordinary
branch builds and pull requests run checks without publishing.

Before tagging, move the relevant changelog entries into a dated version section
and run `task ci`. To inspect an archive locally without publishing:

```sh
task package VERSION=v0.0.0
```

Archives in `dist/` contain the native binary, README, changelog, license,
security policy, contributor guidance, Code of Conduct, and the guides in
`docs/`. Relative documentation links retain the repository layout.
The package task injects the tag into MCP server metadata; local builds report
`dev`. Hosted release publication requires GitHub Actions to be enabled on the
repository. The vanity module path must also resolve to this repository before
external users can install it through `go get`.

## Contributing

Contributions and feedback are welcome. Please follow our
[Code of Conduct](CODE_OF_CONDUCT.md), and report vulnerabilities as described in
[SECURITY.md](SECURITY.md) rather than in a public issue.

## License

Thanks to [DuckDB](https://duckdb.org/), the
[`duckdb-go`](https://github.com/duckdb/duckdb-go) driver,
[Haybarn](https://github.com/Query-farm-haybarn/haybarn) and its
[`haybarn-go`](https://github.com/Query-farm-haybarn/haybarn-go) driver, and
[`mcp-go`](https://github.com/mark3labs/mcp-go), and their authors.

[MIT License](./LICENSE) — Copyright (c) 2026 [Neomantra Corp](https://www.neomantra.com).

----
Made with :heart: and :fire: by the team behind [Nimble Terminal](https://nimbleterminal.com).
