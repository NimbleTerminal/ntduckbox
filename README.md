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
serializes queries through one connection, and waiting counts toward the timeout.
Pass a context to cancel a request earlier than the host-configured deadline.

Zero limits select the defaults shown above; negatives are errors. CSV is the
default format. `QueryFormat(ctx, sql, ntduckbox.Markdown)` selects Markdown for
one call. `Describe(ctx, sql)` returns a query schema;
`DescribeTable(ctx, "main", "events")` quotes each identifier component safely.

Both formats include headers. DuckDB casts every value to VARCHAR before
encoding, so dates, timestamps, intervals, lists, structs, and blobs use DuckDB's
text rendering rather than Go's. SQL NULL is rendered as `NULL` (indistinguishable
from that literal string), and a lone empty string is written as a quoted `""` in
CSV so readers keep the row. MaxChars counts UTF-8 bytes, including headers and
escaping. Results exceeding either limit return `ErrResultLimit` with no partial
output. Add a SQL `LIMIT` or select fewer columns. Trailing semicolons are ignored.

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
| `query` | Required `sql`; optional `format`: `csv` or `markdown` |
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
For named views, create them in a trusted preparation step in a persistent
database, close that writer, then open with ntduckbox. Runtime view creation is
intentionally absent from the public API.

## Security considerations

The untrusted input is SQL supplied through this library or its MCP tools. The
host application, database files, allowlisted directories, DuckDB engine and
native dependencies must be trusted. Do not expose underlying DuckDB connections
or load extensions elsewhere in the process. Do not share an open database with
another driver instance that changes its settings; DuckDB can cache database
instances in-process. Conflicting configuration causes Open to fail closed.

Before returning a DB, Open configures:

- File-backed `access_mode=read_only`.
- Extension autoloading, autoinstallation, unsigned extensions, and community
  extensions disabled at engine creation.
- Explicit `allowed_directories`, empty `allowed_paths` and `allowed_configs`.
- HTTP/S3 filesystems disabled, `enable_external_access=false`, and disk spilling
  disabled with an empty `temp_directory`.
- `lock_configuration=true`, after all other settings are applied.

There is no user SQL during initialization. Every setting must succeed. DuckDB
requires the allowlist to be configured before disabling external access.

Public queries are wrapped as SELECT subqueries whose columns are cast to VARCHAR,
then checked with DuckDB's native single-statement preparation API before execution. This is deliberately not a
keyword/regex SQL filter. Batches are rejected before any batch prefix executes.
Commands such as LOAD, INSTALL, ATTACH, COPY, DDL, DML, SET, and PRAGMA cannot pass
this boundary. Each query also runs in a read-only transaction, rolled back on
completion, blocking mutating expressions such as `nextval`. Existing tables and
views, SELECT expressions, and allowlisted local file readers remain available.

Within this threat model: no SQL writes, no extension loading, no user
configuration changes, no network reads, and no external file reads outside the
explicit allowlist. With no allowlist, queries use only the opened database and
built-in SQL functionality. All data within an allowed directory is readable;
there is no per-table or per-file authorization layer. The engine also permits
writes inside an allowed directory: only the SELECT-only statement check stops
`COPY ... TO` from landing there. Allowlist directories that hold nothing but the
data agents should see, and that can tolerate being written to if that check were
ever bypassed. Query results are untrusted data and may contain prompt injection
or spreadsheet formulas.

This is an engine policy boundary, not an OS/process sandbox. It does not defend
against native-engine vulnerabilities, malicious database/Parquet files, host
filesystem mutation, or CPU/memory exhaustion. Output limits bound returned text,
not engine allocations or intermediate results. Timeouts use driver cancellation
and include connection queueing and execution; native single-statement validation
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
[Haybarn guide](docs/haybarn.md). `scripts/package.sh` builds release archives;
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
