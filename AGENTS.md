# Working on ntduckbox

ntduckbox is a restricted DuckDB query library with a small MCP tool surface.
Read README.md for the public contract and threat model.

## Structure and boundaries

- The root Go module is the library. `cmd/ntduckbox` is the STDIO demo/server
  in the same module; `scripts/package.sh` builds native release archives.
- DuckDB uses CGO. Preserve native builds with the pinned driver and bundled
  bindings; do not copy pure-Go or WASM build assumptions from sibling projects.
- `engine_duckdb.go` and `engine_haybarn.go` are the only files that import an
  engine driver, selected by the `haybarn` build tag. Keep them to aliases of the
  same API; everything else must compile and pass tests under both engines
  (`GOFLAGS=-tags=haybarn task ci`). Both drivers stay pinned in go.mod.
- `db.go` owns connection setup, query validation, and read-only transactions.
  Security settings must all succeed before Open returns. Configure allowed
  directories before disabling external access; lock configuration last.
- Keep the underlying pool private. No write-access option, arbitrary startup
  SQL, runtime extension loading, or user-controlled configuration changes.
  `SetParquetViews` is the one host-side catalog mutation: structured names and
  paths only, in-memory databases only, transactional, and bound by the engine
  under the locked settings. Do not extend it to accept SQL.
- Preserve native single-statement validation. The driver's PrepareContext can
  execute batch prefixes; it is not a safe replacement for Conn.Prepare here.
- Keep transaction rollback and bad-connection disposal on every exit path.
  In-memory databases require internal read-write mode, so their read-only
  transaction enforcement is part of the security boundary.
- `format.go` owns bounded CSV/Markdown output: all-or-error by default, or
  truncated at a whole row only when the caller opts in through `QueryOptions`
  and the `Result` reports it. Per-call limits never exceed the host limits
  from Open. Limits include headers and escaping. They do not bound engine allocations or intermediate data.
  Query values are cast to VARCHAR by DuckDB; keep its text rendering rather than
  reformatting typed values in Go. JSON formats likewise serialize in DuckDB with
  `to_json`, as one cell plus a row count; do not re-encode typed values in Go.
- `mcp.go` registers four tools on a host-owned server. Return tool errors through
  NewToolResultError; agents must not override host limits or security settings.
- Keep CLI logging on stderr using slog. Stdout belongs to the MCP transport.

## Validation

- `task ci` checks formatting, module tidiness and checksums, whitespace, uncached
  race tests, vet, and native builds. It requires Go 1.26+, Task, Git, a POSIX shell,
  and a CGO-capable C/C++ toolchain. No separate DuckDB install is needed.
- Use `task test -- -run TestLockdown -v` for focused security checks. Changes to
  SQL validation or settings need both public API and underlying engine tests.
- Preserve file-backed and memory coverage, denied and allowed reads, traversal
  and symlink rejection, configuration locks, output limits, and timeout recovery.
- `task fuzz` runs SQL isolation and CSV/Markdown property fuzzing for two minutes
  per target with two workers; override FUZZTIME, FUZZ_PARALLEL, and FUZZ_TIMEOUT
  (the Go test timeout, which must exceed FUZZTIME) as needed.
  Keep failing inputs under testdata/fuzz as regression cases. Ordinary tests run
  seed corpora; CI additionally runs a short fuzz campaign on Linux/stable Go.
  Go coverage guides the wrapper, not DuckDB's native CGO implementation. Input
  size and execution deadlines limit workloads, but native preparation is not
  cancellable; a hung worker can still reach the overall Go test timeout.
- Run `actionlint .github/workflows/ci.yml` and `sh -n scripts/package.sh` when
  changing automation. `task package VERSION=v0.0.0` tests packaging locally.
- GitHub Actions runs Go 1.26 and stable on Linux amd64 and macOS arm64, plus the
  Haybarn engine on stable. Report local checks separately from hosted CI; local
  success is not a hosted CI run.

## Documentation and releases

Keep README.md, CHANGELOG.md, and SECURITY.md aligned with public behavior and
security limits.
Use short dated changelog entries, marking unreleased work clearly and calling
out breaking API changes. Do not broaden the documented sandbox guarantees.

The Git tag is the release version. The package task injects it into the CLI's
MCP server metadata; ordinary development builds report `dev`. Do not hand-bump
that default. Pushing a stable `vX.Y.Z` tag runs all CI jobs, then publishes native
CLI archives and SHA256SUMS through GitHub Releases. Update the changelog before
an explicitly requested release. Editing documentation or workflows does not
itself authorize tagging or publishing. Only the release job has contents-write
permission; pull requests must retain read-only permissions.
