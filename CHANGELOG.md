# CHANGELOG

## Unreleased

 * Include linked documentation in both engine release archives, preserving the `docs/` layout for offline reading.
 * Rename the project to ntduckbox. The module path is now `nimbleterminal.dev/ntduckbox`, the command is `ntduckbox`, and the repository is `NimbleTerminal/ntduckbox`.
 * Lockdown tests now assert the engine's Permission and locked-configuration error types instead of any error, so a mistyped payload cannot pass as a denial.
 * Test the file allowlist against `read_text`, `glob`, `sniff_csv`, `query()`, quoted and identifier file sources, quoted function names, sibling-directory prefixes, traversal, symlinks, and HTTP/S3 paths, through the public API and directly against the engine.
 * Document that the engine allows writes inside an allowed directory, blocked only by the statement check, and recommend running the host in a container or on a locked-down filesystem.

## v0.1.0 (2026-10-23)

 * Add the `haybarn` build tag to link Haybarn, an independent derived distribution of DuckDB, through `haybarn-go`; expose `Engine`; package `_haybarn` CLI archives and test both engines in CI.
 * Add SECURITY.md with the disclosure policy, supported versions, and the scope of the sandbox boundary.
 * Cast query values to VARCHAR in DuckDB so dates, timestamps, intervals, lists, structs, and blobs use its text rendering instead of Go formatting.
 * Ignore repeated trailing semicolons and whitespace after a query.
 * Pin GitHub Actions to commit SHAs, upload CI fuzz failures as artifacts, check committed files for whitespace errors, and add a FUZZ_TIMEOUT override.
 * Add SQL-isolation and CSV/Markdown property fuzzing, a Task runner, and CI fuzz smoke tests.
 * Preserve single-column empty strings in CSV output instead of emitting blank lines that readers skip.
 * Add locked-down file-backed and in-memory DuckDB queries with explicit local directory access, immutable configuration, and read-only transactions.
 * Add query, list_tables, list_views, and describe MCP tools, bounded CSV/Markdown output, and a STDIO server.
 * Test engine lockdown, query restrictions, filesystem access, output limits, MCP errors, and cancellation recovery.
 * Add Task-based validation, Linux/macOS CI on Go 1.26 and stable, tagged CLI releases with checksums, and weekly dependency updates.
