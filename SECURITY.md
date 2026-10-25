# Security Policy

ntduckbox exists to let untrusted SQL run against DuckDB without writing data,
changing engine configuration, loading extensions, or reading files outside an
explicit allowlist. A way around any of those restrictions is a security
vulnerability in this project. The full boundary and its limits are described in
the [security considerations](README.md#security-considerations) section of the
README.

## Supported versions

Only the latest tagged release receives security fixes. Before 1.0, each minor
release may change the public API; upgrade to the newest release before
reporting. Development builds from `main` are not supported.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for this repository:

<https://github.com/NimbleTerminal/ntduckbox/security/advisories/new>

Include the ntduckbox version or commit, the operating system and architecture,
the `Options` used, and the exact SQL or MCP tool call that crosses the boundary.
A failing Go test is ideal. You should receive an acknowledgement within seven
days. Fixes are released as a new tag with a changelog entry, and the advisory is
published once a fixed release is available. Please allow up to 90 days before
disclosing publicly.

## In scope

- SQL that writes to a file-backed or in-memory database.
- SQL that changes any engine setting, or loads or installs an extension.
- Reading a file outside the configured `AllowedDirs`, including through
  traversal or symlinks, or any network access.
- Executing more than one statement, or executing any statement before
  validation rejects a batch.
- Returning output that exceeds the configured row or character limits, or
  returning partial output together with an error.
- MCP tool behavior that lets an agent override host-configured limits or
  settings.
- Unsafe defaults in `Open` or the `ntduckbox` command.

## Out of scope

- Vulnerabilities in the DuckDB engine or its Go driver. Report those upstream
  through [DuckDB security](https://duckdb.org/security), or to the
  [Haybarn project](https://github.com/Query-farm-haybarn/haybarn) for builds
  made with `-tags haybarn`. This project updates both pinned drivers through
  Dependabot and releases after upstream fixes.
- CPU, memory, or disk exhaustion. The output limits bound returned text, not
  engine work. Run the host in a container or VM with a read-only filesystem,
  no network, and resource limits for that.
- Malicious database, CSV, or Parquet files in trusted locations, and host
  misconfiguration such as allowlisting a sensitive directory. The engine can
  write inside an allowed directory; only the statement check prevents it, so
  allowlist only directories that can tolerate that.
- Prompt injection or spreadsheet formulas inside query results. Results are
  untrusted data by design.
- Exposing the underlying pool or loading extensions elsewhere in the host
  process, which the README explicitly forbids.
