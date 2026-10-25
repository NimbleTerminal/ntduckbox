# Using ntduckbox with Haybarn

ntduckbox links one of two engines at build time: DuckDB, the default, or
[Haybarn](https://github.com/Query-farm-haybarn/haybarn). This page explains what
Haybarn is, when to choose it, and everything that differs when you do. The
README's [Engines](../README.md#engines) section is the short version.

## What Haybarn is

Haybarn is an independent derived distribution of DuckDB published by Query Farm
LLC. It is a hard fork of `duckdb/duckdb` maintained as a small, curated commit
stack on top of an upstream release tag, so the delta from DuckDB is auditable.
Both projects are MIT licensed.

What Haybarn changes:

- Branding and artifact names (`haybarn` CLI, `libhaybarn` libraries).
- The extension trust root and repository. Extensions are rebuilt and signed by
  Haybarn and served from `haybarn-extensions.query.farm`. DuckDB-signed
  extensions do not load on Haybarn, and vice versa.
- The extension cache directory (`~/.haybarn/extensions`).
- The release and distribution pipeline, with its own cadence.

What Haybarn keeps unchanged:

- The C API, the `duckdb::` namespace, public headers, and exported symbols.
- The database file format and the extension file format.
- SQL syntax, functions, and configuration setting names.

Haybarn is not affiliated with, sponsored by, or endorsed by the DuckDB
Foundation or DuckDB Labs. DuckDB is a trademark of the DuckDB Foundation.

## Why choose it

Reasons teams pick the Haybarn build of ntduckbox:

- **Consistency.** Other components already run on Haybarn, for example a
  browser front end on Haybarn-Wasm, and you want the same engine lineage and
  release cadence everywhere.
- **Supply chain.** Haybarn publishes GPG-signed releases with SLSA build
  provenance attestations and documents its full build pipeline.
- **Release timing.** Haybarn may ship a patch or hold a version independently
  of upstream.

Reasons to stay on the default build:

- You open database files written by the newest DuckDB. See
  [file compatibility](#database-file-compatibility) below.
- You want the smallest dependency surface and the most widely deployed driver.

Nothing about ntduckbox's sandbox differs between the two. The restrictions,
the public API, the MCP tools, and the test suite are identical, and CI runs the
suite against both engines.

## How the engine is selected

Exactly two files import an engine driver:

| File | Build constraint | Driver |
| --- | --- | --- |
| `engine_duckdb.go` | `!haybarn` (default) | `github.com/duckdb/duckdb-go/v2` |
| `engine_haybarn.go` | `haybarn` | `github.com/Query-farm-haybarn/haybarn-go/v2` |

The Haybarn driver is a fork of duckdb-go that keeps the Go package name
`duckdb` and the same API, so each file only aliases the connector, connection,
and statement types that `db.go` uses. The public constant `ntduckbox.Engine`
is `"duckdb"` or `"haybarn"`, and `SELECT version()` reports the engine version.

Both drivers are pinned in `go.mod` so either build is reproducible from the
same lock file. Only the selected driver and its native bindings are compiled
and linked; the other contributes nothing to the binary.

## Library consumers

Add the tag to your own build. Nothing in your code changes.

```sh
go build -tags haybarn ./...
go test -tags haybarn ./...
```

Or set it once for a shell or CI job:

```sh
export GOFLAGS=-tags=haybarn
```

To confirm which engine a binary carries:

```go
fmt.Println(ntduckbox.Engine) // "haybarn"
```

Requirements are the same as the default build: Go 1.26+, `CGO_ENABLED=1`, and a
C/C++ toolchain. The Haybarn bindings bundle static libraries for linux/amd64,
linux/arm64, darwin/amd64, darwin/arm64, and windows/amd64, the same platforms
as upstream. No Haybarn installation is needed.

`go mod download` fetches both drivers' module metadata. The per-platform native
library modules are only downloaded for the platform you build on.

## CLI users

Each release publishes two archives per platform. The Haybarn one carries a
`_haybarn` suffix:

```
ntduckbox_v0.1.0_linux_amd64.tar.gz
ntduckbox_v0.1.0_linux_amd64_haybarn.tar.gz
ntduckbox_v0.1.0_darwin_arm64.tar.gz
ntduckbox_v0.1.0_darwin_arm64_haybarn.tar.gz
SHA256SUMS
```

The binary logs its version and engine to stderr at startup:

```
INFO serving MCP over stdio version=v0.1.0 engine=haybarn
```

An MCP client configuration looks the same for either engine; only the binary
path differs:

```json
{
  "mcpServers": {
    "analytics": {
      "command": "/opt/ntduckbox/ntduckbox",
      "args": ["-db", "/srv/analytics/warehouse.duckdb", "-max-rows", "500"]
    }
  }
}
```

To build an archive yourself:

```sh
task package VERSION=v0.0.0 ENGINE=haybarn
```

## Versions

Both drivers encode the engine version in the second component of their module
version as `MAJOR MINOR PATCH` with two digits each for minor and patch.

| Driver | Pinned | Engine |
| --- | --- | --- |
| `github.com/duckdb/duckdb-go/v2` | `v2.20000.0` | DuckDB 2.0.0 |
| `github.com/Query-farm-haybarn/haybarn-go/v2` | `v2.10506.0` | Haybarn 1.5.6, built from DuckDB 1.5.6 |

Dependabot opens pull requests for both drivers weekly. To upgrade by hand:

```sh
go get github.com/Query-farm-haybarn/haybarn-go/v2@v2.10506.0
go mod tidy
GOFLAGS=-tags=haybarn task ci
```

Run the preflight under both engines after any upgrade. Prefer stable tags;
Haybarn also publishes release candidates such as `v2.20000.0-rc.5` for its
2.0 line, which this project does not pin until they are final.

## Database file compatibility

Each engine reads database files written by its own storage version or an
older one, never a newer one. Because the two pinned engines are a major version
apart, this matters:

| File written by | Opens under default build | Opens under Haybarn build |
| --- | --- | --- |
| DuckDB 2.0.0 (default build) | yes | no |
| Haybarn 1.5.6 or DuckDB 1.5.x | yes | yes |

Opening a 2.0.0 file with the Haybarn build fails at `Open` with an error like:

```
IO Error: Trying to read a database file with version number 69, but we can
only read versions between 64 and 68.
```

Options when that happens:

- Create or export the file with a 1.5.x engine, for example by running the
  preparation step with the Haybarn CLI or with the Haybarn build of a writer.
- Wait for the Haybarn 2.0 line and bump the pin.
- Use the default build for that database.

CSV and Parquet files are unaffected; both engines read them identically.

## Extensions and settings

ntduckbox disables extension loading, installation, autoloading, unsigned and
community extensions, and all external access before locking the configuration.
Haybarn's separate extension repository, trust root, and cache directory are
therefore irrelevant at runtime: no extension is ever fetched, verified, or
written. Built-in CSV and Parquet readers do not need extensions on either
engine.

Setting names and defaults are the same. The `TestLockedSettings` test asserts
every security setting under both engines.

## Security reporting

A way around ntduckbox's restrictions is a vulnerability in this project
regardless of engine; see [SECURITY.md](../SECURITY.md). Engine bugs belong
upstream: report DuckDB issues through [DuckDB security](https://duckdb.org/security)
and Haybarn issues to the [Haybarn project](https://github.com/Query-farm-haybarn/haybarn).

Note that ntduckbox's release archives link the engine statically through the
Go bindings module. Haybarn's own GPG signatures and provenance attestations
cover Haybarn's artifacts, not these archives. For ntduckbox, the engine's
integrity is established by Go module checksums in `go.sum` and the Go checksum
database, and the archives are covered by the published `SHA256SUMS`.
