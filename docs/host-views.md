# Host-Defined Views and Read-Only Attachments

This note records the design for combining read-only file-backed DuckDB
catalogs, typed CSV views, and host-authored SQL views in `ntduckbox`.

## Requirements (`dank-bot420` use case)

A host application such as `dank-bot420` needs to run untrusted, LLM-authored
SQL against:

1. **R1 — Host-defined relations:** a relation `inventory` backed by a CSV file
   with an ordered, type-pinned column schema (and header check), plus a view
   `inventory_brands` defined by a 174-line `WITH ... SELECT` reconciliation
   query embedded in the host binary.
2. **R2 — Read-only base database:** an existing ~19 MB file-backed `.duckdb`
   (`ct_brands`, `ct_retail_locations`, ...) opened read-only, which
   `inventory_brands` and user queries join against (`FROM ct_brands`).
3. **R3 — Unchanged untrusted SQL boundary:** every query from the agent or MCP
   tools still goes through `ntduckbox`'s locked configuration, `Conn.Prepare`
   single-statement `SELECT` check, and `BEGIN TRANSACTION READ ONLY`.
4. **R4 — Runtime CSV reload:** `inventory` is optional and may be loaded,
   replaced, or cleared at runtime without restarting the process, copying the
   19 MB base file, or reopening `*DB`.
5. **R5 — Native CGO builds on both engines:** identical behavior under the
   default DuckDB build and `-tags haybarn`.

## Why "D + B for `inventory`, with A for `inventory_brands`" does not work

An initial hypothesis was to combine **D** (`ATTACH` at `Open`) and **B**
(structured CSV views) for `inventory`, while using **A** (a pre-`Open`
preparation helper on a `.duckdb` file) for `inventory_brands`. DuckDB's catalog
and view-binding mechanics rule that combination out:

1. **Eager view binding at `CREATE VIEW` time.** DuckDB binds every relation
   referenced by a view when `CREATE VIEW` runs. A preparation step (`A`) cannot
   create `inventory_brands` before `inventory` exists, and `inventory` is
   optional and only created at runtime in `memory.main` (`R4`).
2. **Catalog scoping of unqualified identifiers.** Even if `A` created a stub
   `inventory` table inside the file-backed database so `CREATE VIEW` could
   bind, a view stored in an attached catalog `ct` resolves unqualified table
   references (`FROM inventory`, `FROM ct_brands`) against its own catalog
   (`ct.main.inventory`), not `memory.main.inventory`. Replacing
   `memory.main.inventory` at runtime via `B` would never affect what
   `ct.main.inventory_brands` reads.
3. **`ATTACH` is session-scoped.** DuckDB does not persist `ATTACH` statements
   inside a `.duckdb` file across close and reopen. A separate derived `.duckdb`
   file produced by `A` would either have to copy the 19 MB base tables on every
   inventory reload or reopen `*DB` (invalidating long-lived consumers such as
   `RegisterTools` on an MCP server).
4. **Cross-engine file format compatibility.** The default build links DuckDB
   2.0.0 (storage version 69) while `-tags haybarn` links Haybarn 1.5.6 (storage
   versions 64–68). Writing a `.duckdb` file in `A` under the default build
   produces a file that the Haybarn build cannot open, whereas attaching an
   existing 1.5.x base file read-only (`D`) works under both engines.
5. **Broader host privilege surface.** A `Prepare(path, func(ctx, *sql.DB) error)`
   helper would hand the callback an unlocked, read-write `*sql.DB` before
   `allowed_directories`, `enable_external_access=false`, or
   `lock_configuration=true` are applied.

## Chosen mechanism: D (`Options.Attach`) + B (`SetCSVViews`) + C (`SetViews`)

### D. Read-only attachments at `Open` (`Options.Attach`)

```go
type Attachment struct {
    Path  string // Existing database file.
    Alias string // Catalog name in SQL; must not be empty or reserved.
}
```

- When `Options.Path` is `""` or `":memory:"`, the primary catalog (`memory.main`)
  stays in-memory so host views can be created and replaced at runtime.
- During `Open`, before `SET enable_external_access = false` and
  `SET lock_configuration = true`, each entry in `Options.Attach` is canonicalized,
  verified to be an existing non-directory file, and attached with:
  `ATTACH '<abs_path>' AS "<alias>" (READ_ONLY)`.
- There is deliberately **no `ReadOnly bool` field** on `Attachment`: attached
  databases are always read-only, preserving `ntduckbox`'s invariant that there
  is no write-access option.
- Because `ATTACH` runs before `enable_external_access = false`, the directory
  containing the `.duckdb` file does **not** need to be added to `AllowedDirs`.
  Untrusted SQL can query the attached catalog's tables, and cannot read sibling
  files (such as secrets, configs, or other `.duckdb` or `.csv` files) or write
  in that directory. Note that DuckDB treats the attached `.duckdb` file itself
  as an implicit readable path even after `allowed_paths = []` and
  `enable_external_access = false`: queries can see its path in
  `duckdb_databases()` and read its raw file bytes in `MaxChars`-sized slices
  via `read_blob` (including unprojected tables or free-list pages). Only attach
  database files whose full contents agents are allowed to read.
- `Open` sets `search_path` to `main,"<alias>".main,...` (and restores it if a
  pooled connection is ever recycled after a failed rollback). Both qualified
  (`ct.ct_brands`) and unqualified (`ct_brands`) references resolve without
  rewriting host view SQL or agent queries, with `memory.main` taking precedence.

### B. Structured CSV views with ordered column pinning (`SetCSVViews`)

```go
type Column struct {
    Name string // Column name in the relation or CSV header.
    Type string // DuckDB column type, for example "VARCHAR" or "DOUBLE".
}

type CSVColumn = Column

type CSVView struct {
    Path    string   // CSV file path or glob inside AllowedDirs.
    Columns []Column // Optional ordered column names and types.
}
```

- `Columns` is an ordered slice (`[]Column`, aliased as `[]CSVColumn`) rather
  than a `map[string]string` because DuckDB's
  `read_csv(..., columns = {'col': 'TYPE', ...})` struct is positional: sorting
  map keys alphabetically would misalign columns against the CSV file order.
- When `Columns` is non-empty, `SetCSVViews` first queries
  `SELECT * FROM read_csv('<path>', header = true, all_varchar = true) LIMIT 0`
  through the locked engine connection and verifies that the file's header names
  match `Columns` in exact order. All file access goes through DuckDB under
  `allowed_directories`, never `os.Open` in Go.
- It then executes
  `CREATE OR REPLACE VIEW main."<name>" AS SELECT * FROM read_csv('<path>', header = true, columns = {'<name>': '<type>', ...})`
  inside a transaction, rolling back on any permission, header, or type error.

### C. Host-defined SQL views after lockdown (`SetViews` and `SetAllViews`)

```go
func (db *DB) SetViews(ctx context.Context, views map[string]string) error
func (db *DB) SetAllViews(ctx context.Context, set ViewSet) error
```

- Accepts `SELECT` / `WITH ... SELECT` query bodies (without `CREATE VIEW`) from
  Go code only; never exposed through MCP tools or CLI flags.
- Runs after `Open` has locked all security settings.
- Strips trailing semicolons, wraps each body as
  `SELECT * FROM (\n<body>\n) AS ntduckbox_view`, and validates it with
  `Conn.Prepare` and `stmt.StatementType() == STATEMENT_TYPE_SELECT` before
  running `CREATE OR REPLACE VIEW main."<name>" AS SELECT * FROM (\n<body>\n) AS ntduckbox_view`
  inside the transaction. Views in one map are created in dependency passes so
  acyclic views may reference one another regardless of map key order; cycles
  make no progress and roll back.
- `SetParquetViews`, `SetCSVViews`, and `SetViews` each track their own managed
  view names and reject cross-set name collisions. Reloading `inventory` via
  `SetCSVViews` executes `CREATE OR REPLACE VIEW main."inventory"` in place;
  `inventory_brands` (managed by `SetViews`) is re-validated before commit and
  immediately queries the updated CSV without reopening the attached `.duckdb`
  file. Removing or altering a Parquet or CSV view via a standalone call in a
  way that invalidates remaining `SetViews` views enumerates every broken SQL
  view in the returned error (`errors.Join`) and rolls back.
- When a host layers derived SQL views on top of Parquet or CSV views and needs
  to add, update, or drop base and derived views together (for example after a
  cache clear), `SetAllViews(ctx, ViewSet{Parquet, CSV, SQL})` replaces all
  three sets in a single transaction, creating base file views before SQL views.
- A Parquet glob that matches zero files fails at `SetParquetViews` /
  `SetAllViews` time (`No files found that match the pattern`) and rolls back.
  If all matching files are removed from disk after the view is created, the
  view remains in the catalog (`Views(ctx)` still returns it) and queries
  against it fail until matching files exist again or the view is removed.

## Common host view patterns (`dbn-go-mcp-data` use case)

### Small host-provided metadata tables (`VALUES`-backed views)

For small, host-owned metadata tables (such as a cache manifest mapping
`(dataset, schema, start, end, row_count, bytes)` without writing a sidecar
Parquet or CSV file), define a `VALUES`-backed view in `SetViews` or
`ViewSet.SQL`:

```sql
SELECT * FROM (
    VALUES
        ('XNAS.ITCH', 'trades', 130000, 4194304),
        ('EQUS.MINI', 'ohlcv-1m', 87000, 2097152)
) AS t(dataset, schema_name, row_count, byte_size)
```

Guidance on scale: `VALUES`-backed views are parsed and planned as part of the
view definition, so they work well from a few rows up to a few hundred rows. For
larger reference datasets (thousands of instrument definitions, trading
calendars, or symbol mappings), write a Parquet or CSV file inside `AllowedDirs`
or ship a read-only `.duckdb` file attached via `Options.Attach`. Attached
catalogs participate in `search_path` and in `SetViews` / `SetAllViews`
dependency validation just like file views in `main`.

### Nanosecond timestamps (`TIMESTAMPTZ_NS` / `TIMESTAMP_NS`) and sentinel cleanup

Databento and Arrow Parquet files often store nanosecond timestamps as
`TIMESTAMPTZ_NS` (DuckDB 2.0+) or `TIMESTAMP_NS` (both DuckDB 2.0+ and Haybarn
1.5.x) and encode missing prices or timestamps as integer sentinels (such as
`i64::MAX` or `-1` nanoseconds before the Unix epoch):

- **Rendering:** `TIMESTAMPTZ_NS` survives `COLUMNS(*)::VARCHAR` (`CSV` and
  `Markdown`, e.g. `2023-11-14 22:13:20.123456789+00` under default `UTC`) and
  `to_json` (`JSONRows` and `JSONColumnar`, formatted as RFC 3339
  `2023-11-14T22:13:20.123456789Z`) cleanly, preserving all 9 fractional
  nanosecond digits.
- **Comparison and function friction:** DuckDB rejects direct comparisons
  between `TIMESTAMPTZ_NS` and standard microsecond `TIMESTAMPTZ` literals with
  a `Binder Error` (`Cannot compare values of type TIMESTAMPTZ_NS and type
  TIMESTAMP WITH TIME ZONE - an explicit cast is required`).
- **Recommended `SetViews` wrapper:** Expose a cleaned view using DuckDB's
  `SELECT * REPLACE (...)` to normalize `TIMESTAMPTZ_NS` columns to
  `TIMESTAMPTZ` and turn sentinel values into `NULL`:

```sql
SELECT * REPLACE (
    CASE
        WHEN ts_event < TIMESTAMPTZ_NS '1970-01-01 00:00:00Z' THEN NULL
        ELSE ts_event::TIMESTAMPTZ
    END AS ts_event,
    CASE WHEN ask_px > 1e9 THEN NULL ELSE ask_px END AS ask_px
) FROM "EQUS.MINI/bbo-1m"
```

Compare `-1 ns` sentinels against `TIMESTAMPTZ_NS '1970-01-01 00:00:00Z'`
**before** casting to `TIMESTAMPTZ`: casting `-1 ns` to microsecond
`TIMESTAMPTZ` truncates toward zero (`1970-01-01 00:00:00Z`), which would hide
the `-1 ns` sentinel.

### Structured catalog introspection (`Columns` and `Views`)

To build tool descriptions or system prompts after refreshing views without
parsing CSV/Markdown/JSON output from `DescribeTable` or `ListViews`:

- `db.Views(ctx)` returns `[]ViewInfo{Name, Kind, Source}` in ascending name
  order (`ParquetViewKind`, `CSVViewKind`, `SQLViewKind`).
- `db.Columns(ctx, "EQUS.MINI/bbo-1m")` (or `db.Columns(ctx, "ct", "ct_brands")`)
  returns `[]Column{Name, Type}` in column order.

## Threat-model argument and `AGENTS.md` amendment

### Threat model

As documented in `README.md` and `SECURITY.md`, the untrusted input is SQL
supplied to query/describe methods or MCP tools; the Go host application is
trusted. Even so, a host-side catalog API must not weaken engine lockdown or
expose a footgun that executes unvalidated batches or bypasses `AllowedDirs`.

`SetViews` preserves the sandbox boundary because:

1. **Post-lock execution:** `SetViews` runs on the locked pool after
   `allowed_directories`, `allowed_paths = []`, `allowed_configs = []`,
   `disabled_filesystems`, `enable_external_access = false`, and
   `lock_configuration = true` are active.
2. **Single-statement `SELECT` preparation:** `Conn.Prepare` on
   `SELECT * FROM (\n<body>\n) AS ntduckbox_view` rejects multi-statement
   batches before any prefix executes and rejects DDL, DML, `COPY`, `ATTACH`,
   `LOAD`, `INSTALL`, `SET`, `PRAGMA`, and `query()` of a non-`SELECT`
   statement (`query('SELECT ...')` is a normal `SELECT` and executes under the
   same locked filesystem settings and read-only transaction).
3. **No privilege elevation (`SECURITY INVOKER` semantics):** DuckDB views do
   not carry elevated permissions. Binding a view at `CREATE VIEW` time and
   querying it at `Query` time both run under the locked `allowed_directories`
   and `READ_ONLY` attachment settings, and every public query runs inside
   `BEGIN TRANSACTION READ ONLY` (blocking sequence mutations such as `nextval`
   even if referenced in a view).
4. **Proving untrusted SQL cannot reach unexposed resources:**
   `TestAttachAndIsolation`, `TestSetCSVViews`, and
   `TestSetViewsAndDankBotWorkflow` verify via both the public API and direct
   `db.pool.Exec` assertions that:
   - Untrusted SQL can read `ct_brands` (and raw bytes of the attached `.duckdb`
     file itself, which DuckDB exempts from `enable_external_access = false`)
     while sibling files (`secret.csv`, `other.duckdb`) and writes in the same
     un-allowlisted directory fail with `engineErrorPermission` (`Cannot access`).
   - Writes to attached databases fail at the engine level with
     `engineErrorInvalidInput` (`read-only mode`).
   - Paths outside `AllowedDirs`, `..` traversals, and symlinks in `SetCSVViews`
     and `SetViews` fail with `engineErrorPermission` and roll back the
     transaction, leaving existing views untouched.
   - Untrusted SQL cannot `DROP VIEW`, `CREATE OR REPLACE VIEW`, `DETACH`, or
     alter `search_path` or locked settings.

### `AGENTS.md` rule update

- **Upheld:** Keep the underlying pool private; no write-access option,
  arbitrary startup SQL, runtime extension loading, or user-controlled
  configuration changes. `SetParquetViews` and `SetCSVViews` take structured
  names, paths, and column definitions only — never SQL.
- **Amended:** `Options.Attach` attaches existing database files read-only
  before lockdown, and `SetViews` is permitted as a separate Go-only host
  catalog mutation on in-memory databases because every view body is wrapped as
  a subquery, validated with `Conn.Prepare` (`STATEMENT_TYPE_SELECT`), and
  bound transactionally under the locked settings.

## Example usage in `dank-bot420`

```go
db, err := ntduckbox.Open(ntduckbox.Options{
    Attach:       []ntduckbox.Attachment{{Alias: "ct", Path: dbPath}},
    AllowedDirs:  []string{filepath.Dir(inventoryCSVPath)},
    MaxRows:      1000,
    MaxChars:     100_000,
    QueryTimeout: 30 * time.Second,
})
if err != nil {
    return err
}
defer db.Close()

// Load or reload inventory at runtime without reopening dank-mcp.duckdb:
if err := db.SetCSVViews(ctx, map[string]ntduckbox.CSVView{
    "inventory": {Path: inventoryCSVPath, Columns: inventoryColumns},
}); err != nil {
    return err
}
if err := db.SetViews(ctx, map[string]string{
    "inventory_brands": inventoryBrandsSelectSQL,
}); err != nil {
    return err
}
```
