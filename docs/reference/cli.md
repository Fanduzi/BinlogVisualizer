# CLI Reference

This document defines the user-facing contract for the `binlogviz` root command, `binlogviz analyze`, `binlogviz flashback`, `binlogviz compare`, `binlogviz trend`, `binlogviz snapshot`, `binlogviz workflow run`, `binlogviz workflow resume`, `binlogviz workflow status`, `binlogviz workflow clean`, `binlogviz workflow export`, `binlogviz workflow validate`, and `binlogviz workflow describe`.

If you want the fastest operator path instead of the full contract, start with [Quickstart](../recipe/quickstart.md) or [Analyze Local Binlogs](../recipe/analyze-local-binlogs.md).

## Command Syntax

```bash
binlogviz --version
binlogviz --lang zh-CN analyze <binlog files...>
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
binlogviz analyze --from-dir DIR --prefix PREFIX --format json --snapshot-name NAME
binlogviz flashback <binlog files...>
binlogviz flashback --from-dir DIR --prefix PREFIX
binlogviz compare <current.json> <baseline.json>
binlogviz compare --current-snapshot CURRENT --baseline-snapshot BASELINE
binlogviz trend <snapshot...>
binlogviz trend --from-snapshots 'incident-*'
binlogviz snapshot save <report.json> --name NAME
binlogviz snapshot list
binlogviz snapshot show <name>
binlogviz workflow run <plan.yaml>
binlogviz workflow run <plan.yaml> --output-dir ./artifacts
binlogviz workflow resume <output_dir>
binlogviz workflow resume <output_dir> --rerun analyze:week2
binlogviz workflow status <output_dir>
binlogviz workflow status <output_dir> --format json
binlogviz workflow clean <output_dir>
binlogviz workflow clean <output_dir> --format json
binlogviz workflow clean <output_dir> --apply --include-snapshots
binlogviz workflow export <output_dir>
binlogviz workflow export <output_dir> --output ./incident.zip
binlogviz workflow export <output_dir> -o ./incident.zip
binlogviz workflow export <output_dir> --include-snapshots --format json
binlogviz workflow validate <plan.yaml>
binlogviz workflow validate <plan.yaml> --format json
binlogviz workflow describe <plan.yaml>
binlogviz workflow describe <plan.yaml> --format json
```

## Global Flags

These flags are available on the root command and apply before subcommand execution:

| Flag | Default | Description |
|------|---------|-------------|
| `--lang` | env-detected | Runtime output language, for example `en` or `zh-CN`. |
| `--version`, `-v` | `false` | Print the version string and exit. |

## `analyze` Command Syntax

`analyze` accepts exactly one input mode per invocation:

- **Positional file mode**: pass one or more local binlog file paths as positional arguments. `-` reads one binary binlog from stdin.
- **Discovery mode**: pass `--from-dir` and `--prefix` together so BinlogViz resolves matching files from a directory.

Default text and JSON reports name, on each transaction, `server_id`, `thread_id`, GTID, and `xid` or XA xid when the binlog events contain them. `user@host` appears only when the binlog stored an invoker. A field the events do not have is left out; the report does not fill in `0` or an empty string. Markdown and HTML follow the same rule.

## Input Rules

### Positional file mode

Use positional arguments when you already know the exact files to analyze.

```bash
binlogviz analyze mysql-bin.000123
binlogviz analyze mysql-bin.000123 mysql-bin.000124
cat mysql-bin.000123 | binlogviz analyze -
```

`-` reads one binary binlog from a pipe or other non-seekable input. Parsing needs seek, so the bytes are copied to a temporary file and deleted when the command finishes. `binlogviz analyze -` on a terminal, or an empty pipe, fails with `Error:` before parsing. A named pipe path is treated the same way. `mysqlbinlog` text output is not a binlog and fails the magic-header check.

### Discovery mode

Use discovery mode when you want BinlogViz to resolve an ordered file set for you.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### Positional vs discovery behavior

- Positional file arguments and discovery flags are mutually exclusive.
- `--from-dir` and `--prefix` must be provided together.
- If neither positional files nor a complete discovery pair is provided, the command fails.

For the exact discovery matching, ordering, resolved-file reporting, and invalid-combination contract, see [Input Discovery Reference](input-discovery.md).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | none | Start time, inclusive. RFC3339, or `YYYY-MM-DD HH:MM:SS` in the local timezone of the machine running `binlogviz`. Explicit offsets define the instant; prefer RFC3339 with offset across machines. |
| `--end` | none | End time, inclusive. Same formats as `--start`. |
| `--start-position` | none | Exact inclusive event boundary on one explicit binlog file. |
| `--stop-position` | none | Exact exclusive event boundary, or EOF, on one explicit binlog file. |
| `--include-gtids` | none | Include complete groups matching a MySQL UUID range set or exact MariaDB identities. |
| `--exclude-gtids` | none | Exclude matching complete groups after include; exclusion wins. |
| `--from-dir` | none | Discover binlog files from this directory. Must be used with `--prefix`. |
| `--prefix` | none | Filename prefix used with `--from-dir`. Must be used with `--from-dir`. Optional `.` before the numeric suffix; a complete filename selects that file; for one file, pass the path. |
| `--format` | `text` | Report output format: `text`, `json`, `markdown` (alias `md`), or `html`. |
| `--output`, `-o` | auto | HTML output file path. Only supported with `--format html`. Default: derived cwd file on a TTY; stdout when stdout is redirected. Use `-` to force stdout. |
| `--snapshot-name` | none | Save the JSON analyze output as `<name>.json`. Requires `--format json`. |
| `--snapshot-dir` | home-based default | Directory used when saving a snapshot. Default: `~/.binlogviz/snapshots`. |
| `--workload-id` | none | Explicit workload identity persisted in report v3. Use the same non-empty token only for snapshots of the same workload. |
| `--sql-context` | `summary` | Query text in every format, including default text, DDL statements, and `--show-patterns`: `summary` (one bounded line), `off` (omit query text and `--show-rows` cell values), or `full` (stored SQL, including DDL statement text, capped at 4096 bytes and marked when cut). MySQL and MariaDB auth-DDL credential literals are redacted in every mode. |
| `--top-tables` | `10` | Number of top tables to display in human-readable reports; JSON retains all table aggregates. |
| `--top-transactions` | `10` | Number of top transactions to include in the report; `0` is unlimited. |
| `--top-threads` | inherits `--top` | Number of top threads or sessions in every format; `0` is unlimited. |
| `--top-rows` | inherits `--top` | Number of hot primary keys in every format; `0` is unlimited. |
| `--top` | `10` | Default number of ranked items, including Top Threads, Hot Rows, and text detail sections (minutes, patterns). |
| `--details` | `false` | Show minute details and write-shape patterns in the text report. |
| `--show-minutes` | `false` | Show minute-level activity in the text report. |
| `--show-patterns` | `false` | Show write-shape patterns in the text report. |
| `--detail-store` | `none` | Optional transaction detail backend: `none` or `duckdb`. |
| `--detect-spikes` | `false` | Enable write spike detection. |
| `--large-trx-rows` | `1000` | Row threshold for large transaction alerts. |
| `--large-trx-duration` | `30s` | Duration threshold for large transaction alerts. |
| `--top-minutes` | `60` | Number of top active minutes to include in the report. |
| `--spike-window` | `5` | Rolling baseline window in minutes for spike detection. |
| `--spike-factor` | `3.0` | Multiplier over baseline to trigger a spike alert. |
| `--spike-min-rows` | `100` | Minimum row count for a minute to be considered a spike candidate. |
| `--include-schema` | none | Comma-separated list of schemas to analyze (all others excluded). |
| `--exclude-schema` | none | Comma-separated list of schemas to skip. |
| `--include-table` | none | Comma-separated objects to analyze (all others excluded). `TABLE` or `SCHEMA.TABLE`, including VIEW, EVENT, FUNCTION, PROCEDURE, and TRIGGER names. |
| `--exclude-table` | none | Comma-separated objects to skip. `TABLE` or `SCHEMA.TABLE`, including VIEW, EVENT, FUNCTION, PROCEDURE, and TRIGGER names. |
| `--dml` | none | Comma-separated ROW kinds to count: `insert`, `update`, `delete`. Combinable. Composes with schema, table, time, position, and GTID filters. A kind filter that matches nothing exits 2: `Error: dml filter matched no events`. |
| `--show-rows` | `false` | Print bounded row values for listed transactions (DELETE before-image, UPDATE changed columns, INSERT after-image). Column names require `binlog_row_metadata=FULL`; otherwise columns are `@1`..`@N`. `--sql-context off` omits the values. |

When the binlog is MySQL 8 with `binlog_row_metadata=FULL`, TABLE_MAP optional metadata names each table's primary key (`SIMPLE_PRIMARY_KEY` or `PRIMARY_KEY_WITH_PREFIX`) or records that the table has none. Every format then lists tables that have no primary key and received UPDATE or DELETE rows, ranked by those row counts. A replica applying those rows can scan the table. INSERT-only tables without a primary key are named and are not ranked as that lag risk. One UPDATE or DELETE row is enough to raise a `no_primary_key` warning next to the large-transaction and spike alerts; there is no separate threshold flag. Without FULL metadata the report does not guess: each such table is `unknown`, and the report says primary key presence is unknown because `binlog_row_metadata` is not FULL. `--include-table`, schema filters, `--dml`, and time, position, and GTID selectors apply to this section the same way they apply to Top Tables. The analyze report does not generate undo SQL. `binlogviz flashback` does.

Hot Rows ranks individual primary keys by how many UPDATE and DELETE row images touched them. Each entry has the touch count, the number of distinct transactions, the first and last event time, and the GTID plus file:byte of the first and last transaction (the transaction start, so `mysqlbinlog` or BinlogServer can open that group). Identity comes only from MySQL 8 `binlog_row_metadata=FULL` (`SIMPLE_PRIMARY_KEY` or `PRIMARY_KEY_WITH_PREFIX`, using the named columns, not column `@1`). When the key is not in the binlog, that table is not ranked and the report says hot-row tracking is unavailable because `binlog_row_metadata` is not FULL. A table with no primary key is not ranked in this section. `--top` limits the list; `--top-rows` overrides it; `0` keeps every tracked key. `--sql-context off` hides the key values and keeps the counts. The same filters as Top Tables apply. Tracking keeps at most 8192 primary keys. When a new key arrives after that, it replaces the least-touched key and its touch count starts at the dropped count plus one; the report says the cap was hit and marks that row approximate. A key that was never replaced stays exact. JSON fields: `hot_rows`, `hot_rows_listed`, `hot_rows_omitted`, `hot_row_track_limit`, `hot_rows_overflow`, `hot_rows_note`, `hot_row_unavailable`.

Position selectors reject discovery and multiple explicit files, reversed/out-of-range/mid-event values, and use `[start, stop)` semantics. Position and time predicates intersect. GTID selectors operate after complete group reconstruction across ordered rotations; anonymous groups match no active selector, including exclude-only selectors. Standalone anonymous DDL and unkeyed context are discarded without preventing a later matching keyed group from being retained. Mixed/conflicting/unresolved flavors fail, and a valid selection with no retained events exits 2 without a report.

The DDL Timeline names each DDL's transaction when the binlog has a GTID (MySQL `uuid:seq` or MariaDB `domain-server-seq`), the file byte where that transaction starts (the GTID event's start, not the Query event and not `end_log_pos`), and two copyable values: `mysqlbinlog --stop-position=<N> <file>` and BinlogServer `stop_gtid=<gtid>`. Both replay earlier events and exclude that DDL. No GTID (`GTID_MODE=OFF` or anonymous) omits the GTID and says it is unavailable; the position hint remains. `--sql-context off` still omits only the statement. Exit codes are unchanged.

### Snapshot-saving behavior

`analyze` can optionally persist the exact JSON payload it writes to `stdout`.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --workload-id orders-production \
  --format json \
  --snapshot-name incident_current
```

Rules:

- `--snapshot-name` requires `--format json`
- the snapshot name must be a single file stem containing only letters, digits, `-`, or `_`
- `--snapshot-dir` overrides the default home-based snapshot store
- when `--snapshot-name` is present, the report still goes to `stdout`
- the save confirmation is written to `stderr`

If `--snapshot-dir` is omitted, BinlogViz saves to `~/.binlogviz/snapshots/<name>.json`.

Detail flags for the text report:

```bash
binlogviz analyze mysql-bin.000123 --details --show-minutes --show-patterns
```

## `flashback` Command Syntax

```bash
binlogviz flashback <binlog files...>
binlogviz flashback --from-dir DIR --prefix PREFIX
binlogviz flashback mysql-bin.000123 --include-table shop.orders --dml delete
binlogviz flashback mysql-bin.000123 --schema-file schema.sql
binlogviz flashback mysql-bin.000123 --schema-file schema.sql --schema-file-db shop
```

`flashback` prints SQL that undoes the selected row changes. stdout is the script. It does not connect to a database. Input rules match `analyze`: positional files, stdin `-`, or `--from-dir` with `--prefix`.

Selectors match `analyze`: `--include-schema`, `--exclude-schema`, `--include-table`, `--exclude-table`, `--dml`, `--start`, `--end`, `--start-position`, `--stop-position`, `--include-gtids`, and `--exclude-gtids`. `--schema-file` is optional SQL (`mysqldump --no-data`, or one or more `SHOW CREATE TABLE` results, including `mysql --batch` where newlines inside the statement are `\n`) and is not a filter. It must describe each table as it was at incident time. A plain dump needs no `USE`: the database is the `-- Host: ... Database:` header, `--schema-file-db`, or the only schema of that table in the binlog. Several `SHOW CREATE TABLE` results may share one file with no `;` between them. An ambiguous database is a warning, and that definition is not used. A DELETE becomes `INSERT` of the before-image. An INSERT becomes `DELETE` of the after-image. An UPDATE sets every column to the before-image and matches the primary key from the after-image. Order is reverse binlog order. Each original transaction is `START TRANSACTION` / `COMMIT`. A comment names the original GTID, or `GTID unavailable`, and `file:start-position`.

The binlog needs `binlog_row_metadata=FULL` and `binlog_row_image=FULL`. A table with no primary key is matched on every column with `LIMIT 1`, and a comment says so. The `INSERT` of a deleted row does not use `LIMIT 1`.

JSON is rebuilt from the binary document, including JSON inside a `binlog_transaction_compression=ON` transaction, so decimals, datetimes, and the sign of `-0.0` round-trip. A non-`utf8mb4` character column is a charset introducer and hex bytes (`_latin1 0xE9`). `ENUM` is the member index and `SET` is the bitmask. `ENUM` index 0 is restored by saving `@@SESSION.sql_mode`, dropping `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, and `TRADITIONAL` for that statement only, and restoring the saved mode. Generated columns are omitted from `INSERT` and `UPDATE` assignments when `CREATE` or `ALTER` in the parsed files names them, including events excluded by `--exclude-gtids`, or when `--schema-file` names them. A schema-file expression is checked when it is integer or `DECIMAL` `+`, `-`, `*`, `/`, `DIV`, `%`, or `MOD`, or `UPPER`, `LOWER`, `CONCAT`, `CONCAT_WS`, `LENGTH`, `CHAR_LENGTH`, or simple JSON extraction. `/` into an integer rounds half away from zero. `/` into `DECIMAL(p,s)` keeps MySQL's default 9-digit division width for integer operands, then rounds half away from zero to scale `s` (`5 / 2` in `DECIMAL(40,4)` is `2.5000`; `1 / 7` in `DECIMAL(40,9)` is `0.142857142`). A logged value that contradicts the expression exits 1 and prints no SQL, including a quotient written with `div_precision_increment = 0`. A result that does not fit the column, when the logged value is the non-strict clip, is omitted with a warning. An expression that cannot be checked is omitted. stderr warns, and a guard after the session `SET` lines fails apply unless that column is generated on the target. The error names every mismatched column. MySQL 8 `TABLE_MAP` optional metadata does not mark generated columns, and a `FULL` image includes both virtual and stored values, so those values are what the check uses. When they equal a checked expression, a real column still cannot be told apart from a generated one. Flashback checks a schema file against those columns and prints nothing when they differ. The error says the file does not match and names the next step: an incident-time dump, an older dump if an `ALTER` came later, or `--include-table` to leave the table out. If a definition was seen and cannot be read, flashback refuses the table and prints nothing. If a selected table has no definition, the script still lists every column and stderr warns that generated columns cannot be ruled out and that apply can stop at `ERROR 3105` with earlier transactions already committed.

`ON DELETE` / `ON UPDATE CASCADE` child rows are not in the binlog and are not restored. Triggers fire on undo. The script overwrites later changes; the match is the primary key only, with no conflict check. A table filter or `--dml` that keeps only some rows of a transaction prints a warning on stderr. Review the script and test it. Apply it in a single session on the primary (`sql_log_bin=1`) so replicas follow under new GTIDs. A statement that fails leaves earlier transactions in the script committed. Re-running the whole script can duplicate rows in a table with no primary key; resume from the failed `-- gtid:` block and keep the `SET` header at the top of the script, or `TIMESTAMP` values shift by the session time zone with exit 0. The remaining limits, including a real column whose logged values equal a checked expression (#166), an `ALTER` applied twice (#167), and a non-strict row that fails under a strict `sql_mode` with `ERROR 1292` or `ERROR 1365` (#180), are in `docs/concept/limitations.md`. `sql_mode=TRADITIONAL` is handled for `ENUM` index 0 (#168).

Exit 1, one `Error:` line, and no SQL when column names are missing, a row image is incomplete, a column cannot be rendered exactly, a seen table definition cannot be read, `--schema-file` does not match the binlog columns, a generated value contradicts the expression, the selected range contains DDL, or `--sql-context off`. Nothing selected is exit 2 with the same `Error:` line as `analyze` and empty stdout. `--sql-context` otherwise has no effect on the script: flashback does not print original statements, and cell values are not redacted. Auth DDL in the selected range is refused as DDL.

## `compare` Command Syntax

```bash
binlogviz compare <current.json> <baseline.json>
binlogviz compare <current.json> <baseline.json> --format text
binlogviz compare <current.json> <baseline.json> --format json
binlogviz compare <current.json> <baseline.json> --format html
binlogviz compare --current-snapshot current --baseline-snapshot baseline
binlogviz compare --current-snapshot current --baseline-snapshot baseline --snapshot-dir /tmp/binlogviz-snapshots
```

`compare` supports two input modes per invocation:

- **File mode**: exactly two positional JSON reports
- **Snapshot mode**: `--current-snapshot` plus `--baseline-snapshot`

### File mode

`compare` accepts exactly two positional arguments in file mode:

- `current.json`: the current BinlogViz analysis report
- `baseline.json`: the baseline BinlogViz analysis report

### Snapshot mode

Snapshot mode loads previously saved analyze JSON reports by name:

- `--current-snapshot`: snapshot name used as the current report
- `--baseline-snapshot`: snapshot name used as the baseline report
- `--snapshot-dir`: optional snapshot directory override; default is `~/.binlogviz/snapshots`

The command does not support discovery mode, binlog files, Markdown output, or mixing file mode with snapshot mode.

## `compare` Input Rules

`compare` only accepts JSON reports generated by `binlogviz analyze --format json`, whether they are loaded from explicit files or from the snapshot store.

Validation rules:

- file mode requires both positional arguments
- snapshot mode requires both `--current-snapshot` and `--baseline-snapshot`
- file mode and snapshot mode cannot be combined
- each input must be readable local JSON
- each input must match the BinlogViz analyze JSON report shape
- the command rejects non-BinlogViz JSON and malformed JSON before rendering begins

Accepted output formats:

| Flag | Default | Description |
|------|---------|-------------|
| `--current-snapshot` | none | Snapshot name used as the current report in snapshot mode. |
| `--baseline-snapshot` | none | Snapshot name used as the baseline report in snapshot mode. |
| `--snapshot-dir` | home-based default | Snapshot directory used in snapshot mode. Default: `~/.binlogviz/snapshots`. |
| `--format` | `text` | Compare report output format: `text`, `json`, or `html`. |

Representative usage:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --workload-id orders-production --format json --snapshot-name current > current.json
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --workload-id orders-production --format json --snapshot-name baseline > baseline.json

binlogviz compare --current-snapshot current --baseline-snapshot baseline
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json > compare.json
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format html > compare.html

# Legacy file mode remains supported
binlogviz compare current.json baseline.json
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

### Comparability contract

Compare JSON exposes `comparability.verdict` as `comparable`, `not_comparable`, or `unknown`, with stable `reason_codes` and per-input `evidence`. Raw numeric deltas are always rendered. Ordinary causal findings, recommendations, and drilldowns appear only when non-empty workload IDs match, known producer flavor and configured scope are compatible, and report-v3 transaction completeness evidence is adequate.

Different workload IDs, conflicting known flavors or mixed producers, and incompatible scopes are `not_comparable`. Missing identity/provenance/scope, legacy v0-v2 metadata, and missing or partial/unknown completeness evidence are `unknown`. Guarded output contains one prominent comparability finding first and suppresses every ordinary causal narrative. Server ID/version, observed schemas, filenames, and flavor remain evidence only; none can substitute for `--workload-id`.

## `trend` Command Syntax

```bash
binlogviz trend <snapshot-a> <snapshot-b> [<snapshot-c> ...]
binlogviz trend <snapshot-a> <snapshot-b> --baseline-snapshot baseline
binlogviz trend --from-snapshots 'incident-*'
binlogviz trend --from-snapshots 'incident-*' --baseline-snapshot baseline --format html > trend.html
```

`trend` is snapshot-oriented and supports two mutually exclusive input modes per invocation:

- **Explicit snapshot mode**: two or more snapshot names as positional arguments
- **Pattern mode**: `--from-snapshots <pattern>` selects snapshot names from the snapshot store

Rules:

- explicit snapshot mode and pattern mode cannot be combined
- the resolved trend set must contain at least two snapshots
- explicit `trend A B` keeps A→B by default (`--order cli`); pattern mode keeps the snapshot-store selection order
- `--order time` sorts by effective window start time ascending and prints `trend: reordered by window start_time: ...` on stderr when that changes the story
- trend uses `snapshot.window.start_time` when present and falls back to `summary.start_time` for older snapshots
- `--baseline-snapshot` is optional and does not automatically become a trend point unless it was selected separately
- all trend formats include pattern trends; `text` and `json` expose `Top Pattern Trends` / `pattern_trends`, and `html` adds an interactive `Pattern Trends` section
- the comparability check covers the optional baseline and every point; one guarded input suppresses causal trend findings, recommendations, and drilldowns while retaining raw points and movements
- HTML trend output defaults to the `share of rows` view and lets you switch to absolute `rows`

Accepted flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `text` | Trend report output format: `text`, `json`, or `html`. |
| `--from-snapshots` | none | Pattern used to select snapshots by name from the snapshot store. |
| `--baseline-snapshot` | none | Optional snapshot used for per-point delta calculations. |
| `--snapshot-dir` | home-based default | Directory used when loading snapshots. Default: `~/.binlogviz/snapshots`. |
| `--top-tables` | `10` | Number of top-table trend series to include in trend output. |
| `--order` | `cli` | Trend point order: `cli` keeps argument or selection order; `time` sorts by window start time. |

## `snapshot` Command Syntax

```bash
binlogviz snapshot save <report.json> --name NAME
binlogviz snapshot save <report.json> --name NAME --snapshot-dir /tmp/binlogviz-snapshots
binlogviz snapshot list
binlogviz snapshot list --format json
binlogviz snapshot show <name>
binlogviz snapshot show <name> --format json
binlogviz snapshot rename <old-name> <new-name>
binlogviz snapshot delete <name>
```

The `snapshot` command manages analyze JSON reports stored by name.

### `snapshot save`

`snapshot save` copies one analyze JSON report into the snapshot store.

Rules:

- `<report.json>` must be a local JSON file that matches the analyze report shape
- `--name` is required
- `--snapshot-dir` overrides the default `~/.binlogviz/snapshots` location
- successful saves print no payload to `stdout`
- successful saves print `Saved snapshot "<name>" to <path>` to `stderr`

Accepted flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | none | Required snapshot name used as the stored snapshot identifier. |
| `--snapshot-dir` | home-based default | Directory used when saving the snapshot. Default: `~/.binlogviz/snapshots`. |

### `snapshot list`

`snapshot list` supports two output modes:

- text mode (default): prints a human-readable table with `name`, `label`, `created_at`, `input_mode`, and `window`
- JSON mode: prints a machine-readable object with `snapshot_dir` and `snapshots`

Accepted flags:

- `--format text`
- `--format json`
- `--snapshot-dir /path/to/store`

### `snapshot show`

`snapshot show <name>` supports two output modes:

- text mode (default): prints a small summary to `stdout`, including snapshot name, resolved path, identity metadata, filters, and top-level totals
- JSON mode: prints a machine-readable object containing the normalized snapshot descriptor under `snapshot`

Accepted flags:

- `--format text`
- `--format json`
- `--snapshot-dir /path/to/store`

### `snapshot rename`

`snapshot rename <old-name> <new-name>` renames a stored snapshot in the snapshot store.

Rules:

- both names must pass the same snapshot-name validation as `snapshot save`
- the command keeps the stored snapshot identity aligned with the new file name
- successful renames print `Renamed snapshot "<old>" to "<new>" at <path>` to `stderr`

### `snapshot delete`

`snapshot delete <name>` removes one stored snapshot from the snapshot store.

Rules:

- the name must pass snapshot-name validation
- successful deletes print `Deleted snapshot "<name>" at <path>` to `stderr`

## Time and Validation Behavior

### Time filters

`--start` and `--end` accept RFC3339 (recommended across machines) or `YYYY-MM-DD HH:MM:SS`. Space format is the local timezone of the machine running `binlogviz`. Explicit offsets such as `Z` or `+08:00` define the instant. Report timestamps stay UTC.

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"

binlogviz analyze mysql-bin.000123 \
  --start "2026-09-12T00:00:00+08:00" \
  --end "2026-09-14T15:00:00+08:00"

binlogviz analyze mysql-bin.000123 \
  --start "2026-09-12 00:00:00" \
  --end "2026-09-14 15:00:00"
```

Validation rules:

- An invalid `--start` value fails with an `invalid start time format` error.
- An invalid `--end` value fails with an `invalid end time format` error.
- If both are provided, `--end` must not be earlier than `--start`.

### Input validation

Validation happens before analysis starts:

- Missing files fail with `file not found: <path>`.
- Unreadable discovery directories fail with a directory read error.
- Discovery mode with no matches fails with `no matching binlog files found under <dir> with prefix "<prefix>"`.
- Invalid input mode combinations fail fast before parsing.

## Error and Invalid Combination Behavior

The command rejects these invalid combinations:

- positional files plus `--from-dir` or `--prefix`
- `--from-dir` without `--prefix`
- `--prefix` without `--from-dir`
- no positional files and no complete discovery pair
- `--snapshot-name` without `--format json`

Representative failure cases:

```bash
# Invalid: mixed input modes
binlogviz analyze mysql-bin.000123 --from-dir /var/lib/mysql --prefix mysql-bin.

# Invalid: incomplete discovery mode
binlogviz analyze --from-dir /var/lib/mysql
```

## Output Channels

BinlogViz deliberately separates machine-consumable report output from operator-facing status output.

### Standard output (`stdout`)

`stdout` is reserved for the final analysis report:

- text report by default
- JSON report when `--format json` is set
- Markdown report when `--format markdown` (or `--format md`) is set
- HTML report when `--format html` is set

This allows safe shell redirection and scripting.

```bash
binlogviz analyze mysql-bin.000123 --format json > report.json
binlogviz analyze mysql-bin.000123 --format markdown > report.md
binlogviz analyze mysql-bin.000123 --format html > report.html
```

**HTML output note:** On an interactive TTY, `analyze --format html` without `--output` writes a derived cwd file (e.g., `mysql-bin.000123.html`, or `binlogviz-report.html` for multiple files / `--from-dir`). When stdout is a pipe or redirect, the HTML document goes to stdout so `analyze --format html > report.html` is a real report. `--output <path>` writes that path; `--output -` forces stdout. `compare` and `trend` HTML already write stdout.

### Standard error (`stderr`)

`stderr` is used for operator-facing runtime status:

- parse progress output
- `Finalizing analysis...`
- resolved file listings in discovery mode
- snapshot save confirmations when `--snapshot-name` is used
- command errors

This keeps `stdout` clean for pipelines and file redirection.

For the exact channel contract and JSON field-level behavior, see [Output Format Reference](output-format.md).

## `compare` Output Channels

`compare` keeps the same report-to-`stdout` behavior, but it does not emit analyze-style progress output:

- `stdout` carries the final compare report
- `stderr` carries command errors

`compare` output by format:

- `text`: terminal-friendly compare summary with deltas
- `json`: machine-readable compare result
- `html`: self-contained visual compare report with charts

Examples:

```bash
binlogviz compare current.json baseline.json > compare.txt
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

For the compare output structure and user-visible report contents, see [Output Format Reference](output-format.md).

## `snapshot` Output Channels

`snapshot` subcommands use the channels below:

- `snapshot save`: no report payload on `stdout`; save confirmation on `stderr`
- `snapshot list`: snapshot names on `stdout`
- `snapshot show`: snapshot metadata and summary on `stdout`
- command failures: CLI error path on `stderr`

## Examples

### Analyze one file

```bash
binlogviz analyze mysql-bin.000123
```

### Analyze multiple files

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

### Analyze with discovery mode

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

### Render JSON for downstream processing

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

### Tune report size and alerts

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --top-tables 20 \
  --top-transactions 20 \
  --top-minutes 30 \
  --detect-spikes \
  --spike-window 10 \
  --spike-factor 5.0 \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

### Filter by schema or table

```bash
# Only analyze a specific schema
binlogviz analyze mysql-bin.000123 --include-schema mydb

# Exclude system schemas
binlogviz analyze mysql-bin.000123 --exclude-schema mysql,sys,information_schema

# Only analyze specific tables
binlogviz analyze mysql-bin.000123 \
  --include-schema mydb \
  --include-table orders,payments
```

### Compare two JSON reports

```bash
binlogviz compare current.json baseline.json
```

### Render compare JSON for downstream processing

```bash
binlogviz compare current.json baseline.json --format json > compare.json
```

### Render a visual compare report

```bash
binlogviz compare current.json baseline.json --format html > compare.html
```

## `workflow run` Command Syntax

```bash
binlogviz workflow run <plan.yaml>
binlogviz workflow run <plan.yaml> --output-dir ./artifacts
binlogviz workflow run <plan.yaml> --snapshot-dir /tmp/snapshots
```

`workflow run` executes a declarative YAML plan that describes one or more analysis windows, optional compare jobs, and optional trend jobs. It produces a deterministic artifact tree plus a `manifest.json`. The manifest always includes a normalized `workflow_summary` object with `findings`, `recommendations`, and `warnings` arrays. That summary is rebuilt best-effort from successful compare/trend JSON artifacts only, so summary warnings never change workflow or step status semantics.

### Plan format

The plan file uses YAML with `version: 1`. The root sections are:

- `version` — required, must be `1`
- `workflow` — workflow name and output directory
- `defaults` — shared input source, analyze options, and snapshot settings
- `windows` — one or more named time windows to analyze
- `compare` — optional compare jobs referencing named windows
- `trend` — optional trend jobs referencing named windows

A runnable sample plan ships at the repository root as `incident.yaml`. It points `from_dir` at `cmd/binlogviz/testdata/sample-binlog` so `binlogviz workflow run incident.yaml` works without a local MySQL datadir. GitHub Release archives ship a bundled `incident.yaml` with `from_dir: testdata/sample-binlog` so the same command works after extract.

Example plan:

```yaml
version: 1
workflow:
  name: incident-investigation
  output_dir: ./artifacts/incident
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
  analyze:
    format: json
    top_tables: 10
  snapshot:
    save: true
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: incident
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
compare:
  - name: incident_vs_baseline
    current: incident
    baseline: baseline
    formats: [json, html]
trend:
  - name: incident_series
    snapshots: [baseline, incident]
    formats: [json, html]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | plan-defined | Override the plan-defined output directory. |
| `--snapshot-dir` | home-based default | Override the snapshot storage directory. |

### Output layout

```
<output_dir>/
  index.html
  manifest.json
  analyze/
    baseline.json
    incident.json
  compare/
    incident_vs_baseline.json
    incident_vs_baseline.html
  trend/
    incident_series.json
    incident_series.html
```

### Execution order

1. Validate and load the plan
2. Create the output directory layout
3. Run all analyze windows in plan order
4. Run compare jobs in plan order
5. Run trend jobs in plan order
6. Write `manifest.json`
7. Write `index.html`

### Workflow summary rebuild

`workflow run` persists a compact workflow-level rollup into `manifest.json`:

- `workflow_summary.findings`, `workflow_summary.recommendations`, and `workflow_summary.warnings` are always present as normalized arrays
- only successful `compare` and `trend` steps contribute summary items
- summary extraction reads JSON artifacts only
- findings and recommendations are deterministically deduplicated and capped at 5 items each
- missing required top-level arrays append warnings, while present-but-empty arrays remain valid and warning-free
- `index.html` prefers HTML source links for workflow summary items and falls back to JSON when no HTML artifact exists
- workflow summary evidence links append `#anchor` only for HTML source reports; JSON fallback links omit anchors
- summary rebuild is best-effort: missing, unreadable, or invalid summary sources append warning strings instead of failing the workflow
- summary warnings never change workflow or step status semantics

### Error handling

- Plan validation errors fail before any artifact is written
- Runtime step failures stop at the first failed step
- Already written artifacts remain on disk
- `manifest.json` is always written with `status: failed` and the failed step's error
- `index.html` is always written on both success and failure

### Output channels

- `stdout` is unused in v1
- `stderr` carries progress lines and the final manifest path
- `index.html` is written to `<output_dir>/index.html` as a self-contained workflow landing page showing workflow metadata, step status, and artifact links

## `workflow status` Command Syntax

```bash
binlogviz workflow status <output_dir>
binlogviz workflow status <output_dir> --format text
binlogviz workflow status <output_dir> --format json
```

`workflow status` inspects an existing workflow output directory without modifying it. It reads `manifest.json`, checks whether each artifact recorded in the manifest currently exists, carries through the persisted `workflow_summary`, determines whether the workflow root is resumable, and optionally builds a dry-run resume preview when the saved plan can be loaded.

The command is read-only:

- it never executes workflow steps
- it never rewrites `manifest.json`, `index.html`, or any artifact
- it never repairs missing artifacts or snapshots
- it never mutates runtime state on disk

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `text` | Status output format: `text` or `json`. |

### Runtime inspection behavior

`workflow status` reports these top-level runtime facts:

- workflow name, output root, manifest version, mode, attempt, and manifest status
- `runtime_state`, which is `complete` only when the manifest has `resolved_input_files`, at least one successful step, all recorded artifacts are present, and, if the saved plan can be loaded, reusable snapshots needed for resume are intact; otherwise `incomplete`. A failed discovery with empty steps is `incomplete`, not `complete`.
- `resumable`, which is `true` only when the workflow root passes resume validation
- `resume_error`, which explains why resume is unavailable for legacy manifests, missing plan files, plan hash mismatches, invalid plan loads, or other resume guard failures
- persisted `workflow_summary`, with normalized `findings`, `recommendations`, and `warnings` arrays copied from the manifest without recomputation
- per-step artifact presence, using the recorded artifact paths from the manifest
- `resume_preview`, when the saved plan loads successfully and a dry resume plan can be derived

Legacy manifests remain inspectable. When the manifest is from the pre-v2 format, the command still renders status output but reports `resumable: false` and a non-empty `resume_error`.

### Trust-boundary behavior

`workflow status` only trusts workflow-local rooted plan references. When `manifest.plan_path` resolves outside the workflow root or escapes via symlinks, the plan is treated as untrusted: the command still succeeds and reports full status, but sets `resumable` to `false` and populates `resume_error` with a trust-boundary explanation. Outside-root and symlink-escaped plan paths are rejected before the file is opened. The trust check is performed by `ValidateWorkflowPlanPath(outputDir, planPath)`.

### Output behavior

- supports `text` and `json` only
- fails before rendering when `<output_dir>/manifest.json` cannot be read
- keeps all output on `stdout`
- does not use `stderr` for progress reporting
- omits `resume_preview` when the plan is unavailable or cannot be loaded

## `workflow clean` Command Syntax

```bash
binlogviz workflow clean <output_dir>
binlogviz workflow clean <output_dir> --format text
binlogviz workflow clean <output_dir> --format json
binlogviz workflow clean <output_dir> --apply
binlogviz workflow clean <output_dir> --apply --include-snapshots
```

`workflow clean` inspects one existing workflow root and reports or deletes orphaned generated files that are no longer referenced by the current `manifest.json`.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--apply` | `false` | Delete discovered cleanup candidates instead of only previewing them. |
| `--include-snapshots` | `false` | Include orphaned snapshot JSON files from `manifest.snapshot_dir`. |
| `--format` | `text` | Cleanup output format: `text` or `json`. |

### Cleanup scope and safety guarantees

`workflow clean` is intentionally narrow:

- it scans only workflow-generated directories: `analyze/`, `compare/`, and `trend/`
- it considers only known generated extensions in those directories
- it treats `steps[].artifacts` in the current manifest as the live artifact set
- it treats successful analyze `snapshot_name` values as the live snapshot set
- it never deletes `manifest.json`
- it never deletes `index.html`
- it never deletes plan files
- it never deletes unknown files outside the deterministic workflow artifact set

Known generated extensions in scope:

- `analyze`: `.json`
- `compare`: `.json`, `.html`
- `trend`: `.json`, `.html`

### Error behavior

`workflow clean` fails before rendering only when cleanup cannot be evaluated meaningfully:

- `<output_dir>/manifest.json` is missing
- the manifest is unreadable or invalid
- one of the workflow artifact directories is unreadable

Additional rules:

- a missing snapshot directory is not an error; it yields zero snapshot candidates
- per-file delete failures in `--apply` mode do not stop the cleanup pass
- failed deletions are reported in `skipped`
- if any deletion is skipped, the command exits non-zero after writing output

### Output behavior

- `text` mode prints a summary block followed by orphan, deleted, and skipped lists
- `json` mode writes a stable machine-readable object with `workflow_name`, `output_dir`, `mode`, `include_snapshots`, `artifact_orphans`, `snapshot_orphans`, `deleted`, `skipped`, and `counts`
- output is written to `stdout`
- command errors continue to use the normal CLI failure path

### Non-goals

`workflow clean` does not:

- repair workflow state
- rewrite manifest contents
- decide what `resume` should do next
- perform global cleanup outside one workflow root
- implement retention windows, TTLs, or age-based pruning

## `workflow export` Command Syntax

```bash
binlogviz workflow export <output_dir>
binlogviz workflow export <output_dir> --output ./incident.zip
binlogviz workflow export <output_dir> -o ./incident.zip
binlogviz workflow export <output_dir> --include-snapshots
binlogviz workflow export <output_dir> --format json
```

`workflow export` bundles an existing workflow root into a deterministic, read-only zip archive. It reads `manifest.json`, includes `manifest.json` itself, best-effort includes `index.html`, includes only manifest-declared workflow artifacts, and best-effort includes `plan.yaml` from `manifest.plan_path` when present. It never reruns workflow steps, never rebuilds workflow summary, and never mutates files under the workflow root.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--output`, `-o` | `<output_dir>.zip` | Archive output path. The default is derived from `filepath.Clean(output_dir) + ".zip"`. |
| `--include-snapshots` | `false` | Include only snapshot JSON files referenced by the manifest. |
| `--format` | `text` | Export command result format: `text` or `json`. |

### Export rules and safety guarantees

- `manifest.json` is required and is always included in the archive
- `index.html` is included best-effort and becomes a warning if missing
- workflow artifacts are loaded only from `steps[].artifacts` in the manifest
- artifacts outside the workflow root are skipped with warnings
- `plan.yaml` is included as `plan.yaml` only when `manifest.plan_path` is present, resolves inside the workflow root, is readable, still matches `manifest.plan_sha256`, and still parses as the recorded workflow plan metadata; otherwise it becomes a warning
- snapshots are excluded by default
- with `--include-snapshots`, only referenced snapshot JSON files are considered, and an empty `manifest.snapshot_dir` becomes a warning instead of reading from the current working directory
- missing manifest artifacts, missing snapshots, and missing plan/index inputs become warnings rather than fatal errors
- the archive output path must be outside the workflow root; paths inside the root are rejected
- zip entry ordering, timestamps, and file modes are normalized so repeated exports are deterministic

### Failure behavior

`workflow export` fails before writing a successful result when:

- `<output_dir>/manifest.json` is missing, unreadable, or invalid
- an included artifact cannot be read for reasons other than file absence in optional best-effort paths
- archive creation or writing fails
- the archive output path resolves inside the workflow root

### Output behavior

- all result output is written to `stdout`
- the command does not use `stderr` for progress output
- `text` mode writes a compact operator summary with an optional `Warnings` section
- `json` mode writes a machine-readable object with `workflow_name`, `output_dir`, `archive_path`, `format`, `included_files`, `included_snapshots`, and `warnings`

## `workflow validate` Command Syntax

```bash
binlogviz workflow validate <plan.yaml>
binlogviz workflow validate <plan.yaml> --format text
binlogviz workflow validate <plan.yaml> --format json
```

`workflow validate` answers whether a workflow plan is statically runnable. It reads only `plan.yaml`, loads it with strict YAML field validation, and applies the same static plan validation used by `workflow run` before execution begins.

Validation covers workflow metadata, window definitions, named references, duplicate compare/trend job names, and duplicate format entries inside compare/trend jobs. The command does not inspect `output_dir`, `manifest.json`, `index.html`, or any existing runtime artifacts.

A structurally valid plan still exits zero when `defaults.input.from_dir` looks like a placeholder (for example `PLACEHOLDER/binlog`) or does not exist. Those cases are reported as `warnings` in text and JSON output. They do not make the plan invalid.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `text` | Validation result format: `text` or `json`. |

### Success contract

- exits zero when the plan is valid
- writes a text or JSON summary to `stdout`
- reports workflow name, window count, compare job count, trend job count, output root, and any `from_dir` warnings

### Failure contract

- exits non-zero when the plan is invalid or unreadable
- writes a text or JSON error payload to `stdout`
- also returns the CLI error through the normal command failure path, so default CLI execution may emit an error line on `stderr`

## `workflow describe` Command Syntax

```bash
binlogviz workflow describe <plan.yaml>
binlogviz workflow describe <plan.yaml> --format text
binlogviz workflow describe <plan.yaml> --format json
```

`workflow describe` answers how a workflow plan would run without executing it. It reads only `plan.yaml`, requires the plan to pass static validation first, and then renders a deterministic preview derived from the plan alone.

The preview includes workflow metadata, analyze windows, compare jobs, trend jobs, declared dependencies, planned artifact paths, and snapshot names when `defaults.snapshot.save` is enabled. The command does not inspect `output_dir`, `manifest.json`, `index.html`, or any previously generated outputs.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `text` | Description output format: `text` or `json`. |

### Output behavior

- supports `text` and `json` only
- does not render HTML
- fails before rendering if the plan is invalid or unreadable
- on failure, writes the error payload to `stdout` and still returns the command error, so default CLI execution may also emit an error line on `stderr`
- preserves plan order for analyze windows, compare jobs, and trend jobs

## `workflow resume` Command Syntax

```bash
binlogviz workflow resume <output_dir>
binlogviz workflow resume <output_dir> --snapshot-dir /tmp/snapshots
binlogviz workflow resume <output_dir> --rerun analyze:week2 --rerun compare:incident_vs_baseline
```

`workflow resume` continues a previously executed workflow from its output directory. It reads the existing `manifest.json`, reuses successful steps, and reruns failed or missing ones. If every step already succeeded with intact artifacts and no `--rerun` selector is given, the command exits 0 and prints one `stderr` line: `nothing to resume`. Use `--rerun` to force work on a successful root.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--snapshot-dir` | home-based default | Override the snapshot storage directory. |
| `--rerun` | none | Repeatable explicit step selector. Forces a specific step to rerun regardless of its previous status. |

### Selector grammar

The `--rerun` flag accepts step selectors in the form `<kind>:<name>`:

| Kind | Name matches | Example |
|------|-------------|---------|
| `analyze` | Window name from the plan | `analyze:week2` |
| `compare` | Compare job name from the plan | `compare:incident_vs_baseline` |
| `trend` | Trend job name from the plan | `trend:incident_series` |

Multiple `--rerun` flags can be combined to force-rerun several steps in one invocation.

### Resume behavior

1. Load the existing `manifest.json` from `<output_dir>`
2. Validate the manifest version (must be v2; legacy pre-v2 manifests are rejected)
3. Verify the plan file hash matches the original run (refuses if the plan changed)
4. For each plan step:
   - If the step succeeded and is not listed in `--rerun`, mark it as reused
   - If the step failed, is missing, or is listed in `--rerun`, execute it again
5. Dependency-aware rerun: rerunning an `analyze` step invalidates downstream `compare` and `trend` steps that reference it
6. Write an updated `manifest.json` with per-step execution status (`executed` or `reused`)
7. Write an updated `index.html` showing mode, attempt number, and per-step execution labels

### Guard conditions

Resume refuses to proceed when:

- `<output_dir>` does not contain a `manifest.json`
- The manifest was produced by a legacy pre-v2 run (missing `manifest_version` field)
- The plan file SHA-256 does not match the `plan_sha256` recorded in the manifest
- The plan path recorded in the manifest resolves outside the workflow root or escapes via symlinks (trust-boundary hard fail)

`ValidateWorkflowPlanPath(outputDir, planPath)` is called before the plan file is opened. Outside-root and symlink-escaped paths are rejected unconditionally. `ValidateResumableManifest` now takes four arguments `(m Manifest, outputDir string, planPath string, planSHA256 string)` to enforce the trust boundary during resume validation.

### Output layout

The output layout is identical to `workflow run`. Resume overwrites artifacts for rerun steps and leaves reused step artifacts unchanged.

### Output channels

- `stdout` is unused
- `stderr` carries progress lines and the final manifest path
- `index.html` is written to `<output_dir>/index.html` and includes the resume mode, attempt number, and per-step execution labels (`executed` / `reused`)
