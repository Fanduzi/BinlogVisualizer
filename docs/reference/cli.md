# CLI Reference

This document defines the user-facing contract for the `binlogviz` root command, `binlogviz analyze`, `binlogviz compare`, `binlogviz trend`, and `binlogviz snapshot`.

If you want the fastest operator path instead of the full contract, start with [Quickstart](../recipe/quickstart.md) or [Analyze Local Binlogs](../recipe/analyze-local-binlogs.md).

## Command Syntax

```bash
binlogviz --version
binlogviz --lang zh-CN analyze <binlog files...>
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
binlogviz analyze --from-dir DIR --prefix PREFIX --format json --snapshot-name NAME
binlogviz compare <current.json> <baseline.json>
binlogviz compare --current-snapshot CURRENT --baseline-snapshot BASELINE
binlogviz trend <snapshot...>
binlogviz trend --from-snapshots 'incident-*'
binlogviz snapshot save <report.json> --name NAME
binlogviz snapshot list
binlogviz snapshot show <name>
```

## Global Flags

These flags are available on the root command and apply before subcommand execution:

| Flag | Default | Description |
|------|---------|-------------|
| `--lang` | env-detected | Runtime output language, for example `en` or `zh-CN`. |
| `--version`, `-v` | `false` | Print the version string and exit. |

## `analyze` Command Syntax

`analyze` accepts exactly one input mode per invocation:

- **Positional file mode**: pass one or more local binlog file paths as positional arguments.
- **Discovery mode**: pass `--from-dir` and `--prefix` together so BinlogViz resolves matching files from a directory.

## Input Rules

### Positional file mode

Use positional arguments when you already know the exact files to analyze.

```bash
binlogviz analyze mysql-bin.000123
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

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
| `--start` | none | Start time, inclusive, in RFC3339 format. |
| `--end` | none | End time, inclusive, in RFC3339 format. |
| `--from-dir` | none | Discover binlog files from this directory. Must be used with `--prefix`. |
| `--prefix` | none | Filename prefix used with `--from-dir`. Must be used with `--from-dir`. |
| `--format` | `text` | Report output format: `text`, `json`, `markdown` (alias `md`), or `html`. |
| `--snapshot-name` | none | Save the JSON analyze output as `<name>.json`. Requires `--format json`. |
| `--snapshot-dir` | home-based default | Directory used when saving a snapshot. Default: `~/.binlogviz/snapshots`. |
| `--sql-context` | `summary` | SQL context presentation mode: `summary`, `off`, or `full`. |
| `--top-tables` | `10` | Number of top tables to include in the report. |
| `--top-transactions` | `10` | Number of top transactions to include in the report. |
| `--detect-spikes` | `false` | Enable write spike detection. |
| `--large-trx-rows` | `1000` | Row threshold for large transaction alerts. |
| `--large-trx-duration` | `30s` | Duration threshold for large transaction alerts. |
| `--top-minutes` | `60` | Number of top active minutes to include in the report. |
| `--spike-window` | `5` | Rolling baseline window in minutes for spike detection. |
| `--spike-factor` | `3.0` | Multiplier over baseline to trigger a spike alert. |
| `--spike-min-rows` | `100` | Minimum row count for a minute to be considered a spike candidate. |
| `--include-schema` | none | Comma-separated list of schemas to analyze (all others excluded). |
| `--exclude-schema` | none | Comma-separated list of schemas to skip. |
| `--include-table` | none | Comma-separated list of tables to analyze (all others excluded). |
| `--exclude-table` | none | Comma-separated list of tables to skip. |

### Snapshot-saving behavior

`analyze` can optionally persist the exact JSON payload it writes to `stdout`.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
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
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json --snapshot-name current > current.json
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json --snapshot-name baseline > baseline.json

binlogviz compare --current-snapshot current --baseline-snapshot baseline
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json > compare.json
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format html > compare.html

# Legacy file mode remains supported
binlogviz compare current.json baseline.json
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

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
- trend points are always ordered by effective window start time ascending
- trend uses `snapshot.window.start_time` when present and falls back to `summary.start_time` for older snapshots
- `--baseline-snapshot` is optional and does not automatically become a trend point unless it was selected separately
- all trend formats include pattern trends; `text` and `json` expose `Top Pattern Trends` / `pattern_trends`, and `html` adds an interactive `Pattern Trends` section
- HTML trend output defaults to the `share of rows` view and lets you switch to absolute `rows`

Accepted flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `text` | Trend report output format: `text`, `json`, or `html`. |
| `--from-snapshots` | none | Pattern used to select snapshots by name from the snapshot store. |
| `--baseline-snapshot` | none | Optional snapshot used for per-point delta calculations. |
| `--snapshot-dir` | home-based default | Directory used when loading snapshots. Default: `~/.binlogviz/snapshots`. |
| `--top-tables` | `10` | Number of top-table trend series to include in trend output. |

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

`--start` and `--end` use RFC3339 timestamps.

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
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
