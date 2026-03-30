# CLI Reference

This document defines the user-facing contract for `binlogviz analyze`.

If you want the fastest operator path instead of the full contract, start with [Quickstart](../recipe/quickstart.md) or [Analyze Local Binlogs](../recipe/analyze-local-binlogs.md).

## Command Syntax

```bash
binlogviz analyze <binlog files...>
binlogviz analyze --from-dir DIR --prefix PREFIX
```

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
| `--json` | `false` | Shorthand for `--format json`. Deprecated in favour of `--format`. |
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
- JSON report when `--format json` or `--json` is set
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
- command errors

This keeps `stdout` clean for pipelines and file redirection.

For the exact channel contract and JSON field-level behavior, see [Output Format Reference](output-format.md).

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
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --json > analyze.json
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
