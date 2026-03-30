# Output Format Reference

This document explains what `binlogviz analyze` writes to `stdout` and `stderr`.

If you want the fastest operator path first, start with [Quickstart](../recipe/quickstart.md) or [Analyze Local Binlogs](../recipe/analyze-local-binlogs.md).

## Output Channel Contract

BinlogViz uses separate output channels for different purposes:

- `stdout` carries the final analysis report.
- `stderr` carries progress, resolved discovery files, finalization status, and runtime errors.

This separation matters because it keeps report output safe for redirection and automation.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

In the example above, the JSON report is written to `analyze.json`, while progress and discovery status remain visible on the terminal through `stderr`.

## Available Formats

| Flag value | Alias | Description |
|---|---|---|
| `text` | — | Default. Human-readable terminal output. |
| `json` | `--json` | Machine-readable JSON. |
| `markdown` | `md` | GitHub-flavored Markdown with tables. |
| `html` | — | Self-contained HTML with interactive charts and theme switcher. |

## Text Output

Text mode is the default report format. It renders a fixed five-section report.

### 1. Workload Summary

The `Workload Summary` section provides top-level totals for the analyzed result set:

- total transactions
- total rows
- total events
- time range
- duration

Example heading from the shipped sample:

```text
=== Workload Summary ===
```

### 2. Top Tables

The `Top Tables` section ranks tables by total affected rows and includes per-table operation breakdowns.

Useful fields shown in text output include:

- schema and table name
- total rows
- insert row count
- update row count
- delete row count
- distinct transaction count

Example heading:

```text
=== Top Tables ===
```

### 3. Top Transactions

The `Top Transactions` section ranks transactions by total rows and shows transaction size and duration.

Useful fields shown in text output include:

- transaction key
- total rows
- duration
- event count
- SQL context lines when enabled by `--sql-context`

Example heading:

```text
=== Top Transactions ===
```

### 4. Minute Activity

The `Minute Activity` section summarizes write activity by minute.

Useful fields shown in text output include:

- minute timestamp bucket
- total rows for that minute
- transaction count for that minute

Example heading:

```text
=== Minute Activity ===
```

### 5. Alerts

The `Alerts` section lists detected warnings from analysis logic.

Examples include:

- `large_transaction`
- `spike`

Example heading:

```text
=== Alerts ===
```

## JSON Output

Use `--format json` when the result needs to be consumed by scripts, automation, or downstream tools.

```bash
binlogviz analyze mysql-bin.000123 --format json
```

The JSON report exposes the finalized analysis result in a stable, script-friendly shape with snake_case field names.

### Top-level contract

The top-level JSON object always contains these fields:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | Overall totals and time bounds |
| `tables` | array | yes | Top table aggregates; empty array when no table results exist |
| `transactions` | array | yes | Top transaction aggregates; empty array when no transaction results exist |
| `minutes` | array | yes | Per-minute aggregates; empty array when no minute buckets exist |
| `alerts` | array | yes | Detected alerts; empty array when no alerts exist |
| `warnings` | integer | yes | Count of analysis warnings recorded in the finalized result |

### `summary`

`summary` is always present and contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `total_transactions` | integer | yes | Total analyzed transactions |
| `total_rows` | integer | yes | Total affected rows |
| `total_events` | integer | yes | Total normalized events included in analysis |
| `start_time` | string | yes | RFC3339 timestamp, or empty string when no timestamp is available |
| `end_time` | string | yes | RFC3339 timestamp, or empty string when no timestamp is available |
| `duration` | string | yes | Duration string from the finalized result |

### `tables`

`tables` is always present as an array. Each entry contains:

| Field | Type | Required |
|------|------|----------|
| `schema` | string | yes |
| `table` | string | yes |
| `total_rows` | integer | yes |
| `insert_rows` | integer | yes |
| `update_rows` | integer | yes |
| `delete_rows` | integer | yes |
| `txn_count` | integer | yes |

### `transactions`

`transactions` is always present as an array. Each entry always contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `txn_key` | string | yes | Transaction identifier used in reports |
| `start_time` | string | yes | RFC3339 timestamp, or empty string when unset |
| `end_time` | string | yes | RFC3339 timestamp, or empty string when unset |
| `duration` | string | yes | Go duration string |
| `total_rows` | integer | yes | Total rows touched by the transaction |
| `event_count` | integer | yes | Number of events in the transaction |
| `tables` | object | no | JSON object whose keys are table names and whose values are integer counts; omitted when the map is nil or empty (`omitempty`) |
| `operations` | object | no | JSON object whose keys are operation names and whose values are integer counts; omitted when the map is nil or empty (`omitempty`) |
| `query_summary` | string | no | Omitted when SQL-context mode suppresses it or when no summary exists |
| `query_sql` | string | no | Present only in `--sql-context full` when bounded SQL context exists |
| `query_truncated` | boolean | no | Omitted when no query context exists; when present, indicates whether stored SQL was truncated |
| `query_original_bytes` | integer | no | Omitted when no query context exists; when present, reports original SQL byte length |

#### SQL-context mode behavior

`transactions` query fields depend on `--sql-context`:

- `off`: omit all query-related fields
- `summary`: include `query_summary`; include `query_truncated` and `query_original_bytes` only when query context exists
- `full`: include `query_summary`; include `query_sql`, `query_truncated`, and `query_original_bytes` when query context exists

### `minutes`

`minutes` is always present as an array. Each entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `minute` | string | yes | RFC3339 minute bucket timestamp |
| `total_rows` | integer | yes | Total rows in the minute bucket |
| `txn_count` | integer | yes | Transaction count in the minute bucket |
| `table_rows` | object | no | JSON object whose keys are table identifiers and whose values are integer row counts; omitted when the map is nil or empty (`omitempty`) |

### `alerts`

`alerts` is always present as an array. Each entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `type` | string | yes | Alert type such as `large_transaction` or `spike` |
| `severity` | string | yes | Current alert severity string |
| `message` | string | yes | Human-readable alert message |
| `txn_key` | string | no | Present for transaction-scoped alerts |
| `minute` | string | no | Present for minute-scoped alerts; RFC3339 when present |
| `details` | object | no | JSON object containing structured alert details; omitted when no structured detail payload exists |

### `warnings`

`warnings` is always present as an integer count.

It counts analysis warnings accumulated in the finalized result object. This value is part of the machine-readable report on `stdout`; it is not a count of progress lines or `stderr` messages. A non-zero value indicates the analysis completed with warning conditions recorded in the result, not that JSON output is malformed.

## Markdown Output

Markdown mode renders a GitHub-flavored Markdown report with five sections: workload summary, top tables, top transactions, per-minute activity, and alerts. All sections use pipe tables.

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

The output is safe to pipe into any Markdown renderer or paste directly into GitHub issues, pull request comments, or wiki pages.

## HTML Output

HTML mode renders a self-contained single-file report. All styles, chart library (ECharts), and data are embedded inline — no external dependencies or internet connection required.

```bash
binlogviz analyze mysql-bin.000123 --format html > report.html
```

The report includes:

- Summary stat cards (transactions, rows, events, time range)
- Interactive line chart: rows and transactions per minute
- Interactive bar chart: top tables by rows
- Interactive donut chart: INSERT / UPDATE / DELETE operation mix
- Top tables detail table
- Alert list with severity badges

### Themes

The HTML report includes a theme switcher in the header (five coloured dots). Available themes: **Nebula** (default, dark indigo/cyan), **Forest** (dark emerald/amber), **Navy** (dark sky/gold), **Ember** (dark orange/rose), **Light** (light). Theme preference is saved in `localStorage`.

## stderr Isolation

BinlogViz keeps operator-facing runtime output off `stdout`.

### What goes to `stderr`

The command writes these items to `stderr`:

- parse progress output
- `Resolved binlog files:` listings when discovery mode is used
- `Finalizing analysis...`
- command errors

### Why this matters

This behavior lets you safely:

- redirect text output to a file
- redirect JSON output into another tool
- inspect discovery results and progress without contaminating the report stream

## Examples

### Save text output while keeping status in the terminal

```bash
binlogviz analyze mysql-bin.000123 > report.txt
```

### Save JSON output for downstream processing

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > report.json
```

### Generate a Markdown report

```bash
binlogviz analyze mysql-bin.000123 --format markdown > report.md
```

### Generate a self-contained HTML report

```bash
binlogviz analyze mysql-bin.000123 --format html > report.html
```

The HTML file is self-contained — all charts and styles are embedded inline. Open it in any browser without an internet connection.

### Capture channels separately

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```
