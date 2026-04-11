# Output Format Reference

This document explains what `binlogviz analyze`, `binlogviz compare`, `binlogviz trend`, `binlogviz workflow run`, `binlogviz workflow resume`, `binlogviz workflow status`, `binlogviz workflow clean`, `binlogviz workflow validate`, and `binlogviz workflow describe` write to `stdout` and `stderr`.

If you want the fastest operator path first, start with [Quickstart](../recipe/quickstart.md) or [Analyze Local Binlogs](../recipe/analyze-local-binlogs.md).

## Output Channel Contract

BinlogViz uses separate output channels for different purposes:

- `analyze`: `stdout` carries the final analysis report; `stderr` carries progress, resolved discovery files, finalization status, snapshot save confirmations, and runtime errors.
- `compare`: `stdout` carries the final compare report; command failures are reported through the CLI error path on `stderr`.
- `trend`: `stdout` carries the final trend report; command failures are reported through the CLI error path on `stderr`.
- `workflow run`: `stdout` is unused in v1; `stderr` carries progress lines and the final manifest path. All reports are written to the artifact directory tree under `<output_dir>/`. A `manifest.json` and `index.html` are always written regardless of success or failure. `manifest.json` always includes a normalized `workflow_summary` object, and `index.html` renders `Workflow Recommendations`, `Workflow Findings`, and `Workflow Summary Warnings` when summary items are present.
- `workflow resume`: `stdout` is unused; `stderr` carries progress lines and the final manifest path. Resume reuses successful step artifacts and reruns failed, missing, or explicitly selected steps. The updated `manifest.json` records per-step execution status (`executed` or `reused`). `index.html` includes the resume mode, attempt number, and per-step execution labels.
- `workflow status`: `stdout` carries either a text or JSON runtime inspection result. The command reads `manifest.json`, checks artifact presence, reports `runtime_state`, `resumable`, `resume_error`, and per-step status, and may include a dry `resume_preview`. It is read-only and does not use `stderr` for progress output.
- `workflow clean`: `stdout` carries either a text or JSON cleanup summary. The command reads `manifest.json`, reports orphaned workflow artifacts and optional orphaned snapshots, and in `--apply` mode also reports `deleted` and `skipped`. It does not use `stderr` for progress output, but a skipped deletion still causes a non-zero command exit after the `stdout` payload is written.
- `workflow validate`: `stdout` carries either a text or JSON validation result. The command reads only `plan.yaml`, performs static plan validation, and exits non-zero on invalid plans. On failure it also returns the command error, so default CLI execution may emit an error line on `stderr` after the `stdout` payload.
- `workflow describe`: `stdout` carries either a text or JSON static preview of workflow execution order and artifact paths. The command reads only `plan.yaml`, renders no HTML, and does not inspect runtime outputs. On failure it also returns the command error, so default CLI execution may emit an error line on `stderr` after the `stdout` payload.

This separation matters because it keeps report output safe for redirection and automation.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

In the example above, the JSON report is written to `analyze.json`, while progress and discovery status remain visible on the terminal through `stderr`.

If you also pass `--snapshot-name`, the JSON payload still goes to `stdout`, and BinlogViz prints a save confirmation such as `Saved snapshot "incident_current" to /home/user/.binlogviz/snapshots/incident_current.json` on `stderr`.

## Available Formats

### `analyze`

| Flag value | Alias | Description |
|---|---|---|
| `text` | — | Default. Human-readable terminal output. |
| `json` | — | Machine-readable JSON. |
| `markdown` | `md` | GitHub-flavored Markdown with tables. |
| `html` | — | Self-contained HTML with interactive charts and theme switcher. |

### `compare`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable compare summary with deltas. |
| `json` | Machine-readable compare result. |
| `html` | Self-contained visual compare report with interactive charts. |

### `trend`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable multi-snapshot trend report. |
| `json` | Machine-readable trend result with `pattern_trends`. |
| `html` | Self-contained trend report with charts and a Pattern Trends section. |

### `workflow status`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable runtime inspection summary with step artifact presence and optional resume preview. |
| `json` | Machine-readable status object with `runtime_state`, `resumable`, `resume_error`, `steps`, and optional `resume_preview`. |

### `workflow clean`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable cleanup summary with orphan, deleted, and skipped lists. |
| `json` | Machine-readable cleanup result with orphan/deletion arrays and aggregate counts. |

### `workflow validate`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable validation summary or error. |
| `json` | Machine-readable validation result with `valid` and summary/error fields. |

### `workflow describe`

| Flag value | Description |
|---|---|
| `text` | Default. Human-readable static preview of analyze, compare, and trend execution. |
| `json` | Machine-readable static description derived from the plan. |

## Text Output

Text mode is the default report format. It renders a fixed six-section report.

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

### 4. Top Patterns

The `Top Patterns` section groups recurring write transaction shapes.

Useful fields shown in text output include:

- pattern label
- total rows
- transaction count
- average rows per transaction
- optional sample query summary

Example heading:

```text
=== Top Patterns ===
```

### 5. Minute Activity

The `Minute Activity` section summarizes write activity by minute.

Useful fields shown in text output include:

- minute timestamp bucket
- total rows for that minute
- transaction count for that minute

Example heading:

```text
=== Minute Activity ===
```

### 6. Alerts

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
| `report_version` | integer | yes | Analyze report contract version; current version is `2` |
| `summary` | object | yes | Overall totals and time bounds |
| `tables` | array | yes | Top table aggregates; empty array when no table results exist |
| `transactions` | array | yes | Top transaction aggregates; empty array when no transaction results exist |
| `patterns` | array | yes | Top pattern aggregates; empty array when no pattern results exist |
| `minutes` | array | yes | Per-minute aggregates; empty array when no minute buckets exist |
| `alerts` | array | yes | Detected alerts; empty array when no alerts exist |
| `warnings` | integer | yes | Count of analysis warnings recorded in the finalized result |
| `snapshot` | object | no | Present only when `analyze` is invoked with `--snapshot-name` |

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

### `patterns`

`patterns` is always present as an array. Each entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `pattern_key` | string | yes | Deterministic structural identity for the workload pattern |
| `label` | string | yes | Human-readable pattern description |
| `total_rows` | integer | yes | Total rows contributed by transactions in this pattern |
| `txn_count` | integer | yes | Number of transactions grouped into this pattern |
| `event_count` | integer | yes | Number of events grouped into this pattern |
| `share_of_rows` | number | yes | Fraction of total analyzed rows attributed to this pattern |
| `share_of_txns` | number | yes | Fraction of total analyzed transactions attributed to this pattern |
| `avg_rows_per_txn` | number | yes | Average rows per transaction inside this pattern |
| `tables` | object | yes | Aggregate table-to-row map for the pattern |
| `operations` | object | yes | Aggregate operation-to-row map for the pattern |
| `sample_query_summary` | string | no | Optional representative query summary when available |

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

Current implementation increments this count when transaction query context had to be truncated to the bounded stored SQL size.

### `snapshot`

`snapshot` is omitted unless `analyze` is run with `--snapshot-name`. When present, it is a flat top-level object, not a nested metadata wrapper.

| Field | Type | Required | Notes |
|------|------|----------|------|
| `name` | string | yes | Snapshot file stem chosen by `--snapshot-name` |
| `label` | string | yes | Current implementation matches `name` |
| `created_at` | string | yes | RFC3339 timestamp generated when the report is rendered |
| `binlogviz_version` | string | yes | Version string embedded in the binary |
| `input_mode` | string | yes | `files` for positional file mode, `discovery` for `--from-dir`/`--prefix` mode |
| `input` | object | yes | Snapshot input details |
| `window` | object | yes | Snapshot time-window details |
| `filters` | object | yes | Snapshot schema/table filter details |

#### `snapshot.input`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `files` | array | yes | Ordered input file paths used by the analyze run |
| `from_dir` | string | yes | Discovery directory, or empty string in positional file mode |
| `prefix` | string | yes | Discovery prefix, or empty string in positional file mode |

#### `snapshot.window`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `start_time` | string | yes | RFC3339 timestamp, or empty string when `--start` is unset |
| `end_time` | string | yes | RFC3339 timestamp, or empty string when `--end` is unset |

#### `snapshot.filters`

| Field | Type | Required | Notes |
|------|------|----------|------|
| `include_schema` | array | yes | Included schemas, or empty array |
| `exclude_schema` | array | yes | Excluded schemas, or empty array |
| `include_table` | array | yes | Included tables, or empty array |
| `exclude_table` | array | yes | Excluded tables, or empty array |

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

## Trend Output

`binlogviz trend` renders a chronological report for two or more snapshots. It uses the same stdout/stderr separation rules as the other commands and supports `text`, `json`, and `html`.

Text output includes the new `Top Pattern Trends` section and a `Key Findings` section when trend summary findings are present, followed by a `Recommended Next Checks` section when recommendations are available. JSON output always contains a top-level `pattern_trends` array with per-pattern rows and share series, a `trend_summary` array with deterministic finding objects capped at 5, and a `recommendations` array with operator follow-up suggestions derived from findings. Trend recommendation kinds: `track_rising_pattern`, `confirm_declining_pattern`, `review_growing_table`, `watch_workload_concentration`, or `capture_followup_snapshot`. Each finding may include `evidence_refs` linking it back to relevant report sections (`pattern_trends`, `table_trends`, `ordered_points`). HTML output includes an interactive `Pattern Trends` section that defaults to `share of rows` and can switch to absolute `rows`, a `Key Findings` section with clickable evidence ref links when findings are present, and a `Recommended Next Checks` section with priority badges and evidence links.

## Compare JSON Output

Use `binlogviz compare --format json` when the comparison result needs to be consumed by scripts or other tools.

```bash
binlogviz compare --current-snapshot current --baseline-snapshot baseline --format json
```

The compare JSON contract always contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | Rows, transactions, and warning deltas |
| `key_findings` | array | yes | Deterministic finding summaries capped at 5; empty array when signal is low |
| `recommendations` | array | yes | Operator follow-up suggestions derived from key findings; empty array when no suggestions apply |
| `table_changes` | array | yes | Per-table row deltas |
| `pattern_changes` | array | yes | Write-pattern deltas matched by `pattern_key`, sorted by absolute row delta |
| `operation_mix` | array | yes | INSERT / UPDATE / DELETE deltas |
| `alert_changes` | object | yes | Added and removed alerts |
| `current_label` | string | yes | Snapshot-aware label when metadata exists; otherwise `current` |
| `baseline_label` | string | yes | Snapshot-aware label when metadata exists; otherwise `baseline` |
| `current_snapshot` | object | no | Present when the current input report contains analyze snapshot metadata |
| `baseline_snapshot` | object | no | Present when the baseline input report contains analyze snapshot metadata |

`current_snapshot` and `baseline_snapshot` reuse the same field contract as the analyze `snapshot` object above.

### Compatibility with legacy file mode

The snapshot workflow does not replace the original compare path. `compare` still accepts two explicit analyze JSON files:

```bash
binlogviz compare current.json baseline.json --format json
```

Compatibility rules:

- file mode remains fully supported
- reports that already contain top-level `snapshot` metadata will surface it again as `current_snapshot` and `baseline_snapshot`
- older analyze JSON files without snapshot metadata still compare successfully
- when snapshot metadata is absent, compare falls back to `current` and `baseline` labels and omits `current_snapshot` / `baseline_snapshot`

## Snapshot Command Output

The `snapshot` subcommands use these output contracts:

- `snapshot save` writes no payload to `stdout` and prints `Saved snapshot "<name>" to <path>` to `stderr`
- `snapshot list --format text` writes a human-readable table with `name`, `label`, `created_at`, `input_mode`, and `window`
- `snapshot list --format json` writes a machine-readable object with `snapshot_dir` and `snapshots`
- `snapshot show --format text` writes metadata and a summary block to `stdout`
- `snapshot show --format json` writes a machine-readable object with the normalized descriptor under `snapshot`
- `snapshot rename` and `snapshot delete` do not write a report payload to `stdout`; they print success confirmations to `stderr`

### Themes

The HTML report includes a theme switcher in the header (five coloured dots). Available themes: **Nebula** (default, dark indigo/cyan), **Forest** (dark emerald/amber), **Navy** (dark sky/gold), **Ember** (dark orange/rose), **Light** (light). Theme preference is saved in `localStorage`.

## stderr Isolation

BinlogViz keeps final report output on `stdout`.

For `analyze`, `stderr` carries parse progress, discovery resolution, finalization status, and errors. For `compare`, the current implementation writes the rendered report to `stdout` and surfaces command failures through the CLI error path on `stderr`.

## Compare Output

Use `binlogviz compare` when you already have two JSON reports produced by `binlogviz analyze --format json` and want to understand how the current window differs from a baseline.

```bash
binlogviz compare current.json baseline.json
binlogviz compare current.json baseline.json --format json > compare.json
binlogviz compare current.json baseline.json --format html > compare.html
```

The compare command accepts exactly two BinlogViz analyze JSON reports:

- `current.json`: the newer or incident window you want to inspect
- `baseline.json`: the reference window you want to compare against

### Compare Text Output

Text mode renders a fixed compare report for terminal review. It includes:

- `Current Label` and `Baseline Label` lines derived from snapshot metadata when present, otherwise `current` and `baseline`
- snapshot context lines for requested window, input mode, source summary, and active filters when snapshot metadata is present
- top-level deltas for rows, transactions, and warnings
- top table changes sorted by absolute row delta
- `Key Findings` between warnings and `Top Table Changes` when findings are present, with `evidence:` labels linking findings to relevant report sections
- `Recommended Next Checks` after `Key Findings` when recommendations are present, with priority labels and `evidence:` labels
- `Top Pattern Changes` between `Top Table Changes` and `Operation Mix`
- operation mix changes for `INSERT`, `UPDATE`, and `DELETE`
- alert additions and removals

This is the fastest operator view when you want to see whether the current workload is larger, more write-heavy, or triggering different warnings than the baseline.

### Compare JSON Output

Use `--format json` when another tool needs structured compare data.

```bash
binlogviz compare current.json baseline.json --format json
```

The JSON report serializes the compare result in a stable snake_case shape.

#### Top-level compare contract

| Field | Type | Required | Notes |
|------|------|----------|------|
| `summary` | object | yes | Current/baseline totals and delta values |
| `key_findings` | array | yes | Deterministic finding summaries capped at 5; empty array when signal is low |
| `recommendations` | array | yes | Operator follow-up suggestions derived from key findings; empty array when no suggestions apply |
| `table_changes` | array | yes | Table-level row deltas sorted by absolute change |
| `pattern_changes` | array | yes | Write-pattern deltas matched by `pattern_key`, sorted by absolute row delta |
| `operation_mix` | array | yes | Operation deltas for `insert`, `update`, and `delete` |
| `alert_changes` | object | yes | Added and removed alerts |
| `current_label` | string | yes | Snapshot-aware label when current snapshot metadata is present; otherwise `current` |
| `baseline_label` | string | yes | Snapshot-aware label when baseline snapshot metadata is present; otherwise `baseline` |

#### `key_findings` entries

Each entry in the `key_findings` array contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | Finding category: `volume_change`, `pattern_driver`, `table_driver`, `operation_mix_drift`, or `new_pattern` |
| `title` | string | yes | Short human-readable title |
| `summary` | string | yes | One-sentence evidence-based summary |
| `evidence` | object | yes | Structured key-value map with supporting metrics |
| `evidence_refs` | array | no | Traceability links to report sections; omitted when empty |

Each `evidence_refs` entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `section` | string | yes | Report section the finding links to (e.g. `table_changes`, `pattern_changes`, `operation_mix`) |
| `key` | string | no | Item key within the section (e.g. `orders.refunds`); omitted for section-level refs |
| `label` | string | yes | Human-readable label for the linked item |
| `anchor` | string | yes | HTML anchor ID for in-page navigation |

At a user level, the JSON output answers the same operational questions as the text report, but in a deterministic structure for pipelines, dashboards, or follow-up automation.

#### `recommendations` entries

Each entry in the `recommendations` array contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | Recommendation category (compare): `check_pattern_driver`, `check_table_hotspot`, `check_new_write_pattern`, `check_operation_mix_shift`, `check_volume_growth_source`, or `check_volume_drop_source` |
| `priority` | string | yes | Follow-up priority: `high` or `medium` |
| `title` | string | yes | Short human-readable title |
| `summary` | string | yes | One-sentence actionable suggestion |
| `rationale` | string | yes | Why this recommendation was generated |
| `related_finding_kinds` | array | yes | The finding kinds that triggered this recommendation |
| `evidence_refs` | array | no | Traceability links to report sections; omitted when empty |

Recommendations use conservative language (check, confirm, review, capture) and never claim root cause. Priority indicates follow-up urgency, not incident severity. At most 5 recommendations are returned, ordered by priority then kind.

### Compare HTML Output

HTML mode renders a self-contained visual compare report. It is not a text diff page wrapped in HTML.

```bash
binlogviz compare current.json baseline.json --format html > compare.html
```

The report includes chart-based sections for:

- summary comparison between baseline and current totals
- top table changes ranked by row delta
- pattern changes ranked by row delta
- operation mix comparison
- alert change visibility for added and removed alerts

When snapshot metadata is present, the HTML header also exposes compare context such as labels, requested window, input mode, source summary, and active filters.

The page also includes compare summary cards and detailed tables/lists so an operator can move between the chart view and the exact affected tables or alerts without switching tools.

### What `analyze` writes to `stderr`

`analyze` writes these items to `stderr`:

- parse progress output
- `Resolved binlog files:` listings when discovery mode is used
- `Finalizing analysis...`
- command errors

### Compare errors on `stderr`

`compare` does not emit analyze-style progress output today. It writes the rendered compare report to `stdout`; if the command fails, the CLI surfaces the error on `stderr`.

### Why this matters

This behavior lets you safely:

- redirect text output to a file
- redirect JSON output into another tool
- inspect analyze discovery results and progress without contaminating the report stream

## Workflow Status Output

`binlogviz workflow status` reports the runtime state of an existing workflow root without mutating it.

### Text output

Text mode writes a human-readable inspection summary to `stdout`.

The top block includes:

- `Workflow Status`
- `Output Root`
- `Manifest Version`
- `Mode`
- `Attempt`
- `Status`
- `Runtime State`
- `Resumable`
- optional `Reason` when `resume_error` is non-empty

It then renders:

- a `Steps` section with one entry per recorded manifest step
- per-step `status`
- optional per-step `execution`
- per-step artifact presence, showing recorded relative paths and marking missing files as missing
- an optional `Resume Preview` section with dry-run `reuse` / `rerun` decisions and their reasons

Representative meanings:

- `Runtime State: complete` means all recorded artifacts exist and, when the saved plan can still be loaded, reusable snapshots needed for resume are still present
- `Runtime State: incomplete` means at least one recorded artifact is missing, or a reusable snapshot needed by a successful analyze step is missing
- `Resumable: yes` means the root passed resume validation
- `Resumable: no` with `Reason:` means the root is inspectable but not resumable

### JSON output

JSON mode writes a single machine-readable object to `stdout`.

Top-level fields:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | Workflow name from the manifest |
| `output_dir` | string | yes | Output root inspected by the command |
| `manifest_version` | integer | yes | Manifest contract version |
| `mode` | string | yes | `run` or `resume` |
| `attempt` | integer | yes | Attempt counter recorded in the manifest |
| `status` | string | yes | Manifest status, such as `success` or `failed` |
| `runtime_state` | string | yes | `complete` or `incomplete` based on current runtime inspection |
| `resumable` | boolean | yes | Whether resume is currently allowed |
| `resume_error` | string | yes | Empty when resumable; explanatory string otherwise |
| `steps` | array | yes | Per-step runtime inspection records |
| `resume_preview` | array | no | Dry-run resume decisions; omitted when no plan-derived preview is available |

Each `steps` entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | Step kind such as `analyze`, `compare`, or `trend` |
| `name` | string | yes | Step name from the manifest |
| `status` | string | yes | Recorded manifest step status |
| `execution` | string | no | Recorded execution label when present |
| `artifacts` | array | no | Current artifact presence records |

Each `artifacts` entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `path` | string | yes | Artifact path relative to `<output_dir>` |
| `exists` | boolean | yes | Whether that artifact file exists right now |

Each `resume_preview` entry contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | Planned step kind |
| `name` | string | yes | Planned step name |
| `action` | string | yes | `reuse` or `rerun` |
| `reason` | string | yes | Dry-run explanation for the chosen action |

Legacy manifests remain inspectable in both text and JSON output. In that case the command still renders status, but returns `resumable: false` and a non-empty `resume_error`.

## Workflow Clean Output

`binlogviz workflow clean` reports orphaned workflow-generated files that are not referenced by the current manifest, and optionally reports orphaned snapshot JSON files.

### Text output

Text mode writes a cleanup summary to `stdout` with these sections:

- workflow name and output root
- cleanup mode: `dry-run` or `apply`
- whether snapshots were included
- aggregate counts for artifact orphans, snapshot orphans, deleted, and skipped
- `Orphaned Artifacts`
- `Orphaned Snapshots`
- `Deleted`
- `Skipped`

Representative behavior:

- in dry-run mode, `Deleted` remains empty
- without `--include-snapshots`, `Orphaned Snapshots` reports `none`
- in apply mode, successful deletions are listed under `Deleted`
- failed deletions are listed under `Skipped`

### JSON output

JSON mode writes one machine-readable object to `stdout`.

Top-level fields:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | Workflow name from the manifest |
| `output_dir` | string | yes | Output root inspected by the command |
| `mode` | string | yes | `dry-run` or `apply` |
| `include_snapshots` | boolean | yes | Whether snapshot cleanup was enabled |
| `artifact_orphans` | array | yes | Relative artifact paths under `analyze/`, `compare/`, and `trend/` |
| `snapshot_orphans` | array | yes | Snapshot file names from `manifest.snapshot_dir` |
| `deleted` | array | yes | Successfully deleted candidate paths or snapshot file names |
| `skipped` | array | yes | Candidate paths or snapshot file names that could not be deleted |
| `counts` | object | yes | Aggregate orphan/deletion totals |

The `counts` object contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `artifact_orphans` | integer | yes | Count of orphaned workflow artifacts |
| `snapshot_orphans` | integer | yes | Count of orphaned snapshots |
| `deleted` | integer | yes | Count of successful deletions |
| `skipped` | integer | yes | Count of skipped deletions |

### Failure behavior

- missing or unreadable `manifest.json` fails before rendering
- unreadable workflow artifact directories fail before rendering
- a missing snapshot directory yields zero snapshot candidates, not an error
- skipped deletions in apply mode still write the full output, then return a non-zero command error

## Workflow Validate Output

`binlogviz workflow validate` reports whether a plan is statically valid before any execution begins.
It rejects duplicate compare/trend job names and duplicate format entries within a single compare/trend job.

### Text output

Text mode writes one of these shapes to `stdout`:

- success: `Workflow plan valid` followed by workflow name, window count, compare job count, trend job count, and output root
- failure: `Workflow plan invalid` followed by the validation error message

### JSON output

Success output contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `valid` | boolean | yes | `true` for a valid plan |
| `workflow_name` | string | yes | Workflow name from the plan |
| `windows` | integer | yes | Number of analyze windows |
| `compare_jobs` | integer | yes | Number of compare jobs |
| `trend_jobs` | integer | yes | Number of trend jobs |
| `output_dir` | string | yes | Output root declared in the plan |

Failure output contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `valid` | boolean | yes | Always `false` |
| `error` | string | yes | Validation or file-read error message |

## Workflow Describe Output

`binlogviz workflow describe` reports how a valid plan would execute, using only plan-derived data.

### Text output

Text mode writes these sections to `stdout` in order:

1. workflow header with workflow name, output root, and snapshot-save setting
2. `Analyze Windows` with each named window, RFC3339 start/end, planned analyze artifact paths, and optional snapshot name
3. `Compare Jobs` with each job name, declared dependencies, and planned compare artifact paths
4. `Trend Jobs` with each job name, declared snapshot dependencies, and planned trend artifact paths

### JSON output

The JSON description contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `workflow_name` | string | yes | Workflow name from the plan |
| `output_dir` | string | yes | Output root from the plan |
| `snapshot_save` | boolean | yes | Whether named snapshots are planned for analyze windows |
| `windows` | array | yes | Ordered analyze-window descriptions |
| `compare` | array | yes | Ordered compare-job descriptions |
| `trend` | array | yes | Ordered trend-job descriptions |

Each `windows` entry contains `name`, `start`, `end`, `artifacts`, and optional `snapshot_name`.
Each `compare` entry contains `name`, `current`, `baseline`, and `artifacts`.
Each `trend` entry contains `name`, `snapshots`, and `artifacts`.

## Workflow Manifest

`workflow run` and `workflow resume` write a `manifest.json` to `<output_dir>/manifest.json`. Manifest v2 adds fields that support the resume workflow.

### Manifest v2 fields

| Field | Type | Required | Notes |
|------|------|----------|------|
| `manifest_version` | integer | yes | Manifest contract version; current version is `2` |
| `status` | string | yes | `success` or `failed` |
| `mode` | string | yes | `run` for a fresh execution, `resume` for a resumed execution |
| `attempt` | integer | yes | Execution attempt number; starts at `1` for `run`, increments on each `resume` |
| `plan_sha256` | string | yes | SHA-256 hash of the plan file at the time of the first run |
| `resolved_input_files` | array<string> | yes | Resolved input file paths captured during discovery |
| `snapshot_dir` | string | yes | Snapshot directory used during execution |
| `workflow_summary` | object | yes | Workflow-level rollup rebuilt from successful compare/trend JSON artifacts |
| `steps` | array | yes | Per-step status records |
| `error` | string | no | Present when `status` is `failed`; contains the failure message |

### `workflow_summary`

`workflow_summary` is always normalized to this shape:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `findings` | array | yes | Workflow findings sourced from compare `key_findings` and trend `trend_summary` |
| `recommendations` | array | yes | Workflow recommendations sourced from compare/trend `recommendations` |
| `warnings` | array<string> | yes | Best-effort aggregation warnings |

Behavior notes:

- only successful `compare` and `trend` steps contribute summary items
- summary extraction reads JSON artifacts only
- summary warnings capture missing, unreadable, or invalid summary sources
- summary warnings never flip workflow success/failure semantics
- `index.html` renders `Workflow Recommendations`, `Workflow Findings`, and `Workflow Summary Warnings` only when those arrays are non-empty

### Per-step fields

Each entry in the `steps` array contains:

| Field | Type | Required | Notes |
|------|------|----------|------|
| `kind` | string | yes | Step kind: `analyze`, `compare`, or `trend` |
| `name` | string | yes | Step name from the plan |
| `status` | string | yes | `success` or `failed` |
| `execution` | string | no | `executed` (step was run) or `reused` (step was carried over from a previous run) |
| `artifacts` | array<string> | no | Planned artifact paths relative to `<output_dir>` |
| `snapshot_name` | string | no | Present for analyze steps when snapshot saving is enabled |
| `error` | string | no | Present when the step failed |

### Resume and manifest interaction

- `workflow resume` reads the existing `manifest.json` to determine which steps succeeded and can be reused
- Resume refuses legacy pre-v2 manifests (those missing the `manifest_version` field)
- Resume refuses to proceed if the plan file hash does not match `plan_sha256` in the manifest
- The updated manifest preserves all fields from the original run and updates `mode`, `attempt`, `steps`, and `status`

### index.html updates

When the manifest contains `mode: resume`, `index.html` shows:

- the resume mode label
- the current attempt number
- per-step execution labels (`executed` or `reused`) alongside the step status

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
