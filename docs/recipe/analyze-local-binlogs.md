# Analyze Local Binlogs

This guide focuses on practical DBA workflows for running `binlogviz analyze` against local binlog files.

## Start with One File

When you want the fastest confidence check, start with the sample ROW binlog:

```bash
curl -fsSLO https://raw.githubusercontent.com/Fanduzi/BinlogVisualizer/main/cmd/binlogviz/testdata/minimal.binlog
binlogviz analyze minimal.binlog
```

Then point the same command at one of your files:

```bash
binlogviz analyze mysql-bin.000123
```

This first run is the quickest way to verify:

- the file exists locally
- the file parses successfully
- the default text report is already useful

By default, the final report goes to `stdout`, while progress, resolved discovery files, finalization status, and errors go to `stderr`.

## Prefer Discovery Mode for Directory Work

When files live together in one directory and follow a numeric naming pattern, discovery mode is usually the safest operator path:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

On success, BinlogViz will:

1. scan the immediate directory entries
2. keep only files whose suffix after the prefix is numeric
3. sort the matches by numeric suffix
4. print the resolved ordered file list to `stderr`
5. analyze that ordered set

Use positional files instead when you already know the exact subset and order you want:

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 mysql-bin.000125
```

You can also use shell expansion if that matches your workflow:

```bash
binlogviz analyze mysql-bin.*
```

Remember that shell expansion is controlled by your shell, not by BinlogViz.

## Narrow to a Known Incident Window

Use `--start` and `--end` when the file set covers more time than the problem you are investigating:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

This is useful when:

- you are working a known incident window
- the directory contains more history than you want in the report
- you want rankings and totals scoped to a single time slice

Invalid timestamps fail before analysis starts, and `--end` must not be earlier than `--start`.

## Reduce Noise by Schema or Table

Use schema and table filters when you need to isolate one service, one schema, or a short list of hot tables.

### Include only one schema

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders
```

### Exclude internal schemas

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --exclude-schema mysql,sys,information_schema,performance_schema
```

### Focus on specific tables

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --include-schema orders \
  --include-table payments,refunds
```

Filtering happens at analysis time, not just at report rendering, so it is useful both for reducing noise and for tightening the workload scope.

## Choose Text or JSON Intentionally

### Default text output for human review

```bash
binlogviz analyze mysql-bin.000123
```

This is the right default when you want to scan the report directly in a terminal.

### JSON output for scripts and pipelines

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

This is the safer pattern for:

- shell pipelines
- automated validation
- downstream ingestion
- comparisons in CI or operator tooling

## Tune Report Breadth

If the default top-10 output is too narrow for the workload you are investigating, widen it explicitly:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --top-tables 20 \
  --top-transactions 20 \
  --top-minutes 30
```

These flags change report breadth, not the underlying parse scope.

## Turn on Alerts for Anomaly Hunting

When you want BinlogViz to highlight unusual behavior, enable spike detection and, if needed, adjust the large-transaction thresholds:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --detect-spikes \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

Use this when you are looking for abnormal load, not just rankings.

## Control SQL Context Exposure

Use `--sql-context` to control how transaction query context appears in the report:

```bash
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context full
```

Mode guidance:

- `off`: omit query-related fields
- `summary`: keep bounded summaries for operator context
- `full`: include bounded stored SQL text when available

This setting changes presentation, not the workload metrics themselves.

## Redirect Output Channels Intentionally

When you want to archive the report and preserve runtime logs separately, redirect the channels independently:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

This keeps the report stream clean while preserving discovery listings, progress, finalization, and errors.

## Next References

After you have a working operator command, continue with:

- [CLI Reference](../reference/cli.md)
- [Input Discovery Reference](../reference/input-discovery.md)
- [Output Format Reference](../reference/output-format.md)
- [Analysis Model](../concept/analysis-model.md)
