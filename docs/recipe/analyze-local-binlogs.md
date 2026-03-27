# Analyze Local Binlogs

This guide shows practical operator workflows for running `binlogviz analyze` against local binlog files.

## Analyze One File

Start with a single file when you want the fastest path to validating format, output shape, and basic tool behavior.

```bash
binlogviz analyze mysql-bin.000123
```

This is the best first run when you want to confirm:

- the file exists locally
- the file can be parsed successfully
- the default text report is sufficient for an initial review

By default, the final report goes to `stdout`, while progress and runtime status stay on `stderr`.

## Analyze Multiple Files

Use positional files when you already know the exact subset and order you want to inspect.

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 mysql-bin.000125
```

This mode is useful when:

- you want a hand-picked range
- your shell or script already resolved the file list
- you do not want discovery mode to decide the input set

You can also let the shell expand a pattern if that matches your operational workflow:

```bash
binlogviz analyze mysql-bin.*
```

Be aware that shell expansion rules come from your shell, not from BinlogViz itself.

## Use Discovery Mode

Use discovery mode when your files live together in one directory and follow a stable numeric naming pattern.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

When discovery mode succeeds, BinlogViz will:

1. scan the immediate directory entries
2. keep only files whose suffix after the prefix is numeric
3. sort the matches by numeric suffix
4. print the resolved ordered list to `stderr`
5. analyze that ordered set

This is a good fit when you want a repeatable operator command without hand-listing every file.

For the exact matching and ordering contract, continue with the [Input Discovery Reference](../reference/input-discovery.md).

## Filter by Time Window

Use `--start` and `--end` when you only care about activity within a specific RFC3339 time range.

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

This is useful when:

- you are investigating a known incident window
- the input files cover more time than you want in the final result
- you want report totals and rankings scoped to a specific period

Remember that invalid timestamps fail before analysis starts, and `--end` must not be earlier than `--start`.

## Render JSON for Automation

Use `--json` when another tool or script will consume the result.

```bash
binlogviz analyze mysql-bin.000123 --json > analyze.json
```

This pattern keeps the machine-readable report on `stdout` while still allowing progress and runtime status to appear on `stderr`.

It is the safer choice for:

- shell pipelines
- automated checks
- downstream data loading
- CI-style verification of expected output shape

## Tune Top-N Output

If the default top-10 ranking is too small for your investigation, expand it explicitly.

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --top-tables 20 \
  --top-transactions 20
```

Use this when:

- your workload touches many hot tables
- you need more than the default shortlist
- you want wider transaction coverage in the final report

These flags change report breadth, not the underlying parsing scope.

## Enable Spike Detection

Turn on spike detection when you want the analyzer to call out unusually heavy minute-level activity.

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --detect-spikes
```

You can combine it with custom large-transaction thresholds when your environment needs different alert sensitivity. Large-transaction alerts are independent of spike detection, so you can tune those thresholds even if you leave `--detect-spikes` off.

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124 \
  --detect-spikes \
  --large-trx-rows 5000 \
  --large-trx-duration 60s
```

This is useful when you are looking for operational anomalies, not just workload rankings.

## Choose an SQL Context Mode

Use `--sql-context` to control how transaction query context appears in the report.

```bash
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context full
```

Mode guidance:

- `off`: suppress all query-related fields
- `summary`: keep bounded summaries for operator context
- `full`: include the bounded stored SQL text when available

This setting changes presentation, not the workload metrics themselves.

## Redirect Output Channels Intentionally

When you need clean report capture plus separate runtime logs, redirect the two channels independently.

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

Use this pattern when:

- you want to archive the final report
- you want discovery and progress logs separately
- you are feeding the report into another process without status noise

## Next References

After you have a working operator command, use these references to go deeper:

- [CLI Reference](../reference/cli.md)
- [Input Discovery Reference](../reference/input-discovery.md)
- [Output Format Reference](../reference/output-format.md)
- [Analysis Model](../concept/analysis-model.md)
