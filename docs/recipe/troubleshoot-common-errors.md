# Troubleshoot Common Errors

This guide explains common `binlogviz analyze` failures and the operator actions that usually resolve them.

## `file not found`

Representative error:

```text
file not found: mysql-bin.000123
```

What it means:

- a positional file path does not exist at the path you provided
- the shell expanded to a path you did not expect
- the file may have been rotated, moved, or typed incorrectly

What to check:

- confirm the file exists on disk
- confirm you are using the expected working directory or absolute path
- confirm shell expansion produced the files you intended

Typical fix:

```bash
binlogviz analyze /var/lib/mysql/mysql-bin.000123
```

Use absolute paths if your current directory is not the directory holding the binlogs.

## `no matching binlog files found`

Representative error:

```text
no matching binlog files found under /var/lib/mysql with prefix "mysql-bin."
```

What it means:

- discovery mode scanned the directory successfully
- none of the entries matched the required prefix-plus-numeric-suffix contract

Common causes:

- wrong directory
- wrong prefix
- only index files are present
- files have non-numeric suffixes
- the expected binlogs are symlinks or directories rather than regular files

What to check:

- verify the directory is correct
- verify the prefix matches the real file names exactly
- verify the matching files end in digits only after the prefix

Typical fix:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

If your file naming does not fit the discovery contract, use positional file mode instead.

## `--from-dir and --prefix must be provided together`

Representative error:

```text
--from-dir and --prefix must be provided together
```

What it means:

- you attempted discovery mode with only one of its required flags

Typical fix:

Provide both flags in the same command:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

If you already know the exact files, remove the discovery flag entirely and pass positional paths instead.

## `cannot combine positional binlog files with --from-dir/--prefix`

Representative error:

```text
cannot combine positional binlog files with --from-dir/--prefix
```

What it means:

- the command was given two input modes at once
- BinlogViz requires one mode per invocation: positional files or discovery mode

Typical fix:

Choose one of these forms, not both:

```bash
binlogviz analyze mysql-bin.000123 mysql-bin.000124
```

or

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

## `invalid start time format` or `invalid end time format`

Representative errors:

```text
invalid start time format: ... (use RFC3339)
invalid end time format: ... (use RFC3339)
```

What it means:

- the value passed to `--start` or `--end` is not valid RFC3339

Typical fix:

Use explicit RFC3339 timestamps:

```bash
binlogviz analyze mysql-bin.000123 \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

If you need local-time interpretation, convert it to a valid RFC3339 timestamp before passing it to the command.

## `end time must be after start time`

Representative error:

```text
end time must be after start time
```

What it means:

- both time bounds parsed successfully
- the end bound is earlier than the start bound

Typical fix:

Swap or correct the timestamps so the time window moves forward.

## `invalid --sql-context`

Representative error:

```text
invalid --sql-context "invalid" (allowed: summary, off, full)
```

What it means:

- the value passed to `--sql-context` is outside the supported presentation modes

Typical fix:

Use one of the supported values only:

```bash
binlogviz analyze mysql-bin.000123 --sql-context summary
binlogviz analyze mysql-bin.000123 --sql-context off
binlogviz analyze mysql-bin.000123 --sql-context full
```

## `unsupported report_version`

Representative error:

```text
unsupported report_version 2: this BinlogViz build supports up to 1
```

What it means:

- you are trying to load analyze JSON or a snapshot created by a newer BinlogViz contract
- the current binary understands older legacy reports and the current contract, but not that newer one

Typical fix:

- upgrade BinlogViz to a version that supports the newer report contract
- or regenerate the report/snapshot with the current BinlogViz build when that is operationally acceptable

## Unexpected output channel behavior

Typical symptom:

- JSON was redirected to a file, but progress text still appeared in the terminal
- discovery-mode resolved file listings did not appear in the redirected report file
- `Finalizing analysis...` appeared outside the report output

What it means:

- this is usually expected channel separation, not a failure
- BinlogViz writes the final report to `stdout`
- progress, resolved-file reporting, finalization status, and command errors go to `stderr`

Typical usage patterns:

```bash
binlogviz analyze mysql-bin.000123 --format json > report.json
```

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  > report.txt \
  2> analyze.stderr.log
```

If you want both report and runtime status captured, redirect both channels explicitly.

## Parse or analysis errors during execution

Representative prefixes include:

```text
parse error: ...
analysis finalize error: ...
create temp DuckDB store: ...
```

What they usually mean:

- parsing failed before a complete result could be produced
- normalization or analysis rejected an event stream condition
- finalization could not assemble the final result
- the command could not create or initialize its temporary DuckDB store

Operator response:

- preserve the exact error text
- confirm the input files are valid local ROW binlogs
- retry with a smaller known-good subset when possible
- verify the machine can create temporary files and has usable temporary disk space

If the failure persists on a small known-good fixture, treat it as an implementation issue rather than an invocation mistake.

## Quick Triage Checklist

When a command fails, check in this order:

1. input mode: positional files or discovery mode, but not both
2. file existence: do the paths actually exist locally
3. discovery contract: correct directory, exact prefix, numeric suffixes
4. time format: valid RFC3339 timestamps
5. output expectation: did you confuse `stdout` report data with `stderr` status lines
6. runtime environment: can the command create temporary files and complete finalization

## Next References

- [CLI Reference](../reference/cli.md)
- [Input Discovery Reference](../reference/input-discovery.md)
- [Output Format Reference](../reference/output-format.md)
- [Analyze Local Binlogs](analyze-local-binlogs.md)
