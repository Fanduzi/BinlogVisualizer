# Quickstart

This guide is the shortest path from installation to a useful `binlogviz analyze` run.

## 1. Install or Verify BinlogViz

Use a release artifact if you want a tagged binary, or build from source if you are working locally.

For the current install commands and artifact examples, see the project [README](../../README.md#installation).

To verify the binary you installed:

```bash
binlogviz --version
binlogviz version
```

- `binlogviz --version` prints the version string only
- `binlogviz version` prints the ASCII logo plus `binlogviz <version>`

## 2. Validate One File First

Start with one local `ROW` binlog file:

```bash
binlogviz analyze mysql-bin.000123
```

This is the fastest way to confirm:

- the file is readable
- BinlogViz can parse it
- the default text report already gives you useful output

By default, the final report goes to `stdout` and progress/runtime information stays on `stderr`.

## 3. Analyze a Directory in Binlog Order

If your files live in one directory and use a numeric suffix pattern, discovery mode is the most repeatable operator path:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

BinlogViz will resolve the ordered file set before analysis and print the resolved file list to `stderr`.

For the exact matching and ordering rules, see the [Input Discovery Reference](../reference/input-discovery.md).

## 4. Narrow to the Window You Care About

When the files cover more time than the incident you are investigating, scope the report with `--start` and `--end`:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. \
  --start "2026-03-15T10:00:00Z" \
  --end "2026-03-15T10:30:00Z"
```

Both flags use RFC3339 timestamps.

## 5. Redirect JSON Safely

Use `--format json` when another tool or script will consume the result:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin. --format json > analyze.json
```

This keeps the machine-readable report on `stdout` and leaves progress and runtime information on `stderr`.

## 6. Compare Against Sample Outputs

If you want to see what a successful run looks like before wiring it into your own workflow, inspect the shipped samples:

- text report: `docs/examples/analyze-output.txt`
- JSON report: `docs/examples/analyze-output.json`

To try a multi-step workflow without a local MySQL datadir, run the shipped sample plan from the repository root:

```bash
binlogviz workflow run incident.yaml
```

`incident.yaml` points `from_dir` at `cmd/binlogviz/testdata/sample-binlog` (the same 1500-byte ROW fixture as `minimal.binlog`). After a successful run, `binlogviz workflow resume ./artifacts/incident` exits 0 and prints `nothing to resume`.

## Next Steps

Once the first run works, continue with:

- [Analyze Local Binlogs](analyze-local-binlogs.md)
- [CLI Reference](../reference/cli.md)
- [Input Discovery Reference](../reference/input-discovery.md)
- [Output Format Reference](../reference/output-format.md)
