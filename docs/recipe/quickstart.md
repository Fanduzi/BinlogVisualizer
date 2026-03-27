# Quickstart

This guide walks through the shortest path from installation to a successful `binlogviz analyze` run.

## 1. Install BinlogViz

Install BinlogViz first, then come back to this guide for the first successful analysis run.

For the current installation paths, release artifacts, and local build options, see the project [README](../../README.md#installation).

## 2. Analyze One Binlog File

Start with one local ROW-format binlog file:

```bash
binlogviz analyze mysql-bin.000123
```

By default, BinlogViz writes the final text report to `stdout` and keeps progress or runtime status on `stderr`.

The text report includes five sections:

- Workload Summary
- Top Tables
- Top Transactions
- Minute Activity
- Alerts

## 3. Analyze With JSON Output

Use `--json` when you want a machine-readable result for scripts or downstream tooling:

```bash
binlogviz analyze mysql-bin.000123 --json
```

If you want to save the JSON output directly:

```bash
binlogviz analyze mysql-bin.000123 --json > analyze.json
```

## 4. Discover an Ordered Binlog Range

If your binlog files live together in one directory and follow a numeric naming pattern, let BinlogViz resolve the ordered input set for you:

```bash
binlogviz analyze --from-dir /var/lib/mysql --prefix mysql-bin.
```

BinlogViz will resolve the ordered file set before analysis and keep the final report output clean for the terminal or shell redirection.

For the exact discovery rules, ordering behavior, and invalid combinations, see the [Input Discovery Reference](../reference/input-discovery.md).

## 5. Compare Against Example Outputs

You can inspect the shipped sample outputs to understand what successful reports look like:

- text report: `docs/examples/analyze-output.txt`
- JSON report: `docs/examples/analyze-output.json`

These examples are useful for onboarding, demos, and downstream integration checks.

## Next References

For the stable command and output contracts, continue with:

- [CLI Reference](../reference/cli.md)
- [Input Discovery Reference](../reference/input-discovery.md)
- [Output Format Reference](../reference/output-format.md)
