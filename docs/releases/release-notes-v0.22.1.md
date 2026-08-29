# BinlogViz v0.22.1 Release Notes

Release date: 2026-08-29

## Changes

- **Release archives actually ship the sample ROW binlog**: each platform tar.gz includes `testdata/minimal.binlog`, `testdata/sample-binlog/mysql-bin.000001`, and an archive-relative `incident.yaml`. After extract, `./binlogviz analyze testdata/minimal.binlog` and `./binlogviz workflow run incident.yaml` do not need a git clone.
- **Replay commands are copy-pasteable on MariaDB hosts**: `mysqlbinlog_cmd` uses absolute file paths. A MariaDB Format Description selects `mariadb-binlog`; MySQL stays `mysqlbinlog`. XID-only spans still omit the command.

## Bug Fixes

- **`analyze --format html > report.html` is a real report**: when stdout is not a TTY and `--output` is omitted, HTML goes to stdout (same as `compare` / `trend`). A TTY still writes a derived cwd file. `--output path` and `--output -` are unchanged.
- **STATEMENT analyze no longer looks successful**: zero ROW images exits 1 with empty stdout and one `Error:` line. MIXED with ROW images still exits 0 and now adds JSON `alerts[{type: input_format, severity: warning}]`. `warnings` stays the truncated-query integer.
- **No-data is exit 2**: a complete binlog that counted zero events (empty `--start`/`--end` window, or Format Description / rotate only) exits 2, writes nothing to stdout, and prints one `Error:`. Magic-only / no Format Description stays exit 1.
- **Sub-second TPS is not `0.0`**: when the analyzed span is shorter than one second and there is at least one transaction, the text TPS peak is `N/A (sub-second)`. Rows/min and JSON `tps_series` stay numeric.
- **Workflow failures match analyze**: `Error:` once, no Usage dump (`run`, `resume`, `validate`, `describe`, `status`, `clean`, `export`).

## Breaking Changes

- **Empty time window / Format-Description-only files exit 2**, not 0. Scripts that treated a 0-event report as success will now fail.
- **STATEMENT with zero ROW images writes no report** (stdout is empty). v0.21.1 still printed the empty brief, then exited non-zero.
- **`analyze --format html` without `--output`, redirected stdout, writes the HTML document to stdout** instead of a cwd sidecar file.

## Compatibility

- JSON `warnings` remains an integer. MIXED undercount is visible on `alerts` and `diagnostics.input_format_guess` / `ignored_query_dml_events`.
- Snapshot, compare, and trend JSON shapes are otherwise unchanged.
