# Exit codes and JSON errors

Every command maps a failure to one **kind**. The exit code is a function of the kind (ADR 0008, ADR 0009). Every error a command returns has a kind; a test fails the build on an untyped one (#277). The kind string is stable and does not change with `--lang`.

| Exit | Kind | When |
|---|---|---|
| 0 | | success |
| 1 | `error` | any failure with no other kind, including an open `BEGIN` or an unclassified QUERY that analyze cannot count safely (ADR 0001) |
| 2 | `no_data` | a complete binlog with nothing in range or nothing matching the filters (ADR 0001) |
| 3 | `usage` | unknown command, subcommand (`snapshot x`, `workflow x`) or flag, wrong arguments, an invalid `--format` (#251), `--error-format`, `--start`/`--end`, GTID set, `--dml`, `--conflict` or `--output` value, a negative count such as `--top-transactions -1` (#268), a missing required flag such as `snapshot save` without `--name`, an empty `--error-format=`, a bad `workflow resume --rerun` selector, an invalid snapshot name, a position selector that is out of range or not on an event boundary (#277) |
| 4 | `input` | an input file or `--from-dir` is missing, unreadable or a directory, a `compare` report cannot be read, or `--schema-file` cannot be read (#268); a snapshot, snapshot source, workflow manifest or plan cannot be found or read, including a mode-000 snapshot (#277). Only a failed read: a manifest that does not decode is 5 |
| 5 | `corrupt` | not a binlog, empty (a 0-byte file, `/dev/null` and empty stdin alike), truncated, JSON or YAML that does not decode, or a report, snapshot, workflow plan or manifest missing required fields (`{}` included; a `{}` manifest is not legacy), a workflow plan that fails validation (`details.reason: invalid_plan`), a corrupt event (including a CRC32 checksum mismatch), or no Format Description |
| 6 | `refused` | flashback cannot write an exact script: DDL in range, a row image that is not FULL, a `--schema-file` mismatch, `--sql-context off`. `workflow resume` refuses with `details.reason`: `plan_changed` (plan.yaml changed since the run; start a fresh run) or `trust_boundary` (the manifest's `plan_path` is outside the workflow root or not `<root>/plan.yaml`). `workflow status` and `export` are read-only and still exit 0 on an out-of-root `plan_path` |
| 7 | `conflict` | flashback: a selected row was changed again later in the scanned binlogs (`--conflict refuse`, the default; #259) |
| 8 | `output` | writing the report, the script or the flashback temp file failed (for example ENOSPC). SIGPIPE stays 141 |
| 9 | `unsupported` | format and version limits only: a STATEMENT or MIXED binlog with no ROW images, a `report_version` newer than this build, a workflow plan version, or a legacy or newer `manifest_version`, this build cannot run or resume (#277). Valid JSON missing required fields is 5, not 9 |

Signals keep their shell codes: SIGHUP 129, SIGINT 130, SIGQUIT 131, SIGTERM 143.

## JSON errors

`--error-format json` (any command), or `BINLOGVIZ_ERROR_FORMAT=json` in the environment, replaces the `Error:` line with one JSON object on stderr. `--error-format` takes `text` or `json`; any other value is a usage error. With JSON errors that object is the only line a failed run writes to stderr: no usage text and no info lines (#268). Warnings still print.

```json
{"error":{"kind":"corrupt","code":5,"message":"parse error: corrupt event, type 31, ending at position 500, size 45: invalid table id 0","details":{"event_type":31,"end_position":500,"event_size":45,"cause":"invalid table id 0"}}}
```

- `kind` and `code` are stable; `message` is localized.
- `details` is present only when there is something machine-readable: a `reason` (`invalid_plan`, `plan_changed`, `trust_boundary`); a corrupt event's `event_type`, `end_position`, `event_size` and `cause`; a flashback conflict's `count` and `listed` (table, key, later GTID and position).
- A corrupt event never prints the parser's Go struct or raw event bytes (#254).

## Progress and quiet mode

The progress bar, carriage returns and ANSI erase codes are written only when stderr is a terminal and `TERM` is not `dumb`. A pipe or file gets one plain `Finalizing analysis...` line. `--quiet` (`-q`, any command) drops progress and info lines (resolved paths, save confirmations); errors and warnings still print (#253). JSON errors drop the info lines too (#268). An unknown command prints the error only, not the usage text; run `--help` for that.
