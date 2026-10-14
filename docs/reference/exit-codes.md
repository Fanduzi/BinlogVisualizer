# Exit codes and JSON errors

Every command maps a failure to one **kind**. The exit code is a function of the kind (ADR 0008). The kind string is stable and does not change with `--lang`.

| Exit | Kind | When |
|---|---|---|
| 0 | | success |
| 1 | `error` | any failure with no other kind, including an open `BEGIN` or an unclassified QUERY that analyze cannot count safely (ADR 0001) |
| 2 | `no_data` | a complete binlog with nothing in range or nothing matching the filters (ADR 0001) |
| 3 | `usage` | unknown command or flag, wrong arguments, an invalid `--format` (#251), `--start`/`--end`, GTID set, `--dml`, `--conflict` or `--output` value |
| 4 | `input` | an input file or `--from-dir` is missing or unreadable, or `--schema-file` cannot be read |
| 5 | `corrupt` | not a binlog, empty, truncated, a corrupt event, or no Format Description |
| 6 | `refused` | flashback cannot write an exact script: DDL in range, a row image that is not FULL, a `--schema-file` mismatch, `--sql-context off` |
| 7 | `conflict` | flashback: a selected row was changed again later in the scanned binlogs (`--conflict refuse`, the default; #259) |
| 8 | `output` | writing the report, the script or the flashback temp file failed (for example ENOSPC). SIGPIPE stays 141 |

Signals keep their shell codes: SIGHUP 129, SIGINT 130, SIGQUIT 131, SIGTERM 143.

## JSON errors

`--error-format json` (any command), or `BINLOGVIZ_ERROR_FORMAT=json` in the environment, replaces the `Error:` line with one JSON object on stderr:

```json
{"error":{"kind":"corrupt","code":5,"message":"parse error: corrupt event, type 31, ending at position 500, size 45: invalid table id 0","details":{"event_type":31,"end_position":500,"event_size":45,"cause":"invalid table id 0"}}}
```

- `kind` and `code` are stable; `message` is localized.
- `details` is present only when there is something machine-readable: a corrupt event's `event_type`, `end_position`, `event_size` and `cause`; a flashback conflict's `count` and `listed` (table, key, later GTID and position).
- A corrupt event never prints the parser's Go struct or raw event bytes (#254).
