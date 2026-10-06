# BinlogViz v0.23.14 Release Notes

Release date: 2026-10-06

## Overview

v0.23.14 ranks sessions on the default analyze report, bounds and redacts statement text, and removes the stdin spool when analyze stops. Default text, Markdown, JSON, and HTML include Top Threads/Sessions. `--top` limits that section; `--top-threads` overrides it. `--sql-context off` omits query text and DDL statement text. `summary` keeps one line. `full` prints stored SQL, including DDL, capped at 4096 bytes. A cut ends with `… [truncated: <shown> of <original> bytes]`. MySQL and MariaDB auth-DDL credential literals become `<secret>`. MariaDB `SET PASSWORD` is DDL: it closes its GTID group and stays on the DDL Timeline, redacted. The timeline keeps VIEW, TRIGGER, routine, and EVENT changes. `CREATE TRIGGER` and `DROP TRIGGER` use the trigger name. `--include-table` and `--exclude-table` match those names. A match exits 0 even when no rows changed. A filter that matches nothing stays exit 2. `binlogviz analyze -` copies stdin to a temporary file and removes that copy on SIGHUP, SIGINT, SIGQUIT, and SIGTERM. Only a real terminal is reported as a terminal. `/dev/null` and an empty pipe say `stdin has no data`. Stdin replay hints name positions and do not invent a file path. Workflow and trend behavior are unchanged.

## New Features

- **Top Threads/Sessions (#117)**: Default text, Markdown, JSON, and HTML analyze reports include Top Threads/Sessions. Ranking is by rows when any session wrote rows, otherwise by events, then bytes, then transactions. Each row shows thread id, and server id, user@host, and schema when the binlog carried them. `--top` limits the section. `--top-threads` overrides that limit. `0` is unlimited. JSON field: `threads`. `threads_ranked_by` is `rows`, `events`, `bytes`, or `transactions`, and is omitted when no session had a thread id or actor.
- **`--sql-context` bounds query text and DDL text (#117)**: `off` omits query text and DDL statement text from every format, including default text and `--show-patterns`. `summary` keeps one whitespace-normalized line whose SQL body is at most 160 characters. `full` prints stored SQL capped at 4096 bytes. A cut in any format ends with `… [truncated: <shown> of <original> bytes]`. `query_truncated` means that 4096-byte store cap, not the 160-character summary line.
- **Analyze one binary binlog from stdin (#117)**: `binlogviz analyze -` and a non-seekable path such as a pipe read a binary binlog from stdin. Parsing needs seek, so the bytes are copied to a temporary file. `mysqlbinlog` text is not a binlog and still fails the magic-header check.

## Bug Fixes

- **Auth-DDL credentials become `<secret>` (#114, #120, #124)**: Before display, in every `--sql-context` mode, credential literals are rewritten to `<secret>`. MySQL forms: `IDENTIFIED BY`, `IDENTIFIED WITH … AS` or `BY`, `GRANT … IDENTIFIED`, and `SET PASSWORD`. MariaDB forms: `IDENTIFIED VIA` or `WITH` plus `USING`, `AS`, or `BY`, including `PASSWORD('…')` and a hash literal, and a chain of `OR` plugin rules. `IDENTIFIED BY RANDOM PASSWORD` has no literal and is left as written. `--sql-context off` omits the statement, so neither the secret nor `<secret>` is printed.
- **MariaDB `SET PASSWORD` is DDL (#125)**: `SET PASSWORD` closes its GTID group, appears on the DDL Timeline, and the password is `<secret>`. It is not an Ignored QUERY. A following statement is its own group.
- **`--sql-context full` caps DDL at 4096 bytes (#126)**: DDL statement text uses the same 4096-byte store cap as query text. A cut appends `… [truncated: <shown> of <original> bytes]`. `summary` still uses the 160-character line and the original byte length.
- **DDL Timeline keeps views, triggers, routines, and events (#122)**: `CREATE`, `ALTER`, and `DROP` of `VIEW`, `TRIGGER`, `PROCEDURE`, `FUNCTION`, and `EVENT` stay on the timeline. Object type is `view`, `trigger`, `routine`, or `event`. `CREATE TRIGGER` and `DROP TRIGGER` both use the trigger name, not the table after `ON`. DDL that does not match a known object is still listed as generic `DDL` instead of being dropped.
- **Table filters match those object names (#128)**: `--include-table` and `--exclude-table` match view, event, function, procedure, and trigger names the same way as tables (`TABLE` or `SCHEMA.TABLE`). A filter that matches one of those objects exits 0 and prints that DDL even when no rows changed. `CREATE TRIGGER … ON table` is kept by the trigger name, not by the table after `ON`. A filter that matches nothing is still exit 2, `Error: schema/table filter matched no events`.
- **Stdin spool cleanup and messaging (#118, #119, #127)**: The temporary copy is removed when the command finishes, including SIGHUP (exit 129), SIGINT (exit 130, `Error: interrupted`), SIGQUIT (exit 131), and SIGTERM (exit 143). Only a real terminal is reported as a terminal (`stdin is a terminal and has no binlog data`). `/dev/null` and an empty pipe say `stdin has no data`. Stdin replay hints name the positions (`input came from stdin; no replay path (start-position=… stop-position=…)`) and do not invent a file path.

## Breaking Changes

None.

## Compatibility

- Exit codes 0, 1, and 2 keep ADR-0001 meaning. A filter that matches a view, event, function, procedure, or trigger exits 0 even when that object changed no rows. A filter that matches nothing stays exit 2.
- `--top` still limits ranked sections. `--top-threads` overrides the Top Threads limit. `0` is unlimited. The default `--sql-context` remains `summary`.
- `--sql-context off` omits query text and DDL statement text. `full` caps stored SQL and DDL statement text at 4096 bytes. `query_truncated` means that store cap.
- Auth-DDL credential literals are `<secret>` in every mode that prints the statement. `IDENTIFIED BY RANDOM PASSWORD` is unchanged.
- On Unix, SIGHUP exits 129, SIGINT exits 130 (`Error: interrupted`), SIGQUIT exits 131, and SIGTERM exits 143, after the stdin temp copy is removed.
- Snapshots, workflows, artifact names, and supported platforms are otherwise unchanged from v0.23.13.
- Workflow and trend behavior are unchanged.
