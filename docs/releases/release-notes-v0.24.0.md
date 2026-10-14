# BinlogViz v0.24.0 Release Notes

Release date: 2026-10-15

## Overview

v0.24.0 makes `binlogviz flashback` safe to run on large and busy binlogs, and gives every command machine-readable failures. ADR 0008 records the design (`docs/adr/0008-row-stream-streaming-flashback-typed-errors.md`).

For a DBA:

- flashback streams row images through a temp file, so memory no longer grows with the size of the undo set.
- flashback refuses by default (exit 7) when a row it would undo was changed again later in the scanned binlogs. `--conflict guard` and `--plan` let you choose.
- Failures have distinct exit codes and an optional one-line JSON error. Corrupt or hostile binlogs exit 5 instead of crashing, hanging or printing wrong SQL.

## Changes

- **ADR 0008**: row stream, streaming flashback and typed errors. The ADR lists the follow-up work (#255, #256, #257) that is not in this release.
- **Streaming flashback (#258)**: row images go to an unlinked temp file and are rendered in a second pass. On BinlogQA's runs, a 1.17 GB binlog with one 450k-row transaction peaks at about 205 MB RSS, and a 306 MB binlog with 140k small transactions at about 164 MB. `$TMPDIR` needs room for about the size of the selected row images.
- **Conflict detection (#259, #267)**: flashback checks whether a selected row was changed again later in the scanned binlogs.
  - `--conflict refuse` (the default) writes no script and exits 7. The error lists the first 20 conflicts (table, key, later GTID and position) and the total count.
  - `--conflict guard` writes the script anyway. Each undo matches the logged after image, so a row changed later is left alone.
  - `--plan` prints a JSON plan (`plan_version` 1: transactions, rows per table, conflicts, guard) instead of SQL. Its exit code is the one the real run would return.
  - A later change that is itself part of the selected undo set is not a conflict (#267).
- **Typed exit codes and JSON errors (#263, #268)**: each failure maps to one kind and exit code: 1 error, 2 no_data, 3 usage, 4 input, 5 corrupt, 6 refused, 7 conflict, 8 output. `--error-format json` (or `BINLOGVIZ_ERROR_FORMAT=json`) prints exactly one `{"error":{"kind","code","message","details"}}` line on stderr. An invalid `--format` is a usage error (#251), and the parser's Go structs never reach the user (#254). See [docs/reference/exit-codes.md](../reference/exit-codes.md).
- **Non-TTY output and `--quiet` (#253)**: there is no progress bar and no ANSI escape codes when stderr is not a terminal or `TERM=dumb`. `-q`/`--quiet` also drops info lines; errors and warnings still print.
- **Password redaction (#250)**: every flashback message that quotes SQL redacts credentials first (CREATE USER, GRANT, SET PASSWORD), including the password hash.
- **Faster top transactions (#252)**: ranking uses a bounded heap instead of linear insertion, so `analyze` no longer slows down quadratically on files with many transactions. The ranking order is unchanged.
- **CRC32 and hostile-input hardening (#271, #272)**: BinlogViz reads each event's bytes itself before go-mysql decodes them.
  - Each event's CRC32 footer is checked. An open or crashed binlog is still accepted, because the in-use flag is masked the way MySQL does.
  - Count and size fields, rows-event widths, and JSON values are bounded: offsets must stay inside their container, and nesting is capped at 100 like MySQL.
  - A MySQL 8 transaction payload must declare at most 1 GiB uncompressed and decompress to exactly that size.
  - A corrupt event exits 5 with the file and event offset. Before, it could panic (exit 2), hang, run out of memory, or print wrong SQL with exit 0.
- **MariaDB 11.4 collations and leading comments (#269)**: flashback knows MariaDB 11.4 collation ids, including `utf8mb4_uca1400_ai_ci`. A QUERY's leading comments are stripped before classification, so MariaDB's verbatim `/* c */ CREATE USER` classifies like MySQL's. Executable `/*!` and `/*M!` comments stay.

## Behavior changes

- **Exit codes**: several failures that exited 1 now exit 3 (usage), 4 (input), 5 (corrupt) or 6 (refused). Exit 1 now covers only failures with no other kind, including an open `BEGIN` or an unclassified QUERY (ADR 0001). Scripts that match on 1 must read the new codes or the JSON `kind`.
- **flashback can refuse with exit 7**: a flashback that used to exit 0 now exits 7, with no script, when a selected row was changed again later. Choose `--conflict guard`, narrow the range, or check with `--plan` first.
- **Corrupt input exits 5**: a CRC32 mismatch, an event that cannot be bounded, or a decoder failure exits 5 (`corrupt`). Before, it could exit 0 with wrong output, or exit 2 after a panic.
- Flashback SQL is byte-identical to v0.23.34 on BinlogQA's offline corpus (4,898 cases, en and zh-CN), apart from the exit codes above.

## Verification

BinlogQA's dogfood on main `7649df9a` passed.

- **Against v0.23.34**:
  - Offline flashback: 4,672 identical and 225 with only an exit-code change. The one SQL difference is the #261 fix.
  - Guard: 252/252 identical.
  - Analyze: 1,241 of 1,460 identical; the other 219 are the corrupt-input and exit-code changes above.
- **Live flashback on MySQL 8.0.46 and MariaDB 11.4 (CRC32 on)**: a spike with 33 later changes exits 7 and reports 33 conflicts. `--conflict guard` applied keeps those 33 rows and undoes the other 1,167; no other rows change.
- **Hostile input**: self-referencing JSON, broken zstd payloads and random byte flips all exit 5 with one JSON line, with no hang and no exit 2.
- CI and `go test ./...` pass.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names and supported platforms are unchanged from v0.23.34.

## Known gaps

- #277: an empty `--error-format` is accepted, and a missing workflow plan or snapshot exits 1 instead of 4.
- #278: a payload header field longer than 8 bytes is refused before the 1 GiB size check can report it.
- #255: agent UX. There is no AGENTS.md, skill or MCP server, no `--help` examples, and the release archive has no docs.
- #256: feature gaps compared with mysqlbinlog, binlog2sql, my2sql and pt-query-digest.
- #257: `internal/analyzer` is still one large package; ADR 0008 proposes the seams.
- #270: `--conflict guard` / `--plan` nits. A PK-moving undo hits ERROR 1062 when the old key was reused, `--plan` prints nothing on exit 6, and only 20 conflicts are listed.

See `docs/concept/limitations.md` for the flashback limitations carried over from v0.23.34.
