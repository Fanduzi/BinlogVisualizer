# BinlogViz v0.34.1 Release Notes

Release date: 2026-10-20

## Overview

v0.34.1 is a patch release for a P1 in filtered flashback. **Upgrade if you run `flashback` with `--include-table`, `--exclude-table`, `--include-schema`, or `--exclude-schema`.** In v0.34.0 and earlier, the object filter judged an in-range DDL by the first table it named, so a `RENAME TABLE` or `DROP TABLE` that named the selected table later was not refused, and the undo could write rows into the wrong table (#368, ADR 0015). The release also adds a runnable example to every command's `--help` and tests that no command prompts or waits on stdin (#365). Analyze output, unfiltered flashback, `schema_version` 1, `plan_version` 2, `report_version` 3, and exit codes are unchanged from v0.34.0.

## Fixed

- **P1: filtered flashback now refuses a DDL that names a selected table anywhere (#368, ADR 0015).** Flashback refuses a range that contains DDL (exit 6, `ddl_in_range`), because it does not undo schema changes. With a table or schema filter, v0.34.0 kept or dropped a DDL by its first name only, and a dropped DDL never reached the refusal. On MySQL 8.4.11:

  ```sql
  DELETE FROM rn.t1 WHERE id=1;
  RENAME TABLE rn.t2 TO rn.tmp, rn.t1 TO rn.t2, rn.tmp TO rn.t1;
  DELETE FROM rn.t1 WHERE id=5;   -- this row came from the old rn.t2
  ```

  `flashback --include-table rn.t1` exited 0, `verify | mysql` printed `binlogviz: undo committed, 2 statements`, and the old rn.t2 row was inserted into rn.t1 while rn.t2 stayed without it. `DROP TABLE rn.z, rn.t1` followed by `RENAME TABLE rn.t2 TO rn.t1`, and `--exclude-table rn.t2` on the swap, did the same. Runs with no filter, or where the selected table was the DDL's first name, were already refused.

  Now every in-range DDL reaches the flashback planner. With a filter, it reads every table the DDL names as a source or a target (every pair of a `RENAME TABLE` list, every name of a multi-table `DROP TABLE`, `ALTER TABLE ... RENAME TO`, `REFERENCES`, `CREATE TABLE ... LIKE`, `CREATE`/`DROP INDEX ... ON`, `TRUNCATE`) and every schema a `CREATE`, `ALTER`, or `DROP DATABASE` touches, and refuses when any of them could hold a selected row. Unqualified names resolve in the session's default database, and include lists match with letter case folded. A statement it cannot read completely refuses with a message that says so. A DDL that names only tables the filter leaves out still passes. With no table or schema filter, and with `--dml` alone, the refusal and its message are unchanged.

## Added

- **Every command's `--help` has a runnable example (#365, part of #255).** `TestEveryCommandHasExample` walks the command tree, including `help`, `completion`, `snapshot`, `workflow`, and `flashback verify` subcommands, and fails when a command has no example or `--help` does not print it. `TestCommandExamplesRun` runs each example on repository fixtures. The `flashback` example no longer carries an `--include-gtids <gtid>` placeholder; the comment above it says to pass the GTID with `--include-gtids`. Workflow examples use a relative `incident-out` directory instead of `/tmp/incident`.
- **No command prompts (#365).** `TestCommandsDoNotPrompt` runs every command with no TTY and a blocked stdin and fails if it hangs or asks for confirmation. `TestNoInteractivePromptAPI` rejects prompt libraries, `fmt.Scan`, and `ReadPassword`, and allows stdin reads only in `analyze -`. `TestFlashbackPlanRefusalPrintsReasons` checks that `flashback --plan` on exit 6 still prints the plan JSON with `would_refuse` and `refusal`. These are tests; command behavior does not change.

## Verification

BinlogQA's PR QA passed #365 and #368 at `29b01043`, which is the release commit's parent and the tip of main, so it is also the tip QA. 0 P0, 0 P1, 0 P2.

- **The P1**: on live MySQL 8.4.11 and 8.0.36, the swap, multi-table `DROP` plus `RENAME`, and `ALTER TABLE ... RENAME` binlogs exit 6 `ddl_in_range` with no SQL. About 60 DDL shapes were captured live. Every dangerous one refuses: later and cross-database `RENAME` pairs, multi-table `DROP`, `DROP`/`CREATE`/`ALTER DATABASE`, `TRUNCATE`, `CREATE ... LIKE`, `REFERENCES`, views, triggers, indexes, backtick escaping, `ANSI_QUOTES`, and `/*! */` comments. DDL on unrelated tables passes and the undo applies correctly. No shape produced wrong SQL.
- **Unchanged elsewhere**: 260 unfiltered and `--dml`-only flashback runs are byte-identical to v0.34.0. Analyze output is identical on 1914/1914 renderings, and flashback samples for the mf, mi, pc, and n316 corpora are byte-identical.
- **Mutants**: checking only the first name, ignoring rename targets, disabling the schema check, and passing an unreadable statement each fail a test (14/14 killed). For #365, an example with a wrong flag and a `ReadPassword` prompt are caught.
- `go test ./...` passes at the default `GOMAXPROCS` and at `GOMAXPROCS=1`. CI green.

## Compatibility

- No breaking changes for unfiltered flashback or analyze. `report_version` stays 3, `plan_version` stays 2, and `flashback verify --format json` `schema_version` stays 1. Exit codes, snapshots, artifact names, and supported platforms are unchanged.
- Filtered flashback refuses more: the ranges above, and a range whose in-range DDL the reader cannot read completely, even when that DDL is on another table. To flashback such a range, narrow it with `--start-position`, `--stop-position`, or `--exclude-gtids` so it leaves out the DDL.
- `--help` text adds examples on commands that had none and rewords the `flashback` and `workflow` examples.
- v0.33.0 through v0.34.0 flashback scripts verify and apply unchanged. If you generated a filtered flashback script with v0.34.0 or earlier from a range that contains a `RENAME TABLE` or multi-table `DROP TABLE`, regenerate it with v0.34.1 before applying it.

## Known issues

- **Over-cautious refusals under a filter (P3)**: a DDL on an unrelated table that the reader does not know completely, such as `ADD PARTITION ... VALUES LESS THAN`, `EXCHANGE PARTITION`, `CREATE TABLE ... SELECT`, `CHECK (...)`, `FOREIGN KEY`, or a comment with a backslash under `NO_BACKSLASH_ESCAPES`, refuses flashback of the selected table. This is the fail-closed rule in ADR 0015.
- **Exit 2 instead of 6 (P3)**: when a filtered range has a DDL on the selected table but no row events for it, flashback exits 2 (`schema/table filter matched no events`) instead of 6 `ddl_in_range`. No SQL is written, but the DDL is not named.
- A whole statement inside an executable comment (`/*!80000 RENAME ... */`) exits 1 `Unclassified QUERY`, as in v0.34.0.
- **#303 (P2)**: the Linux release binaries report an empty `commit` and `commit_time` in `binlogviz version`. Check a Linux binary against `binlogviz_0.34.1_checksums.txt`.
- **#352**: text and Markdown choose different top transactions. `compare` and `trend` still have a default arm in their format switch (#357).
- `flashback verify` does not accept `-` for stdin (exit 4).
- **#270**, **#255**, **#256**, and **#257** (P3) carry over.

See `docs/concept/limitations.md`, `docs/adr/0014-flashback-no-partial-commit.md`, and `docs/adr/0015-flashback-ddl-under-a-table-filter.md` for the flashback guarantees and their limits.
