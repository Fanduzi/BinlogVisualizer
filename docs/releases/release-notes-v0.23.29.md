# BinlogViz v0.23.29 Release Notes

Release date: 2026-10-09

## Overview

v0.23.29 fixes three `binlogviz flashback --schema-file` binding cases (#167). A correct incident-time dump taken after an `ADD COLUMN` that is also in the parsed binlogs is no longer refused. Each `Database:` header in a file of joined mysqldump outputs now binds the dump that follows it. When an ambiguous unqualified name leaves several tables without a definition, each of them is warned about. Every newly accepted case was restored correctly in live tests, and off-timeline dumps are still refused. `analyze` output is unchanged from v0.23.28.

## Changes

- **`ADD COLUMN` already in the dump (#167)**: an `ADD COLUMN` from the parsed binlogs whose column already exists in the schema file with the same definition is treated as already applied. Before, it was applied a second time and the dump was refused with `reordered (schema file ...; binlog ...)` and the column listed twice (`c, c`). A dump whose column differs in type, length, charset, signedness, generated expression, decimal precision or `ENUM` members is still refused. `NOT NULL`, `DEFAULT` and collation-only differences do not change values and are accepted.
- **Joined dumps (#167)**: each mysqldump `-- Host: ... Database:` header sets the database for the dump that follows it, as `USE` does. Before, only the first header was used, so later dumps were bound to the wrong database. Text before the first header is not bound and gets a warning.
- **Warnings for every unbound table (#167)**: when an unqualified table name occurs in more than one schema and drops a pending binding, every table left without a definition gets the per-table warning, not only one of them.

## Bug Fixes

- #167: an `ALTER` in the parsed binlog was applied again on top of a dump that already contained it, and a file of joined dumps used only the first `Database:` header. Both refused or ignored a correct incident-time dump.

## Verification

- CI on the merge commit, `go test ./...` (including the four new #167 unit tests), and the MySQL 8.0 end-to-end tests including `TestFlashbackRoundTripMySQL80` on a fresh MySQL 8.0.46: pass.
- 34-case adversarial #167 matrix on MySQL 8.0.46 (FULL) and 5.7.44, each script applied to the post-incident state and compared row by row and with `CHECKSUM TABLE`: 89 restored with v0.23.29 against 54 with v0.23.28, and no wrong data caused by the change. Newly accepted: plain, stored and virtual generated columns added `AFTER` / `FIRST`, `ADD` then `DROP`, several `ALTER`s across several tables, dumps taken mid-range, and incident-time dumps. Off-timeline and stale dumps and changed definitions are still refused by name.
- 561 of 561 undo statements across 169 scripts match the `mysqlbinlog -vv` before-images column by column.
- Joined dumps in any header order, with `USE` mixed in, now restore; v0.23.28 refused them. Wherever both versions exit 0, the SQL is identical apart from the per-run token (67/67).
- Regressions: #187/#215 binding cases unchanged apart from the intended joined-dump change; #192/#208 guard cases, the #183 bypass set, #180 and correct apply on 8.0 and 5.7 match v0.23.28. Offline flashback differential over 4898 cases: 63 differences, all intended #167 changes. `analyze` is byte-identical to v0.23.28 on 1500 cases.

## Breaking Changes

None. Some dumps that v0.23.28 refused now produce a script, and a joined dump binds each part to its own `Database:` header.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.28.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a non-default `div_precision_increment` with `DECIMAL` operands is refused with a message that blames the dump. It fails safe.
- [#215](https://github.com/Fanduzi/BinlogVisualizer/issues/215): the letter-case fold assumes an all-lower-case binlog name comes from a `lower_case_table_names=1` or `2` server. On a `lower_case_table_names=0` server with both `t` and `T`, a dump that has only `T` is used for `t`, and the guard stops the apply before any write (or flashback exits 1). A refused folded table is named by its binlog name, and the fold warning is not printed. Both fail safe. Use a dump with the exact binlog name.
- A column added in the parsed binlogs and later modified or renamed there (`MODIFY`, `CHANGE`, `RENAME COLUMN`) still makes a correct incident-time dump fail with `reordered ... c, c`. It fails safe. Parse only the incident binlog, or use a dump taken between those `ALTER`s.
- When `--schema-file-db` conflicts with several `Database:` headers, the warning names only the last conflicting header, and its ending "unqualified tables were not used" is not exact: tables of a dump whose header matches `--schema-file-db` are used.
- A database name that starts and ends with a literal backtick, or starts with a space, loses that character in the `Database:` header and gets no definition.
- In the guard error line, a `|` in a column name is shown as `/`. The guard's `SELECT` on stdout and the SQL keep the real names.
- A generated column the script does not know about (no `--schema-file` and no `CREATE` in the parsed binlogs) is still assigned, and apply can stop at `ERROR 3105`.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
