# BinlogViz v0.23.24 Release Notes

Release date: 2026-10-09

## Overview

v0.23.24 closes the last two ways a `binlogviz flashback` script could still write wrong rows after its apply guard failed (#197, #191). The guard is now bound to the script it belongs to, and every undo statement checks it. Scripts without a guard and `analyze` output are unchanged from v0.23.23.

## Changes

- **The guard is bound to the script (#197)**: the guard sets `@binlogviz_ok` to a token derived from the script's own text, and only when the check ran in that session, every column matched, the server is supported, and the session was writable. Each transaction block unlocks only with that token. A stale unlock left by another binlogviz script in the same session no longer unlocks a header-less block.
- **Every undo statement checks the token (#197)**: an `INSERT` undo is `INSERT ... SELECT ... FROM DUAL WHERE @binlogviz_ok <=> '<token>'`, and `UPDATE` / `DELETE` add `AND @binlogviz_ok <=> '<token>'` to the `WHERE`. A statement run in a session without the token changes no row: after an interactive client reconnects in the middle of a block, and inside an `XA START` transaction where the lock itself fails with `ERROR 1399`.
- **Clear message in a read-only session (#191)**: a script applied in a session that is already read-only, for example after an earlier script's guard failed, stops at its own guard with `binlogviz: this session is already read-only so the script cannot write. Disconnect and apply the script again in a new session`, instead of a bare `ERROR 1792` on every write. On a mismatch, the guard's message line adds `This session is now read-only: disconnect, fix the schema file, and apply the script again in a new session.`

## Bug Fixes

- #197: a header-less block pasted into a session that had already applied a matching binlogviz script unlocked itself and wrote wrong rows with exit 0. It now fails with `ERROR 1792` and changes nothing.
- #197: when an interactive `mysql` / `mariadb` client or `mysqlsh --interactive` reconnected in the middle of a block, the rest of that block committed in the new, writable session. Those statements now change no row, and every later block fails with `ERROR 1792`.
- #191: auto-reconnect dropped the session lock (between blocks, fixed in v0.23.23; inside a block, fixed here), XA could commit the wrong rows, and a correct script later in a locked session failed with a bare `ERROR 1792`.

## Verification

- CI job `flashback e2e` on MySQL 8.0: `--- PASS: TestFlashbackRoundTripMySQL80` (with new `STALE_TOKEN` and `RECONNECT_DML` cases) and `--- PASS: TestNumericDecodeMySQL80`, no skips.
- Live matrix on MySQL 8.0.46, 5.7.44, 5.7.19 and MariaDB 10.6.28, 10.11.19, 11.4.13, v0.23.23 against this release. On v0.23.23, a correct script followed by a header-less wrong-target block, a forged stale unlock, a `KILL` inside a block at the client prompt, and `XA START` wrote wrong rows with exit 0. On this release every one of them leaves the rows and the GTID set unchanged. A correct schema file restores identical rows in `mysql`, `mysql --force` (autocommit on and off), `source`, a pasted session, and a continue-on-error runner (autocommit on and off).
- #183 bypass matrix on MySQL 8.0.46: 62 of 62 wrong-dump applies (every non-`mysqlsh` client mode) changed no rows and left the GTID set unchanged.

## Breaking Changes

None for a correct apply. In a script with a guard, an `INSERT` undo is now written as `INSERT ... SELECT ... FROM DUAL WHERE @binlogviz_ok <=> '<token>'` instead of `INSERT ... VALUES`, and `UPDATE` / `DELETE` carry one more `WHERE` condition. Running only the DML lines of such a script, without its header, now changes no row. A guarded script applied in a session that is already read-only now stops at the guard.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer. MySQL 5.6 and MariaDB 10.1 still fail the guard and stay read-only.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.23.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- On a correct target, a reconnect in the middle of a block leaves that block not applied: the statements before the drop roll back and the rest change nothing. Resume from that block, with the header, in a new session.
- [#167](https://github.com/Fanduzi/BinlogVisualizer/issues/167): an `ALTER` in the parsed binlog is applied again on top of a dump that already contains it. Use a dump taken before that `ALTER`, or leave out the binlog that holds it. A joined multi-database dump uses only the first `Database:` header.
- [#180](https://github.com/Fanduzi/BinlogVisualizer/issues/180): rows written by a non-strict session can fail under a strict session with `ERROR 1292` or `ERROR 1365` after earlier transactions committed. Apply with the original session's `sql_mode`.
- [#187](https://github.com/Fanduzi/BinlogVisualizer/issues/187), [#192](https://github.com/Fanduzi/BinlogVisualizer/issues/192), [#193](https://github.com/Fanduzi/BinlogVisualizer/issues/193): a `Database:` header with a space and mixed-case names under `lower_case_table_names`; non-ASCII column names in the guard error; a non-default `div_precision_increment` with `DECIMAL` operands.
- Resuming after a failed apply: keep the header (every line before the first `-- gtid:`), delete only the blocks above the failed one, and run the header plus the failed block and everything below it in a new session.
