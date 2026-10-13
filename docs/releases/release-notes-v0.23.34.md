# BinlogViz v0.23.34 Release Notes

Release date: 2026-10-13

## Overview

v0.23.34 only changes `binlogviz flashback` messages, mostly the refusals for generated `/` columns that fit a non-default `div_precision_increment` (#227, #231, #243, #244, #246). Generated SQL, exit codes and every accept/refuse decision are unchanged from v0.23.33, and `analyze` output is unchanged.

For a DBA:

- zh-CN output no longer mixes in English prefixes: errors start with `错误：` and warnings with `警告：`.
- The hand-restore advice is now correct: it tells you to `SET SESSION div_precision_increment=N`, then ``FLUSH TABLES `db`.`tbl` ``, then the INSERT. It also says what that FLUSH costs and gives a way to skip it.
- A correct dump is no longer called wrong when the real increment is one flashback can't check.
- Refusal stderr is about 31-41% smaller, because the long advice is printed once per run instead of on every refused column.

## Changes

- **Error and warning prefixes in zh-CN (#227, #244)**: under `--lang zh-CN` (or a Chinese locale), errors start with `错误：` instead of `Error:`, and the letter-case fold warnings start with `警告：` instead of `warning:`. `window matched 0 events`, `invalid MariaDB GTID` and "another image with the same inputs" are now translated too (#246).
- **Hand-restore advice (#227, #244, #246)**: MySQL fixes a generated-column expression when it opens the table, so a table that is already open keeps the increment it was opened with. The advice now reads:
  - Run `SET SESSION div_precision_increment=N`, where N is the incident server's value, one of the increments named for that column.
  - Then run ``FLUSH TABLES `db`.`tbl` `` (the message names the real table), then the INSERT.
  - `FLUSH TABLES` waits for statements still running on that table, and new queries on the table queue behind it (thread state `Waiting for table flush`). Idle open transactions do not block it. On a busy server, run it in a quiet window. You can also skip FLUSH: open a fresh connection that has not touched the table and run `SET SESSION` as its first statement.
  - Both routes were checked live on MySQL 8.0.46 and restore correct values.
- **Advice printed once per run (#246)**: the advice above is added only to the first `div_precision_increment` refusal line, not to every refused column. Each refusal is still a single `Error:` line, so line-based log parsing is unaffected. On large wrong dumps, refusal stderr is about 31-41% smaller.
- **Example N (#244, #246)**: the example increment skips 0 when other values are listed and prefers the start of a range. For example, `0, 28-30` gives 28 and `0-3` gives 1.
- **"May be right" wording (#231, #243)**:
  - A refused column that fits no increment at all now leads with a plain "does not match" message (#231).
  - A column that fits no increment flashback could check, but has increments it could not check (for example `10-30`), still says the dump may be right at one of those increments (#243). The wrong-dump advice comes after that.
  - On BinlogQA's correct dz193 dump, 913 correct columns were told the dump was wrong during development. The release tells none of them that.

## Bug Fixes

- #227: zh-CN refusals started with the English `Error:`, and the hint did not mention `FLUSH TABLES`.
- #231: genuinely wrong dumps that fit no increment got the `div_precision_increment` wording.
- #243: correct-dump columns whose real increment can't be checked would have been told the dump is wrong.
- #244: N was not defined, the hint gave no `FLUSH TABLES` syntax or cost warning, and some zh-CN output was still in English.
- #246: the cost note wrongly said `FLUSH TABLES` waits for open transactions, one zh-CN phrase was still English, and the example N could be 0.

## Verification

- CI on the merge commit and `go test ./...` (including guard tests on MySQL 8.0 and 5.7): pass.
- Offline flashback corpus (4,898 cases, en and zh), BinlogQA's harness against v0.23.33 and the previous main:
  - Every script is byte-identical, every exit code matches, and accept/refuse is unchanged.
  - Every stderr change is one of the message changes above.
- `analyze`: 1500/1500 the same.
- dz193 checked against MySQL 8.0.46: 0 correct-dump columns are told the dump is wrong.
- The FLUSH and fresh-connection restore routes were checked live on 8.0.46. So was the `Waiting for table flush` behaviour.

## Breaking Changes

None. Scripts and exit codes are unchanged. Wording and prefixes changed: if you match zh-CN stderr on `Error:` or `warning:`, match `错误：` and `警告：` as well.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.33.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- flashback cannot tell a correct dump whose increment it can't check from a wrong dump. Such columns are still refused, with the "may be right" wording.
- A `/` that needs more than 30 fractional digits is still not verified.
- On MySQL 5.7, a JSON number written as `1e2` renders as `100.0`, so a few such columns are refused. It fails safe.
- A column added and then modified or renamed within the parsed binlogs still makes a correct incident-time dump fail with `reordered ... c, c` (fails safe).
