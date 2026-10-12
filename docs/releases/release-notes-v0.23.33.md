# BinlogViz v0.23.33 Release Notes

Release date: 2026-10-13

## Overview

v0.23.33 fixes two cases where `binlogviz flashback` checked a generated column differently from MySQL 8.0 (#237, #238). Both could give a false accept, so flashback could take a dump as matching when MySQL would compute a different value. Both now follow MySQL. More DECIMAL columns that use `DIV`, `%` or `MOD` are verified and kept in the script. A few columns that were "verified" only by luck now count as can't-verify: they are left out of the script with a guard, and are never accepted wrongly. It also ships three internal refactors with byte-identical output (#234, #235, #236). One of them fixes the #225 message label. `analyze` output is unchanged from v0.23.32.

**History rewrite.** The BinlogViz history was rewritten on 2026-10-12. Commit hashes from before that date changed, so a hash quoted in older notes, issues or QA reports may no longer resolve. Release tags point at the rewritten commits, and published release binaries did not change. Use the tag or the PR number to find a change.

## Changes

- **JSON `[0]` on a scalar (#237)**: in a generated column, `->'$.v[0]'` on a JSON value that is not an array now autowraps the value, as MySQL 8.0 does. `[0]` is the value itself, and any other index is missing (NULL). Before, `[0]` was NULL, which could refuse a correct dump or accept a wrong one. In practice most of these columns are now reported as can't-verify rather than verified. That is fail-safe.
- **MySQL rounding under `DIV`, `%` and `MOD` (#238)**: a DECIMAL expression such as `(a / b) DIV c` or `(a / b) % c` now keeps MySQL's rounding of each `/`. Before, it was computed exactly. In rare cases that accepted a value MySQL does not produce. The rules are the same as in MySQL:
  - A zero divisor gives NULL.
  - A zero result has scale 0.
  - `DIV` truncates toward zero.
  - A quotient outside BIGINT is not claimed.
- **More columns verified**: DECIMAL columns with `(a DIV b)` or `%` that used to be left out of the script as "not verified" are now checked against the logged values and kept. In the offline corpus, 4 cases now produce a full script instead of a guarded partial one. Restores of those scripts on MySQL 8.0.46 match the pre-incident rows.
- **Refactors, output unchanged**: the schema catalog (#234, ADR-0005), the apply guard as one module (#235, ADR-0006), and one expression evaluator for generated columns (#236, ADR-0007). Scripts, guard SQL and messages are byte-identical, apart from the #225 fix below.

## Bug Fixes

- #225: a refusal for a table matched through the letter-case fold now names the schema-file table in zh-CN too, and on every refused table, not only the first.
- #237: `$.v[0]` on a non-array JSON value was NULL. MySQL returns the value.
- #238: a `/` inside `DIV`, `%` or `MOD` on DECIMAL was computed exactly instead of with MySQL's rounding, which could give a rare false accept.

## Verification

- CI on the merge commit, `go test ./...` (guard tests on MySQL 8.0 and 5.7), and the MySQL 8.0 end-to-end tests on a fresh MySQL 8.0.46: pass.
- BinlogQA fuzz corpus: 500k cases, about 1.8 M probes, with MySQL 8.0.46 and 5.7.44 as truth.
  - MySQL 8.0, wrong verdicts: v0.23.32 had 5,943 JSON false refusals, 5,943 JSON false accepts, 9 numeric false refusals and 3 numeric false accepts. v0.23.33 has **0**.
  - About 31,800 more numeric probes are verified correct, and about 70 numeric cases move to can't-verify.
  - MySQL 5.7: JSON false accepts drop from 1,267 to 0. 74 JSON refusals remain; they are the 5.7-only `1e2` vs `100.0` rendering. The other counts are unchanged.
  - No new wrong verdict in any family.
- Offline flashback corpus (4,898 cases, en and zh): every script is byte-identical to v0.23.32 except the 4 cases above. Exit codes match in all cases.
- `analyze`: 1500/1500 the same.
- The refactors were each checked against the previous main on the same corpora (byte-identical) before they merged.

## Breaking Changes

None. Some generated columns move between verified and can't-verify, as described above. A can't-verify column is left out of the script with a guard, as before.

## Compatibility

- Apply is supported on MySQL 5.7 and newer and on MariaDB 10.2 and newer.
- `flashback` never connects to MySQL. Review the script, test it, and apply it in one new session on the primary with `sql_log_bin=1`, with the script header. A statement that fails leaves earlier transactions in the script committed.
- JSON `report_version` stays `3`. Snapshots, workflows, artifact names, and supported platforms are unchanged from v0.23.32.

## Known issues

See `docs/concept/limitations.md` for the full text and workarounds.

- [#227](https://github.com/Fanduzi/BinlogVisualizer/issues/227) (remaining): a zh-CN refusal still starts with the English `Error:` prefix, and the hint does not mention `FLUSH TABLES`.
- [#231](https://github.com/Fanduzi/BinlogVisualizer/issues/231): a column of a genuinely wrong dump that fits no increment often gets the `div_precision_increment` wording instead of a plain "the dump is wrong" message. It is still refused.
- On MySQL 5.7, a JSON number written as `1e2` renders as `100.0`, so a few such columns are refused. It fails safe.
- A `/` that needs more than 30 fractional digits is still not verified.
- The letter-case fold, column `MODIFY`/`RENAME` within the parsed binlogs, `--schema-file-db` header warnings, unknown generated columns, and resuming after a failed apply are unchanged from v0.23.32.
