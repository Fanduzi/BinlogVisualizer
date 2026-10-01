# Analyze

Analyze parses MySQL ROW-format binlog files and prints a DBA-facing summary (transactions, top tables, findings) in text, JSON, markdown, or HTML.

## Sub-features

- `analyze-file` analyzes one or more explicit binlog paths.
- `analyze-from-dir` discovers files with `--from-dir` + `--prefix`.
- `analyze-window` filters with `--start` / `--end` (RFC3339 or `YYYY-MM-DD HH:MM:SS`).
- `analyze-format` emits `--format text|json|markdown|html`.
- `analyze-exit` surfaces exit `0` (counted events), `1` (hard fail), `2` (no-data).

## How to get to it (user POV)

- Run `binlogviz analyze <binlog files…>` from a terminal.
- Run `binlogviz analyze --from-dir DIR --prefix PREFIX`.
- Optionally add `--start` / `--end`, `--format`, schema/table filters, and ranking `--top*` flags.

## Driving it with drive.sh

Preconditions:

- `helpers/doctor.sh` PASS.
- Fixture `cmd/binlogviz/testdata/minimal.binlog` present (ROW; contains `testdb.users` DML + DDL).
- Cwd effectively repo root (drive.sh cds there).

- **Text analyze (baseline proof).** Run `.cursor/skills/verify-binlogviz/helpers/drive.sh analyze-text -- analyze cmd/binlogviz/testdata/minimal.binlog --format text`. Expect exit `0`, stdout contains `=== Summary ===` and `testdb.users`, `meta.txt` records exit=`0`.
- **JSON analyze.** Run `…/drive.sh analyze-json -- analyze cmd/binlogviz/testdata/minimal.binlog --format json`. Expect exit `0`, stdout JSON with `"summary"` / `"total_transactions"`.
- **From-dir discovery.** Run `…/drive.sh analyze-from-dir -- analyze --from-dir cmd/binlogviz/testdata/sample-binlog --prefix mysql-bin. --format text`. Expect exit `0` and a non-empty summary.
- **Time window (wide, fixture timestamp).** Fixture events are at `2026-03-15T14:10:26Z`. Run `…/drive.sh analyze-window -- analyze cmd/binlogviz/testdata/minimal.binlog --start 2026-03-15T00:00:00Z --end 2026-03-15T23:59:59Z --format text`. Expect exit `0`. A window that misses all events should exit `2` (no-data).
- **Proof.** Keep `evidence/<RUN_ID>/{meta.txt,stdout,stderr}`. Confirm `exit=0` and non-empty stdout. Do not require HTML browser open for CLI proof.

## Gotchas

- ROW-only counting: STATEMENT/MIXED Query-DML is not counted; the text banner says so.
- Progress bars often write to stderr; a non-empty stderr alone is not failure.
- Space-separated `--start`/`--end` use the machine local timezone; prefer RFC3339 with offset across machines.
- Exit `2` means no counted data in the window — not a crash. Treat it as an expected outcome when proving empty windows.
- Never omit isolation for snapshot-saving analyze; see [snapshot.md](./snapshot.md).
