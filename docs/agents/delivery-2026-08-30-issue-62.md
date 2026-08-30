# Delivery: issue #62

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base: `444e7853bd4c6ce321884e3c076b6c6d6b00c0df`
- Candidate and fast-forwarded `main`: `ebb547cbb3f31ea1198ff956db46153a67db72ad`
- Merge type: direct fast-forward push to `main`
- Push range: `444e7853bd4c6ce321884e3c076b6c6d6b00c0df..ebb547cbb3f31ea1198ff956db46153a67db72ad`
- Issue: https://github.com/Fanduzi/BinlogVisualizer/issues/62

## Root-cause evidence

MariaDB writes XA PREPARE as physical event type `XA_PREPARE_LOG_EVENT`; go-mysql exposes that event as `XAPrepareLogEvent`. The parser preserved that name, but normalization discarded it, leaving the preceding GTID group open and causing the next legal GTID to hit the real conflict guard. The fix maps the physical event to the existing normalized `XA_PREPARE` boundary without changing GTID conflict detection.

## Local gates

CWD: `/Users/fan/GolangProjects/BinlogVisualizer`

| Command | Result |
|---|---|
| `go test ./...` | pass: 1,229 tests in 12 packages |
| `go test ./... -cover -coverprofile=/tmp/binlogviz-issue62-coverage.out` | pass: total statement coverage 84.8% |
| `go build ./...` | pass |
| `go vet ./...` | pass |
| `go mod tidy` plus `git diff --exit-code -- go.mod go.sum` | pass; module files unchanged |
| `golangci-lint run internal/binlog/normalize.go internal/binlog/normalize_test.go cmd/binlogviz/integration_test.go` | pass: no issues in changed Go files |
| `goreleaser check` | pass: one configuration validated |
| `check_three_level_doc.sh --staged` | pass |
| `git diff --check` | pass |

No tests were skipped. Repository-wide golangci-lint also inspected a preserved historical `.worktrees/issue-44` directory and reported pre-existing findings there; the changed production and regression files pass lint directly.

## Real MariaDB golden path

A disposable `mariadb:11.8.3` container generated a physical ROW binlog with this sequence:

`GTID 0-7-3 -> XA START -> INSERT -> XA END -> XA PREPARE -> GTID 0-7-4 -> XA COMMIT -> GTID 0-7-5 -> INSERT`

Before the fix, `go run . analyze active-open.binlog` exited 1 with `conflicting GTID "0-7-4" for transaction txn-3 with canonical GTID "0-7-3"` and empty stdout. At candidate `ebb547c`, these checks passed:

- default analyze: exit 0, 2 transactions, 2 rows;
- `--include-gtids 0-7-5`: exit 0, 1 transaction, 1 row, matched GTID `0-7-5`;
- `--start-position 1105`: exit 0, 1 transaction, 1 row, GTID `0-7-5`.

The existing explicit-conflict regression still passes, proving the integrity guard was not weakened.

## Review

- Independent diagnosis: confirmed the missing physical event boundary; no file changes.
- Independent code review: pass, P1 0, P2 0.
- Independent delivery verification: code, refs, tests, CI, and preserved paths verified; P1 0, P2 0.
- Commit trailer scan: zero `Co-authored-by:` trailers.

## CI

- Workflow/job: `ci` / `verify`
- Run: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33297498393
- Job: https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33297498393/job/99219577149
- Head SHA: `ebb547cbb3f31ea1198ff956db46153a67db72ad`
- Conclusion: `success`
- Cloudflare Pages check: `success` for the same SHA.

## Root worktree preservation and cleanup

The following user-owned untracked paths were preserved unchanged and excluded from the delivery:

- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`

No stash, reset, clean, or force-push was used. The disposable `binlogviz-xa62` container and its `/tmp/binlogviz-xa62.wxOKMr` fixture are intentionally retained through release verification; the completed herdr review tab was closed.
