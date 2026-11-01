# Delivery: issue #63

## Refs

- Repository: `Fanduzi/BinlogVisualizer`
- Base: `8cf5680fb5dc8ebfe092f7a1b46e5080973fbdbe`
- Fix candidate: `2679cae20f9af5d7c33f09f0fcd9ad6615d7b0e4`
- Release-preparation commit and immutable `v0.23.3` tag target: `ea1085de096cf0bfef32aa55be8cd25327662eae`
- Merge type: direct fast-forward push to `main`
- Delivered push range: `8cf5680fb5dc8ebfe092f7a1b46e5080973fbdbe..ea1085de096cf0bfef32aa55be8cd25327662eae`
- Post-release state: `main` advances only for this documentation-only audit follow-up; the published tag remains on `ea1085d`.
- Issue: https://github.com/Fanduzi/BinlogVisualizer/issues/63

## Root-cause evidence

The GTID selector accepted MariaDB GTIDs correctly. The transaction builder instead discarded a GTID-backed, zero-row `XA_COMMIT`, and the analyzer retained only transactions with affected rows. MariaDB's physical XA PREPARE event also carried an XID that was not copied into the normalized transaction. The fix extracts that physical XID, propagates it through transaction provenance, and retains zero-row XA transactions only when they have an in-window physical span.

## Local gates

CWD: `/Users/fan/GolangProjects/BinlogVisualizer`

| Command | Result |
|---|---|
| `go test ./... -count=1` | pass: 1,230 tests in 12 packages |
| `go vet ./...` | pass |
| `goreleaser check` | pass: one configuration validated |
| `check_three_level_doc.sh` | pass; L1 reminder reviewed |
| `git diff --check` | pass |
| local v0.23.3 build, pack, and `scripts/release_smoke.sh` | pass |

No tests were skipped.

## Real MariaDB golden path

A disposable `mariadb:11.8.3` container generated a physical ROW binlog with this sequence:

`GTID 0-7-3 -> XA START -> INSERT -> XA END -> XA PREPARE -> GTID 0-7-4 -> XA COMMIT -> GTID 0-7-5 -> INSERT`

Before the fix, default JSON omitted GTID `0-7-4` and all XA identity, while `--include-gtids 0-7-4` exited 2 with `window matched 0 events`. At candidate `2679cae`, these checks passed:

- default JSON: 3 transactions and 2 rows; prepared GTID `0-7-3` and commit GTID `0-7-4` share the expected `xa_xid`;
- `--include-gtids 0-7-4`: exit 0, exactly 1 zero-row transaction with the commit GTID and `xa_xid`;
- `--exclude-gtids 0-7-4`: exit 0, the two row transactions remain and the commit is absent;
- position window `[1026,1120)`: exactly the zero-row commit transaction, with no out-of-window prepared-transaction ghost;
- DuckDB detail-store mode preserves the selected zero-row commit transaction.

## Review

- Independent diagnosis confirmed the two retention boundaries and the missing physical XA identity.
- Independent code review: pass, P1 0, confirmed behavior defects 0; one parser-adapter test gap was reported and added before the final test run.
- The physical payload assumption was verified against the disposable MariaDB 11.8.3 binlog, not only a synthetic unit payload.
- Commit trailer scan: zero `Co-authored-by:` trailers.

## CI

- Fix workflow/job: `ci` / `verify`, [run 33301558063](https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33301558063), head `2679cae`, `success`.
- Release-preparation workflow/job: `ci` / `verify`, [run 33301815895](https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33301815895), head `ea1085d`, `success`.
- Cloudflare Pages: `success` for `ea1085d`; https://binlogviz.pages.dev served the v0.23.3 badge, current-release copy, install commands, and tag-pinned release-note link.

## Release verification

- Annotated tag: `v0.23.3^{}` resolves locally and remotely to `ea1085de096cf0bfef32aa55be8cd25327662eae`.
- Release workflow: [run 33301900790](https://github.com/Fanduzi/BinlogVisualizer/actions/runs/33301900790), `success`.
- Successful jobs: `test`, `validate-homebrew`, Darwin amd64/arm64 builds, Linux amd64/arm64 manylinux2014 builds, and `github-release`.
- Public release: https://github.com/Fanduzi/BinlogVisualizer/releases/tag/v0.23.3; not draft or prerelease.
- Assets: four platform archives plus `binlogviz_0.23.3_checksums.txt`.
- Downloaded Darwin arm64 archive checksum: `c6f464741ac9fb9615471f6390618f3b17a49b86a021ffd371f5cd80564ba187`; manifest, GitHub asset digest, and Homebrew cask match.
- Downloaded archive reports `0.23.3` and passes `scripts/release_smoke.sh`.
- `Fanduzi/homebrew-binlogviz/Casks/binlogviz.rb` is version `0.23.3`; all four cask checksums match the release manifest.
- Issue #63 closed at `2026-08-30T08:37:38Z` with the v0.23.3 verification summary.

## Root worktree preservation and cleanup

The following user-owned untracked paths were preserved unchanged and excluded from the delivery:

- `AGENTS.md.bak-pre-gitnexus-uninstall`
- `CLAUDE.md.bak-pre-gitnexus-uninstall`

No stash, reset, clean, force-push, or destructive worktree operation was used. The disposable `binlogviz-xa63` container was removed. Its real-binlog fixture, coverage output, local package, and downloaded release-verification directory were moved to `/Users/fan/.Trash/` and remain recoverable until Trash is emptied. The completed herdr review tab was closed.
