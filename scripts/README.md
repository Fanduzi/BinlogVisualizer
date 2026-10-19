# Scripts

Release packaging helpers used by GitHub Actions and local smoke checks.

## Files

| File | Responsibility |
|------|----------------|
| `pack_release_archive.sh` | Packs a built `binlogviz` binary with the sample ROW fixture, a workflow plan whose `from_dir` exists inside the archive, the READMEs, and the agent usage guide. |
| `release-incident.yaml` | Archive-relative sample plan copied into each platform tar.gz as `incident.yaml`. |
| `release_smoke.sh` | Extracts a packed archive, checks `version --format json` (and, when asked, the embedded commit), and runs analyze / compare / trend / workflow against the bundled sample. |

## Interfaces

- `bash scripts/pack_release_archive.sh <binary> <archive.tar.gz>` — writes a tar.gz with `binlogviz`, `testdata/minimal.binlog`, `testdata/sample-binlog/mysql-bin.000001`, `incident.yaml`, `README.md`, `README_ZH.md`, and `docs/agents/USING-BINLOGVIZ.md` plus `docs/agents/USING-BINLOGVIZ.zh-CN.md`.
- `bash scripts/release_smoke.sh <archive.tar.gz>` — operator smoke path on an already packed archive. Optional env: `BINLOGVIZ_SMOKE_EXPECT_COMMIT=<sha>` fails unless `version --format json` reports exactly that commit; `BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1` fails when `commit` or `commit_time` is empty (ADR 0010).

## Dependencies

- Upstream: `cmd/binlogviz/testdata/minimal.binlog` and `cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001` (the 1500-byte ROW fixture).
- Downstream: `.github/workflows/release.yml`, `.github/workflows/ci.yml`, and `cmd/binlogviz/release_archive_test.go`.

## Update Rule

If packing members, extra-file paths, or smoke checks change, update this file in the same change.
