# Scripts

Release packaging helpers used by GitHub Actions and local smoke checks.

## Files

| File | Responsibility |
|------|----------------|
| `pack_release_archive.sh` | Packs a built `binlogviz` binary with the sample ROW fixture and a workflow plan whose `from_dir` exists inside the archive. |
| `release-incident.yaml` | Archive-relative sample plan copied into each platform tar.gz as `incident.yaml`. |
| `release_smoke.sh` | Extracts a packed archive and runs analyze / compare / trend / workflow against the bundled sample. |

## Interfaces

- `bash scripts/pack_release_archive.sh <binary> <archive.tar.gz>` — writes a tar.gz with `binlogviz`, `testdata/minimal.binlog`, `testdata/sample-binlog/mysql-bin.000001`, and `incident.yaml`.
- `bash scripts/release_smoke.sh <archive.tar.gz>` — operator smoke path on an already packed archive.

## Dependencies

- Upstream: `cmd/binlogviz/testdata/minimal.binlog` and `cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001` (the 1500-byte ROW fixture).
- Downstream: `.github/workflows/release.yml`, `.github/workflows/ci.yml`, and `cmd/binlogviz/release_archive_test.go`.

## Update Rule

If packing members, extra-file paths, or smoke checks change, update this file in the same change.
