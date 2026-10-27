# Scripts

Release packaging helpers used by GitHub Actions and local smoke checks.

## Files

| File | Responsibility |
|------|----------------|
| `pack_release_archive.sh` | Packs a built `binlogviz` binary with the sample ROW fixture, a workflow plan whose `from_dir` exists inside the archive, the READMEs, the agent usage guide, the user docs pages the READMEs link to (`docs/reference/analyze-behavior`, `docs/recipe/flashback`, `docs/recipe/compare-trend-workflow`, `docs/concept/architecture`, EN and zh-CN), and `docs/schema/*.json`. |
| `release-incident.yaml` | Archive-relative sample plan copied into each platform tar.gz as `incident.yaml`. |
| `release_smoke.sh` | Extracts a packed archive, checks `version --format json` (and, when asked, the embedded commit), and runs analyze / compare / trend / workflow against the bundled sample. |
| `check-release-prep.sh` | Release-prep gate (`make release-check`, and `release.yml`'s `release-prep` job on a tag before any build): for `vX.Y.Z` (optionally at a git ref; default the working tree) requires `docs/releases/release-notes-vX.Y.Z.md` and `.zh-CN.md` titled for that version, a `## vX.Y.Z` section in `CHANGELOG.md`, no README/README_ZH install pin naming another release (installs resolve `releases/latest`, #281/#285), and `docs/landing/index.html` naming `vX.Y.Z` and changed since the previous release tag. It checks short pins only; nothing has to append history to the README or landing page. |
| `check-release-prep-test.sh` | Self-test for `check-release-prep.sh`: builds fake complete and one-gap-each releases in a temp git repo; the complete one must pass and every incomplete one must fail. `ci.yml` runs it on every PR. |
| `compare-analyze-formats.sh` | Diffs analyze text/JSON/Markdown/HTML and flashback SQL / `--plan` of two binaries on every `*.binlog` under a tree. HTML generated-at timestamps are normalized first. |

## Interfaces

- `bash scripts/pack_release_archive.sh <binary> <archive.tar.gz>` — writes a tar.gz with `binlogviz`, `testdata/minimal.binlog`, `testdata/sample-binlog/mysql-bin.000001`, `incident.yaml`, `README.md`, `README_ZH.md`, `docs/agents/USING-BINLOGVIZ.md` plus `docs/agents/USING-BINLOGVIZ.zh-CN.md`, the eight README-linked docs pages, and every `docs/schema/*.json`.
- `bash scripts/release_smoke.sh <archive.tar.gz>` — operator smoke path on an already packed archive. Optional env: `BINLOGVIZ_SMOKE_EXPECT_COMMIT=<sha>` fails unless `version --format json` reports exactly that commit; `BINLOGVIZ_SMOKE_REQUIRE_COMMIT=1` fails when `commit` or `commit_time` is empty (ADR 0010).
- `make release-check VERSION=vX.Y.Z [REF=<git-ref>]` / `bash scripts/check-release-prep.sh vX.Y.Z [git-ref]` — exit 0 when every release surface is ready for the version, 1 with one `FAIL` line per gap, 2 on bad arguments.
- `make release-check-test` / `bash scripts/check-release-prep-test.sh` — exit 0 when the gate passes the complete fake release and fails each incomplete one.
- `bash scripts/compare-analyze-formats.sh <old-binary> <new-binary> [repo-root]` — exit 0 when every testdata binlog matches on stdout, stderr, and exit code.

## Dependencies

- Upstream: `cmd/binlogviz/testdata/minimal.binlog` and `cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001` (the 1500-byte ROW fixture).
- Upstream for the release-prep gate: `CHANGELOG.md`, `README.md`, `README_ZH.md`, `docs/releases/`, `docs/landing/index.html`, and the repository's `v*` tags.
- Downstream: `.github/workflows/release.yml`, `.github/workflows/ci.yml`, and `cmd/binlogviz/release_archive_test.go`.

## Update Rule

If packing members, extra-file paths, or smoke checks change, update this file in the same change.
